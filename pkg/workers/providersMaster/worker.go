package providersmaster

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	agentclient "mytonprovider-backend/pkg/agentClient"
	agentregistry "mytonprovider-backend/pkg/agentRegistry"
	tonclient "mytonprovider-backend/pkg/clients/ton"
	"mytonprovider-backend/pkg/clients/ifconfig"
	"mytonprovider-backend/pkg/models/db"
)

const (
	lastLTKey                     = "masterWalletLastLT"
	prefix                        = "tsp-"
	storageRewardWithdrawalOpCode = 0xa91baf56

	getTxTimeout = 20 * time.Second
)

type providers interface {
	GetAllProvidersPubkeys(ctx context.Context) (pubkeys []string, err error)
	GetAllProvidersWallets(ctx context.Context) (wallets []db.ProviderWallet, err error)
	UpdateProvidersLT(ctx context.Context, providers []db.ProviderWalletLT) (err error)
	AddStorageContracts(ctx context.Context, contracts []db.StorageContract) (err error)
	GetStorageContracts(ctx context.Context) (contracts []db.ContractToProviderRelation, err error)
	UpdateRejectedStorageContracts(ctx context.Context, storageContracts []db.ContractToProviderRelation) (err error)
	AddProviders(ctx context.Context, providers []db.ProviderCreate) (err error)
	UpdateProvidersIPs(ctx context.Context, ips []db.ProviderIP) (err error)
	UpdateProviders(ctx context.Context, providers []db.ProviderUpdate) (err error)
	AddStatuses(ctx context.Context, providers []db.ProviderStatusUpdate) (err error)
	UpdateContractProofsChecks(ctx context.Context, contractsProofs []db.ContractProofsCheck) (err error)
	UpdateStatuses(ctx context.Context) (err error)
	UpdateUptime(ctx context.Context) (err error)
	UpdateRating(ctx context.Context) (err error)
	GetProvidersIPs(ctx context.Context) (ips []db.ProviderIP, err error)
	UpdateProvidersIPInfo(ctx context.Context, ips []db.ProviderIPInfo) (err error)
}

type system interface {
	SetParam(ctx context.Context, key string, value string) (err error)
	GetParam(ctx context.Context, key string) (value string, err error)
}

type ton interface {
	GetTransactions(ctx context.Context, addr string, lastProcessedLT uint64) (tx []*tonclient.Transaction, err error)
	GetStorageContractsInfo(ctx context.Context, addrs []string) (contracts []tonclient.StorageContract, err error)
	GetProvidersInfo(ctx context.Context, addrs []string) (contractsProviders []tonclient.StorageContractProviders, err error)
}

type providersMasterWorker struct {
	providers   providers
	system      system
	ton         ton
	ipinfo      ifconfig.IFConfig
	agentClient *agentclient.Client
	agentReg    *agentregistry.Registry
	masterAddr  string
	batchSize   uint32
	logger      *slog.Logger
}

type Worker interface {
	CollectNewProviders(ctx context.Context) (interval time.Duration, err error)
	DistributeProviderPing(ctx context.Context) (interval time.Duration, err error)
	CollectProvidersNewStorageContracts(ctx context.Context) (interval time.Duration, err error)
	DistributeStoreProof(ctx context.Context) (interval time.Duration, err error)
	UpdateUptime(ctx context.Context) (interval time.Duration, err error)
	UpdateRating(ctx context.Context) (interval time.Duration, err error)
	UpdateIPInfo(ctx context.Context) (interval time.Duration, err error)
}

func (w *providersMasterWorker) CollectNewProviders(ctx context.Context) (interval time.Duration, err error) {
	const (
		successInterval = 1 * time.Minute
		failureInterval = 5 * time.Second
	)

	log := w.logger.With("worker", "CollectNewProviders")
	log.Debug("collecting new providers")

	interval = successInterval

	lv, err := w.system.GetParam(ctx, lastLTKey)
	if err != nil {
		interval = failureInterval
		return
	}

	lastProcessedLT, _ := strconv.ParseInt(lv, 10, 64)

	p, err := w.providers.GetAllProvidersPubkeys(ctx)
	if err != nil {
		interval = failureInterval
		return
	}

	knownProviders := make(map[string]struct{}, len(p))
	for _, pubkey := range p {
		knownProviders[strings.ToLower(pubkey)] = struct{}{}
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, getTxTimeout)
	defer cancel()

	txs, err := w.ton.GetTransactions(timeoutCtx, w.masterAddr, uint64(lastProcessedLT))
	if err != nil {
		interval = failureInterval
		return
	}

	uniqueProviders := make(map[string]db.ProviderCreate)
	biggestLT := uint64(lastProcessedLT)
	for i := range txs {
		if txs[i].LT <= uint64(lastProcessedLT) {
			continue
		}

		if biggestLT < txs[i].LT {
			biggestLT = txs[i].LT
		}

		pos := strings.Index(txs[i].Message, prefix)
		if pos < 0 {
			continue
		}

		pos += len(prefix)
		if pos >= len(txs[i].Message) {
			continue
		}

		pubkey := strings.ToLower(txs[i].Message[pos:])

		if len(pubkey) != 64 {
			continue
		}

		if _, ok := knownProviders[pubkey]; ok {
			continue
		}

		prv, err := hex.DecodeString(pubkey)
		if err != nil || len(prv) != 32 {
			continue
		}

		uniqueProviders[pubkey] = db.ProviderCreate{
			Pubkey:       pubkey,
			Address:      txs[i].From,
			RegisteredAt: txs[i].CreatedAt,
		}
	}

	if len(uniqueProviders) == 0 {
		return
	}

	if biggestLT > uint64(lastProcessedLT) {
		if errP := w.system.SetParam(ctx, lastLTKey, strconv.FormatUint(biggestLT, 10)); errP != nil {
			log.Error("cannot update last processed LT for master wallet", "error", errP.Error())
		}
	}

	providersInit := make([]db.ProviderCreate, 0, len(uniqueProviders))
	for _, provider := range uniqueProviders {
		providersInit = append(providersInit, provider)
	}

	err = w.providers.AddProviders(ctx, providersInit)
	if err != nil {
		interval = failureInterval
		return
	}

	log.Info("successfully collected new providers", "count", len(providersInit))
	return
}

func (w *providersMasterWorker) DistributeProviderPing(ctx context.Context) (interval time.Duration, err error) {
	const (
		successInterval  = 1 * time.Minute
		failureInterval  = 5 * time.Second
		noAgentsInterval = 10 * time.Second
	)

	log := w.logger.With("worker", "DistributeProviderPing")
	log.Debug("distributing provider ping")

	agents := w.agentReg.Active()
	if len(agents) == 0 {
		log.Warn("no agents registered, skipping provider ping")
		return noAgentsInterval, nil
	}

	pubkeys, err := w.providers.GetAllProvidersPubkeys(ctx)
	if err != nil {
		return failureInterval, err
	}

	if len(pubkeys) == 0 {
		return successInterval, nil
	}

	statuses, updates, err := w.agentClient.DistributePing(ctx, agents, pubkeys)
	if err != nil {
		log.Error("all agents failed for provider ping", "error", err.Error())
		return failureInterval, err
	}

	if err = w.providers.AddStatuses(ctx, statuses); err != nil {
		return failureInterval, err
	}

	if err = w.providers.UpdateProviders(ctx, updates); err != nil {
		return failureInterval, err
	}

	log.Info("provider ping distributed", "pinged", len(statuses), "online", len(updates))
	return successInterval, nil
}

func (w *providersMasterWorker) CollectProvidersNewStorageContracts(ctx context.Context) (interval time.Duration, err error) {
	const (
		successInterval = 60 * time.Minute
		failureInterval = 15 * time.Second
	)

	log := w.logger.With("worker", "ProvidersContracts")
	log.Debug("collect new providers contracts")

	interval = successInterval

	providersWallets, err := w.providers.GetAllProvidersWallets(ctx)
	if err != nil {
		interval = failureInterval
		return
	}

	providersToUpdate := make([]db.ProviderWalletLT, 0, len(providersWallets))
	storageContracts := make(map[string]db.StorageContract)

	var wg sync.WaitGroup
	smu := sync.Mutex{}
	pmu := sync.Mutex{}

	wg.Add(len(providersWallets))
	for _, provider := range providersWallets {
		go func(ctx context.Context, provider db.ProviderWallet) {
			defer wg.Done()

			sc, lastLT, err := w.scanProviderTransactions(ctx, provider)
			if err != nil {
				log.Error("failed to scan provider transactions", "address", provider.Address, "error", err)
				return
			}

			if len(sc) > 0 {
				smu.Lock()
				for src, tx := range sc {
					if v, ok := storageContracts[src]; ok {
						for p := range tx.ProvidersAddresses {
							v.ProvidersAddresses[p] = struct{}{}
						}
						if v.LastLT < tx.LastLT {
							v.LastLT = tx.LastLT
						}
						storageContracts[src] = v
					} else {
						storageContracts[src] = tx
					}
				}
				smu.Unlock()
			}

			if lastLT != provider.LT {
				pmu.Lock()
				providersToUpdate = append(providersToUpdate, db.ProviderWalletLT{
					PubKey: provider.PubKey,
					LT:     lastLT,
				})
				pmu.Unlock()
			}
		}(ctx, provider)
	}

	wg.Wait()

	if len(storageContracts) == 0 {
		return
	}

	contractsAddresses := make([]string, 0, len(storageContracts))
	for addr := range storageContracts {
		contractsAddresses = append(contractsAddresses, addr)
	}

	contractsInfo, err := w.ton.GetStorageContractsInfo(ctx, contractsAddresses)
	if err != nil {
		log.Error("failed to get storage contracts info", "error", err)
		interval = failureInterval
		return
	}

	newContracts := make([]db.StorageContract, 0, len(contractsInfo))
	for _, contract := range contractsInfo {
		sc, ok := storageContracts[contract.Address]
		if !ok {
			log.Error("storage contract not found in scanned transactions", "address", contract.Address)
			continue
		}
		newContracts = append(newContracts, db.StorageContract{
			ProvidersAddresses: sc.ProvidersAddresses,
			Address:            contract.Address,
			BagID:              contract.BagID,
			OwnerAddr:          contract.OwnerAddr,
			Size:               contract.Size,
			ChunkSize:          contract.ChunkSize,
			LastLT:             sc.LastLT,
		})
	}

	if err = w.providers.UpdateProvidersLT(ctx, providersToUpdate); err != nil {
		log.Error("failed to update providers wallets lt", "error", err)
		interval = failureInterval
		return
	}

	if err = w.providers.AddStorageContracts(ctx, newContracts); err != nil {
		log.Error("failed to add storage contracts", "error", err)
		interval = failureInterval
		return
	}

	log.Info("successfully collected new storage contracts", "count", len(newContracts))
	return
}

func (w *providersMasterWorker) DistributeStoreProof(ctx context.Context) (interval time.Duration, err error) {
	const (
		successInterval  = 60 * time.Minute
		failureInterval  = 15 * time.Second
		noAgentsInterval = 10 * time.Second
	)

	log := w.logger.With(slog.String("worker", "DistributeStoreProof"))
	log.Debug("distributing store proof")

	agents := w.agentReg.Active()
	if len(agents) == 0 {
		log.Warn("no agents registered, skipping store proof")
		return noAgentsInterval, nil
	}

	storageContracts, err := w.providers.GetStorageContracts(ctx)
	if err != nil {
		log.Error("failed to get storage contracts", "error", err)
		return failureInterval, err
	}

	// Phase 1 (coordinator-only): remove rejected contracts via TON.
	// Hard abort on failure — proceeding with a stale list would skip deleting closed contracts.
	activeContracts, err := w.updateRejectedContracts(ctx, storageContracts)
	if err != nil {
		log.Error("failed to update rejected contracts, aborting cycle", "error", err)
		return failureInterval, err
	}

	if len(activeContracts) == 0 {
		return successInterval, nil
	}

	// Phase 2 (agents): resolve IPs and verify storage proofs.
	proofProviders := contractsToProofProviders(activeContracts)

	ips, proofResults, err := w.agentClient.DistributeProofs(ctx, agents, proofProviders)
	if err != nil {
		log.Error("all agents failed for store proof", "error", err.Error())
		return failureInterval, err
	}

	if len(ips) > 0 {
		if err = w.providers.UpdateProvidersIPs(ctx, ips); err != nil {
			log.Error("failed to update providers IPs", "error", err)
			return failureInterval, err
		}
	}

	if len(proofResults) > 0 {
		if err = w.providers.UpdateContractProofsChecks(ctx, proofResults); err != nil {
			log.Error("failed to update contract proofs checks", "error", err)
			return failureInterval, err
		}
	} else {
		log.Warn("no proof results received from agents (partial cycle accepted)")
	}

	if err = w.providers.UpdateStatuses(ctx); err != nil {
		log.Error("failed to update provider statuses", "error", err)
		return failureInterval, err
	}

	log.Info("store proof distributed",
		"active_contracts", len(activeContracts),
		"proof_results", len(proofResults),
	)
	return successInterval, nil
}

func (w *providersMasterWorker) UpdateUptime(ctx context.Context) (interval time.Duration, err error) {
	const (
		successInterval = 5 * time.Minute
		failureInterval = 5 * time.Second
	)

	w.logger.With(slog.String("worker", "UpdateUptime")).Debug("updating provider uptime")

	if err = w.providers.UpdateUptime(ctx); err != nil {
		return failureInterval, err
	}
	return successInterval, nil
}

func (w *providersMasterWorker) UpdateRating(ctx context.Context) (interval time.Duration, err error) {
	const (
		successInterval = 5 * time.Minute
		failureInterval = 5 * time.Second
	)

	w.logger.With(slog.String("worker", "UpdateRating")).Debug("updating provider ratings")

	if err = w.providers.UpdateRating(ctx); err != nil {
		return failureInterval, err
	}
	return successInterval, nil
}

func (w *providersMasterWorker) UpdateIPInfo(ctx context.Context) (interval time.Duration, err error) {
	const (
		successInterval = 240 * time.Minute
		failureInterval = 30 * time.Second
	)

	log := w.logger.With(slog.String("worker", "UpdateIPInfo"))
	log.Debug("updating provider IP info")

	ips, err := w.providers.GetProvidersIPs(ctx)
	if err != nil {
		log.Error("failed to get provider IPs", "error", err)
		return failureInterval, err
	}

	if len(ips) == 0 {
		log.Info("no provider IPs to update")
		return successInterval, nil
	}

	ipsInfo := make([]db.ProviderIPInfo, 0, len(ips))
	for _, ip := range ips {
		time.Sleep(time.Second)

		ipErr := func() error {
			tCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			info, iErr := w.ipinfo.GetIPInfo(tCtx, ip.Provider.IP)
			if iErr != nil {
				return fmt.Errorf("failed to get IP info: %w", iErr)
			}

			s, jErr := json.Marshal(info)
			if jErr != nil {
				return fmt.Errorf("failed to marshal IP info: %w, ip: %s, info: %v", jErr, ip.Provider.IP, info)
			}

			ipsInfo = append(ipsInfo, db.ProviderIPInfo{
				PublicKey: ip.PublicKey,
				IPInfo:    string(s),
			})
			return nil
		}()
		if ipErr != nil {
			log.Error(ipErr.Error())
		}
	}

	if err = w.providers.UpdateProvidersIPInfo(ctx, ipsInfo); err != nil {
		log.Error("failed to update provider IP info", "error", err)
		return failureInterval, err
	}

	return successInterval, nil
}

func (w *providersMasterWorker) updateRejectedContracts(ctx context.Context, storageContracts []db.ContractToProviderRelation) (activeContracts []db.ContractToProviderRelation, err error) {
	log := w.logger.With(slog.String("worker", "updateRejectedContracts"))

	if len(storageContracts) == 0 {
		log.Debug("no storage contracts to process")
		return
	}

	uniqueContractAddresses := make(map[string]uint64, len(storageContracts))
	for _, sc := range storageContracts {
		uniqueContractAddresses[sc.Address] = sc.Size
	}

	contractAddresses := make([]string, 0, len(uniqueContractAddresses))
	for addr := range uniqueContractAddresses {
		contractAddresses = append(contractAddresses, addr)
	}

	contractsProvidersList, err := w.ton.GetProvidersInfo(ctx, contractAddresses)
	if err != nil {
		log.Error("failed to get providers info", "error", err)
		return
	}

	type contractInfo struct {
		providers map[string]struct{}
		skip      bool
	}

	activeRelations := make(map[string]contractInfo, len(contractsProvidersList))
	for _, contract := range contractsProvidersList {
		contractProviders := make(map[string]struct{}, len(contract.Providers))
		for _, provider := range contract.Providers {
			providerPublicKey := fmt.Sprintf("%x", provider.Key)
			if isRemovedByLowBalance(new(big.Int).SetUint64(uniqueContractAddresses[contract.Address]), provider, contract) {
				log.Warn("storage contract has not enough balance for too long, will be removed",
					"provider", providerPublicKey,
					"address", contract.Address,
					"balance", contract.Balance)
				continue
			}
			contractProviders[providerPublicKey] = struct{}{}
		}
		activeRelations[contract.Address] = contractInfo{
			providers: contractProviders,
			skip:      contract.LiteServerError,
		}
	}

	activeContracts = make([]db.ContractToProviderRelation, 0, len(storageContracts))
	closedContracts := make([]db.ContractToProviderRelation, 0, len(storageContracts))

	for _, sc := range storageContracts {
		if info, exists := activeRelations[sc.Address]; exists {
			if info.skip {
				log.Debug("lite servers unavailable, skip providers check for", "address", sc.Address)
				continue
			}
			if _, providerExists := info.providers[sc.ProviderPublicKey]; providerExists {
				activeContracts = append(activeContracts, sc)
			} else {
				closedContracts = append(closedContracts, sc)
			}
		} else {
			closedContracts = append(closedContracts, sc)
		}
	}

	if err = w.providers.UpdateRejectedStorageContracts(ctx, closedContracts); err != nil {
		log.Error("failed to update rejected storage contracts", "error", err)
		return nil, err
	}

	log.Info("updated rejected storage contracts",
		"closed_count", len(closedContracts),
		"active_count", len(activeContracts))
	return
}

func (w *providersMasterWorker) scanProviderTransactions(ctx context.Context, provider db.ProviderWallet) (contracts map[string]db.StorageContract, lastLT uint64, err error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, getTxTimeout)
	defer cancel()

	txs, err := w.ton.GetTransactions(timeoutCtx, provider.Address, provider.LT)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get transactions: %w", err)
	}

	contracts = make(map[string]db.StorageContract, len(txs))
	lastLT = provider.LT

	for _, tx := range txs {
		if tx == nil || tx.Op != storageRewardWithdrawalOpCode {
			continue
		}

		s := db.StorageContract{
			ProvidersAddresses: make(map[string]struct{}),
			Address:            tx.From,
			LastLT:             tx.LT,
		}
		s.ProvidersAddresses[provider.Address] = struct{}{}

		if tx.LT > lastLT {
			lastLT = tx.LT
		}

		contracts[tx.From] = s
	}

	return
}

func contractsToProofProviders(contracts []db.ContractToProviderRelation) []agentclient.ProofProvider {
	grouped := make(map[string]*agentclient.ProofProvider)
	for _, sc := range contracts {
		pp, ok := grouped[sc.ProviderPublicKey]
		if !ok {
			grouped[sc.ProviderPublicKey] = &agentclient.ProofProvider{
				Pubkey:          sc.ProviderPublicKey,
				ProviderAddress: sc.ProviderAddress,
				Contracts:       []agentclient.ProofContract{},
			}
			pp = grouped[sc.ProviderPublicKey]
		}
		pp.Contracts = append(pp.Contracts, agentclient.ProofContract{
			Address: sc.Address,
			BagID:   sc.BagID,
		})
	}

	result := make([]agentclient.ProofProvider, 0, len(grouped))
	for _, pp := range grouped {
		result = append(result, *pp)
	}
	return result
}

func NewWorker(
	providers providers,
	system system,
	ton ton,
	ipinfo ifconfig.IFConfig,
	agentClient *agentclient.Client,
	agentReg *agentregistry.Registry,
	masterAddr string,
	batchSize uint32,
	logger *slog.Logger,
) Worker {
	return &providersMasterWorker{
		providers:   providers,
		system:      system,
		ton:         ton,
		ipinfo:      ipinfo,
		agentClient: agentClient,
		agentReg:    agentReg,
		masterAddr:  masterAddr,
		batchSize:   batchSize,
		logger:      logger,
	}
}
