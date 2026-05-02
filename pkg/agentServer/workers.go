package agentserver

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-storage-provider/pkg/transport"
	"github.com/xssnick/tonutils-storage/storage"

	agentclient "mytonprovider-backend/pkg/agentClient"
	"mytonprovider-backend/pkg/constants"
	"mytonprovider-backend/pkg/models/db"
)

const (
	maxConcurrentProviderChecks = 30
	maxConcurrentBagChecks      = 30
	fakeSize                    = 1
	verifyStorageRetries        = 3

	providerResponseTimeout = 14 * time.Second
	dhtTimeout              = 14 * time.Second
	pingTimeout             = 7 * time.Second
	rlQueryTimeout          = 10 * time.Second
)

type Workers struct {
	providerClient *transport.Client
	dhtClient      *dht.Client
	prv            ed25519.PrivateKey
	logger         *slog.Logger
}

func NewWorkers(
	providerClient *transport.Client,
	dhtClient *dht.Client,
	prv ed25519.PrivateKey,
	logger *slog.Logger,
) *Workers {
	return &Workers{
		providerClient: providerClient,
		dhtClient:      dhtClient,
		prv:            prv,
		logger:         logger,
	}
}

// PingProviders pings each pubkey via ADNL and fetches storage rates.
func (w *Workers) PingProviders(ctx context.Context, providers []agentclient.PingProvider) []agentclient.PingResult {
	log := w.logger.With(slog.String("worker", "PingProviders"))

	results := make([]agentclient.PingResult, 0, len(providers))
	var mu sync.Mutex

	semaphore := make(chan struct{}, maxConcurrentProviderChecks)
	var wg sync.WaitGroup

	for _, p := range providers {
		wg.Add(1)
		go func(pubkey string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			result := w.pingOne(ctx, pubkey, log)

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(p.Pubkey)
	}

	wg.Wait()
	return results
}

func (w *Workers) pingOne(ctx context.Context, pubkey string, log *slog.Logger) agentclient.PingResult {
	result := agentclient.PingResult{Pubkey: pubkey, IsOnline: false}

	d, err := hex.DecodeString(pubkey)
	if err != nil || len(d) != 32 {
		return result
	}

	tCtx, cancel := context.WithTimeout(ctx, providerResponseTimeout)
	rates, err := w.providerClient.GetStorageRates(tCtx, d, fakeSize)
	cancel()
	if err != nil {
		log.Debug("provider ping failed", slog.String("pubkey", pubkey), slog.String("error", err.Error()))
		return result
	}

	result.IsOnline = true
	result.RatePerMBDay = new(big.Int).SetBytes(rates.RatePerMBDay).Int64()
	result.MinBounty = new(big.Int).SetBytes(rates.MinBounty).Int64()
	result.MinSpan = rates.MinSpan
	result.MaxSpan = rates.MaxSpan
	return result
}

// CheckProofs resolves provider IPs and verifies storage proofs for each provider's bags.
func (w *Workers) CheckProofs(ctx context.Context, providers []agentclient.ProofProvider) ([]db.ProviderIP, []db.ContractProofsCheck) {
	log := w.logger.With(slog.String("worker", "CheckProofs"))

	resolvedIPs, allContracts := w.resolveIPs(ctx, providers, log)

	proofResults := w.verifyProofs(ctx, allContracts, resolvedIPs, log)

	ipList := make([]db.ProviderIP, 0, len(resolvedIPs))
	for _, ip := range resolvedIPs {
		ipList = append(ipList, ip)
	}

	return ipList, proofResults
}

func (w *Workers) resolveIPs(ctx context.Context, providers []agentclient.ProofProvider, log *slog.Logger) (map[string]db.ProviderIP, []db.ContractToProviderRelation) {
	var allContracts []db.ContractToProviderRelation
	for _, pp := range providers {
		for _, c := range pp.Contracts {
			allContracts = append(allContracts, db.ContractToProviderRelation{
				ProviderPublicKey: pp.Pubkey,
				ProviderAddress:   pp.ProviderAddress,
				Address:           c.Address,
				BagID:             c.BagID,
			})
		}
	}

	// One representative contract per provider is sufficient for IP resolution.
	uniqueProviders := make(map[string]db.ContractToProviderRelation)
	for _, sc := range allContracts {
		if _, exists := uniqueProviders[sc.ProviderPublicKey]; !exists {
			uniqueProviders[sc.ProviderPublicKey] = sc
		}
	}

	resolvedIPs := make(map[string]db.ProviderIP, len(uniqueProviders))
	var notFoundKeys []string

	semaphore := make(chan struct{}, maxConcurrentProviderChecks)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, sc := range uniqueProviders {
		wg.Add(1)
		go func(contract db.ContractToProviderRelation) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			ip, err := w.findProviderIPs(ctx, contract)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				log.Debug("failed to find provider IPs",
					slog.String("pubkey", contract.ProviderPublicKey),
					slog.String("error", err.Error()))
				notFoundKeys = append(notFoundKeys, contract.ProviderPublicKey)
				resolvedIPs[contract.ProviderPublicKey] = db.ProviderIP{PublicKey: contract.ProviderPublicKey}
			} else {
				resolvedIPs[contract.ProviderPublicKey] = ip
			}
		}(sc)
	}
	wg.Wait()

	// Fallback: try overlay DHT for providers whose storage IP wasn't found directly.
	for _, pk := range notFoundKeys {
		ip := resolvedIPs[pk]
		if ip.Provider.IP == "" {
			log.Info("provider IP not found, no overlay fallback possible", "provider_pubkey", pk)
			delete(resolvedIPs, pk)
			continue
		}

		var providerContracts []db.ContractToProviderRelation
		for _, sc := range allContracts {
			if sc.ProviderPublicKey == pk {
				providerContracts = append(providerContracts, sc)
			}
		}

		if len(providerContracts) == 0 {
			delete(resolvedIPs, pk)
			continue
		}

		storageIP, err := w.findStorageIPOverlay(ctx, ip.Provider.IP, providerContracts, log)
		if err != nil {
			log.Error("failed to find storage IP via overlay", "provider_pubkey", pk, "error", err)
			delete(resolvedIPs, pk)
			continue
		}

		ip.Storage = storageIP
		resolvedIPs[pk] = ip
	}

	return resolvedIPs, allContracts
}

func (w *Workers) verifyProofs(ctx context.Context, contracts []db.ContractToProviderRelation, availableIPs map[string]db.ProviderIP, log *slog.Logger) []db.ContractProofsCheck {
	providersContracts := make(map[string][]db.ContractToProviderRelation)
	for _, sc := range contracts {
		providersContracts[sc.ProviderPublicKey] = append(providersContracts[sc.ProviderPublicKey], sc)
	}

	semaphore := make(chan struct{}, maxConcurrentBagChecks)
	var bagsStatuses sync.Map
	var wg sync.WaitGroup

	for pubkey, provContracts := range providersContracts {
		wg.Add(1)
		go func(pubkey string, provContracts []db.ContractToProviderRelation) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			gw := adnl.NewGateway(w.prv)
			defer func() {
				if closeErr := gw.Close(); closeErr != nil {
					log.Error("failed to close ADNL gateway", "error", closeErr)
				}
			}()

			if sErr := gw.StartClient(); sErr != nil {
				log.Error("failed to start ADNL gateway", "error", sErr)
				return
			}

			ip, ok := availableIPs[pubkey]
			if !ok {
				fillStatuses(&bagsStatuses, provContracts, constants.IPNotFound)
				return
			}

			checkProviderFiles(ctx, gw, ip, provContracts, &bagsStatuses, log)
		}(pubkey, provContracts)
	}
	wg.Wait()

	results := make([]db.ContractProofsCheck, 0)
	bagsStatuses.Range(func(_, value any) bool {
		if proof, ok := value.(db.ContractProofsCheck); ok {
			results = append(results, proof)
		}
		return true
	})
	return results
}

func (w *Workers) findProviderIPs(ctx context.Context, sc db.ContractToProviderRelation) (result db.ProviderIP, err error) {
	result.PublicKey = sc.ProviderPublicKey

	pk, err := hex.DecodeString(sc.ProviderPublicKey)
	if err != nil {
		return result, fmt.Errorf("failed to decode provider public key: %w", err)
	}

	result.Provider, err = w.findProviderIP(ctx, pk)
	if err != nil {
		return result, fmt.Errorf("failed to find provider IP: %w", err)
	}

	addr, err := address.ParseAddr(sc.Address)
	if err != nil {
		return result, fmt.Errorf("failed to parse contract address: %w", err)
	}

	result.Storage, err = w.findStorageIP(ctx, addr, pk)
	if err != nil {
		return result, fmt.Errorf("failed to find storage IP: %w", err)
	}

	return result, nil
}

func (w *Workers) findStorageIP(ctx context.Context, addr *address.Address, pk []byte) (ip db.IPInfo, err error) {
	var proof []byte
	err = tryNTimes(func() (cErr error) {
		tCtx, cancel := context.WithTimeout(ctx, providerResponseTimeout)
		defer cancel()
		proof, cErr = w.providerClient.VerifyStorageADNLProof(tCtx, pk, addr)
		return
	}, verifyStorageRetries)
	if err != nil {
		return ip, fmt.Errorf("failed to verify storage ADNL proof: %w", err)
	}

	tCtx, cancel := context.WithTimeout(ctx, dhtTimeout)
	defer cancel()
	l, pub, err := w.dhtClient.FindAddresses(tCtx, proof)
	if err != nil {
		return ip, fmt.Errorf("failed to find addresses in DHT: %w", err)
	}

	if l == nil || len(l.Addresses) == 0 {
		return ip, errors.New("no storage addresses found")
	}

	ip.PublicKey = pub
	ip.IP = l.Addresses[0].IP.String()
	ip.Port = l.Addresses[0].Port
	return ip, nil
}

func (w *Workers) findProviderIP(ctx context.Context, pk []byte) (ip db.IPInfo, err error) {
	channelKeyId, err := tl.Hash(keys.PublicKeyED25519{Key: pk})
	if err != nil {
		return ip, fmt.Errorf("failed to calc hash of provider key: %w", err)
	}

	tCtx, cancel := context.WithTimeout(ctx, dhtTimeout)
	defer cancel()
	dhtVal, _, err := w.dhtClient.FindValue(tCtx, &dht.Key{
		ID:    channelKeyId,
		Name:  []byte("storage-provider"),
		Index: 0,
	})
	if err != nil {
		return ip, fmt.Errorf("failed to find storage-provider in DHT: %w", err)
	}

	var nodeAddr transport.ProviderDHTRecord
	if _, pErr := tl.Parse(&nodeAddr, dhtVal.Data, true); pErr != nil {
		return ip, fmt.Errorf("failed to parse node DHT value: %w", pErr)
	}

	if len(nodeAddr.ADNLAddr) == 0 {
		return ip, errors.New("no ADNL addresses in node DHT value")
	}

	tCtx2, cancel2 := context.WithTimeout(ctx, dhtTimeout)
	defer cancel2()
	l, pub, err := w.dhtClient.FindAddresses(tCtx2, nodeAddr.ADNLAddr)
	if err != nil {
		return ip, fmt.Errorf("failed to find ADNL addresses in DHT: %w", err)
	}

	if l == nil || len(l.Addresses) == 0 {
		return ip, errors.New("no provider addresses found")
	}

	ip.PublicKey = pub
	ip.IP = l.Addresses[0].IP.String()
	ip.Port = l.Addresses[0].Port
	return ip, nil
}

func (w *Workers) findStorageIPOverlay(ctx context.Context, providerIP string, contracts []db.ContractToProviderRelation, log *slog.Logger) (ip db.IPInfo, err error) {
	if len(contracts) == 0 {
		return ip, errors.New("no contracts provided")
	}

	bagsToCheck := len(contracts)
	switch {
	case len(contracts) > 100:
		bagsToCheck = max(1, len(contracts)*10/100)
	case len(contracts) > 5:
		bagsToCheck = max(1, len(contracts)*20/100)
	}

	log = log.With("provider_ip", providerIP, "bags_to_check", bagsToCheck, "total_bags", len(contracts))

	shuffled := make([]db.ContractToProviderRelation, len(contracts))
	copy(shuffled, contracts)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	for i := 0; i < bagsToCheck && i < len(shuffled); i++ {
		sc := shuffled[i]

		bag, dErr := hex.DecodeString(sc.BagID)
		if dErr != nil {
			log.Error("failed to decode bag ID", "bag_id", sc.BagID, "error", dErr)
			continue
		}

		tCtx, cancel := context.WithTimeout(ctx, dhtTimeout)
		nodesList, _, fErr := w.dhtClient.FindOverlayNodes(tCtx, bag)
		cancel()

		if fErr != nil {
			if !errors.Is(fErr, dht.ErrDHTValueIsNotFound) {
				log.Error("failed to find bag overlay nodes", "bag_id", sc.BagID, "error", fErr)
			}
			continue
		}

		if nodesList == nil || len(nodesList.List) == 0 {
			continue
		}

		for _, node := range nodesList.List {
			key, ok := node.ID.(keys.PublicKeyED25519)
			if !ok {
				continue
			}

			adnlID, hErr := tl.Hash(key)
			if hErr != nil {
				log.Error("failed to hash overlay key", "error", hErr)
				continue
			}

			tCtx2, cancel2 := context.WithTimeout(ctx, dhtTimeout)
			addrList, pubKey, fErr := w.dhtClient.FindAddresses(tCtx2, adnlID)
			cancel2()

			if fErr != nil {
				if !errors.Is(fErr, dht.ErrDHTValueIsNotFound) {
					log.Debug("failed to find addresses in DHT", "error", fErr)
				}
				continue
			}

			if addrList == nil || len(addrList.Addresses) == 0 {
				continue
			}

			for _, addr := range addrList.Addresses {
				if addr.IP.String() == providerIP {
					ip.PublicKey = pubKey
					ip.IP = addr.IP.String()
					ip.Port = addr.Port
					log.Info("found storage IP via overlay DHT",
						"provider_pubkey", sc.ProviderPublicKey,
						"ip", ip.IP, "port", ip.Port)
					return ip, nil
				}
			}
		}
	}

	return ip, fmt.Errorf("storage IP not found via overlay DHT after checking %d bags", bagsToCheck)
}

// --- package-local helpers ---

func fillStatuses(bagsStatuses *sync.Map, contracts []db.ContractToProviderRelation, reason constants.ReasonCode) {
	for _, sc := range contracts {
		bagsStatuses.Store(sc.ProviderAddress+sc.BagID, db.ContractProofsCheck{
			ContractAddress: sc.Address,
			ProviderAddress: sc.ProviderAddress,
			Reason:          reason,
		})
	}
}

func getKey(bagID, ip string, port int32) string {
	return ip + ":" + strconv.Itoa(int(port)) + "/" + bagID
}

func checkProviderFiles(ctx context.Context, gw *adnl.Gateway, ip db.ProviderIP, storageContracts []db.ContractToProviderRelation, bagsStatuses *sync.Map, log *slog.Logger) {
	log = log.With(slog.String("provider_pubkey", ip.PublicKey))

	maxFailureThreshold := uint32(float32(len(storageContracts)) / 100.0 * 20.0)
	var failsInARow uint32

	addr := ip.Storage.IP + ":" + strconv.Itoa(int(ip.Storage.Port))
	peer, rErr := gw.RegisterClient(addr, ip.Storage.PublicKey)
	if rErr != nil {
		log.Debug("failed to create ADNL peer", "error", rErr)
		fillStatuses(bagsStatuses, storageContracts, constants.CantCreatePeer)
		return
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, pingTimeout)
	_, pErr := peer.Ping(pingCtx)
	pingCancel()
	if pErr != nil {
		log.Debug("initial provider ping failed", "error", pErr)
		fillStatuses(bagsStatuses, storageContracts, constants.FailedInitialPing)
		return
	}

	rl := rldp.NewClientV2(peer)
	defer rl.Close()

	for _, sc := range storageContracts {
		statusKey := getKey(sc.BagID, ip.Storage.IP, ip.Storage.Port)

		if failsInARow > maxFailureThreshold {
			bagsStatuses.Store(statusKey, db.ContractProofsCheck{
				ContractAddress: sc.Address,
				ProviderAddress: sc.ProviderAddress,
				Reason:          constants.UnavailableProvider,
			})
			continue
		}

		reason := checkPiece(ctx, rl, sc.BagID, log)
		bagsStatuses.Store(statusKey, db.ContractProofsCheck{
			ContractAddress: sc.Address,
			ProviderAddress: sc.ProviderAddress,
			Reason:          reason,
		})

		if reason == constants.ValidStorageProof {
			failsInARow = 0
		} else {
			failsInARow++
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func checkPiece(ctx context.Context, rl *rldp.RLDP, bagID string, log *slog.Logger) (reason constants.ReasonCode) {
	log = log.With(slog.String("bag_id", bagID))

	peer, ok := rl.GetADNL().(adnl.Peer)
	if !ok {
		log.Error("failed to get ADNL peer")
		return constants.UnknownPeer
	}

	peer.Reinit()
	est := time.Now()

	pingCtx, pc := context.WithTimeout(ctx, pingTimeout)
	_, err := peer.Ping(pingCtx)
	pc()
	if err != nil {
		log.Debug("ping to provider failed", "error", err)
		return constants.PingFailed
	}

	bag, dErr := hex.DecodeString(bagID)
	if dErr != nil {
		log.Error("failed to decode bag ID", "error", dErr)
		return constants.InvalidBagID
	}

	over, err := tl.Hash(keys.PublicKeyOverlay{Key: bag})
	if err != nil {
		log.Debug("failed to hash overlay key", "error", err)
		return constants.InvalidBagID
	}

	if time.Since(est) > 5*time.Second {
		peer.Reinit()
		est = time.Now()
	}

	var res storage.TorrentInfoContainer
	rlCtx, rlc := context.WithTimeout(ctx, rlQueryTimeout)
	err = rl.DoQuery(rlCtx, 32<<20, overlay.WrapQuery(over, &storage.GetTorrentInfo{}), &res)
	rlc()
	if err != nil {
		log.Debug("failed to get torrent info from provider", "error", err)
		return constants.GetInfoFailed
	}

	cl, err := cell.FromBOC(res.Data)
	if err != nil {
		log.Debug("failed to parse BoC of torrent info", "error", err)
		return constants.InvalidHeader
	}

	if !bytes.Equal(cl.Hash(), bag) {
		log.Debug("hash not equal bag", "hash", cl.Hash(), "bag", bag)
		return constants.InvalidHeader
	}

	var info storage.TorrentInfo
	if err = tlb.LoadFromCell(&info, cl.BeginParse()); err != nil {
		log.Debug("failed to load torrent info from cell", "error", err)
		return constants.InvalidHeader
	}

	pieceID := int32(1)
	if info.PieceSize != 0 {
		if p := int32(info.FileSize / uint64(info.PieceSize)); p != 0 {
			pieceID = rand.Int31n(p)
		}
	}

	if time.Since(est) > 5*time.Second {
		peer.Reinit()
	}

	var piece storage.Piece
	rl2Ctx, rl2c := context.WithTimeout(ctx, rlQueryTimeout)
	err = rl.DoQuery(rl2Ctx, 32<<20, overlay.WrapQuery(over, &storage.GetPiece{PieceID: pieceID}), &piece)
	rl2c()
	if err != nil {
		log.Debug("failed to get piece from provider", "error", err)
		return constants.CantGetPiece
	}

	proof, err := cell.FromBOC(piece.Proof)
	if err != nil {
		log.Debug("failed to parse BoC of piece", "error", err)
		return constants.CantParseBoC
	}

	if err = cell.CheckProof(proof, info.RootHash); err != nil {
		log.Debug("proof check failed", "error", err)
		return constants.ProofCheckFailed
	}

	return constants.ValidStorageProof
}

func tryNTimes(f func() error, n int) (err error) {
	for i := 0; i < n; i++ {
		err = f()
		if err == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return err
}
