package providersmaster

import (
	"math/big"
	"time"

	"github.com/xssnick/tonutils-go/tlb"

	tonclient "mytonprovider-backend/pkg/clients/ton"
)

func isRemovedByLowBalance(bagSize *big.Int, provider tonclient.Provider, contract tonclient.StorageContractProviders) bool {
	var storageFee = tlb.MustFromTON("0.05").Nano()

	mul := new(big.Int).Mul(new(big.Int).SetUint64(provider.RatePerMBDay), bagSize)
	mul = mul.Mul(mul, new(big.Int).SetUint64(uint64(provider.MaxSpan)))
	bounty := new(big.Int).Div(mul, big.NewInt(24*60*60*1024*1024))
	bounty = bounty.Add(bounty, storageFee)

	if new(big.Int).SetUint64(contract.Balance).Cmp(bounty) < 0 {
		if provider.LastProofTime.Unix() <= 0 {
			return false
		}

		deadline := provider.LastProofTime.Unix() + int64(provider.MaxSpan) + 3600
		if time.Now().Unix() > deadline {
			return true
		}
	}

	return false
}
