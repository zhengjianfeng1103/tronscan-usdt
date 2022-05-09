package impl

import (
	"github.com/fbsobreira/gotron-sdk/pkg/address"
	"github.com/fbsobreira/gotron-sdk/pkg/proto/api"
	"go.uber.org/zap"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"testing"
	"time"
)

type FlySwap struct {
	WalletMap map[string]bool
	RW        sync.RWMutex
}

func NewFlySwap() *FlySwap {
	walletMap := make(map[string]bool)
	return &FlySwap{
		WalletMap: walletMap,
	}
}

func (f *FlySwap) AddWallet(base58Address string) {
	f.RW.Lock()
	defer f.RW.Unlock()

	if _, ok := f.WalletMap[base58Address]; ok {
		zap.L().Error("wallet exist")
		return
	}

	_, err := address.Base58ToAddress(base58Address)
	if err != nil {
		zap.L().Error("not base58 address")
		return
	}

	f.WalletMap[base58Address] = true
}

func (f *FlySwap) RemoveWallet(base58Address string) {
	f.RW.Lock()
	defer f.RW.Unlock()

	if _, ok := f.WalletMap[base58Address]; !ok {
		zap.L().Error("wallet is not exist")
		return
	}

	delete(f.WalletMap, base58Address)
}

func (f *FlySwap) Notify(result Trc20Result) error {
	zap.L().Debug("fly swap receive", zap.Any("result", result))

	if _, ok := f.WalletMap[result.To]; ok {
		//入帐
		zap.L().Info("入账", zap.Any("_to", result.To),
			zap.Any("hash", result.TxHash),
			zap.Any("_value", result.Amount),
			zap.Any("from", result.From),
			zap.Any("contractAddress", result.ContractAddress))

	}
	return nil
}

func TestTronScanner_Start(t *testing.T) {

	development, _ := zap.NewProduction()
	zap.ReplaceGlobals(development)

	swap := NewFlySwap()
	swap.AddWallet("TPJtG4WcqBFVVagoWhjpQZdbxT7LX4bADx")

	scanner := NewTronScanner()
	scanner.RegisterObservers(swap)

	var slowBlock *api.BlockExtention
	slowBlock, err := scanner.FullNode.GetBlockByNum(40319037) //40319038
	if err != nil {
		t.Fatal(err)
	}

	names := scanner.GetBucketNames()
	t.Log(names)

	simpleBlock := SimpleBlock{Height: slowBlock.GetBlockHeader().GetRawData().GetNumber()}
	err = scanner.SetCurrentBlock(&simpleBlock)
	if err != nil {
		t.Fatal(err)
	}

	err = scanner.DeleteTxHash("b7a489ab27f6f99f239277e90412116f2cfcb5aa6393252f4be80976c9a6c067")
	if err != nil {
		t.Fatal(err)
	}

	scanner.Start()
	defer scanner.Stop()

	time.Sleep(10000 * time.Second)
}

func TestTronScanner_Start_MaxHeight(t *testing.T) {

	development, _ := zap.NewProduction()
	zap.ReplaceGlobals(development)

	swap := NewFlySwap()
	swap.AddWallet("TPJtG4WcqBFVVagoWhjpQZdbxT7LX4bADx")

	scanner := NewTronScanner()
	scanner.RegisterObservers(swap)

	var slowBlock *api.BlockExtention
	slowBlock, err := scanner.FullNode.GetNowBlock() //40319038
	if err != nil {
		t.Fatal(err)
	}

	names := scanner.GetBucketNames()
	t.Log(names)

	simpleBlock := SimpleBlock{Height: slowBlock.GetBlockHeader().GetRawData().GetNumber()}
	err = scanner.SetCurrentBlock(&simpleBlock)
	if err != nil {
		t.Fatal(err)
	}

	scanner.Start()
	defer scanner.Stop()

	go func() {
		http.ListenAndServe("0.0.0.0:8899", nil)
	}()

	time.Sleep(100000 * time.Second)
}
