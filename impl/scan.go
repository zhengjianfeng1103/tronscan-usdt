package impl

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/asdine/storm"
	"github.com/btcsuite/btcutil/base58"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/fbsobreira/gotron-sdk/pkg/address"
	"github.com/fbsobreira/gotron-sdk/pkg/client"
	"github.com/fbsobreira/gotron-sdk/pkg/proto/api"
	"github.com/fbsobreira/gotron-sdk/pkg/proto/core"
	"github.com/gogo/protobuf/proto"
	"github.com/panjf2000/ants/v2"
	"github.com/zhengjianfeng1103/tronscan-usdt/timer"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"math/big"
	"sync"
	"time"
)

const (
	blockFile        = "block.db"
	txFile           = "tx.db"
	blockChainBucket = "blockChainBucket"
	currentBlockKey  = "currentBlockKey"
	scanTimeInterval = 5 * time.Second
)

type UnScanBlock struct {
	ID     string `storm:"id"`
	Height int64  `json:"height"`
}

type TronScanner struct {
	ContractMap          map[string]string
	RW                   sync.RWMutex
	FullNode             *client.GrpcClient
	BlockDB              *storm.DB
	TxDB                 *storm.DB
	Observers            map[ObserverNotify]bool
	IsScanning           bool
	IsClose              bool
	scanTask             *timer.TaskTimer //扫描定时器
	scanTimeInterval     time.Duration
	RescanLastBlockCount uint64 //重扫上N个区块数量
	AntPools             *ants.Pool
}

func NewTronScanner() *TronScanner {
	contractMap := map[string]string{
		"TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t": "USDT",
	}

	grpcClient := client.NewGrpcClient("")
	err := grpcClient.Start(grpc.WithInsecure())
	if err != nil {
		panic("start grpc err: " + err.Error())
	}

	p, err := ants.NewPool(10000)
	if err != nil {
		panic("init pools err: " + err.Error())
	}

	blockDB, err := storm.Open(blockFile)
	if err != nil {
		panic("open storm file err: " + err.Error())
	}

	txDB, err := storm.Open(txFile)
	if err != nil {
		panic("open storm file err: " + err.Error())
	}

	obs := make(map[ObserverNotify]bool, 0)

	return &TronScanner{
		ContractMap:          contractMap,
		RW:                   sync.RWMutex{},
		FullNode:             grpcClient,
		Observers:            obs,
		BlockDB:              blockDB,
		TxDB:                 txDB,
		IsScanning:           false,
		scanTimeInterval:     scanTimeInterval,
		RescanLastBlockCount: 0, //重扫上N个区块数量
		AntPools:             p,
	}
}

func (ts *TronScanner) Start() {
	if ts.IsScanning {
		zap.L().Info("tran scanner is starting")
		return
	}

	timerTask := timer.NewTask(ts.scanTimeInterval, ts.Task)
	ts.IsScanning = true
	ts.scanTask = timerTask
	timerTask.Start()
}

func (ts *TronScanner) Stop() {
	if !ts.IsScanning {
		zap.L().Info("tran scanner is stoping")
		return
	}
	ts.IsScanning = false
	ts.scanTask.Stop()
}

func (ts *TronScanner) UnRegister(observer ObserverNotify) {
	ts.RW.Lock()
	defer ts.RW.Unlock()

	if observer == nil {
		return
	}

	if _, ok := ts.Observers[observer]; !ok {
		return
	}

	delete(ts.Observers, observer)
}

func (ts *TronScanner) RegisterObservers(observer ObserverNotify) {
	ts.RW.Lock()
	defer ts.RW.Unlock()

	if observer == nil {
		return
	}

	if _, ok := ts.Observers[observer]; ok {
		return
	}

	ts.Observers[observer] = true
}

func (ts *TronScanner) ReceiveNotify(tr Trc20Result) {

	var trSample Trc20ResultSimple
	err := ts.TxDB.One("TxHash", tr.TxHash, &trSample)
	switch err {
	case nil:
		zap.L().Info("trc20ResultSample hash already notify", zap.Any("txHash", trSample.TxHash))

	case storm.ErrNotFound:

		trSample = Trc20ResultSimple{TxHash: tr.TxHash}
		err = ts.TxDB.Save(&trSample)

		if err != nil {
			zap.L().Error("save trc20  result", zap.Error(err))
			return
		}

		for observer, ok := range ts.Observers {
			if !ok {
				return
			}

			err := observer.Notify(tr)
			if err != nil {
				zap.L().Error("notify result to observers", zap.Error(err))
			}
		}
	default:
		zap.L().Error("storm get one trc20 result", zap.Error(err))
	}
}

func (ts *TronScanner) Task() {
	zap.L().Info("Task invoke...")

	var (
		currentHeight int64
	)

	var currentBlock *SimpleBlock
	err := ts.BlockDB.Get(blockChainBucket, currentBlockKey, &currentBlock)
	switch err {
	case nil:
		currentHeight = currentBlock.Height
	case storm.ErrNotFound:
		hBlock, err := ts.FullNode.GetNowBlock()
		if err != nil {
			zap.L().Error("get now block", zap.Error(err))
			return
		}
		currentHeight = hBlock.GetBlockHeader().GetRawData().GetNumber()
	default:
		zap.L().Error("read block from local db", zap.Error(err))
		return
	}

	//扫块
	for {

		//已停止扫块
		if !ts.IsScanning {
			zap.L().Error("stop scanning")
			return
		}

		var maxHeightBlock *api.BlockExtention
		maxHeightBlock, err = ts.FullNode.GetNowBlock()
		if err != nil {
			zap.L().Error("query heightest block ", zap.Error(err))
			return
		}

		maxHeight := maxHeightBlock.GetBlockHeader().GetRawData().GetNumber()

		currentHeight++

		if currentHeight >= maxHeight {
			zap.L().Info("currentHeight >= maxHeight not handle currentHeight++ ", zap.Any("currentHeight++", currentHeight), zap.Any("maxHeight", maxHeight))
			break
		}

		diffBlockNum := maxHeight - currentHeight
		zap.L().Info("init read block", zap.Any("currentHeight", currentHeight), zap.Any("maxHeight", maxHeight), zap.Any("diffBlockNum", diffBlockNum))

		//并发追高
		if diffBlockNum > 2 {
			zap.L().Info("slowBlock InBatch", zap.Any("StartHeight", currentHeight))

			var batchWait sync.WaitGroup
			var safeCurrentHeight = atomic.NewInt64(currentHeight)

			for i := currentHeight; i < currentHeight+diffBlockNum; i++ {
				batchWait.Add(1)

				//不考虑分叉
				err = ts.AntPools.Submit(func() {
					safeH := safeCurrentHeight.Inc()
					var slowBlock *api.BlockExtention
					slowBlock, err = ts.FullNode.GetBlockByNum(safeH)

					zap.L().Info("in block batch pool", zap.Any("safeCurrentHeight", safeH))
					batchWait.Done()

					if err != nil {
						zap.L().Error("query block by num", zap.Error(err), zap.Any("height", safeH))
						//丢块 等待重新扫
						err = ts.BlockDB.Save(&UnScanBlock{
							Height: safeH,
						})
					} else {
						err = ts.batchExtractTransaction(slowBlock)
						if err != nil {
							zap.L().Error("extract block txs err, may need rescan", zap.Error(err), zap.Any("height", safeH))
							//扫块里交易数据不全 等待重新扫
							err = ts.BlockDB.Save(&UnScanBlock{
								Height: safeH,
							})
						}
					}
				})

				time.Sleep(1 * time.Second)
			}

			batchWait.Wait()

			zap.L().Debug("L", zap.Any("time", time.Now()))
			currentHeight = safeCurrentHeight.Load()

			zap.L().Debug("P", zap.Any("time", time.Now()))

			saveBlock := SimpleBlock{
				Height: currentHeight,
			}
			err = ts.BlockDB.Set(blockChainBucket, currentBlockKey, &saveBlock)
			if err != nil {
				zap.L().Error("save current/unScan block", zap.Error(err))
			}

			zap.L().Debug("H", zap.Any("time", time.Now()))

			zap.L().Info("slowBlock InBatch", zap.Any("EndHeight", currentHeight))

		} else {

			var slowBlock *api.BlockExtention
			slowBlock, err = ts.FullNode.GetBlockByNum(currentHeight)
			if err != nil {
				zap.L().Error("query block by num", zap.Error(err), zap.Any("height", currentHeight))
				//丢块 等待重新扫
				err = ts.BlockDB.Save(&UnScanBlock{
					Height: currentHeight,
				})
				continue
			}

			err = ts.batchExtractTransaction(slowBlock)
			if err != nil {
				zap.L().Error("extract block txs err, may need rescan", zap.Error(err), zap.Any("height", currentHeight))
				//扫块里交易数据不全 等待重新扫
				err = ts.BlockDB.Save(&UnScanBlock{
					Height: currentHeight,
				})
				continue
			}

			if slowBlock.GetBlockHeader().GetRawData().GetNumber() == 0 {
				zap.L().Error("slowBlock height is 0")
				break
			}

			saveBlock := SimpleBlock{
				Height: currentHeight,
			}
			err = ts.BlockDB.Set(blockChainBucket, currentBlockKey, &saveBlock)
			if err != nil {
				zap.L().Error("save current/unScan block", zap.Error(err))
			}

			zap.L().Debug("slowBlock", zap.Any("slowBlockHeight", slowBlock.GetBlockHeader().GetRawData().GetNumber()))
		}

		zap.L().Info("producer status: ", zap.Any("pool", ts.AntPools.Running()))
	}

	//执行重新扫块
	for i := currentHeight; currentHeight-i < int64(ts.RescanLastBlockCount); i-- {
		zap.L().Info("rescan block", zap.Any("height", i))
		block, err := ts.FullNode.GetBlockByNum(i)
		if err != nil {
			zap.L().Error("rescan block", zap.Error(err))
		}
		err = ts.batchExtractTransaction(block)
		if err != nil {
			zap.L().Error("rescan block when batch extract transaction", zap.Error(err))
		}
	}
}

func (ts *TronScanner) batchExtractTransaction(block *api.BlockExtention) error {

	txs := block.Transactions
	if len(txs) <= 0 {
		return nil
	}

	zap.L().Debug("batchExtractTransaction", zap.Any("height", block.GetBlockHeader().GetRawData().GetNumber()), zap.Any("txs length", len(txs)))
	//生产者 提取
	//消费者 消费提取数据
	//结合起来 通知notify

	failedNum := 0
	var shouldDone = len(txs)
	var done = 0
	quit := make(chan struct{})
	producer := make(chan Trc20Result)
	defer func() {
		close(producer)
	}()

	consumer := make(chan Trc20Result)
	defer close(consumer)

	// 3000 笔交易 预计5秒完成
	extractWork := func(mProducer chan Trc20Result) {
		for _, tx := range txs {
			tsCopy := tx
			if !ts.AntPools.IsClosed() {
				err := ts.AntPools.Submit(func() {

					err := ts.extractContract(tsCopy, mProducer)
					if err != nil {
						zap.L().Error("extract contract ", zap.Error(err))
						return
					}
				})
				if err != nil {
					zap.L().Error("submit extract tx func", zap.Error(err))
					failedNum++
				}
			}
		}
	}

	saveWork := func(mConsumer chan Trc20Result) {
		for result := range mConsumer {

			if result.Success {
				go ts.ReceiveNotify(result)
			}

			done++
			if shouldDone == done {
				close(quit)
			}
		}

	}

	go extractWork(producer)
	go saveWork(consumer)
	ts.producerToConsumer(producer, consumer, quit)

	if failedNum > 0 {
		return fmt.Errorf("block scanner extractWork failed")
	} else {
		return nil
	}

}

func (ts *TronScanner) extractContract(tx *api.TransactionExtention, producer chan Trc20Result) error {

	var response Trc20Result
	txHash := common.Bytes2Hex(tx.Txid)
	contracts := tx.GetTransaction().GetRawData().GetContract()
	if len(contracts) > 1 && len(contracts) <= 0 {
		zap.S().Error("contracts len: ", len(contracts), " not support now")
		return errors.New("not support now")
	}
	rets := tx.GetTransaction().GetRet()

	for index, contractInfo := range tx.GetTransaction().GetRawData().GetContract() {
		result := rets[index]
		if result.ContractRet.String() != core.Transaction_ResultContractResult_name[int32(core.Transaction_Result_SUCCESS)] {
			zap.L().Debug("contract ret not success: ", zap.Any("contract ret", result.ContractRet), zap.Any("hash", txHash))
			continue
		}

		switch contractInfo.Type {
		case core.Transaction_Contract_TriggerSmartContract:
			var msg core.TriggerSmartContract
			err := proto.Unmarshal(contractInfo.Parameter.GetValue(), &msg)
			if err != nil {
				zap.S().Error("unmarshal parameter value", err)
				continue
			}

			from := address.HexToAddress(common.Bytes2Hex(msg.OwnerAddress)).String()
			contractAddress := address.HexToAddress(common.Bytes2Hex(msg.ContractAddress)).String()
			zap.L().Debug("parameter: ", zap.Any("contractAddress", contractAddress), zap.Any("ownerAddress", from), zap.Any("hash", txHash))

			var symbol string
			var ok bool
			if symbol, ok = ts.ContractMap[contractAddress]; !ok {
				zap.L().Debug("contractAddress not in contractMap")
				continue
			}

			methodSignature := common.Bytes2Hex(msg.Data[:4])
			sig := crypto.Keccak256Hash([]byte(Trc20TransferMethod.Sig)).Hex()[2:10]

			if methodSignature != sig {
				zap.L().Debug("methodSignature not transfer(address,uint256s)")
				continue
			}

			unpack, err := Trc20TransferMethod.Inputs.Unpack(msg.Data[4:])
			if err != nil {
				zap.S().Error("unpack input", zap.Error(err))
				continue
			}
			to := address.HexToAddress("41" + unpack[0].(ethCommon.Address).Hex()[2:]).String()
			amount := unpack[1].(*big.Int)
			zap.L().Debug("unpack trc20 transfer method", zap.Any("_to", to), zap.Any("_value", amount), zap.Any("hash", txHash))

			response = Trc20Result{
				TxHash:          txHash,
				BlockHash:       common.Bytes2Hex(tx.GetTransaction().GetRawData().RefBlockHash),
				BlockHeight:     tx.GetTransaction().GetRawData().RefBlockNum,
				Amount:          amount,
				ContractAddress: contractAddress,
				From:            from,
				To:              to,
				Symbol:          symbol,
				Success:         true,
			}
		default:
			zap.L().Debug("not supper contract type", zap.Any("type", contractInfo.Type.Enum().String()), zap.Any("hash", txHash))
		}
	}

	producer <- response

	return nil
}

func (ts *TronScanner) SetCurrentBlock(block *SimpleBlock) error {
	return ts.BlockDB.Set(blockChainBucket, currentBlockKey, &block)
}

func (ts *TronScanner) GetCurrentBlock() (*SimpleBlock, error) {
	var block *SimpleBlock
	err := ts.BlockDB.Get(blockChainBucket, currentBlockKey, &block)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (ts *TronScanner) DeleteTxHash(txHash string) error {
	var result Trc20Result
	err := ts.TxDB.One("TxHash", txHash, &result)
	switch err {
	case storm.ErrNotFound:
	case nil:
		err = ts.TxDB.DeleteStruct(&result)
		if err != nil {
			return err
		}
	default:
		return err
	}
	return nil
}

func (ts *TronScanner) GetBucketNames() []string {
	return ts.BlockDB.Bucket()
}

func (ts *TronScanner) GetTxByHash(txHash string) (*Trc20Result, error) {
	var result Trc20Result
	err := ts.TxDB.One("TxHash", txHash, &result)
	switch err {
	case storm.ErrNotFound:
		return nil, err
	case nil:
		return &result, nil
	default:
		return nil, err
	}
}

func (ts *TronScanner) RegisterContractListen(address string, symbol string) error {
	ts.RW.Lock()
	defer ts.RW.Unlock()

	if _, ok := ts.ContractMap[address]; ok {
		return nil
	}

	ts.ContractMap[address] = symbol
	return nil
}

func (ts *TronScanner) GenerateBase58AddressOffline() (address map[string]string, err error) {

	privateKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}
	privateKeyBytes := crypto.FromECDSA(privateKey)

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("public not ecdsa algorithm")
	}
	publicKeyBytes := crypto.FromECDSAPub(publicKeyECDSA)
	zap.S().Debug("publicKey: ", hexutil.Encode(publicKeyBytes)[2:])

	hexAddress := crypto.PubkeyToAddress(*publicKeyECDSA).Hex()
	hexAddress = "41" + hexAddress[2:]

	zap.S().Debug("address hex: ", hexAddress)
	addb, _ := hex.DecodeString(hexAddress)
	hash1 := s256(s256(addb))
	secret := hash1[:4]
	for _, v := range secret {
		addb = append(addb, v)
	}

	address = map[string]string{
		"Address":    base58.Encode(addb),
		"PrivateKey": hexutil.Encode(privateKeyBytes)[2:],
	}
	return address, nil
}

func (ts *TronScanner) producerToConsumer(producer chan Trc20Result, consumer chan Trc20Result, quit chan struct{}) {

	var values = make([]Trc20Result, 0)

	for {
		var activeWorker chan<- Trc20Result
		var activeValue Trc20Result
		if len(values) > 0 {
			//获取顶部1个数据
			activeWorker = consumer
			activeValue = values[0]
		}

		select {
		case data := <-producer:
			values = append(values, data)
		case activeWorker <- activeValue:
			values = values[1:]
		case <-quit:
			return
		}
	}
}

func s256(s []byte) []byte {
	h := sha256.New()
	h.Write(s)
	bs := h.Sum(nil)
	return bs
}

var AddressType, _ = abi.NewType("address", "", nil)
var Uint256, _ = abi.NewType("uint256", "", nil)
var Bool, _ = abi.NewType("bool", "", nil)
var Trc20TransferMethod = abi.NewMethod("transfer", "transfer", abi.Function, "", false, true, abi.Arguments{
	{
		Name: "_to",
		Type: AddressType,
	},
	{
		Name: "_value",
		Type: Uint256,
	},
}, abi.Arguments{{
	Name: "name",
	Type: Bool,
}})

type Trc20ResultSimple struct {
	TxHash string `json:"txHash" storm:"id"`
}

type SimpleBlock struct {
	Height int64 `json:"height" storm:"id"`
	//Hash   string `json:"hash"`
}

type Trc20Result struct {
	TxHash          string   `json:"txHash" storm:"id"`
	BlockHash       string   `json:"blockHash"`
	BlockHeight     int64    `json:"blockHeight"`
	Amount          *big.Int `json:"amount"`
	ContractAddress string   `json:"contractAddress"` //base56
	From            string   `json:"from"`            //owner address base56
	To              string   `json:"to"`              //_to address base56
	Symbol          string   `json:"symbol"`          //
	Success         bool
}

type ObserverNotify interface {
	Notify(result Trc20Result) error
}
