package impl

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	"github.com/zhengjianfeng1103/tronscan-usdt/timer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"math/big"
	"sync"
	"time"
)

const (
	blockFile        = "block.db"
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
	DB                   *storm.DB
	Observers            map[ObserverNotify]bool
	IsScanning           bool
	IsClose              bool
	Producer             chan interface{}
	scanTask             *timer.TaskTimer //扫描定时器
	scanTimeInterval     time.Duration
	RescanLastBlockCount uint64 //重扫上N个区块数量
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

	producer := make(chan interface{}, 100)

	db, err := storm.Open(blockFile)
	if err != nil {
		panic("open storm file err: " + err.Error())
	}

	obs := make(map[ObserverNotify]bool, 0)

	return &TronScanner{
		ContractMap:          contractMap,
		RW:                   sync.RWMutex{},
		FullNode:             grpcClient,
		Producer:             producer,
		Observers:            obs,
		DB:                   db,
		IsScanning:           false,
		scanTimeInterval:     scanTimeInterval,
		RescanLastBlockCount: 0, //重扫上N个区块数量

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

	go ts.ReceiveNotify()
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

func (ts *TronScanner) ReceiveNotify() {
	for {
		select {
		case result := <-ts.Producer:

			tr, ok := result.(Trc20Result)
			if !ok {
				return
			}

			var trc20Result Trc20Result
			err := ts.DB.One("TxHash", tr.TxHash, &trc20Result)
			switch err {
			case nil:
				zap.L().Info("trc20Result hash already notify", zap.Any("txHash", tr.TxHash))

			case storm.ErrNotFound:
				err = ts.DB.Save(&tr)
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
	}
}

func (ts *TronScanner) Task() {
	zap.L().Info("Task invoke...")

	var (
		currentHeight int64
		currentHash   string
	)

	var currentBlock *api.BlockExtention
	err := ts.DB.Get(blockChainBucket, currentBlockKey, &currentBlock)
	switch err {
	case nil:
		currentHeight = currentBlock.GetBlockHeader().GetRawData().GetNumber()
		currentHash = common.Bytes2Hex(currentBlock.GetBlockid())
	case storm.ErrNotFound:
		currentBlock, err = ts.FullNode.GetNowBlock()
		currentHeight = currentBlock.GetBlockHeader().GetRawData().GetNumber()
		currentHash = common.Bytes2Hex(currentBlock.GetBlockid())

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

		zap.L().Info("init read block", zap.Any("currentHeight", currentHeight), zap.Any("maxHeight", maxHeight))

		var slowBlock *api.BlockExtention
		slowBlock, err = ts.FullNode.GetBlockByNum(currentHeight)
		if err != nil {
			zap.L().Error("query block by num", zap.Error(err), zap.Any("height", currentHeight))
			//丢块 等待重新扫
			err = ts.DB.Save(&UnScanBlock{
				Height: currentHeight,
			})
			continue
		}

		parentHash := common.Bytes2Hex(slowBlock.GetBlockHeader().GetRawData().GetParentHash())
		hash := common.Bytes2Hex(slowBlock.GetBlockid())

		if currentHash != parentHash {
			zap.L().Error("可能分叉了 hash info", zap.Any("parentHash", parentHash), zap.Any("currentHash", currentHash), zap.Any("hash", hash))
			break
		}

		err = ts.batchExtractTransaction(slowBlock)
		if err != nil {
			zap.L().Error("extract block txs err, may need rescan", zap.Error(err), zap.Any("height", currentHeight))
			//扫块里交易数据不全 等待重新扫
			err = ts.DB.Save(&UnScanBlock{
				Height: currentHeight,
			})
			continue
		}

		if slowBlock.GetBlockHeader().GetRawData().GetNumber() == 0 {
			zap.L().Error("slowBlock height is 0")
			break
		}

		err = ts.DB.Set(blockChainBucket, currentBlockKey, &slowBlock)
		if err != nil {
			zap.L().Error("save current/unScan block", zap.Error(err))
		}
		currentHash = hash

		zap.L().Debug("slowBlock", zap.Any("slowBlockHeight", slowBlock.GetBlockHeader().GetRawData().GetNumber()))
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

	for _, tx := range txs {
		err := ts.extractContract(tx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ts *TronScanner) extractContract(tx *api.TransactionExtention) error {

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

			zap.L().Debug("method ", zap.Any("methodSignature", methodSignature), zap.Any("sig", sig))
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

			ts.Producer <- Trc20Result{
				TxHash:          txHash,
				BlockHash:       common.Bytes2Hex(tx.GetTransaction().GetRawData().RefBlockHash),
				BlockHeight:     tx.GetTransaction().GetRawData().RefBlockNum,
				Amount:          amount,
				ContractAddress: contractAddress,
				From:            from,
				To:              to,
				Symbol:          symbol,
			}
		default:
			zap.L().Debug("not supper contract type", zap.Any("type", contractInfo.Type.Enum().String()), zap.Any("hash", txHash))
		}
	}

	return nil
}

func (ts *TronScanner) SetCurrentBlock(block *api.BlockExtention) error {
	return ts.DB.Set(blockChainBucket, currentBlockKey, &block)
}

func (ts *TronScanner) GetCurrentBlock() (*api.BlockExtention, error) {
	var block *api.BlockExtention
	err := ts.DB.Get(blockChainBucket, currentBlockKey, &block)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (ts *TronScanner) DeleteTxHash(txHash string) error {
	var result Trc20Result
	err := ts.DB.One("TxHash", txHash, &result)
	switch err {
	case storm.ErrNotFound:
	case nil:
		err = ts.DB.DeleteStruct(&result)
		if err != nil {
			return err
		}
	default:
		return err
	}
	return nil
}

func (ts *TronScanner) GetBucketNames() []string {
	return ts.DB.Bucket()
}

func (ts *TronScanner) GetTxByHash(txHash string) (*Trc20Result, error) {
	var result Trc20Result
	err := ts.DB.One("TxHash", txHash, &result)
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

type Trc20Result struct {
	TxHash          string   `json:"txHash" storm:"id"`
	BlockHash       string   `json:"blockHash"`
	BlockHeight     int64    `json:"blockHeight"`
	Amount          *big.Int `json:"amount"`
	ContractAddress string   `json:"contractAddress"` //base56
	From            string   `json:"from"`            //owner address base56
	To              string   `json:"to"`              //_to address base56
	Symbol          string   `json:"symbol"`          //
}

type ObserverNotify interface {
	Notify(result Trc20Result) error
}
