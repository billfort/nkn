package lnode

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/nknorg/nkn/v2/chain/pool"
	"github.com/nknorg/nkn/v2/common"
	"github.com/nknorg/nkn/v2/config"
	"github.com/nknorg/nkn/v2/node"
	"github.com/nknorg/nkn/v2/pb"
	"github.com/nknorg/nkn/v2/transaction"
	"github.com/nknorg/nkn/v2/util"
	"github.com/nknorg/nkn/v2/util/log"
)

// Number of random neighbors to sync txn
const NumNeighborToSync = 1

func (localNode *LocalNode) StartSyncTxnPool() {
	for {
		dur := util.RandDuration(config.ConsensusDuration/2, 0.4)
		time.Sleep(dur)
		localNode.SyncTxnPool()
	}
}

// sync txn pool process
func (localNode *LocalNode) SyncTxnPool() {

	log.Info("Begin sync txn now.........")

	neighborNodes := localNode.GetNeighbors(nil)
	neighborCount := len(neighborNodes)

	if neighborCount <= 0 { // don't have any neighbor yet
		return
	}

	mapNeighbors := make(map[string]*node.RemoteNode)

	if NumNeighborToSync > neighborCount { // not enough neighbors, select all of them
		for _, nb := range neighborNodes {
			mapNeighbors[nb.Addr] = nb
		}
	} else { // choose random neighbors

		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < NumNeighborToSync; i++ {
			j := r.Intn(neighborCount) // return index [0, neighborCount)

			// check if this neighbor has been added or not.
			_, ok := mapNeighbors[neighborNodes[j].Addr]
			if ok {
				continue
			}

			mapNeighbors[neighborNodes[j].Addr] = neighborNodes[j]

		}
	}

	myHash, _ := localNode.TxnPool.GetXorHashAndCount()

	maxCount := uint32(0)
	var neighborToSync *node.RemoteNode // the neighbor which is sync txn target
	var wg sync.WaitGroup

	for _, neighbor := range mapNeighbors {
		wg.Add(1)

		go func(nb *node.RemoteNode) {
			defer wg.Done()

			replyMsg, err := localNode.sendReqTxnPoolHash(nb)
			if err != nil {
				log.Warningf("sendReqTxnPoolHash neighbor %v error: %v", nb.GetAddr(), err)
				return
			}
			log.Infof("Get RplTxnPoolHash, neighbor's txn count is: %v, hash is: %v, my hash: %v ",
				replyMsg.TxnCount, hex.EncodeToString(replyMsg.PoolHash)[:8], hex.EncodeToString(myHash)[:8])

			if !bytes.Equal(replyMsg.PoolHash, myHash) {
				if replyMsg.TxnCount > maxCount { // Choose the neighbor which has most txn count in txn pool
					neighborToSync = nb
				}
			}
		}(neighbor)

	}

	wg.Wait()

	// request for syncing
	if neighborToSync != nil {
		_, err := localNode.reqNeighborSyncTxnPool(neighborToSync)
		if err != nil {
			log.Error("req sync txn pool error: ", err)
		}
	}
}

// send neighbor a REQ_TXN_POOL_HASH request message, and wait for reply.
func (localNode *LocalNode) sendReqTxnPoolHash(remoteNode *node.RemoteNode) (*pb.RplTxnPoolHash, error) {

	msg, err := NewReqTxnPoolHashMessage()
	if err != nil {
		return nil, err
	}

	buf, err := localNode.SerializeMessage(msg, false)
	if err != nil {
		return nil, err
	}

	replyBytes, err := remoteNode.SendBytesSyncWithTimeout(buf, syncReplyTimeout)
	if err != nil {
		return nil, err
	}

	replyMsg := &pb.RplTxnPoolHash{}
	err = proto.Unmarshal(replyBytes, replyMsg)
	if err != nil {
		return nil, err
	}

	return replyMsg, nil
}

// build a REQ_TXN_POOL_HASH request message
func NewReqTxnPoolHashMessage() (*pb.UnsignedMessage, error) {

	msg := &pb.UnsignedMessage{
		MessageType: pb.MessageType_REQ_TXN_POOL_HASH,
	}

	return msg, nil

}

// build RPL_TXN_POOL_HASH reply message, reply with my txn pool's hash and txn count.
func NewRplTxnPoolHashMessage(tp *pool.TxnPool) (*pb.UnsignedMessage, error) {

	poolHash, txnCount := tp.GetXorHashAndCount()

	msgBody := &pb.RplTxnPoolHash{
		TxnCount: (uint32)(txnCount),
		PoolHash: poolHash,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &pb.UnsignedMessage{
		MessageType: pb.MessageType_RPL_TXN_POOL_HASH,
		Message:     buf,
	}

	return msg, nil
}

// build REQ_SYNC_TXN_POOL request message. This message includse my txn pool's txn address and maximum nonce.
func NewReqSyncTxnPoolMessage(tp *pool.TxnPool) (*pb.UnsignedMessage, error) {

	addrNonceList := make([]*pb.AddrNonce, 0)
	mapAddrNonce := make(map[common.Uint160]uint64)

	// remove time over txn first
	tp.RemoveTimeoverTxnWithTime()

	myTxnList := tp.GetMapTxnWithTime()

	myTxnList.Range(func(k, v interface{}) bool {
		txnWithTime := v.(pool.TxnWithTime)
		sender, err := txnWithTime.Txn.GetProgramHashes()
		if err != nil {
			return true
		}
		nonce, _ := mapAddrNonce[sender[0]]
		if txnWithTime.Txn.UnsignedTx.Nonce > nonce {
			mapAddrNonce[sender[0]] = txnWithTime.Txn.UnsignedTx.Nonce
		}
		return true
	})

	for addr, nonce := range mapAddrNonce {
		addrNonceList = append(addrNonceList, &pb.AddrNonce{
			Address: addr.ToArray(),
			Nonce:   nonce,
		})
	}

	// get the duration since last sync txn time.
	duration := time.Now().UnixMilli() - tp.GetSyncTime()
	if tp.GetSyncTime() == 0 { // never synced before
		duration = 0
	}
	msgBody := &pb.ReqSyncTxnPool{Duration: duration, AddrNonce: addrNonceList}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &pb.UnsignedMessage{
		MessageType: pb.MessageType_REQ_SYNC_TXN_POOL,
		Message:     buf,
	}

	return msg, nil
}

// build RPL_SYNC_TXN_POOL message. reply transactions which are needed to sync to the requestor.
func NewRplSyncTxnPoolMessage(reqMsg *pb.ReqSyncTxnPool, tp *pool.TxnPool) (*pb.UnsignedMessage, error) {

	mapReqAddrNonce := make(map[common.Uint160]uint64)

	// if there is address and nonce in the request message, save them in a map for better searching.
	if len(reqMsg.AddrNonce) > 0 {
		for _, addrNonce := range reqMsg.AddrNonce {
			mapReqAddrNonce[common.BytesToUint160(addrNonce.Address)] = addrNonce.Nonce
		}
	}

	var respTxnList []*pb.TxnWithDuration
	var err error

	if reqMsg.Duration > 0 {
		respTxnList, err = syncInDuration(tp, mapReqAddrNonce, reqMsg.Duration)
	} else {
		respTxnList, err = syncFully(tp, mapReqAddrNonce)
	}
	if err != nil {
		return nil, err
	}

	msgBody := &pb.RplSyncTxn{Transactions: respTxnList}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &pb.UnsignedMessage{
		MessageType: pb.MessageType_RPL_SYNC_TXN_POOL,
		Message:     buf,
	}

	return msg, nil
}

// Only sync txn in the last duration seconds, partially of the txn pool
func syncInDuration(tp *pool.TxnPool, mapReqAddrNonce map[common.Uint160]uint64, duration int64) ([]*pb.TxnWithDuration, error) {
	respTxnList := make([]*pb.TxnWithDuration, 0) // response txn list buffer

	iNow := time.Now().UnixMilli()
	earliest := iNow - duration - 1000 // push 1 second ahead
	myTxnList := tp.GetMapTxnWithTime()

	now := time.Now().UnixMilli()
	myTxnList.Range(func(k, v interface{}) bool {
		txnWithTime := v.(pool.TxnWithTime)
		if txnWithTime.ArriveTime < earliest { // skip too old txn
			return true
		}

		sender, err := txnWithTime.Txn.GetProgramHashes()
		if err != nil {
			return true
		}
		addr := sender[0]

		nonce, ok := mapReqAddrNonce[addr]
		if ok { // if addr is in request list, we append bigger nonce txns to response list

			if txnWithTime.Txn.UnsignedTx.Nonce > nonce {
				respTxnList = append(respTxnList, &pb.TxnWithDuration{
					Txn: &pb.Transaction{
						UnsignedTx: txnWithTime.Txn.UnsignedTx,
						Programs:   txnWithTime.Txn.Programs,
					},
					Duration: now - txnWithTime.ArriveTime,
				})
			}

		} else { // if the address is not in request list, we append it to response list

			respTxnList = append(respTxnList, &pb.TxnWithDuration{
				Txn: &pb.Transaction{
					UnsignedTx: txnWithTime.Txn.UnsignedTx,
					Programs:   txnWithTime.Txn.Programs,
				},
				Duration: now - txnWithTime.ArriveTime,
			})
		}

		return true
	})

	return respTxnList, nil
}

// sync all my txns in the pool to the neighbor when the neighbor just start.
func syncFully(tp *pool.TxnPool, mapReqAddrNonce map[common.Uint160]uint64) ([]*pb.TxnWithDuration, error) {

	rplTxnList := make([]*pb.TxnWithDuration, 0) // response txn list buffer
	mapMyTxnList := tp.GetAllTransactionLists()

	for addr, myTxnList := range mapMyTxnList {
		nonce, ok := mapReqAddrNonce[addr]
		if ok { // if addr is in request list, we append bigger nonce txns to response list
			for _, txn := range myTxnList {
				if txn.UnsignedTx.Nonce > nonce {
					rplTxnList = append(rplTxnList, &pb.TxnWithDuration{
						Txn: &pb.Transaction{
							UnsignedTx: txn.UnsignedTx,
							Programs:   txn.Programs,
						},
					})
				}
			}
		} else { // if the address is not in request list, we append it to response list
			for _, txn := range myTxnList {
				rplTxnList = append(rplTxnList, &pb.TxnWithDuration{
					Txn: &pb.Transaction{
						UnsignedTx: txn.UnsignedTx,
						Programs:   txn.Programs,
					},
				})
			}
		}
	}

	log.Info("Sync txn, send ", len(rplTxnList), " txns to neighbor")

	return rplTxnList, nil
}

// handle a REQ_TXN_POOL_HASH request message, return a RPL_TXN_POOL_HASH response message buffer.
func (localNode *LocalNode) reqTxnPoolHashHandler(remoteMessage *node.RemoteMessage) ([]byte, bool, error) {

	replyMsg, err := NewRplTxnPoolHashMessage(localNode.TxnPool)
	if err != nil {
		return nil, false, err
	}

	replyBuf, err := localNode.SerializeMessage(replyMsg, false)
	return replyBuf, false, err
}

// send to a neighbor a REQ_SYNC_TXN_POOL request message, and wait for reply with timeout
// return the reply messge RPL_SYNC_TXN_POOL
func (localNode *LocalNode) reqNeighborSyncTxnPool(remoteNode *node.RemoteNode) (*pb.RplSyncTxn, error) {

	msg, err := NewReqSyncTxnPoolMessage(localNode.TxnPool)
	if err != nil {
		return nil, err
	}

	buf, err := localNode.SerializeMessage(msg, false)
	if err != nil {
		return nil, err
	}

	replyBytes, err := remoteNode.SendBytesSyncWithTimeout(buf, syncReplyTimeout)
	if err != nil {
		return nil, err
	}

	// get RSP_SYNC_TXN_POOL message, update my txn pool
	replyMsg := &pb.RplSyncTxn{}
	err = proto.Unmarshal(replyBytes, replyMsg)
	if err != nil {
		return nil, err
	}

	// sort it by nonce
	sort.Slice(replyMsg.Transactions, func(i, j int) bool {
		return replyMsg.Transactions[i].Txn.UnsignedTx.Nonce < replyMsg.Transactions[j].Txn.UnsignedTx.Nonce
	})

	for _, txnWithDur := range replyMsg.Transactions {
		pbTxn := txnWithDur.Txn
		txn := &transaction.Transaction{
			Transaction: pbTxn,
		}

		localNode.TxnPool.AppendTxnPool(txn)

		if txnWithDur.Duration > 0 {
			localNode.TxnPool.SetTxnTime(txn.Hash(), txnWithDur.Duration)
		}
	}

	log.Info("Sync txn, get ", len(replyMsg.Transactions), "txns from neighbor")

	// after update my txn pool, reset sync time.
	localNode.TxnPool.SetSyncTime()

	return replyMsg, nil
}

// Handle REQ_SYNC_TXN_POOL request message, return RPL_SYNC_TXN_POOL response message buffer.
func (localNode *LocalNode) reqSyncTxnPoolHandler(remoteMessage *node.RemoteMessage) ([]byte, bool, error) {

	reqMsg := &pb.ReqSyncTxnPool{}
	err := proto.Unmarshal(remoteMessage.Message, reqMsg)
	if err != nil {
		return nil, false, err
	}

	replyMsg, err := NewRplSyncTxnPoolMessage(reqMsg, localNode.TxnPool)
	if err != nil {
		return nil, false, err
	}

	replyBuf, err := localNode.SerializeMessage(replyMsg, false)
	return replyBuf, false, err
}
