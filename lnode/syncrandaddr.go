package lnode

import (
	"fmt"
	"math/rand"
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

// Number of random neighbors to sync rand addr
const NumNbToSyncRandAddr = 1

func (localNode *LocalNode) StartSyncRandAddrTxn() {
	for {
		dur := util.RandDuration(config.ConsensusDuration/2, 0.4)
		time.Sleep(dur)
		localNode.SyncRandAddrTxn()
	}
}

// sync rand addr process
func (localNode *LocalNode) SyncRandAddrTxn() {

	log.Info("Begin sync rand addr now.........")

	neighborNodes := localNode.GetNeighbors(nil)
	neighborCount := len(neighborNodes)
	if neighborCount <= 0 { // don't have neighbor yet
		return
	}

	mapNeighbors := make(map[string]*node.RemoteNode)

	if NumNbToSyncRandAddr > neighborCount { // not enough neighbors, select all of them
		for _, nb := range neighborNodes {
			mapNeighbors[nb.Addr] = nb
		}
	} else {
		// choose rand neighbors
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < NumNbToSyncRandAddr; i++ {
			j := r.Intn(neighborCount) // return index [0, neighborCount)

			// check if this neighbor has been added or not.
			_, ok := mapNeighbors[neighborNodes[j].Addr]
			if ok {
				continue
			}

			mapNeighbors[neighborNodes[j].Addr] = neighborNodes[j]
		}
	}

	for _, neighbor := range mapNeighbors {

		go func(nb *node.RemoteNode) {

			replyMsg, err := localNode.sendReqAddrNonce(nb)
			if err != nil {
				log.Warningf("sendReqAddrNonce neighbor %v error: %v\n", nb.GetAddr(), err)
				return
			}

			var addr160 common.Uint160
			addr160.SetBytes(replyMsg.Address)
			myNonce, err := localNode.TxnPool.GetNonceByTxnPool(addr160)
			if err != nil {
				log.Info("localNode.TxnPool.GetNonceByTxnPool err: ", err)
			} else {
				myNonce = myNonce - 1
			}

			if myNonce < replyMsg.Nonce {
				localNode.sendReqSyncAddrTxn(nb, replyMsg.Address, myNonce)
			}

		}(neighbor)
	}

}

// send neighbor a REQ_ADDR_NONCE request message, and wait for reply.
func (localNode *LocalNode) sendReqAddrNonce(remoteNode *node.RemoteNode) (*pb.AddrNonce, error) {

	msg, err := NewReqAddrNonceMessage()
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

	replyMsg := &pb.AddrNonce{}
	err = proto.Unmarshal(replyBytes, replyMsg)
	if err != nil {
		return nil, err
	}

	return replyMsg, nil
}

// send to a neighbor a REQ_SYNC_ADDR_TXN request message, and wait for reply with timeout
// return the reply messge RPL_SYNC_ADDR_TXN
func (localNode *LocalNode) sendReqSyncAddrTxn(remoteNode *node.RemoteNode, addr []byte, nonce uint64) (*pb.RplSyncTxn, error) {

	msg, err := NewReqSyncAddrTxnMessage(addr, nonce)
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

	// Handle RSP_SYNC_ADDR_TXN message, update my txn pool
	replyMsg := &pb.RplSyncTxn{}
	err = proto.Unmarshal(replyBytes, replyMsg)
	if err != nil {
		return nil, err
	}

	for _, txnWithDur := range replyMsg.Transactions {

		txn := &transaction.Transaction{
			Transaction: txnWithDur.Txn,
		}

		localNode.TxnPool.AppendTxnPool(txn)

		localNode.TxnPool.SetTxnTime(txn.Hash(), txnWithDur.Duration)

	}

	log.Info("Sync txn by random address, get ", len(replyMsg.Transactions), "txns from neighbor")

	return replyMsg, nil
}

// build a REQ_ADDR_NONCE request message
func NewReqAddrNonceMessage() (*pb.UnsignedMessage, error) {

	msg := &pb.UnsignedMessage{
		MessageType: pb.MessageType_REQ_ADDR_NONCE,
	}

	return msg, nil

}

// build RPL_ADDR_NONCE reply message
// reply with rand address my txn pool's with this address' nonce.
func NewRplAddrNonceMessage(tp *pool.TxnPool) (*pb.UnsignedMessage, error) {
	addrList := tp.GetAddressList()
	addrCount := len(addrList)
	if addrCount <= 0 {
		return nil, fmt.Errorf("no transaction in my pool")
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rn := r.Intn(addrCount)

	i := 0
	var addr common.Uint160
	var nonce uint64
	for adr, _ := range addrList {
		if i == rn {
			addr = adr
			n, err := tp.GetNonceByTxnPool(adr) // here get expected nonce of next txn
			if err != nil {
				return nil, err
			}
			nonce = n - 1
		} else {
			i++
		}
	}

	msgBody := &pb.AddrNonce{
		Address: addr.ToArray(),
		Nonce:   nonce,
	}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &pb.UnsignedMessage{
		MessageType: pb.MessageType_RPL_ADDR_NONCE,
		Message:     buf,
	}

	return msg, nil
}

// build REQ_SYNC_ADDR_TXN request message
// This message includse my txn pool's txn address and maximum nonce.
func NewReqSyncAddrTxnMessage(addr []byte, nonce uint64) (*pb.UnsignedMessage, error) {

	msgBody := &pb.AddrNonce{Address: addr, Nonce: nonce}

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &pb.UnsignedMessage{
		MessageType: pb.MessageType_REQ_SYNC_ADDR_TXN,
		Message:     buf,
	}

	return msg, nil

}

// build RPL_SYNC_ADDR_TXN message. reply transactions which are needed to sync to the requestor.
func NewRplSyncAddrTxnMessage(reqMsg *pb.AddrNonce, tp *pool.TxnPool) (*pb.UnsignedMessage, error) {

	var addr160 common.Uint160
	addr160.SetBytes(reqMsg.Address)

	list := tp.GetAllTransactionsBySender(addr160)

	i := 0
	for i = 0; i < len(list); i++ {
		if list[i].UnsignedTx.Nonce > reqMsg.Nonce {
			break
		}
	}

	var respTxnList []*pb.TxnWithDuration
	for ; i < len(list); i++ {
		respTxnList = append(respTxnList, &pb.TxnWithDuration{
			Txn: &pb.Transaction{
				UnsignedTx: list[i].UnsignedTx,
				Programs:   list[i].Programs,
			},
			Duration: 0,
		})
	}

	msgBody := &pb.RplSyncTxn{Transactions: respTxnList}

	log.Info("Sync txn by rand address, send ", len(respTxnList), " txns to neighbor")

	buf, err := proto.Marshal(msgBody)
	if err != nil {
		return nil, err
	}

	msg := &pb.UnsignedMessage{
		MessageType: pb.MessageType_RPL_SYNC_ADDR_TXN,
		Message:     buf,
	}

	return msg, nil
}

// handle a REQ_ADDR_NONCE request message. return a RPL_ADDR_NONCE response message buffer.
func (localNode *LocalNode) reqAddrNonceHandler(remoteMessage *node.RemoteMessage) ([]byte, bool, error) {

	msgBody := &pb.AddrNonce{}
	err := proto.Unmarshal(remoteMessage.Message, msgBody)
	if err != nil {
		return nil, false, err
	}

	replyMsg, err := NewRplAddrNonceMessage(localNode.TxnPool)
	if err != nil {
		return nil, false, err
	}

	replyBuf, err := localNode.SerializeMessage(replyMsg, false)
	return replyBuf, false, err
}

// Handle REQ_SYNC_ADDR_TXN request message, return RPL_SYNC_ADDR_TXN response message buffer.
func (localNode *LocalNode) reqSyncAddrTxnHandler(remoteMessage *node.RemoteMessage) ([]byte, bool, error) {

	reqMsg := &pb.AddrNonce{}
	err := proto.Unmarshal(remoteMessage.Message, reqMsg)
	if err != nil {
		return nil, false, err
	}

	replyMsg, err := NewRplSyncAddrTxnMessage(reqMsg, localNode.TxnPool)
	if err != nil {
		return nil, false, err
	}

	replyBuf, err := localNode.SerializeMessage(replyMsg, false)
	return replyBuf, false, err
}
