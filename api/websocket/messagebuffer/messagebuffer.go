package messagebuffer

import (
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/nknorg/nkn/v2/pb"
)

const MaxClientMsgBufSize = 1024 * 1024 * 1024 // 1M
const MaxSeqId int64 = 1000000

var seqId int64

func getNextSeq() int64 {

	atomic.AddInt64(&seqId, 1)

	if seqId >= MaxSeqId {
		atomic.StoreInt64(&seqId, 0)
	}

	return seqId
}

// msg node with create time and expire time
type msgNode struct {
	msg      *pb.Relay
	createId int64
	expireId int64
}

func (m *msgNode) GetSize() int {
	// m.createId(int64), m.expireId(int64), m.msg.MaxHoldingSeconds(int32), m.msg.SigChainLen(int32)
	return 8 + 8 + 4 + 4 + len(m.msg.SrcIdentifier) + len(m.msg.SrcPubkey) + len(m.msg.DestId) +
		len(m.msg.Payload) + len(m.msg.BlockHash) + len(m.msg.LastHash)
}

// MessageBuffer is the buffer to hold message for clients not online
type MessageBuffer struct {
	sync.Mutex
	buffer       map[string][]msgNode // map ClinetId to msg
	totalBufSize int                  // the buffer size in bytes
}

var treeCreate *rbt.Tree // index tree of create time
var treeExpire *rbt.Tree // index tree of expire time

// comparation function of red and black tree
func compare(a, b interface{}) int {
	aInt := a.(int64)
	bInt := b.(int64)
	if aInt < bInt {
		return -1
	} else if aInt == bInt {
		return 0
	} else {
		return 1
	}
}

// NewMessageBuffer creates a MessageBuffer
func NewMessageBuffer() *MessageBuffer {
	treeCreate = rbt.NewWith(compare)
	treeExpire = rbt.NewWith(compare)

	msgBuffer := &MessageBuffer{
		buffer: make(map[string][]msgNode), // *pb.Relay
	}

	go msgBuffer.BufferMonitor()

	return msgBuffer
}

// AddMessage adds a message to message buffer
func (messageBuffer *MessageBuffer) AddMessage(clientID []byte, msg *pb.Relay) {

	if msg.MaxHoldingSeconds <= 0 { // no need buffer
		return
	}

	now := time.Now().UnixMilli()
	seqId := getNextSeq()
	createId := now*MaxSeqId + seqId
	expireId := now*MaxSeqId + (int64)(msg.MaxHoldingSeconds*1000) + seqId

	msgNode := msgNode{
		msg:      msg,
		createId: createId,
		expireId: expireId,
	}

	clientIDStr := hex.EncodeToString(clientID)

	messageBuffer.Lock()
	defer messageBuffer.Unlock()

	messageBuffer.buffer[clientIDStr] = append(messageBuffer.buffer[clientIDStr], msgNode)
	messageBuffer.totalBufSize += msgNode.GetSize()
	// add it to index tree
	treeCreate.Put(createId, clientIDStr)
	treeExpire.Put(expireId, clientIDStr)

}

// PopMessages reads and clears all messages of a client
func (messageBuffer *MessageBuffer) PopMessages(clientID []byte) []*pb.Relay {
	clientIDStr := hex.EncodeToString(clientID)

	messageBuffer.Lock()
	defer messageBuffer.Unlock()

	messages := messageBuffer.buffer[clientIDStr]
	messageBuffer.buffer[clientIDStr] = nil

	relayMessage := make([]*pb.Relay, len(messages), len(messages))
	for i, msgNode := range messages { // delete them from index tree
		treeCreate.Remove(msgNode.createId)
		treeExpire.Remove(msgNode.expireId)
		messageBuffer.totalBufSize -= msgNode.GetSize()

		relayMessage[i] = msgNode.msg
	}

	return relayMessage
}

func (messageBuffer *MessageBuffer) removeExpired() {
	now := time.Now().UnixMilli() + 1 // plus 1 milli-second
	nowId := now * MaxSeqId

	for minNode := treeExpire.Left(); minNode != nil; minNode = treeExpire.Left() {
		expireId := minNode.Key.(int64)

		if expireId < nowId { // expired
			clientID := minNode.Value.(string)
			messages, ok := messageBuffer.buffer[clientID]

			if ok {
				var i int
				for i = 0; i < len(messages); i++ {
					if messages[i].expireId == expireId {
						break
					}
				}

				if i < len(messages) {
					removeNode := messages[i]
					messages := append(messages[:i], messages[i+1:]...)

					messageBuffer.buffer[clientID] = messages
					treeCreate.Remove(removeNode.createId)
					messageBuffer.totalBufSize -= removeNode.GetSize()
				}
			}

			treeExpire.Remove(expireId)

			// printTree(os.Stdout, treeExpire.Root, 0, 'M')

		} else {
			break
		}
	}

}

// remove oldest message
func (messageBuffer *MessageBuffer) removeOldest() {
	minNode := treeCreate.Left()
	if minNode == nil { // the tree is empty
		return
	}

	createId := minNode.Key.(int64)
	clientID := minNode.Value.(string)
	messages, ok := messageBuffer.buffer[clientID]
	if ok {
		var i int
		for i = 0; i < len(messages); i++ {
			if messages[i].createId == createId {
				break
			}
		}
		if i < len(messages) {
			removeNode := messages[i]
			messages := append(messages[0:i], messages[i:]...)
			messageBuffer.buffer[clientID] = messages
			messageBuffer.totalBufSize -= removeNode.GetSize()

			treeExpire.Remove(removeNode.expireId)
		}
	}

	treeCreate.Remove(createId)
}

// monitor message buffer usage, and remove expired message .
func (messageBuffer *MessageBuffer) BufferMonitor() {
	for {
		time.Sleep(10 * time.Second)
		// delete expired msg
		messageBuffer.removeExpired()
		// still over size
		for messageBuffer.totalBufSize > MaxClientMsgBufSize { // repeat until totalBufSize less than MaxClientMsgBufSize
			messageBuffer.removeOldest()
		}
	}
}

// print out tree in indent way.
func printTree(w io.Writer, node *rbt.Node, ns int, ch rune) {
	if node == nil {
		return
	}

	for i := 0; i < ns; i++ {
		fmt.Fprint(w, " ")
	}
	fmt.Fprintf(w, "%c: %v, clientId: %v\n", ch, node.Key, node.Value)
	printTree(w, node.Left, ns+2, 'L')
	printTree(w, node.Right, ns+2, 'R')
}
