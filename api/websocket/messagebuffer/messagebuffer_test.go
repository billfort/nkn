package messagebuffer

import (
	"os"
	"testing"

	"github.com/nknorg/nkn/v2/pb"
	"github.com/stretchr/testify/require"
)

var clientId [][]byte = [][]byte{[]byte{byte(0x01)}, []byte{byte(0x02)}, []byte{byte(0x03)}, []byte{byte(0x04)}}

var msg *pb.Relay = &pb.Relay{SrcIdentifier: "from", DestId: []byte("to"), Payload: []byte("payload"),
	MaxHoldingSeconds: 10}

var msgBuffer *MessageBuffer

func createTestData() {
	msgBuffer = NewMessageBuffer()

	msgBuffer.AddMessage(clientId[1], msg)
	msgBuffer.AddMessage(clientId[0], msg)
	msgBuffer.AddMessage(clientId[2], msg)
	msgBuffer.AddMessage(clientId[1], msg)
	msgBuffer.AddMessage(clientId[0], msg)
	msgBuffer.AddMessage(clientId[1], msg)
	msgBuffer.AddMessage(clientId[3], msg)
	msgBuffer.AddMessage(clientId[1], msg)

	printTree(os.Stdout, treeCreate.Root, 0, 'M')
}

func TestAddMessage(t *testing.T) {
	createTestData()

	t.Log("totalBufSize before add new message: ", msgBuffer.totalBufSize)
	sizeCreate := treeCreate.Size()
	sizeExpire := treeExpire.Size()

	msgBuffer.AddMessage(clientId[2], msg)

	t.Log("totalBufSize after add new message: ", msgBuffer.totalBufSize)
	require.Equal(t, sizeCreate+1, treeCreate.Size())
	require.Equal(t, sizeExpire+1, treeExpire.Size())

	printTree(os.Stdout, treeCreate.Root, 0, 'M')

}

func TestRemoveOldest(t *testing.T) {
	// createTestData()
	t.Log("totalBufSize before removeOldest new message: ", msgBuffer.totalBufSize)
	sizeCreate := treeCreate.Size()
	sizeExpire := treeExpire.Size()

	msgBuffer.removeOldest()

	t.Log("totalBufSize after removeOldest new message: ", msgBuffer.totalBufSize)
	require.Equal(t, sizeCreate-1, treeCreate.Size())
	require.Equal(t, sizeExpire-1, treeExpire.Size())

	printTree(os.Stdout, treeCreate.Root, 0, 'M')
}

func TestPopMessages(t *testing.T) {
	// createTestData()
	printTree(os.Stdout, treeCreate.Root, 0, 'M')
	t.Logf("before pop clientId %v message, totalBufSize is: %v \n", clientId[0], msgBuffer.totalBufSize)

	msgBuffer.PopMessages(clientId[0])

	t.Logf("after pop clientId %v message, totalBufSize is: %v \n", clientId[0], msgBuffer.totalBufSize)

	msgBuffer.AddMessage(clientId[0], msg)

	printTree(os.Stdout, treeCreate.Root, 0, 'M')
}

func TestRemoveExpired(t *testing.T) {
	// createTestData()
	msgBuffer.removeExpired()
	t.Log("after removeExpired message, totalBufSize is: ", msgBuffer.totalBufSize)
	printTree(os.Stdout, treeCreate.Root, 0, 'M')
}

func BenchmarkAddMessage(b *testing.B) {

	for i := 0; i < b.N; i++ {
		msgBuffer.AddMessage(clientId[0], msg)
	}
}

func BenchmarkPopMessage(b *testing.B) {

	for i := 0; i < b.N; i++ {
		// msgBuffer.AddMessage(clientId[0], msg)
		msgBuffer.PopMessages(clientId[0])
	}
}

func BenchmarkRemoveExpired(b *testing.B) {

	for i := 0; i < b.N; i++ {
		// msgBuffer.AddMessage(clientId[0], msg)
		msgBuffer.removeExpired()
	}
}

func BenchmarkRemoveOldest(b *testing.B) {

	for i := 0; i < b.N; i++ {
		// msgBuffer.AddMessage(clientId[0], msg)
		msgBuffer.removeOldest()
	}
}
