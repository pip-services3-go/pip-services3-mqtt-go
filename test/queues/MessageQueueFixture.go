package test_queues

import (
	"testing"
	"time"

	msgqueues "github.com/pip-services3-go/pip-services3-messaging-go/queues"
	"github.com/stretchr/testify/assert"
)

type MessageQueueFixture struct {
	queue msgqueues.IMessageQueue
}

func NewMessageQueueFixture(queue msgqueues.IMessageQueue) *MessageQueueFixture {
	mqf := MessageQueueFixture{}
	mqf.queue = queue
	return &mqf
}

func (c *MessageQueueFixture) TestSendReceiveMessage(t *testing.T) {
	var envelope1 *msgqueues.MessageEnvelope = msgqueues.NewMessageEnvelope("123", "Test", "Test message")
	var envelope2 *msgqueues.MessageEnvelope

	c.queue.Send("", envelope1)

	count, err := c.queue.ReadMessageCount()
	assert.Nil(t, err)
	assert.True(t, (count > 0))

	result, err := c.queue.Receive("", 10000)
	assert.Nil(t, err)
	envelope2 = result

	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)
}

func (c *MessageQueueFixture) TestReceiveSendMessage(t *testing.T) {
	var envelope1 *msgqueues.MessageEnvelope = msgqueues.NewMessageEnvelope("123", "Test", "Test message")
	var envelope2 *msgqueues.MessageEnvelope

	time.AfterFunc(500*time.Millisecond, func() {
		c.queue.Send("", envelope1)
	})

	result, err := c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, err)
	envelope2 = result

	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

}

func (c *MessageQueueFixture) TestReceiveCompleteMessage(t *testing.T) {
	var envelope1 *msgqueues.MessageEnvelope = msgqueues.NewMessageEnvelope("123", "Test", "Test message")
	var envelope2 *msgqueues.MessageEnvelope

	err := c.queue.Send("", envelope1)
	assert.Nil(t, err)

	count, err := c.queue.ReadMessageCount()
	assert.Nil(t, err)
	assert.True(t, (count > 0))

	result, err := c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, err)
	envelope2 = result

	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

	c.queue.Complete(envelope2)
	assert.Nil(t, envelope2.GetReference())

}

func (c *MessageQueueFixture) TestReceiveAbandonMessage(t *testing.T) {
	var envelope1 *msgqueues.MessageEnvelope = msgqueues.NewMessageEnvelope("123", "Test", "Test message")
	var envelope2 *msgqueues.MessageEnvelope

	err := c.queue.Send("", envelope1)
	assert.Nil(t, err)

	result, err := c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, err)
	envelope2 = result

	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

	err = c.queue.Abandon(envelope2)
	assert.Nil(t, err)

	result, err = c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, err)
	envelope2 = result

	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

}

func (c *MessageQueueFixture) TestSendPeekMessage(t *testing.T) {
	var envelope1 *msgqueues.MessageEnvelope = msgqueues.NewMessageEnvelope("123", "Test", "Test message")
	var envelope2 *msgqueues.MessageEnvelope

	err := c.queue.Send("", envelope1)
	assert.Nil(t, err)

	result, err := c.queue.Peek("")
	assert.Nil(t, err)
	envelope2 = result

	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

}

func (c *MessageQueueFixture) TestPeekNoMessage(t *testing.T) {
	result, err := c.queue.Peek("")
	assert.Nil(t, err)
	assert.Nil(t, result)
}

func (c *MessageQueueFixture) TestMoveToDeadMessage(t *testing.T) {
	var envelope1 *msgqueues.MessageEnvelope = msgqueues.NewMessageEnvelope("123", "Test", "Test message")
	var envelope2 *msgqueues.MessageEnvelope

	err := c.queue.Send("", envelope1)
	assert.Nil(t, err)

	result, err := c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, err)
	envelope2 = result

	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

	err = c.queue.MoveToDeadLetter(envelope2)
	assert.Nil(t, err)

}

func (c *MessageQueueFixture) TestOnMessage(t *testing.T) {
	var envelope1 *msgqueues.MessageEnvelope = msgqueues.NewMessageEnvelope("123", "Test", "Test message")
	var envelope2 *msgqueues.MessageEnvelope

	var reciver TestMsgReciver = TestMsgReciver{}

	c.queue.BeginListen("", &reciver)

	select {
	case <-time.After(1000 * time.Millisecond):
	}

	sndErr := c.queue.Send("", envelope1)
	assert.Nil(t, sndErr)

	select {
	case <-time.After(1000 * time.Millisecond):
	}

	envelope2 = reciver.envelope

	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

	c.queue.EndListen("")
}

type TestMsgReciver struct {
	envelope *msgqueues.MessageEnvelope
}

func (c *TestMsgReciver) ReceiveMessage(envelope *msgqueues.MessageEnvelope, queue msgqueues.IMessageQueue) (err error) {
	c.envelope = envelope
	return nil
}
