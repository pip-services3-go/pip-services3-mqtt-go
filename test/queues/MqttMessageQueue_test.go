package test_queues

import (
	"os"
	"testing"
	"time"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	msgqueues "github.com/pip-services3-go/pip-services3-messages-go/queues"
	mqueue "github.com/pip-services3-go/pip-services3-mqtt-go/queues"
	"github.com/stretchr/testify/assert"
)

var queue *mqueue.MqttMessageQueue

func TestMqttMessageQueue(t *testing.T) {

	//var fixture *MessageQueueFixture

	brokerHost := os.Getenv("MOSQUITTO_HOST")
	if brokerHost == "" {
		brokerHost = "localhost"
	}
	brokerPort := os.Getenv("MOSQUITTO_PORT")
	if brokerPort == "" {
		brokerPort = "1883"
	}
	brokerTopic := os.Getenv("MOSQUITTO_TOPIC")
	if brokerTopic == "" {
		brokerTopic = "/test"
	}
	if brokerHost == "" && brokerPort == "" {
		return
	}

	queueConfig := cconf.NewConfigParamsFromTuples(
		"connection.protocol", "tcp",
		"connection.host", brokerHost,
		"connection.port", brokerPort,
		"connection.topic", brokerTopic,
		//"credential.username", "user",
		//"credential.password", "pa$$wd",
	)

	queue = mqueue.NewMqttMessageQueue("testQueue")
	queue.Configure(queueConfig)

	//fixture = NewMessageQueueFixture(queue)

	qOpnErr := queue.Open("")
	if qOpnErr == nil {
		queue.Clear("")
	}

	defer queue.Close("")

	// t.Run("Receive and Send Message", fixture.TestReceiveSendMessage)
	// queue.Clear("")
	// t.Run("On Message", fixture.TestOnMessage)

	t.Run("Receive and Send Message", ReceiveAndSendMessage)
	queue.Clear("")
	t.Run("On Message", OnMessage)

}
func ReceiveAndSendMessage(t *testing.T) {
	var envelop1 *msgqueues.MessageEnvelope = msgqueues.NewMessageEnvelope("123", "Test", "Test message")
	var envelop2 *msgqueues.MessageEnvelope

	time.AfterFunc(500*time.Millisecond, func() {
		queue.Send("", envelop1)
	})

	result, err := queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, err)
	envelop2 = result

	assert.NotNil(t, envelop2)
	assert.NotNil(t, envelop1.Message)
	assert.NotNil(t, envelop2.Message)
	assert.Equal(t, envelop1.Message, envelop2.Message)
}

func OnMessage(t *testing.T) {
	var envelop1 *msgqueues.MessageEnvelope = msgqueues.NewMessageEnvelope("123", "Test", "Test message")
	var envelop2 *msgqueues.MessageEnvelope

	var reciver TestMsgReciver = TestMsgReciver{}

	queue.BeginListen("", &reciver)

	select {
	case <-time.After(1000 * time.Millisecond):
	}

	sndErr := queue.Send("", envelop1)
	assert.Nil(t, sndErr)

	select {
	case <-time.After(1000 * time.Millisecond):
	}

	envelop2 = reciver.envelope

	assert.NotNil(t, envelop2)

	assert.NotNil(t, envelop1.Message)
	assert.NotNil(t, envelop2.Message)
	assert.Equal(t, envelop1.Message, envelop2.Message)

	queue.EndListen("")

}
