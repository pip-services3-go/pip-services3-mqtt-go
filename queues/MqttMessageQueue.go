package queues

import (
	"strconv"
	"sync"
	"time"

	cauth "github.com/pip-services3-go/pip-services3-components-go/auth"
	ccon "github.com/pip-services3-go/pip-services3-components-go/connect"
	msgqueues "github.com/pip-services3-go/pip-services3-messaging-go/queues"
	mcon "github.com/pip-services3-go/pip-services3-mqtt-go/connect"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

/*
MqttMessageQueue are message queue that sends and receives messages via MQTT message broker.

MQTT is a popular light-weight protocol to communicate IoT devices.

 Configuration parameters:

- topic:                         name of MQTT topic to subscribe
- connection(s):
  - discovery_key:               (optional) a key to retrieve the connection from  IDiscovery
  - host:                        host name or IP address
  - port:                        port number
  - uri:                         resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                   (optional) a key to retrieve the credentials from  ICredentialStore
  - username:                    user name
  - password:                    user password

 References:

- *:logger:*:*:1.0             (optional)  ILogger components to pass log messages
- *:counters:*:*:1.0           (optional)  ICounters components to pass collected measurements
- *:discovery:*:*:1.0          (optional)  IDiscovery services to resolve connections
- *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials

See MessageQueue
See MessagingCapabilities

Example:

    queue := NewMqttMessageQueue("myqueue")
    queue.Configure(cconf.NewConfigParamsFromTuples(
      "topic", "mytopic",
      "connection.protocol", "mqtt"
      "connection.host", "localhost"
      "connection.port", 1883
    ))

    queue.open("123")

    queue.Send("123", NewMessageEnvelope("", "mymessage", "ABC"))

	message, err := queue.Receive("123")
    	if (message != nil) {
           ...
           queue.Complete("123", message);
        }

*/
type MqttMessageQueue struct {
	*msgqueues.MessageQueue
	client          mqtt.Client
	topic           string
	subscribed      bool
	optionsResolver *mcon.MqttConnectionResolver
	receiver        msgqueues.IMessageReceiver
	messages        []msgqueues.MessageEnvelope
}

// NewMqttMessageQueue are creates a new instance of the message queue.
// - name  string (optional) a queue name.
func NewMqttMessageQueue(name string) *MqttMessageQueue {
	c := MqttMessageQueue{}
	c.MessageQueue = msgqueues.NewMessageQueue(name)
	c.MessageQueue.IMessageQueue = &c
	c.Capabilities = msgqueues.NewMessagingCapabilities(false, true, true, true, true, false, false, false, true)
	c.subscribed = false
	c.optionsResolver = mcon.NewMqttConnectionResolver()
	return &c
}

// IsOpen methos are checks if the component is opened.
// Return true if the component has been opened and false otherwise.
func (c *MqttMessageQueue) IsOpen() bool {
	return c.client != nil
}

// Opens the component with given connection and credential parameters.
// Parameters:
// - correlationId     (optional) transaction id to trace execution through call chain.
// - connection        connection parameters
// - credential        credential parameters
// Returns 	error
// error or nil no errors occured.
func (c *MqttMessageQueue) OpenWithParams(correlationId string, connection *ccon.ConnectionParams, credential *cauth.CredentialParams) (err error) {
	c.topic = connection.GetAsString("topic")

	options, err := c.optionsResolver.Compose(correlationId, connection, credential)
	if err != nil {
		return err
	}

	opts := mqtt.NewClientOptions().AddBroker(options.Get("uri"))
	user := options.Get("username")
	passwd := options.Get("password")
	if user != "" {
		opts.SetUsername(user)
	}
	if passwd != "" {
		opts.SetPassword(passwd)
	}
	opts.SetAutoReconnect(true)
	//opts.SetClientID("go-simple")
	//opts.SetDefaultPublishHandler(f)

	//create and start a client using the above ClientOptions
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	c.client = client
	return nil
}

// Close method are Closes component and frees used resources.
// Parameters:
// - correlationId string 	(optional) transaction id to trace execution through call chain.
// Retruns error
// error or nil no errors occured.
func (c *MqttMessageQueue) Close(correlationId string) (err error) {
	if c.client != nil {
		c.messages = make([]msgqueues.MessageEnvelope, 0)
		c.subscribed = false
		c.receiver = nil
		c.client.Disconnect(250)
		c.client = nil
		c.Logger.Trace(correlationId, "Closed queue %s", c)
	}
	return nil
}

// Clear method are clears component state.
// Parameters:
// - correlationId 	string (optional) transaction id to trace execution through call chain.
// Retruns error or nil no errors occured.
func (c *MqttMessageQueue) Clear(correlationId string) (err error) {
	c.messages = make([]msgqueues.MessageEnvelope, 0)
	return nil
}

// ReadMessageCount method are reads the current number of messages in the queue to be delivered.
// Returns count int64, err error
// number of messages or error.
func (c *MqttMessageQueue) ReadMessageCount() (count int64, err error) {
	// Subscribe to get messages
	c.Subscribe()
	count = int64(len(c.messages))
	return count, nil
}

// Send method are sends a message into the queue.
// Parameters:
// - correlationId string    (optional) transaction id to trace execution through call chain.
// - envelope *msgqueues.MessageEnvelope  a message envelop to be sent.
// Returns: error
// error or nil for success.
func (c *MqttMessageQueue) Send(correlationId string, envelop *msgqueues.MessageEnvelope) (err error) {
	c.Counters.IncrementOne("queue." + c.GetName() + ".sent_messages")
	c.Logger.Debug(envelop.Correlation_id, "Sent message %s via %s", envelop.ToString(), c.ToString())
	token := c.client.Publish(c.topic, 0, false, envelop.Message)
	token.Wait()
	return token.Error()
}

// Peek method are peeks a single incoming message from the queue without removing it.
// If there are no messages available in the queue it returns nil.
// Parameters:
// - correlationId  string  (optional) transaction id to trace execution through call chain.
// Returns: result *msgqueues.MessageEnvelope, err error
// message or error.
func (c *MqttMessageQueue) Peek(correlationId string) (result *msgqueues.MessageEnvelope, err error) {
	// Subscribe to get messages
	c.Subscribe()
	var message msgqueues.MessageEnvelope
	if len(c.messages) > 0 {
		message = c.messages[0]
		return &message, nil
	}
	return nil, nil
}

// PeekBatch method are peeks multiple incoming messages from the queue without removing them.
// If there are no messages available in the queue it returns an empty list.
// Important: This method is not supported by MQTT.
// Parameters:
// - correlationId     (optional) transaction id to trace execution through call chain.
// - messageCount      a maximum number of messages to peek.
// Returns:          callback function that receives a list with messages or error.
func (c *MqttMessageQueue) PeekBatch(correlationId string, messageCount int64) (result []msgqueues.MessageEnvelope, err error) {
	// Subscribe to get messages
	c.Subscribe()
	return c.messages, nil
}

// Receive method are receives an incoming message and removes it from the queue.
// Parameters:
// - correlationId   string   (optional) transaction id to trace execution through call chain.
// - waitTimeout  time.Duration     a timeout in milliseconds to wait for a message to come.
// Returns:  result *msgqueues.MessageEnvelope, err error
// receives a message or error.
func (c *MqttMessageQueue) Receive(correlationId string, waitTimeout time.Duration) (result *msgqueues.MessageEnvelope, err error) {

	var message *msgqueues.MessageEnvelope
	var msgBuf msgqueues.MessageEnvelope
	// Subscribe to get messages
	c.Subscribe()
	// Return message immediately if it exist
	if len(c.messages) > 0 {
		for len(c.messages) > 0 {
			msgBuf, c.messages = c.messages[0], c.messages[1:]
			message = &msgBuf
		}
		return message, nil
	}

	// Otherwise wait and return
	var checkIntervalMs time.Duration = 100 * time.Millisecond
	var i time.Duration = 0
	var wait sync.WaitGroup = sync.WaitGroup{}
	wait.Add(1)

	go func() {
		var wg sync.WaitGroup = sync.WaitGroup{}
		for c.client != nil && i < waitTimeout && message == nil {
			i = i + checkIntervalMs
			wg.Add(1)
			time.AfterFunc(checkIntervalMs, func() {
				for len(c.messages) > 0 {
					msgBuf, c.messages = c.messages[0], c.messages[1:]
					message = &msgBuf
				}
				wg.Done()
			})
			wg.Wait()
		}
		wait.Done()
	}()

	wait.Wait()

	return message, nil
}

// RenewLock method are renews a lock on a message that makes it invisible from other receivers in the queue.
// This method is usually used to extend the message processing time.
// Important: This method is not supported by MQTT.
// Parameters:
// - message   *msgqueues.MessageEnvelope    a message to extend its lock.
// - lockTimeout  time.Duration  a locking timeout in milliseconds.
// Returns: error
// receives an error or nil for success.
func (c *MqttMessageQueue) RenewLock(message *msgqueues.MessageEnvelope, lockTimeout time.Duration) (err error) {
	// Not supported
	return nil
}

// Complete method are permanently removes a message from the queue.
// This method is usually used to remove the message after successful processing.
// Important: This method is not supported by MQTT.
// Parameters:
// - message  *msgqueues.MessageEnvelope a message to remove.
// Returns: error
// error or nil for success.
func (c *MqttMessageQueue) Complete(message *msgqueues.MessageEnvelope) (err error) {
	// Not supported
	return nil
}

// Abandon method are returnes message into the queue and makes it available for all subscribers to receive it again.
// This method is usually used to return a message which could not be processed at the moment
// to repeat the attempt. Messages that cause unrecoverable errors shall be removed permanently
// or/and send to dead letter queue.
// Important: This method is not supported by MQTT.
// Parameters:
// - message *msgqueues.MessageEnvelope  a message to return.
// Returns: error
//  error or nil for success.
func (c *MqttMessageQueue) Abandon(message *msgqueues.MessageEnvelope) (err error) {
	// Not supported
	return nil
}

// Permanently removes a message from the queue and sends it to dead letter queue.
// Important: This method is not supported by MQTT.
// Parameters:
// - message  *msgqueues.MessageEnvelope a message to be removed.
// Returns: error
//  error or nil for success.
func (c *MqttMessageQueue) MoveToDeadLetter(message *msgqueues.MessageEnvelope) (err error) {
	// Not supported
	return nil
}

func (c *MqttMessageQueue) toMessage(msg mqtt.Message) msgqueues.MessageEnvelope {

	envelop := msgqueues.NewMessageEnvelope("", msg.Topic(), string(msg.Payload()))
	envelop.Message_id = strconv.FormatUint(uint64(msg.MessageID()), 16)
	return *envelop
}

// Subscribe method are subscribes to the topic.
func (c *MqttMessageQueue) Subscribe() {
	//Exit if already subscribed or
	if c.subscribed || c.client == nil {
		return
	}

	c.Logger.Trace("", "Started listening messages at %s", c.ToString())

	// Subscribe to the topic
	c.client.Subscribe(c.topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		envelop := c.toMessage(msg)

		c.Counters.IncrementOne("queue." + c.GetName() + ".receivedmessages")
		c.Logger.Debug(envelop.Correlation_id, "Received message %s via %s", msg, c.ToString())

		if c.receiver != nil {

			err := c.receiver.ReceiveMessage(&envelop, c)
			if err != nil {
				c.Logger.Error("", err, "Failed to receive the message")
			}

		} else {
			// Keep message queue managable
			for len(c.messages) > 1000 {
				//c.messages.shift()
				for len(c.messages) > 0 {
					_, c.messages = c.messages[0], c.messages[1:]
				}
			}

			// Push into the message queue
			c.messages = append(c.messages, envelop)
		}
	})

	c.subscribed = true
}

/*
Listens for incoming messages and blocks the current thread until queue is closed.
Parameters:
- correlationId   string  (optional) transaction id to trace execution through call chain.
- receiver    msgqueues.IMessageReceiver      a receiver to receive incoming messages.

See IMessageReceiver
See receive
*/
func (c *MqttMessageQueue) Listen(correlationId string, receiver msgqueues.IMessageReceiver) {
	c.receiver = receiver
	var wg = sync.WaitGroup{}
	wg.Add(1)
	go func() {
		var message *msgqueues.MessageEnvelope
		for len(c.messages) > 0 && c.receiver != nil {
			//message = c.messages.shift();
			var msg msgqueues.MessageEnvelope
			message = nil
			for len(c.messages) > 0 {
				msg, c.messages = c.messages[0], c.messages[1:]
				message = &msg
			}
			if message != nil {
				receiver.ReceiveMessage(message, c)
			}
		}
		c.Subscribe()
		wg.Done()
	}()

	wg.Wait()
}

// EndListen method are ends listening for incoming messages.
// When this method is call listen unblocks the thread and execution continues.
// Parameters:
// - correlationId  string   (optional) transaction id to trace execution through call chain.
func (c *MqttMessageQueue) EndListen(correlationId string) {
	c.receiver = nil
	if c.subscribed {
		c.client.Unsubscribe(c.topic)
		c.subscribed = false
	}
}
