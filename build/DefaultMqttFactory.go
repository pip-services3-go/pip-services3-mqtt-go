package build

import (
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cbuild "github.com/pip-services3-go/pip-services3-components-go/build"
	connect "github.com/pip-services3-go/pip-services3-mqtt-go/connect"
	queues "github.com/pip-services3-go/pip-services3-mqtt-go/queues"
)

// Creates MqttMessageQueue components by their descriptors.
// See MqttMessageQueue
type DefaultMqttFactory struct {
	*cbuild.Factory
}

// NewDefaultMqttFactory method are create a new instance of the factory.
func NewDefaultMqttFactory() *DefaultMqttFactory {
	c := DefaultMqttFactory{}
	c.Factory = cbuild.NewFactory()

	mqttQueueFactoryDescriptor := cref.NewDescriptor("pip-services", "queue-factory", "mqtt", "*", "1.0")
	mqttConnectionDescriptor := cref.NewDescriptor("pip-services", "connection", "mqtt", "*", "1.0")
	mqttQueueDescriptor := cref.NewDescriptor("pip-services", "message-queue", "mqtt", "*", "1.0")

	c.RegisterType(mqttQueueFactoryDescriptor, NewMqttMessageQueueFactory)

	c.RegisterType(mqttConnectionDescriptor, connect.NewMqttConnection)

	c.Register(mqttQueueDescriptor, func(locator interface{}) interface{} {
		name := ""
		descriptor, ok := locator.(*cref.Descriptor)
		if ok {
			name = descriptor.Name()
		}

		return queues.NewMqttMessageQueue(name)
	})

	return &c
}
