package build

import (
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cbuild "github.com/pip-services3-go/pip-services3-components-go/build"
	mqueue "github.com/pip-services3-go/pip-services3-mqtt-go/queues"
)

// DefaultMqttFactory are creates MqttMessageQueue components by their descriptors.
// See MqttMessageQueue
type DefaultMqttFactory struct {
	cbuild.Factory
	Descriptor          *cref.Descriptor
	MqttQueueDescriptor *cref.Descriptor
}

// NewDefaultMqttFactory are create a new instance of the factory.
func NewDefaultMqttFactory() *DefaultMqttFactory {
	c := DefaultMqttFactory{}
	c.Factory = *cbuild.NewFactory()
	c.Descriptor = cref.NewDescriptor("pip-services", "factory", "mqtt", "default", "1.0")
	c.MqttQueueDescriptor = cref.NewDescriptor("pip-services", "message-queue", "mqtt", "*", "1.0")
	c.Register(c.MqttQueueDescriptor, func() interface{} {
		return mqueue.NewMqttMessageQueue(c.MqttQueueDescriptor.Name())
	})
	return &c
}
