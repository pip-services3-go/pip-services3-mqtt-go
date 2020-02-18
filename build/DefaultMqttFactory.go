package build

import (
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cbuild "github.com/pip-services3-go/pip-services3-components-go/build"
	mqueue "github.com/pip-services3-go/pip-services3-mqtt-go/queues"
)

/*
Creates MqttMessageQueue components by their descriptors.
See MqttMessageQueue
*/
type DefaultMqttFactory struct {
	cbuild.Factory
	Descriptor          *cref.Descriptor
	MqttQueueDescriptor *cref.Descriptor
}

/**
Create a new instance of the factory.
*/
func NewDefaultMqttFactory() *DefaultMqttFactory {
	dmf := DefaultMqttFactory{}
	dmf.Factory = *cbuild.NewFactory()
	dmf.Descriptor = cref.NewDescriptor("pip-services", "factory", "mqtt", "default", "1.0")
	dmf.MqttQueueDescriptor = cref.NewDescriptor("pip-services", "message-queue", "mqtt", "*", "1.0")
	dmf.Register(dmf.MqttQueueDescriptor, func() interface{} {
		return mqueue.NewMqttMessageQueue(dmf.MqttQueueDescriptor.Name())
	})
	return &dmf
}
