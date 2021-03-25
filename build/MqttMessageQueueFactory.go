package build

import (
	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	"github.com/pip-services3-go/pip-services3-components-go/build"
	"github.com/pip-services3-go/pip-services3-mqtt-go/queues"
)

// MqttMessageQueueFactory are creates MqttMessageQueue components by their descriptors.
// Name of created message queue is taken from its descriptor.
//
// See Factory
// See MqttMessageQueue
type MqttMessageQueueFactory struct {
	build.Factory
	config     *cconf.ConfigParams
	references cref.IReferences
}

// NewMqttMessageQueueFactory method are create a new instance of the factory.
func NewMqttMessageQueueFactory() *MqttMessageQueueFactory {
	c := MqttMessageQueueFactory{}

	mqttQueueDescriptor := cref.NewDescriptor("pip-services", "message-queue", "mqtt", "*", "1.0")

	c.Register(mqttQueueDescriptor, func(locator interface{}) interface{} {
		name := ""
		descriptor, ok := locator.(*cref.Descriptor)
		if ok {
			name = descriptor.Name()
		}

		queue := queues.NewMqttMessageQueue(name)

		if c.config != nil {
			queue.Configure(c.config)
		}
		if c.references != nil {
			queue.SetReferences(c.references)
		}

		return queue
	})

	return &c
}
