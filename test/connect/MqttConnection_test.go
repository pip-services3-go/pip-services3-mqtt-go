package test_connect

import (
	"os"
	"testing"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	connect "github.com/pip-services3-go/pip-services3-mqtt-go/connect"
	"github.com/stretchr/testify/assert"
)

type mqttConnectionTest struct {
	connection *connect.MqttConnection
}

func newMqttConnectionTest() *mqttConnectionTest {
	mqttUri := os.Getenv("MQTT_SERVICE_URI")
	mqttHost := os.Getenv("MQTT_SERVICE_HOST")
	if mqttHost == "" {
		mqttHost = "localhost"
	}

	mqttPort := os.Getenv("MQTT_SERVICE_PORT")
	if mqttPort == "" {
		mqttPort = "1883"
	}

	mqttUser := os.Getenv("MQTT_USER")
	// if mqttUser == "" {
	// 	mqttUser = ""
	// }
	mqttPassword := os.Getenv("MQTT_PASS")
	// if mqttPassword == "" {
	// 	mqttPassword = ""
	// }

	if mqttUri == "" && mqttHost == "" {
		return nil
	}

	connection := connect.NewMqttConnection()
	connection.Configure(cconf.NewConfigParamsFromTuples(
		"connection.uri", mqttUri,
		"connection.host", mqttHost,
		"connection.port", mqttPort,
		"credential.username", mqttUser,
		"credential.password", mqttPassword,
	))

	return &mqttConnectionTest{
		connection: connection,
	}
}

func (c *mqttConnectionTest) TestOpenClose(t *testing.T) {
	err := c.connection.Open("")
	assert.Nil(t, err)
	assert.True(t, c.connection.IsOpen())
	assert.NotNil(t, c.connection.GetConnection())

	err = c.connection.Close("")
	assert.Nil(t, err)
	assert.False(t, c.connection.IsOpen())
	assert.Nil(t, c.connection.GetConnection())
}

func TestMqttConnection(t *testing.T) {
	c := newMqttConnectionTest()
	if c == nil {
		return
	}

	t.Run("Open and Close", c.TestOpenClose)
}
