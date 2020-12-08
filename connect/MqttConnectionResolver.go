package connect

import (
	"sync"

	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cauth "github.com/pip-services3-go/pip-services3-components-go/auth"
	ccon "github.com/pip-services3-go/pip-services3-components-go/connect"
)

/*
MqttConnectionResolver helper class that resolves MQTT connection and credential parameters,
validates them and generates connection options.

  Configuration parameters:

- connection(s):
  - discovery_key:               (optional) a key to retrieve the connection from IDiscovery
  - host:                        host name or IP address
  - port:                        port number
  - uri:                         resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                   (optional) a key to retrieve the credentials from ICredentialStore
  - username:                    user name
  - password:                    user password

 References:

- *:discovery:*:*:1.0          (optional) IDiscovery services to resolve connections
- *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
*/
type MqttConnectionResolver struct {
	// The connections resolver.
	ConnectionResolver *ccon.ConnectionResolver
	//The credentials resolver.
	CredentialResolver *cauth.CredentialResolver
}

func NewMqttConnectionResolver() *MqttConnectionResolver {
	c := MqttConnectionResolver{}
	c.ConnectionResolver = ccon.NewEmptyConnectionResolver()
	c.CredentialResolver = cauth.NewEmptyCredentialResolver()
	return &c
}

// Configure are configures component by passing configuration parameters.
// Parameters:
//   - config   *cconf.ConfigParams
// configuration parameters to be set.
func (c *MqttConnectionResolver) Configure(config *cconf.ConfigParams) {
	c.ConnectionResolver.Configure(config)
	c.CredentialResolver.Configure(config)
}

// SetReferences are sets references to dependent components.
// Parameters:
//   - references  cref.IReferences
//	references to locate the component dependencies.
func (c *MqttConnectionResolver) SetReferences(references cref.IReferences) {
	c.ConnectionResolver.SetReferences(references)
	c.CredentialResolver.SetReferences(references)
}

func (c *MqttConnectionResolver) validateConnection(correlationId string, connection *ccon.ConnectionParams) error {
	if connection == nil {
		return cerr.NewConfigError(correlationId, "NO_CONNECTION", "MQTT connection is not set")
	}

	uri := connection.Uri()
	if uri != "" {
		return nil
	}

	protocol := connection.GetAsString("protocol")
	if protocol == "" {
		return cerr.NewConfigError(correlationId, "NO_PROTOCOL", "Connection protocol is not set")
	}

	host := connection.Host()
	if host == "" {
		return cerr.NewConfigError(correlationId, "NO_HOST", "Connection host is not set")
	}

	port := connection.Port()
	if port == 0 {
		return cerr.NewConfigError(correlationId, "NO_PORT", "Connection port is not set")
	}

	return nil
}

func (c *MqttConnectionResolver) composeOptions(connection *ccon.ConnectionParams, credential *cauth.CredentialParams) *cconf.ConfigParams {

	// Define additional parameters parameters
	if credential == nil {
		credential = cauth.NewEmptyCredentialParams()
	}
	options := connection.Override(&credential.ConfigParams)

	// Compose uri
	if options.Get("uri") == "" {
		options.SetAsObject("uri", options.Get("protocol")+"://"+options.Get("host"))

		if options.Get("port") != "" {
			options.SetAsObject("uri", options.Get("uri")+":"+options.Get("port"))
		}
	}
	return options
}

// Resolves MQTT connection options from connection and credential parameters.
// Parameters:
//   - correlationId   string
//   (optional) transaction id to trace execution through call chain.
// Retruns options *cconf.ConfigParams, err error
// receives resolved options or error.
func (c *MqttConnectionResolver) Resolve(correlationId string) (options *cconf.ConfigParams, err error) {
	var connection *ccon.ConnectionParams
	var credential *cauth.CredentialParams
	var errCred, errConn error
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		connection, errConn = c.ConnectionResolver.Resolve(correlationId)
		//Validate connections
		if errConn == nil {
			errConn = c.validateConnection(correlationId, connection)
		}
	}()
	go func() {
		defer wg.Done()
		credential, errCred = c.CredentialResolver.Lookup(correlationId)
		// Credentials are not validated right now
	}()
	wg.Wait()

	if errConn != nil {
		return nil, errConn
	}
	if errCred != nil {
		return nil, errCred
	}
	options = c.composeOptions(connection, credential)
	return options, nil
}

// Compose method are composes MQTT connection options from connection and credential parameters.
// Parameters:
//   - correlationId  string  (optional) transaction id to trace execution through call chain.
//   - connection  *ccon.ConnectionParams    connection parameters
//   - credential  *cauth.CredentialParams   credential parameters
// Returns: options *cconf.ConfigParams, err error
// resolved options or error.
func (c *MqttConnectionResolver) Compose(correlationId string, connection *ccon.ConnectionParams, credential *cauth.CredentialParams) (options *cconf.ConfigParams, err error) {
	// Validate connections
	err = c.validateConnection(correlationId, connection)
	if err != nil {
		return nil, err
	} else {
		options := c.composeOptions(connection, credential)
		return options, nil
	}
}
