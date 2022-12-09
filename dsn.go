package dyndb

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/viant/scy"
	"github.com/viant/scy/cred"
	//default enc key
	_ "github.com/viant/scy/kms/blowfish"
	"github.com/viant/toolbox"
	"net/url"
	"strings"
	"sync"
)

const (
	dsnSecret           = "secret"
	dsnKey              = "key"
	dsnToken            = "token"
	dsnRoleArn          = "roleArn"
	dsnSession          = "session"
	dsnAwsCloudEndpoint = "aws"
	dsnCredentials      = "cred"
	dsnCredentialsURL   = "credURL"
	dsnCredentialsKey   = "credKey"
	dsnCredID           = "credID"
	dsnExecCacheSize    = "execMaxCache"
)

//Config represent Connection config
type Config struct {
	url.Values
	CredURL string
	CredKey string
	CredID  string
	cred.Aws
	ExecMaxCache int
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (*Config, error) {
	URL, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid dsn: %v", err)
	}
	if URL.Scheme != scheme {
		return nil, fmt.Errorf("invalid dsn scheme, expected %v, but had: %v", scheme, URL.Scheme)
	}
	path := strings.Trim(URL.Path, "/")
	cfg := &Config{
		Values: URL.Query(),
	}
	host := URL.Host
	if host != "" && host != dsnAwsCloudEndpoint {
		port := URL.Port()
		if !strings.Contains(host, ":") {
			host = host + ":" + port
		}
		if !strings.Contains(host, "://") {
			host = "http://" + host
		}
		cfg.Endpoint = host
	}
	cfg.Region = path
	cfg.ExecMaxCache = 100
	if len(cfg.Values) > 0 {
		if _, ok := cfg.Values[dsnSecret]; ok {
			cfg.Secret = cfg.Values.Get(dsnSecret)
			delete(cfg.Values, dsnSecret)
		}
		if _, ok := cfg.Values[dsnKey]; ok {
			cfg.Key = cfg.Values.Get(dsnKey)
			delete(cfg.Values, dsnKey)
		}
		if _, ok := cfg.Values[dsnExecCacheSize]; ok {
			cfg.ExecMaxCache = toolbox.AsInt(cfg.Values.Get(dsnExecCacheSize))
			delete(cfg.Values, dsnExecCacheSize)
		}
		if _, ok := cfg.Values[dsnRoleArn]; ok {
			if cfg.Session == nil {
				cfg.Session = &cred.AwsSession{}
			}
			cfg.Session.RoleArn = cfg.Values.Get(dsnRoleArn)
		}
		if _, ok := cfg.Values[dsnSession]; ok {
			if cfg.Session == nil {
				cfg.Session = &cred.AwsSession{}
			}
			cfg.Session.Name = cfg.Values.Get(dsnSession)
			delete(cfg.Values, dsnSession)
		}

		if _, ok := cfg.Values[dsnToken]; ok {
			cfg.Token = cfg.Values.Get(dsnToken)
			delete(cfg.Values, dsnToken)
		}
		if _, ok := cfg.Values[dsnCredentialsKey]; ok {
			cfg.CredKey = cfg.Values.Get(dsnCredentialsKey)
			delete(cfg.Values, dsnCredentialsKey)
		}
		if _, ok := cfg.Values[dsnCredentials]; ok {
			value := cfg.Values.Get(dsnCredentials)
			cfg.CredURL = cred.DiscoverLocation(context.Background(), value)

			delete(cfg.Values, dsnCredentials)
		}
		if _, ok := cfg.Values[dsnCredentialsURL]; ok {
			cfg.CredURL = cfg.Values.Get(dsnCredentialsURL)
			delete(cfg.Values, dsnCredentialsURL)
		}
		if _, ok := cfg.Values[dsnCredID]; ok {
			cfg.CredID = cfg.Values.Get(dsnCredID)
			delete(cfg.Values, dsnCredID)
		}
	}
	if len(cfg.Values) > 0 {
		var unsupported []string
		for k := range cfg.Values {
			unsupported = append(unsupported, k)
		}
		return nil, fmt.Errorf("unsupported options: %v", unsupported)
	}

	if cfg.CredKey != "" {
		if URL, err := base64.RawURLEncoding.DecodeString(cfg.CredKey); err == nil {
			cfg.CredKey = string(URL)
		}
	}
	if err = cfg.initialiseSecrets(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) initialiseSecrets() error {
	if c.CredURL != "" {
		if URL, err := base64.RawURLEncoding.DecodeString(c.CredURL); err == nil {
			c.CredURL = string(URL)
		}
	}
	if c.CredKey != "" {
		if URL, err := base64.RawURLEncoding.DecodeString(c.CredKey); err == nil {
			c.CredKey = string(URL)
		}
	}
	var err error
	var awsCred *cred.Aws
	if c.CredURL != "" && c.CredKey == "" {
		c.CredKey = "blowfish://default"
	}
	if c.CredID != "" {
		resource := scy.Resources().Lookup(c.CredID)
		if resource == nil {
			return fmt.Errorf("failed to lookup secretID: %v", c.CredID)
		}
		if awsCred, err = credRegistry.lookup(resource); err != nil {
			return err
		}
	}
	if awsCred == nil && c.CredURL != "" {
		res := scy.NewResource(&cred.Aws{}, c.CredURL, c.CredKey)
		if awsCred, err = credRegistry.lookup(res); err != nil {
			return err
		}
	}
	if awsCred != nil {
		c.Key = awsCred.Key
		c.Secret = awsCred.Secret
		if c.Token == "" {
			c.Token = awsCred.Token
		}
		if c.Session == nil {
			c.Session = awsCred.Session
		}

	}
	return nil
}

type credentialsRegistry struct {
	registry map[string]*cred.Aws
	sync.RWMutex
	service *scy.Service
}

func (r *credentialsRegistry) lookup(resource *scy.Resource) (*cred.Aws, error) {
	r.RWMutex.RLock()
	result, ok := r.registry[resource.URL]
	r.RWMutex.RUnlock()
	if ok {
		return result, nil
	}

	secrets, err := r.service.Load(context.Background(), resource)
	if err != nil {
		return nil, fmt.Errorf("failed to load secret from :%v, %w", resource.URL, err)
	}
	awsCred, ok := secrets.Target.(*cred.Aws)
	if !ok {
		return nil, fmt.Errorf("expected %T, but had %T", awsCred, secrets.Target)
	}
	r.RWMutex.Lock()
	r.registry[resource.URL] = awsCred
	r.RWMutex.Unlock()
	return awsCred, nil
}

var credRegistry = credentialsRegistry{registry: map[string]*cred.Aws{}, service: scy.New()}
