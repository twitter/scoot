package jsonconfig

import (
	"encoding/json"
	"fmt"
	log "github.com/inconshreveable/log15"
	"path"
	"regexp"

	"github.com/scootdev/scoot/ice"
)

// Schema holds the different Implementations's the client wants to configure
type Schema map[string]Implementations

// EmptySchema returns an empty Schema, needed if you don't allow configuration
func EmptySchema() Schema {
	return map[string]Implementations{}
}

// Implementations maps the the names of implementations to the Implementation
// As a special case, "" maps to a default implementation that will not be unmarshal'ed,
// and so the Implementation will be used as-is.
type Implementations map[string]Implementation

type Implementation interface {
	// The Implementation needs to do 3 things:
	// 1) parse the JSON config
	// 2) add the relevant providers to the ice MagicBag
	// 3) print its configuration
	// 1 & 3 are handled implicitly (ugh) by json.(Un)marshal
	// 2 is handled by being an ice Module
	ice.Module
}

type Configuration map[string]ice.Module

// Configuration is itself a Module, that installs each Impl as a Module
// (in Design Patterns terminology, it's a Composite)
func (c Configuration) Install(bag *ice.MagicBag) {
	for _, v := range c {
		bag.InstallModule(v)
	}
}

var emptyJson = []byte("{}")

func (schema Schema) Parse(text []byte) (Configuration, error) {
	// TODO(dbentley): allow text to refer to a file instead of being a giant string
	var parsedConfig map[string]json.RawMessage
	if len(text) == 0 {
		text = emptyJson
	}
	err := json.Unmarshal(text, &parsedConfig)
	if err != nil {
		return nil, fmt.Errorf("Couldn't parse top-level config: %v", err)
	}
	log.Info("config parsed to:%+v\n", parsedConfig)

	result := Configuration(make(map[string]ice.Module))
	// Parse each option (aka Implementations, which isn't a valid variable name)
	for optionName, impls := range schema {
		optionText := parsedConfig[optionName]
		// Parse this Implementations's JSON just enough to get the type
		implName, err := parseType(optionText)
		if err != nil {
			return nil, fmt.Errorf("Error parsing type for Implementations %v: %v", optionName, err)
		}
		impl, ok := impls[implName]
		if !ok {
			return nil, fmt.Errorf("Error parsing Implementations %v: %q is not a valid Implementation %v %v %T", optionName, implName, impls, impl, impl)
		}
		if len(optionText) > 0 {
			// Now parse it fully, with the right Implementation
			err = json.Unmarshal(optionText, &impl)
			if err != nil {
				return nil, fmt.Errorf("Error parsing variable %v: %v", optionName, err)
			}
		}
		result[optionName] = impl
	}
	return result, nil
}

// Find the type, which is simply the string value for the key "Type"
func parseType(data json.RawMessage) (string, error) {
	if len(data) == 0 {
		return "", nil
	}
	var t struct{ Type string }
	err := json.Unmarshal(data, &t)
	if err != nil {
		return "", err
	}
	return t.Type, nil
}

// GetConfigText finds the right text for a configFlag.
// If configFlag looks like a filename (of the form foo.bar where foo and bar are just alphanumeric),
// read it as an asset.
// Otherwise, assume it's the literal json text.
func GetConfigText(configFlag string, asset func(string) ([]byte, error)) ([]byte, error) {
	if matched, _ := regexp.Match(`^[[:alnum:]]*\.[[:alnum:]]*$`, []byte(configFlag)); matched {
		configFileName := path.Join("config", configFlag)
		log.Info("reading config filename %v", configFileName)
		configText, err := asset(configFileName)
		if err != nil {
			return nil, fmt.Errorf("Scheduler: Error Loading Config File %v: %v", configFileName, err)
		}
		return configText, nil
	}
	log.Info("Scheduler: using -config as JSON config: %v", configFlag)
	return []byte(configFlag), nil
}
