package jsonconfig

import (
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/davecgh/go-spew/spew"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/ice"
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
	log.Infof("config parsed to:%s", spew.Sdump(parsedConfig))

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

// No frills recursive merging of two maps with string keys, no support for arrays etc.
func mergeMaps(x1, x2 map[string]interface{}) interface{} {
	for k, v2 := range x2 {
		if v1, ok := x1[k]; ok {
			if m1, ok := v1.(map[string]interface{}); ok {
				if m2, ok := v2.(map[string]interface{}); ok {
					x1[k] = mergeMaps(m1, m2)
					continue
				}
			}
		}
		x1[k] = v2
	}
	return x1
}

// GetConfigText finds the right text for a configFlag.
// If configFlag looks like a filename (of the form foo.bar where foo and bar are just alphanumeric),
// read it as an asset. This filename can optionally have appended json (foo.bar.<json>) to override the file.
// Otherwise, assume it's the literal json text.
func GetConfigText(configFlag string, asset func(string) ([]byte, error)) ([]byte, error) {
	if matched, _ := regexp.Match(`^[[:alnum:]]*\.[[:alnum:]]*\.*.*$`, []byte(configFlag)); matched {
		split := strings.SplitN(configFlag, ".", 3)
		configFileName := path.Join("config", split[0]+"."+split[1])
		log.Infof("reading config filename %v", configFileName)
		configText, err := asset(configFileName)
		if err != nil {
			return nil, fmt.Errorf("Scheduler: Error Loading Config File %v: %v", configFileName, err)
		}
		if len(split) == 3 {
			var jsonConfig interface{}
			var jsonOverride interface{}
			if err := json.Unmarshal(configText, &jsonConfig); err != nil {
				return nil, fmt.Errorf("Scheduler: Error Parsing Config File %v: %v", configFileName, err)
			}
			if err := json.Unmarshal([]byte(split[2]), &jsonOverride); err != nil {
				return nil, fmt.Errorf("Scheduler: Error Parsing Config File Suffix %v: %v", configFileName, err)
			}
			if jc, ok := jsonConfig.(map[string]interface{}); ok {
				if jo, ok := jsonOverride.(map[string]interface{}); ok {
					return json.Marshal(mergeMaps(jc, jo))
				}
			}
			return nil, fmt.Errorf("Scheduler: Error Merging Config File Suffix %v: %v + %v", configFileName, jsonConfig, jsonOverride)
		}
		return configText, nil
	}
	log.Infof("Scheduler: using -config as JSON config: %v", configFlag)
	return []byte(configFlag), nil
}
