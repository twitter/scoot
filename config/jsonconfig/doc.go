/*
Jsonconfig implements configuration, reading json into an ice Module.

To use:

1) Create the Schema. List your configurable Implementations. Each Implementations
can be backed by several named Implementations.
 2. Options.Parse parses bytes and creates a Configuration.
    a) for each Implementations, pick which Implementation.
    b) json.Unmarshal the json into that Implementation
    c) Implementation can now be used as a Module or json.Marshal'ed to print its configuration
 3. Configuration is an ice Module that installs each Implementation

Example:
1) Create the Schema

	schema := jsonconfig.Schema(map[string]jsonconfig.Implementations{
	 "Storage": {
	  "foo": StorageFooConfig{},
	  "baz": StorageBazConfig{},
	  "": StorageFooConfig{Type: "foo", Param: 17},
	 },
	 "Cluster": {
	  "bar": ClusterBarConfig{},
	  "quuz": ClusterQuuxConfig{},
	 }
	}

2) Parse

	mod, _ := schema.Parse([]byte(`{
	 "Storage": {
	  "Type": "FooStorage",
	  "FooArg": "Val"
	 },
	 "Cluster": {
	  "Type": "BarCluster",
	  "BarArg": 17
	 }
	}`)

3) Install the Confniguration

bag.InstallModule(mod)

# Notes

Right now, that's an oddity that config's should be unset except the default, which should be.
It would be better if we solved this.
*/
package jsonconfig
