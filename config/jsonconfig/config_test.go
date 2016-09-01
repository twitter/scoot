package jsonconfig_test

import (
	"encoding/json"
	"testing"

	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"
)

type fooDefaultConfig struct {
	Type string
}

func (c *fooDefaultConfig) Install(e *ice.MagicBag) {}

type fooNoargConfig struct {
	Type string
}

func (c *fooNoargConfig) Install(e *ice.MagicBag) {}

type barDefaultConfig struct {
	Type string
	Arg3 string
	Arg4 map[string]string
}

func (c *barDefaultConfig) Install(e *ice.MagicBag) {}

type barTwoargConfig struct {
	Type string
	Arg1 int
	Arg2 []int
}

func (c *barTwoargConfig) Install(e *ice.MagicBag) {}

const (
	defaultConfig = `{
 "Bar": {
  "Type": "default",
  "Arg3": "3",
  "Arg4": {
   "a": "b"
  }
 },
 "Foo": {
  "Type": "default"
 }
}`
	config1 = `{
 "Bar": {
  "Type": "twoarg",
  "Arg1": 1,
  "Arg2": [
   1,
   2,
   3
  ]
 },
 "Foo": {
  "Type": "noarg"
 }
}`
	config2 = `{
 "Bar": {
  "Type": "twoarg",
  "Arg1": 1,
  "Arg2": [
   1,
   2,
   3,
   4
  ]
 },
 "Foo": {
  "Type": "default"
 }
}`
	config3 = `{
 "Bar": {
  "Type": "twoarg",
  "Arg1": 1,
  "Arg2": [1,2,3,4]
 }
}`
)

type parsedAndMarshaled struct {
	input  string
	output string
}

func TestParse(t *testing.T) {
	tests := []parsedAndMarshaled{
		{defaultConfig, defaultConfig},
		{"", defaultConfig},
		{config1, config1},
		{config2, config2},
		{config3, config2},
	}
	for _, test := range tests {
		o := jsonconfig.Schema(map[string]jsonconfig.Implementations{
			"Foo": {
				"default": &fooDefaultConfig{},
				"noarg":   &fooNoargConfig{},
				"":        &fooDefaultConfig{Type: "default"},
			},
			"Bar": {
				"default": &barDefaultConfig{},
				"twoarg":  &barTwoargConfig{},
				"": &barDefaultConfig{
					Type: "default",
					Arg3: "3",
					Arg4: map[string]string{"a": "b"},
				},
			},
		})

		m, err := o.Parse([]byte(test.input))
		if err != nil {
			t.Fatalf("Error parsing input %v: %v", test.input, err)
		}
		bytes, err := json.MarshalIndent(&m, "", " ")
		if err != nil {
			t.Fatalf("Error marshaling %v from input %v: %v", m, test.input, err)
		}
		actual := string(bytes)
		if actual != test.output {
			t.Fatalf("unexpected output:\n%v\n######\n%v$", actual, test.output)
		}
	}
}
