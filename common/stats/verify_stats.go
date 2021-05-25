package stats

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
)

/*
Utilities for validating the stats registry contents
*/
/*
add new Checker functions here as needed
*/
type RuleChecker struct {
	name    string
	checker func(interface{}, interface{}) bool
}

func nilCheck(a, b interface{}) (nilFound, eqValues bool) {
	nilFound = false
	if b == nil && a == nil {
		nilFound = true
		eqValues = true
	} else if b == nil || a == nil {
		nilFound = true
		eqValues = false
	}
	return
}

/*
errors if a is not float64, returns true if a == b
*/
func floatEqTest(a, b interface{}) bool {
	if nilFound, eqValue := nilCheck(a, b); nilFound {
		return eqValue
	}
	aflt := a.(float64)
	bflt := b.(float64)
	return aflt == bflt
}

var FloatEqTest = RuleChecker{name: "floatEqTest", checker: floatEqTest}

/*
errors if a is not float64, returns true if a > b
*/
func floatGTTest(a, b interface{}) bool {
	if nilFound, eqValue := nilCheck(a, b); nilFound {
		return eqValue
	}
	aflt := a.(float64)
	bflt := b.(float64)
	return aflt > bflt
}

var FloatGTTest = RuleChecker{name: "floatGTTest", checker: floatGTTest}

/*
errors if a is not int64, returns true if a == b
*/
func int64EqTest(a, b interface{}) bool {
	if nilFound, eqValue := nilCheck(a, b); nilFound {
		return eqValue
	}
	aint := a.(int64)
	bint := b.(int)
	return aint == int64(bint)
}

var Int64EqTest = RuleChecker{name: "IntEqTest", checker: int64EqTest}

/*
errors if a is not int64, returns true if a > b
*/
func int64GTTest(a, b interface{}) bool {
	if nilFound, eqValue := nilCheck(a, b); nilFound {
		return eqValue
	}
	aint := a.(int64)
	bint := b.(int)
	return aint > int64(bint)
}

var Int64GTTest = RuleChecker{name: "IntGTTest", checker: int64GTTest}

func doesNotExistTest(a, b interface{}) bool {
	return a == nil
}

var DoesNotExistTest = RuleChecker{name: "NotExistCheck", checker: doesNotExistTest}

/*
defines the condition checker to use to validate the measurement.  Each Checker(a, b) implementation
will expect a to be the 'got' value and b to be the 'expected' value.
*/
type Rule struct {
	Checker RuleChecker
	Value   interface{}
}

/*
Verify that the stats registry object contains values for the keys in the contains map parameter and that
each entry conforms to the rule (condition) associated with that key.
*/
func StatsOk(tag string, statsRegistry StatsRegistry, t *testing.T, contains map[string]Rule) bool {

	asFinagleRegistry, ok := statsRegistry.(*finagleStatsRegistry)
	statsOk := true
	var msg bytes.Buffer
	msg.WriteString(tag)
	msg.WriteString(":stats registry error:\n")

	if ok {
		asJson := asFinagleRegistry.MarshalAll()

		for key, rule := range contains {
			gotValue, _ := asJson[key]
			if !rule.Checker.checker(gotValue, rule.Value) {
				if strings.Compare(rule.Checker.name, DoesNotExistTest.name) == 0 {
					statsOk = false
					ruleMsg := fmt.Sprintf("%s: found stat entry when there should not be one", key)
					msg.WriteString(fmt.Sprintln(ruleMsg))

				} else {
					statsOk = false
					ruleMsg := fmt.Sprintf("%s: got %v, expected to pass %s with %v", key, gotValue, rule.Checker.name, rule.Value)
					msg.WriteString(fmt.Sprintln(ruleMsg))
				}
			}
		}
		if !statsOk {
			fmt.Println(msg.String())
			PPrintStats(tag, asFinagleRegistry)
		}
	}
	return statsOk
}

func PPrintStats(tag string, statsRegistry StatsRegistry) {
	log.Infof("%s:  Stats Registry:\n", tag)
	asFinagleRegistry, _ := statsRegistry.(*finagleStatsRegistry)
	regBytes, _ := asFinagleRegistry.MarshalJSONPretty()
	log.Printf("%s\n", regBytes)
}
