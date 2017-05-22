package testhelpers

import (
	"bytes"
	"strings"
	"reflect"
	"fmt"
)
import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/scootdev/scoot/common/stats"
	"strings"
)

/*
add new Checker functions here as needed
 */
/*
errors if a is not float64, returns true if a == b
 */
func FloatEqTest(a, b interface{}) bool {
	if b == nil && a == nil {
		return true
	}
	aflt := a.(float64)
	bflt := b.(float64)
	return aflt == bflt
}
/*
errors if a is not float64, returns true if a > b
 */
func FloatGTTest(a, b interface{}) bool {
	if b == nil && a == nil {
		return true
	}
	aflt := a.(float64)
	bflt := b.(float64)
	return aflt > bflt
}
/*
errors if a is not int64, returns true if a == b
 */
func Int64EqTest(a, b interface{}) bool {
	if b == nil && a == nil {
		return true
	}
	aint := a.(int64)
	bint := b.(int)
	return aint == int64(bint)
}
func DoesNotExist(a, b interface{}) bool {
	return a == nil
}

/*
defines the condition checker to use to validate the measurement.  Each Checker(a, b) implementation
will expect a to be the 'got' value and b to be the 'expected' value.
 */
type Rule struct {
	Checker func(interface{}, interface{}) bool
	Value interface{}
}

/*
Verify that the stats registry object contains values for the keys in the contains map parameter and that
each entry conforms to the rule (condition) associated with that key.
 */
func VerifyStats(statsRegistry stats.StatsRegistry, t *testing.T, contains map[string]Rule) {

	asFinagleRegistry, ok := statsRegistry.(*stats.FinagleStatsRegistry)
	err := false
	var msg bytes.Buffer
	msg.WriteString("stats registry error:\n")

	if ok {
		asJson := asFinagleRegistry.MarshalAll()

		for key, rule := range contains {
			gotValue, ok := asJson[key]
			if !ok {
				if !strings.Contains(runtime.FuncForPC(reflect.ValueOf(rule.Checker).Pointer()).Name(), "DoesNotExist") {
					err = true
					ruleMsg := fmt.Sprintf("%s: no stat entry, and checker:%s", key, runtime.FuncForPC(reflect.ValueOf(rule.Checker).Pointer()).Name())
					msg.WriteString(fmt.Sprintln(ruleMsg))
				}
			} else if rule.Checker != nil && !rule.Checker(gotValue, rule.Value) {
				err = true
				ruleMsg := fmt.Sprintf("%s: got %v, expected to pass %s with %v", key, gotValue, runtime.FuncForPC(reflect.ValueOf(rule.Checker).Pointer()).Name(), rule.Value)
				msg.WriteString(fmt.Sprintln(ruleMsg))
			}
		}
		if err {
			t.Error(msg.String())

		}
	}
}

