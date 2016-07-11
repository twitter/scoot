package server

import (
	"fmt"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"reflect"
	"testing"
)

func TestTranslateJob(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 10
	properties := gopter.NewProperties(parameters)

	properties.Property("Error translating a valid job definition", prop.ForAll(
		func(def *scoot.JobDefinition) bool {
			_, err := thriftJobToScoot(def)
			return err == nil
		},
		genJobDef()))
	properties.TestingRun(t)
}

const MAX_TASKS = 1000
const MAX_ARGS = 10

func genTask() gopter.Gen {
	numArgs := gen.IntRange(1, MAX_ARGS)
	args := numArgs.FlatMap(func(n interface{}) gopter.Gen {
		return gen.SliceOfN(n.(int), gen.AnyString())
	}, reflect.TypeOf([]string{}))
	return args.FlatMap(func(args interface{}) gopter.Gen {
		c := scoot.NewCommand()
		c.Argv = args.([]string)
		t := scoot.NewTaskDefinition()
		t.Command = c
		return gen.Const(t)
	}, reflect.TypeOf(scoot.NewTaskDefinition()))
}

func genTasks(n interface{}) gopter.Gen {
	return gen.SliceOfN(n.(int), genTask())
}

func genJobDef() gopter.Gen {
	tasksGen := gen.IntRange(1, MAX_TASKS).FlatMap(genTasks, reflect.TypeOf([]*scoot.TaskDefinition{}))
	taskMapGen := tasksGen.FlatMap(func(vs interface{}) gopter.Gen {
		r := make(map[string]*scoot.TaskDefinition)
		for idx, v := range vs.([]*scoot.TaskDefinition) {
			r[fmt.Sprintf("task%d", idx)] = v
		}
		return gen.Const(r)
	}, reflect.TypeOf(map[string]*scoot.TaskDefinition{}))

	unknown := scoot.JobType_UNKNOWN
	iron_tests := scoot.JobType_IRON_TESTS
	jobTypeGen := gen.OneConstOf(&unknown, &iron_tests, nil)

	jobGens := gopter.CombineGens(taskMapGen, jobTypeGen)

	return jobGens.FlatMap(func(vs interface{}) gopter.Gen {
		values := vs.([]interface{})
		j := scoot.NewJobDefinition()
		j.Tasks = values[0].(map[string]*scoot.TaskDefinition)
		j.JobType = values[1].(*scoot.JobType)
		return gen.Const(j)
	}, reflect.TypeOf(scoot.NewJobDefinition()))
}
