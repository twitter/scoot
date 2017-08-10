// +build property_test

package server

import (
	"reflect"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
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

const MAX_TASKS = 10
const MAX_ARGS = 10

func genTask() gopter.Gen {
	numArgs := gen.IntRange(1, MAX_ARGS)
	args := numArgs.FlatMap(func(n interface{}) gopter.Gen {
		return gen.SliceOfN(n.(int), gen.AnyString())
	}, reflect.TypeOf([]string{}))
	return args.FlatMap(func(args interface{}) gopter.Gen {
		taskId := ""
		c := scoot.NewCommand()
		c.Argv = args.([]string)
		t := scoot.NewTaskDefinition()
		t.Command = c
		t.TaskId = &taskId
		return gen.Const(t)
	}, reflect.TypeOf(scoot.NewTaskDefinition()))
}

func genTasks(n interface{}) gopter.Gen {
	return gen.SliceOfN(n.(int), genTask())
}

func genJobDef() gopter.Gen {
	tasksGen := gen.IntRange(1, MAX_TASKS).FlatMap(genTasks, reflect.TypeOf([]*scoot.TaskDefinition{}))
	taskMapGen := tasksGen.FlatMap(func(vs interface{}) gopter.Gen {
		r := []*scoot.TaskDefinition{}
		for _, v := range vs.([]*scoot.TaskDefinition) {
			r = append(r, v)
		}
		return gen.Const(r)
	}, reflect.TypeOf([]*scoot.TaskDefinition{}))

	unknown := ""
	iron_tests := "iron"
	jobTypeGen := gen.OneConstOf(&unknown, &iron_tests, nil)

	jobGens := gopter.CombineGens(taskMapGen, jobTypeGen)

	return jobGens.FlatMap(func(vs interface{}) gopter.Gen {
		values := vs.([]interface{})
		j := scoot.NewJobDefinition()
		j.Tasks = values[0].([]*scoot.TaskDefinition)
		j.JobType = values[1].(*string)
		return gen.Const(j)
	}, reflect.TypeOf(scoot.NewJobDefinition()))
}
