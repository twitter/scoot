package ice

import (
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"

	log "github.com/sirupsen/logrus"
)

// Extract extracts a value from the MagicBag and puts it into dest, returning any errors
// dest must be a pointer to a type this MagicBag knows how to construct.
func (bag *MagicBag) Extract(dest interface{}) (result error) {
	defer func() {
		if r := recover(); r != nil {
			result = fmt.Errorf("Error injecting: %v", r)
		}
	}()

	// dest must be a pointer to our target
	destVal := reflect.ValueOf(dest)
	destType := destVal.Type()
	if destType.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer; was %v", destType)
	}

	// our target is what dest points to
	targetVal := destVal.Elem()
	targetType := destType.Elem()
	eval := &evaluation{
		bag:    bag,
		values: make(map[Key]Value),
	}
	targetVal.Set(reflect.Value(eval.construct(targetType)))

	return nil
}

// An evaluation holds the mutable state for an ice evaluation
type evaluation struct {
	bag    *MagicBag
	values map[Key]Value
	stack  stack
}

// what we are evaluating to construct at this level
type frame struct {
	key Key
}

// a stack is just the in-order frames of our evaluation
type stack []frame

func (s stack) LogStack() {
	log.Info("goice stacktrace (constructor chain):")
	for _, f := range s {
		log.Info(fmt.Sprintf("\t%s", f.key))
	}
	log.Info("end goice stacktrace")
}

// one level of our evaluation. Constructs a Value for key
func (e *evaluation) construct(key Key) Value {
	// Maintain our stack
	e.enter(key)
	defer e.exit()

	// Check for cycles (instead of just recurring infinitely and overflowing stack
	for _, f := range e.stack[0 : len(e.stack)-1] {
		if f.key == key {
			throw("cycle in object (dependency) graph: already constructing %v", key)
		}
	}

	// Have we already constructed this?
	v, ok := e.values[key]
	if ok {
		return v
	}

	// Find who makes this
	provider, ok := e.bag.bindings[key]
	if !ok {
		throw("target type %v is unbound (no constructor for %v found in bag)", key, key)
	}

	// providerType must be a function; we check this in Put so we assume it here
	providerVal := reflect.ValueOf(provider)
	providerType := providerVal.Type()

	// construct arguments. (Here's the recursion, and our basecase is NumIn() == 0)
	args := make([]reflect.Value, providerType.NumIn())
	for i := 0; i < providerType.NumIn(); i++ {
		argType := providerType.In(i)
		args[i] = reflect.Value(e.construct(argType))
	}

	// Call the provider
	results := providerVal.Call(args)

	// If provider returned an error, throw the error
	if len(results) == 2 {
		var err error
		reflect.ValueOf(&err).Elem().Set(results[1])
		if err != nil {
			throw("provider %s threw error: %v", getFunctionName(provider), err)
		}
	}
	e.values[key] = Value(results[0])
	log.Infof("Constructed: %T", results[0].Interface())
	return e.values[key]
}

func (e *evaluation) enter(key Key) {
	// Push a frame
	e.stack = append(e.stack, frame{key})
}

func (e *evaluation) exit() {
	if r := recover(); r != nil {
		// construct panic'ed. OK. Let's take what happened, and wrap it.
		// First into an error, and then into an InjectionError.
		err, ok := r.(error)
		if !ok {
			err = fmt.Errorf("%v", r)
		}
		iceErr, ok := err.(*InjectionError)
		if !ok {
			stackCopy := stack(nil)
			stackCopy = append(stackCopy, e.stack...)
			iceErr = &InjectionError{
				underlying: err,
				goiceStack: stackCopy,
				goStack:    string(debug.Stack()),
			}
		}
		panic(iceErr)
	}

	// Pop a frame
	e.stack = e.stack[:len(e.stack)-1]
}

type InjectionError struct {
	underlying error
	goiceStack stack
	goStack    string
}

func (e *InjectionError) String() string {
	return fmt.Sprintf("goice injection error:\n\t%v\n%v\n%v",
		e.underlying.Error(),
		e.goiceStack,
		e.goStack,
	)

}

func (e *InjectionError) Error() string {
	return e.String()
}

func throw(format string, a ...interface{}) {
	panic(fmt.Errorf(format, a...))
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
