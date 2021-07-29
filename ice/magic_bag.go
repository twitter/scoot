package ice

import (
	"fmt"
	"reflect"
)

type Key reflect.Type

type Provider interface{}

// MagicBag binds Providers to Keys which an Evaluation can use
type MagicBag struct {
	bindings map[Key]Provider
}

func (b *MagicBag) Bindings() map[Key]Provider {
	return b.bindings
}

// Module can install many things at once.
// It could be just []Provider, but this lets Module code look a little nicer,
type Module interface {
	Install(b *MagicBag)
}

func NewMagicBag() *MagicBag {
	return &MagicBag{
		bindings: make(map[Key]Provider),
	}
}

func (e *MagicBag) InstallModule(m Module) error {
	// TODO(dbentley): defer a recover to turn a panic into an error
	m.Install(e)
	return nil
}

type Value reflect.Value

func (e *MagicBag) checkResult(t reflect.Type) reflect.Type {
	if t.NumOut() == 1 {
		return t.Out(0)
	}
	if t.NumOut() == 2 {
		errType := reflect.TypeOf(new(error)).Elem()
		if !t.Out(1).Implements(errType) {
			throw("f returns two results so the second must implement error; was %v %v", t, errType)
		}
		return t.Out(0)
	}
	throw("f must return either exactly 1 value or 2 values with the second an error; was %v with %v results", t, t.NumOut())
	return nil
}

func (b *MagicBag) Put(f interface{}) {
	v := reflect.ValueOf(f)
	t := v.Type()
	if t.Kind() != reflect.Func {
		panic(fmt.Errorf("f must be a func; was %v", t))
	}
	if t.IsVariadic() {
		panic(fmt.Errorf("f must not be variadic %v", 5))
	}
	created := b.checkResult(t)
	b.bindings[created] = f
}

func (b *MagicBag) PutMany(fs ...interface{}) {
	for _, f := range fs {
		b.Put(f)
	}
}
