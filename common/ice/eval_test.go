package ice

import (
	"fmt"
	"testing"
)

func TestEasy(t *testing.T) {
	env := NewMagicBag()
	env.Put(MakeIntBox1)
	var target intBox
	err := env.Extract(&target)
	if err != nil {
		t.Fatalf("error creating: %v", err)
	}
	if target.i != 1 {
		t.Fatalf("Incorrect value: %d, expected 1", target.i)
	}
}

func TestArgs(t *testing.T) {
	env := NewMagicBag()
	env.Put(NewDB)
	env.Put(NewMemStorage)
	env.Put(NewYesAuther)
	var db DB
	err := env.Extract(&db)
	if err != nil {
		t.Fatal(err)
	}
	e, err := db.IsEven("token")
	if e != true || err != nil {
		t.Fatalf("expected true, nil; was %v %v", e, err)
	}
	if err = db.Inc("token"); err != nil {
		t.Fatalf("expected no error; was %v", err)
	}
	e, err = db.IsEven("token")
	if e != false || err != nil {
		t.Fatalf("expected false, nil; was %v %v", e, err)
	}
}

func TestCycle(t *testing.T) {
	env := NewMagicBag()
	env.Put(MakeEvener)
	env.Put(MakeOdder)
	var evener *Evener
	err := env.Extract(&evener)
	if err == nil {
		t.Fatal("expected error creating a cycle")
	}
}

func TestErrors(t *testing.T) {
	env := NewMagicBag()
	env.Put(func() (intBox, error) {
		return intBox{2}, nil
	})
	var b intBox
	err := env.Extract(&b)
	if err != nil || b.i != 2 {
		t.Fatalf("expected 2, nil; was %v %v", b.i, err)
	}

	env = NewMagicBag()
	env.Put(func() (intBox, error) {
		return intBox{3}, fmt.Errorf("Error!")
	})
	err = env.Extract(&b)
	if err == nil {
		t.Fatal("expected error when provider returns error")
	}
}
