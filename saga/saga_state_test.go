package saga

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSagaStateFactory(t *testing.T) {
	sagaId := "testSaga"
	job := []byte{0, 1, 2, 3, 4, 5}

	state, _ := makeSagaState("testSaga", job)
	if state.sagaId != sagaId {
		t.Error(fmt.Sprintf("SagaState SagaId should be the same as the SagaId passed to Factory Method"))
	}

	if !bytes.Equal(state.Job(), job) {
		t.Error(fmt.Sprintf("SagaState Job should be the same as the supplied Job passed to Factory Method"))
	}
}

func TestSagaState_ValidateSagaId(t *testing.T) {
	err := validateSagaId("")
	if err == nil {
		t.Error("Invalid Saga Id Should Return Error")
	}

	// validate the correct error is returned
	_, sErrorOk := err.(InvalidSagaMessageError)
	if !sErrorOk {
		t.Error("Expected Returned Error to be InvalidSagaMessageError")
	}
}

func TestSagaState_ValidateTaskId(t *testing.T) {
	err := validateTaskId("")
	if err == nil {
		t.Error(fmt.Sprintf("Invalid Task Id Should Return Error"))
	}

	// validate the correct error is returned
	_, sErrorOk := err.(InvalidSagaMessageError)
	if !sErrorOk {
		t.Error("Expected Returned Error to be InvalidSagaMessageError")
	}
}

func TestSagaState_Copy(t *testing.T) {
	s1, _ := makeSagaState("sagaId", nil)
	s2 := copySagaState(s1)

	if s1.SagaId() != s2.SagaId() {
		t.Error(fmt.Sprintf("Copy Should Preserve SagaId"))
	}
}
