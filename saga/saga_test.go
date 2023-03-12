package saga

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/wisechengyi/scoot/common/stats"
)

func TestEndSaga(t *testing.T) {
	entry := MakeEndSagaMessage("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry)

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.EndSaga()
	if err != nil {
		t.Error("Expected EndSaga to not return an error", err)
	}

	if !s.GetState().IsSagaCompleted() {
		t.Error("Expected Saga to be completed")
	}
}

func TestEndSagaLogError(t *testing.T) {
	entry := MakeEndSagaMessage("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndSaga Message"))

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.EndSaga()

	if err == nil {
		t.Error("Expected EndSaga to not return an error when write to SagaLog Fails")
	}

	if s.GetState().IsSagaCompleted() {
		t.Error("Expected saga to not be completed")
	}
}

func TestAbortSaga(t *testing.T) {
	entry := MakeAbortSagaMessage("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry)

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.AbortSaga()

	if err != nil {
		t.Error("Expected AbortSaga to not return an error", err)
	}

	if !s.GetState().IsSagaAborted() {
		t.Error("expected Saga to be Aborted")
	}
}

func TestAbortSagaLogError(t *testing.T) {
	entry := MakeAbortSagaMessage("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log AbortSaga Message"))

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.AbortSaga()

	if err == nil {
		t.Error("Expected AbortSaga to return an error when write to SagaLog Fails")
	}

	if s.GetState().IsSagaAborted() {
		t.Error("Expected abort to not be applied on error")
	}
}

func TestStartTask(t *testing.T) {
	entry := MakeStartTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry)

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.StartTask("task1", nil)

	if err != nil {
		t.Error("Expected StartTask to not return an error", err)
	}

	if !s.GetState().IsTaskStarted("task1") {
		t.Error("Expected task1 to be started")
	}
}

func TestStartTaskLogError(t *testing.T) {
	entry := MakeStartTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log StartTask Message"))

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.StartTask("task1", nil)

	if err == nil {
		t.Error("Expected StartTask to not return an error when write to SagaLog Fails")
	}

	if s.GetState().IsTaskStarted("task1") {
		t.Error("Expected task1 to not be started")
	}
}

func TestEndTask(t *testing.T) {
	entry := MakeEndTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(entry)

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.StartTask("task1", nil)
	err = s.EndTask("task1", nil)

	if err != nil {
		t.Error("Expected EndTask to not return an error", err)
	}

	if !s.GetState().IsTaskCompleted("task1") {
		t.Error("expected task1 to be completed")
	}
}

func TestEndTaskLogError(t *testing.T) {
	entry := MakeEndTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndTask Message"))

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.StartTask("task1", nil)
	err = s.EndTask("task1", nil)

	if err == nil {
		t.Error("Expected EndTask to not return an error when write to SagaLog Fails", err)
	}

	if s.GetState().IsTaskCompleted("task1") {
		t.Error("Expected task1 to not be completed")
	}
}

func TestStartCompTask(t *testing.T) {
	entry := MakeStartCompTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(MakeAbortSagaMessage("testSaga"))
	sagaLogMock.EXPECT().LogMessage(entry)

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.StartTask("task1", nil)
	err = s.AbortSaga()
	err = s.StartCompensatingTask("task1", nil)

	if err != nil {
		t.Error("Expected StartCompensatingTask to not return an error", err)
	}

	if !s.GetState().IsCompTaskStarted("task1") {
		t.Error("Expected Comp Task to be started")
	}
}

func TestStartCompTaskLogError(t *testing.T) {
	entry := MakeStartCompTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(MakeAbortSagaMessage("testSaga"))
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log StartCompTask Message"))

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.StartTask("task1", nil)
	err = s.AbortSaga()
	err = s.StartCompensatingTask("task1", nil)

	if err == nil {
		t.Error("Expected StartCompTask to not return an error when write to SagaLog Fails")
	}

	if s.GetState().IsCompTaskStarted("task1") {
		t.Error("Expected task1 to not be completed")
	}
}

func TestEndCompTask(t *testing.T) {
	entry := MakeEndCompTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(MakeAbortSagaMessage("testSaga"))
	sagaLogMock.EXPECT().LogMessage(MakeStartCompTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(entry)

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.StartTask("task1", nil)
	err = s.AbortSaga()
	err = s.StartCompensatingTask("task1", nil)
	err = s.EndCompensatingTask("task1", nil)

	if err != nil {
		t.Error("Expected EndCompensatingTask to not return an error", err)
	}

	if !s.GetState().IsCompTaskCompleted("task1") {
		t.Error("Expected Comp task1 to be completed")
	}
}

func TestEndCompTaskLogError(t *testing.T) {
	entry := MakeEndCompTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(MakeAbortSagaMessage("testSaga"))
	sagaLogMock.EXPECT().LogMessage(MakeStartCompTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndCompTask Message"))

	s, err := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	err = s.StartTask("task1", nil)
	err = s.AbortSaga()
	err = s.StartCompensatingTask("task1", nil)
	err = s.EndCompensatingTask("task1", nil)

	if err == nil {
		t.Error("Expected EndCompTask to not return an error when write to SagaLog Fails")
	}

	if s.GetState().IsCompTaskCompleted("task1") {
		t.Error("Expected task1 to not be completed")
	}
}

func TestMessageAfterEndSagaDoesntPanic(t *testing.T) {
	entry := MakeEndSagaMessage("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry)

	s, _ := newSaga("testSaga", nil, sagaLogMock, stats.NilStatsReceiver())
	_ = s.EndSaga()

	s.StartTask("task1", nil)
}

func TestFatalError_InvalidSagaState(t *testing.T) {
	err := NewInvalidSagaStateError("invalid state")
	if !FatalErr(err) {
		t.Error("Exepected InvalidSagaState to be a FatalE Error")
	}
}

func TestFatalError_InvalidSagaMessage(t *testing.T) {
	err := NewInvalidSagaMessageError("invalid saga message")
	if !FatalErr(err) {
		t.Error("Exepected InvalidSagaState to be a FatalE Error")
	}
}

func TestFatalError_InvalidRequestError(t *testing.T) {
	err := NewInvalidRequestError("invalid request")
	if !FatalErr(err) {
		t.Error("Exepected InvalidRequestError to be a FatalE Error")
	}
}

func TestFatalError_InternalLogError(t *testing.T) {
	err := NewInternalLogError("too busy")
	if FatalErr(err) {
		t.Error("Exepected InternalLogError to not be a FatalE Error")
	}
}

func TestFatalError_CorruptedSagaLogError(t *testing.T) {
	err := NewCorruptedSagaLogError("123", "corrupted sagalog")
	if !FatalErr(err) {
		t.Error("Expected CorruptedSagaLog to be a Fatal Error")
	}
}

func TestSagaStats(t *testing.T) {
	statsReg := stats.NewFinagleStatsRegistry()
	regFn := func() stats.StatsRegistry { return statsReg }
	stat, _ := stats.NewCustomStatsReceiver(regFn, 0)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(MakeEndTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(MakeEndSagaMessage("testSaga"))

	s, err := newSaga("testSaga", nil, sagaLogMock, stat)
	if err != nil {
		t.Error("Expected newSaga to not return an error", err)
	}

	err = s.StartTask("task1", nil)
	if err != nil {
		t.Error("Expected StartTask to not return an error", err)
	}

	err = s.EndTask("task1", nil)
	if err != nil {
		t.Error("Expected EndTask to not return an error", err)
	}

	err = s.EndSaga()
	if err != nil {
		t.Error("Expected EndSaga to not return an error", err)
	}

	if !stats.StatsOk("", statsReg, t,
		map[string]stats.Rule{
			stats.SagaUpdateStateLoopLatency_ms + ".avg":  {Checker: stats.FloatGTTest, Value: 0.0},
			stats.SagaUpdateStateLatency_ms + ".avg":      {Checker: stats.FloatGTTest, Value: 0.0},
			stats.SagaStartOrEndTaskLatency_ms + ".avg":   {Checker: stats.FloatGTTest, Value: 0.0},
			stats.SagaStartOrEndTaskLatency_ms + ".count": {Checker: stats.Int64EqTest, Value: 2},
			stats.SagaNumUpdatesProcessed + ".min":        {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Error("stats check did not pass.")
	}
}
