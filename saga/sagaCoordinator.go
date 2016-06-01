package saga

/*
 * Get the State of the specified saga at this current moment.
 * Modifying this pointer does not update the Saga.
 * Returns nil if Saga has not been started, or does not exist.
 * Returns an error if it fails
 */
/*
func GetSagaState(sagaId string) (*SagaState, error) {
  messages, error := s.log.GetMessages(sagaId)

  if error != nil {
    return nil, error
  }

  if len(messages) == 0 {
    return nil, nil
  }

  startmsg := messages[0]
  state, err := SagaStateFactory(startmsg.sagaId, startmsg.data)
  if err != nil {
    return nil, nil
  }

  for _, msg := range messages {
    state, err = updateSagaState(state, msg)
    if err != nil {
      return nil, err
    }
  }

  return state, nil
}*/
/*
 * TODO::: Returns Saga State - Change Name to Startup
 */
/*func (s Saga) Startup(recoveryType SagaRecoveryType) (map[string]*SagaState, error) {

  sagaIds, err := s.log.Startup()
  if err != nil {
    return nil, err
  }

  retSagas := make(map[string]*SagaState)

  for _, sagaId := range sagaIds {

    state, err := s.getSagaState(sagaId)
    if err != nil {
      //This means that the Durable Saga Log is corrupted!  This is really bad
      return nil, err
    }

    //send back a different pointer to the same object that the caller can't mutate
    retSagas[sagaId] = *&state
    s.sagas[sagaId] = state
  }

  return retSagas, nil
}*/

//test cases
/*
func TestGetSagaState_EmptyState(t *testing.T) {
  mockCtrl := gomock.NewController(t)
  defer mockCtrl.Finish()

  sagaLogMock := NewMockSagaLog(mockCtrl)
  sagaLogMock.EXPECT().GetMessages("testSaga").Return(nil, nil)

  s := Saga{
    log: sagaLogMock,
  }

  state, err := s.getSagaState("testSaga")
  if state != nil {
    t.Error("Expected Empty Saga to be Returned")
  }
  if err != nil {
    t.Error("Expected GetSagaState to not return an erorr")
  }
}

func TestGetSagaState_Error(t *testing.T) {
  mockCtrl := gomock.NewController(t)
  defer mockCtrl.Finish()

  sagaLogMock := NewMockSagaLog(mockCtrl)
  sagaLogMock.EXPECT().GetMessages("testSaga").Return(nil, errors.New("InvalidSagaState"))

  s := Saga{
    log: sagaLogMock,
  }

  state, err := s.getSagaState("testSaga")
  if err == nil {
    t.Error("Expected GetSagaState to return an erorr")
  }
  if state != nil {
    t.Error("Expectd SagaState to be nil when error returned")
  }
}

func TestGetSagaState_InProgressSaga(t *testing.T) {
  mockCtrl := gomock.NewController(t)
  defer mockCtrl.Finish()

  sagaLogMock := NewMockSagaLog(mockCtrl)
  sagaLogMock.EXPECT().GetMessages("testSaga").Return(
    []sagaMessage{
      StartSagaMessageFactory("testSaga", nil),
      StartTaskMessageFactory("testSaga", "task1"),
    }, nil)

  s := Saga{
    log: sagaLogMock,
  }

  state, err := s.getSagaState("testSaga")
  if err != nil {
    t.Error("Expected GetSagaState to not return an erorr")
  }
  if state == nil {
    t.Error("Expected GetSagaState to return state for saga with messages")
  }

  if state.IsSagaCompleted() == true {
    t.Error("Expected Saga to not be completed")
  }

  if state.IsSagaAborted() == true {
    t.Error("Expected Saga to not be aborted")
  }

  if state.IsTaskCompleted("task1") == true {
    t.Error("Expected Saga task1 to not be completed")
  }

  if state.IsTaskStarted("task1") != true {
    t.Error("Expected Saga task1 to be started")
  }
}*/
