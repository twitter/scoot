package scheduler

import (
	"github.com/scootdev/scoot/saga"
)

type chaosController struct {
	log *interceptorLog
}

func (c *chaosController) SetLogError(err error) {
	c.log.Err = err
}

type interceptorLog struct {
	Err error
	Del saga.SagaLog
}

func (l *interceptorLog) StartSaga(sagaId string, job []byte) error {
	if l.Err != nil {
		return l.Err
	}
	return l.Del.StartSaga(sagaId, job)
}

func (l *interceptorLog) LogMessage(msg saga.Message) error {
	if l.Err != nil {
		return l.Err
	}
	return l.Del.LogMessage(msg)
}

func (l *interceptorLog) GetMessages(sagaId string) ([]saga.Message, error) {
	if l.Err != nil {
		return nil, l.Err
	}
	return l.Del.GetMessages(sagaId)
}

func (l *interceptorLog) GetActiveSagas() ([]string, error) {
	if l.Err != nil {
		return nil, l.Err
	}
	return l.Del.GetActiveSagas()
}
