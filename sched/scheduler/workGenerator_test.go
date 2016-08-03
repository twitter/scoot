package scheduler

// import (
// 	"github.com/golang/mock/gomock"
// 	"github.com/scootdev/scoot/saga"
// 	"github.com/scootdev/scoot/sched"
// 	"github.com/scootdev/scoot/sched/queue"
// 	"testing"
// )

// func TestGenerateWork_ReadsAllItemsFromChannel(t *testing.T) {
// 	mockCtrl := gomock.NewController(t)
// 	defer mockCtrl.Finish()

// 	job := sched.Job{
// 		Id: "job1",
// 	}
// 	workCh := make(chan queue.WorkItem, 1)
// 	workItem := queue.NewMockWorkItem(mockCtrl)
// 	workItem.EXPECT().Job().Return(job)
// 	workItem.EXPECT().Dequeue()
// 	workCh <- workItem
// 	close(workCh)

// 	schedulerMock := NewMockScheduler(mockCtrl)
// 	schedulerMock.EXPECT().ScheduleJob(job).Return(nil)

// 	GenerateWork(schedulerMock, workCh)
// }

// func TestGenerateWork_PanicsFatalError(t *testing.T) {
// 	mockCtrl := gomock.NewController(t)
// 	defer mockCtrl.Finish()

// 	job := sched.Job{
// 		Id: "job1",
// 	}
// 	workCh := make(chan queue.WorkItem, 1)
// 	workItem := queue.NewMockWorkItem(mockCtrl)
// 	workItem.EXPECT().Job().Return(job)
// 	workCh <- workItem
// 	close(workCh)

// 	schedulerMock := NewMockScheduler(mockCtrl)
// 	schedulerMock.EXPECT().ScheduleJob(job).Return(saga.NewInvalidRequestError("test error"))

// 	defer func() {
// 		if r := recover(); r != nil {
// 		}
// 	}()

// 	GenerateWork(schedulerMock, workCh)
// 	t.Errorf("Expected Schedule Job returning a Fatal Error to cause a panic")
// }
