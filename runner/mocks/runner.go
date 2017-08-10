// Automatically generated by MockGen. DO NOT EDIT!
// Source: github.com/twitter/scoot/runner (interfaces: Service)

package mocks

import (
	gomock "github.com/golang/mock/gomock"
	runner "github.com/twitter/scoot/runner"
)

// Mock of Service interface
type MockService struct {
	ctrl     *gomock.Controller
	recorder *_MockServiceRecorder
}

// Recorder for MockService (not exported)
type _MockServiceRecorder struct {
	mock *MockService
}

func NewMockService(ctrl *gomock.Controller) *MockService {
	mock := &MockService{ctrl: ctrl}
	mock.recorder = &_MockServiceRecorder{mock}
	return mock
}

func (_m *MockService) EXPECT() *_MockServiceRecorder {
	return _m.recorder
}

func (_m *MockService) Abort(_param0 runner.RunID) (runner.RunStatus, error) {
	ret := _m.ctrl.Call(_m, "Abort", _param0)
	ret0, _ := ret[0].(runner.RunStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServiceRecorder) Abort(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Abort", arg0)
}

func (_m *MockService) Erase(_param0 runner.RunID) error {
	ret := _m.ctrl.Call(_m, "Erase", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockServiceRecorder) Erase(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Erase", arg0)
}

func (_m *MockService) Query(_param0 runner.Query, _param1 runner.Wait) ([]runner.RunStatus, runner.ServiceStatus, error) {
	ret := _m.ctrl.Call(_m, "Query", _param0, _param1)
	ret0, _ := ret[0].([]runner.RunStatus)
	ret1, _ := ret[1].(runner.ServiceStatus)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockServiceRecorder) Query(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Query", arg0, arg1)
}

func (_m *MockService) QueryNow(_param0 runner.Query) ([]runner.RunStatus, runner.ServiceStatus, error) {
	ret := _m.ctrl.Call(_m, "QueryNow", _param0)
	ret0, _ := ret[0].([]runner.RunStatus)
	ret1, _ := ret[1].(runner.ServiceStatus)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockServiceRecorder) QueryNow(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "QueryNow", arg0)
}

func (_m *MockService) Release() {
	_m.ctrl.Call(_m, "Release")
}

func (_mr *_MockServiceRecorder) Release() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Release")
}

func (_m *MockService) Run(_param0 *runner.Command) (runner.RunStatus, error) {
	ret := _m.ctrl.Call(_m, "Run", _param0)
	ret0, _ := ret[0].(runner.RunStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockServiceRecorder) Run(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Run", arg0)
}

func (_m *MockService) Status(_param0 runner.RunID) (runner.RunStatus, runner.ServiceStatus, error) {
	ret := _m.ctrl.Call(_m, "Status", _param0)
	ret0, _ := ret[0].(runner.RunStatus)
	ret1, _ := ret[1].(runner.ServiceStatus)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockServiceRecorder) Status(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Status", arg0)
}

func (_m *MockService) StatusAll() ([]runner.RunStatus, runner.ServiceStatus, error) {
	ret := _m.ctrl.Call(_m, "StatusAll")
	ret0, _ := ret[0].([]runner.RunStatus)
	ret1, _ := ret[1].(runner.ServiceStatus)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (_mr *_MockServiceRecorder) StatusAll() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "StatusAll")
}
