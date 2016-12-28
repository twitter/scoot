package sagalogs

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/scootdev/scoot/saga"
)

// Writes Saga Log to file system.  Not durable beyond machine failure
// Sagas are stored in a directory.  Each saga has a corresponding directory
// each saga directory contains a log file, and associated data files.

// StartSaga Message
// StartSaga \n
// job data filename \n

// StartTask Message
// StartTask \n
// taskId \n
// taskData filename \n

// EndTask Message
// EndTask \n
// taskId \n
// taskData filename \n

// AbortSaga Message
// AbortSaga \n

// StartCompTask Message
// StartCompTask \n
// taskId \n
// taskData filename \n

// EndCompTask Message
// EndComptTask \n
// taskId \n
// taskData filename \n

// EndSaga Message
// EndSaga
type fileSagaLog struct {
	dirName string
}

// Creates a FileSagaLog with files stored at the specified directory
// If the directory does not exist it will create it.
func MakeFileSagaLog(dirName string) (*fileSagaLog, error) {

	if _, err := os.Stat(dirName); err != nil {
		if os.IsNotExist(err) {
			os.Mkdir(dirName, os.ModePerm)
		} else {
			return nil, err
		}
	}

	return &fileSagaLog{
		dirName: dirName,
	}, nil
}

// all files for a saga log are stored in a directory named
// by the specified sagaId.
func (log *fileSagaLog) getSagaDirectory(sagaId string) string {
	return fmt.Sprintf("%v/%v", log.dirName, sagaId)
}

// Returns the name of the sagalog for the specified file.
func (log *fileSagaLog) getSagaLogFileName(sagaId string) string {
	return fmt.Sprintf("%v/%v", log.getSagaDirectory(sagaId), "log")
}

// Returns the name of the file to store task data in
// SagaId for the saga
// TaskId corresponding to taskdata
// SagaMessageType type of SagaMessage
func (log *fileSagaLog) createTaskDataFileName(
	sagaId string,
	taskId string,
	msgType saga.SagaMessageType) string {

	//format sagaDir/MsgType_taskId_data_timestamp
	return fmt.Sprintf("%v/%v_%v_data_%v",
		log.getSagaDirectory(sagaId),
		msgType.String(),
		taskId,
		time.Now().Format(time.StampMilli),
	)
}

func (log *fileSagaLog) createJobDataFileName(sagaId string) string {
	//format sagaDir/StartSagaData_timestamp
	return fmt.Sprintf(
		"%v/StartSagaData_%v",
		log.getSagaDirectory(sagaId),
		time.Now().Format(time.StampNano),
	)
}

//TODO: all the writers, write partial messages.  Ideally
// we'd like the write to succeed or not, if something goes wrong
// then we may have part of a message written.
// Create message to write, and only do oen write

// Log a Start Saga Message message to the log.
// Returns an error if it fails.
func (log *fileSagaLog) StartSaga(sagaId string, job []byte) error {

	// Create directory for this saga if it doesn't exist
	dirName := log.getSagaDirectory(sagaId)
	if _, err := os.Stat(dirName); err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(dirName, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	// Write Data File
	dataFileName := log.createJobDataFileName(sagaId)
	err := ioutil.WriteFile(dataFileName, job, os.ModePerm)
	if err != nil {
		return err
	}

	// Write Message to Log File
	var logFile *os.File
	defer logFile.Close()
	logFileName := log.getSagaLogFileName(sagaId)

	// Append StartSaga message to the log
	// Get File Handle for Saga Create it if it doesn't exist
	if _, err = os.Stat(logFileName); err != nil {
		if os.IsNotExist(err) {
			logFile, err = os.Create(logFileName)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		logFile, err = os.OpenFile(logFileName, os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil {
			return err
		}
	}

	// write log message
	msg := []byte(fmt.Sprintf("%v\n%v\n",
		saga.StartSaga.String(),
		dataFileName))

	_, err = logFile.Write(msg)
	if err != nil {
		return err
	}

	logFile.Sync()
	return nil
}

// Update the State of the Saga by Logging a message.
// Returns an error if it fails.
func (log *fileSagaLog) LogMessage(message saga.SagaMessage) error {
	fileName := log.getSagaLogFileName(message.SagaId)

	// Get file handle for Saga if it doesn't exist return error,
	// Saga wasn't started.  OpenFile so we can append to it
	logFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, os.ModePerm)
	defer logFile.Close()
	if err != nil {
		return err
	}

	// Write MessageType
	msg := []byte(fmt.Sprintf("%v\n", message.MsgType.String()))

	// If its a Task Type Write the TaskId and Data
	if message.MsgType == saga.StartTask ||
		message.MsgType == saga.EndTask ||
		message.MsgType == saga.StartCompTask ||
		message.MsgType == saga.EndCompTask {

		// write task data to file
		dataFileName := log.createTaskDataFileName(
			message.SagaId, message.TaskId, message.MsgType)
		err = ioutil.WriteFile(dataFileName, message.Data, os.ModePerm)
		if err != nil {
			return err
		}

		// update log message
		msg = append(msg, []byte(
			fmt.Sprintf("%v\n%v\n",
				message.TaskId,
				dataFileName))...)
	}

	_, err = logFile.Write(msg)
	if err != nil {
		return err
	}

	logFile.Sync()
	return nil
}

// Returns all of the messages logged so far for the
// specified saga.
func (log *fileSagaLog) GetMessages(sagaId string) ([]saga.SagaMessage, error) {
	fileName := log.getSagaLogFileName(sagaId)
	logFile, err := os.Open(fmt.Sprintf(fileName))
	if err != nil {
		return nil, err
	}
	defer logFile.Close()

	msgs := make([]saga.SagaMessage, 0)
	scanner := bufio.NewScanner(logFile)
	nextToken := scanner.Scan()

	for nextToken == true {
		msg, err := parseMessage(sagaId, scanner)
		if err != nil {
			return nil, err
		}

		msgs = append(msgs, msg)
		nextToken = scanner.Scan()
	}

	return msgs, nil
}

// Helper Function that Parses a SagaMessage.  Returns a message if succesfully parsed
// Returns and error otherwise
func parseMessage(sagaId string, scanner *bufio.Scanner) (saga.SagaMessage, error) {

	switch scanner.Text() {

	// Parse Start Saga Message
	case saga.StartSaga.String():
		if ok := scanner.Scan(); !ok {
			return saga.SagaMessage{}, saga.NewCorruptedSagaLogError(
				sagaId,
				fmt.Sprintf("Error Parsing SagaLog expected Data after StartSaga message.  Error: %v",
					createUnexpectedScanEndMsg(scanner)),
			)
		}
		dataFileName := scanner.Text()
		data, err := ioutil.ReadFile(dataFileName)
		if err != nil {
			return saga.SagaMessage{},
				saga.NewCorruptedSagaLogError(
					sagaId,
					fmt.Sprintf("Error Reading DataFile %v, Error: %v", dataFileName, err),
				)
		}

		return saga.MakeStartSagaMessage(sagaId, data), nil

		// Parse End Saga Message
	case saga.EndSaga.String():
		return saga.MakeEndSagaMessage(sagaId), nil

		// Parse Abort Saga Message
	case saga.AbortSaga.String():
		return saga.MakeAbortSagaMessage(sagaId), nil

		// Parse Start Task Message
	case saga.StartTask.String():
		taskId, data, err := parseTask(sagaId, scanner)
		if err != nil {
			return saga.SagaMessage{}, err
		}
		return saga.MakeStartTaskMessage(sagaId, taskId, data), nil

		// Parse End Task Message
	case saga.EndTask.String():
		taskId, data, err := parseTask(sagaId, scanner)
		if err != nil {
			return saga.SagaMessage{}, err
		}
		return saga.MakeEndTaskMessage(sagaId, taskId, data), nil

		// Parse Start Comp Task Message
	case saga.StartCompTask.String():
		taskId, data, err := parseTask(sagaId, scanner)
		if err != nil {
			return saga.SagaMessage{}, err
		}
		return saga.MakeStartCompTaskMessage(sagaId, taskId, data), nil

		// Parse End Comp Task Message
	case saga.EndCompTask.String():
		taskId, data, err := parseTask(sagaId, scanner)
		if err != nil {
			return saga.SagaMessage{}, err
		}
		return saga.MakeEndCompTaskMessage(sagaId, taskId, data), nil

		// Unrecognized Message
	default:
		return saga.SagaMessage{}, saga.NewCorruptedSagaLogError(
			sagaId,
			fmt.Sprintf("Error Parsing SagaLog unrecognized message type, %v", scanner.Text()),
		)
	}
}

// Helper function that parses a task message, StartTask, EndTask, StartCompTask,
// EndCompTasks.  Message is of structure
// line1: MessageType
// line2: TaskId
// line3: TaskData FileName
// Returns a tuple of TaskId, TaskData, Error
func parseTask(sagaId string, scanner *bufio.Scanner) (string, []byte, error) {
	// read taskId and datafileName
	if ok := scanner.Scan(); !ok {
		return "", nil,
			saga.NewCorruptedSagaLogError(
				sagaId,
				fmt.Sprintf("Error Parsing SagaLog expected TaskId, Error: %v",
					createUnexpectedScanEndMsg(scanner)),
			)
	}
	taskId := scanner.Text()
	if ok := scanner.Scan(); !ok {
		return "", nil,
			saga.NewCorruptedSagaLogError(
				sagaId,
				fmt.Sprintf("Error Parsing SagaLog expected Data, Error: %v",
					createUnexpectedScanEndMsg(scanner)),
			)
	}
	dataFileName := scanner.Text()

	data, err := ioutil.ReadFile(dataFileName)
	if err != nil {
		return "", nil,
			saga.NewCorruptedSagaLogError(
				sagaId,
				fmt.Sprintf("Error Reading DataFile %v, Error: %v", dataFileName, err),
			)
	}

	return taskId, data, nil
}

func createUnexpectedScanEndMsg(scanner *bufio.Scanner) string {
	var errMsg string
	if scanner.Err() != nil {
		errMsg = scanner.Err().Error()
	} else {
		errMsg = "Unexpected EOF"
	}
	return errMsg
}

//
// Returns a list of all in progress sagaIds.
// This MUST include all not completed sagaIds.
// It may also included completed sagas
// Returns an error if it fails.
//
// This is a very dumb implementation currently just returns
// a list of all sagas.  This can be improved with an index
//
func (log *fileSagaLog) GetActiveSagas() ([]string, error) {
	files, err := ioutil.ReadDir(log.dirName)
	if err != nil {
		return nil, err
	}

	sagaIds := make([]string, len(files), len(files))
	for i, file := range files {
		sagaIds[i] = file.Name()
	}

	return sagaIds, nil
}
