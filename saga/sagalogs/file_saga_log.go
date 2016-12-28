package sagalogs

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/scootdev/scoot/saga"
)

// Writes Saga Log to file system.  Not durable beyond machine failure
// Sagas are stored in a directory.  Each file contains the log of
// one saga.

// StartSaga Message
// StartSaga \n
// job data \n

// StartTask Message
// StartTask \n
// taskId \n
// taskData \n

// EndTask Message
// EndTask \n
// taskId \n
// taskData \n

// AbortSaga Message
// AbortSaga \n

// StartCompTask Message
// StartCompTask \n
// taskId \n
// taskData \n

// EndCompTask Message
// EndComptTask \n
// taskId \n
// taskData \n

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
			os.Mkdir(dirName, 0777)
		} else {
			return nil, err
		}
	}

	return &fileSagaLog{
		dirName: dirName,
	}, nil
}

func (log *fileSagaLog) getFileName(sagaId string) string {
	return fmt.Sprintf("%v/%v", log.dirName, sagaId)
}

//TODO: all the writers, write partial messages.  Ideally
// we'd like the write to succeed or not, if something goes wrong
// then we may have part of a message written.
// Create message to write, and only do oen write

// Log a Start Saga Message message to the log.
// Returns an error if it fails.
func (log *fileSagaLog) StartSaga(sagaId string, job []byte) error {

	var file *os.File
	var err error
	fileName := log.getFileName(sagaId)

	// Get File Handle for Saga Create it if it doesn't exist
	if _, err = os.Stat(fileName); err != nil {
		if os.IsNotExist(err) {
			file, err = os.Create(fileName)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	} else {
		fmt.Println("Opening Already Existing File")
		file, err = os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0777)
		if err != nil {
			return err
		}
	}
	defer file.Close()

	msg := append([]byte(fmt.Sprintf("%v\n", saga.StartSaga.String())), job...)
	msg = append(msg, []byte("\n")...)

	_, err = file.Write(msg)
	if err != nil {
		return err
	}

	file.Sync()
	return nil
}

//
// Update the State of the Saga by Logging a message.
// Returns an error if it fails.
//
func (log *fileSagaLog) LogMessage(message saga.SagaMessage) error {
	var file *os.File
	var err error
	fileName := log.getFileName(message.SagaId)

	// Get file handle for Saga if it doesn't exist return error,
	// Saga wasn't started
	if _, err = os.Stat(fileName); err != nil {
		return err
	} else {
		file, err = os.OpenFile(fileName, os.O_APPEND|os.O_RDWR, 0777)
		if err != nil {
			return err
		}
	}
	defer file.Close()

	// Write MessageType
	msg := []byte(fmt.Sprintf("%v\n", message.MsgType.String()))

	// If its a Task Type Write the TaskId and Data
	if message.MsgType == saga.StartTask ||
		message.MsgType == saga.EndTask ||
		message.MsgType == saga.StartCompTask ||
		message.MsgType == saga.EndCompTask {

		msg = append(msg, []byte(fmt.Sprintf("%v\n", message.TaskId))...)
		msg = append(msg, message.Data...)
		msg = append(msg, []byte("\n")...)
	}

	_, err = file.Write(msg)
	if err != nil {
		return err
	}

	file.Sync()
	return nil
}

//
// Returns all of the messages logged so far for the
// specified saga.
//
func (log *fileSagaLog) GetMessages(sagaId string) ([]saga.SagaMessage, error) {
	fileName := log.getFileName(sagaId)
	fmt.Printf("FileName: %v", fileName)
	file, err := os.Open(fmt.Sprintf(fileName))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	msgs := make([]saga.SagaMessage, 0)
	scanner := bufio.NewScanner(file)
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

	fmt.Println("Parse Message of Type:", scanner.Text())
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
		return saga.MakeStartSagaMessage(sagaId, scanner.Bytes()), nil

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
// line3: TaskData
// Returns a tuple of TaskId, TaskData, Error
func parseTask(sagaId string, scanner *bufio.Scanner) (string, []byte, error) {
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
	data := scanner.Bytes()

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
