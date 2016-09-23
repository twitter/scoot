package thrifthelpers

import (
	"github.com/apache/thrift/lib/go/thrift"
)

// Json behavior
func JsonDeserialize(targetStruct thrift.TStruct, sourceBytes []byte) (err error) {
	if len(sourceBytes) == 0 {
		return nil
	}

	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTJSONProtocol(transport)

	d := &thrift.TDeserializer{Transport: transport, Protocol: protocol}
	err = d.Read(targetStruct, sourceBytes)

	return err
}

func JsonSerialize(sourceStruct thrift.TStruct) (b []byte, err error) {
	if sourceStruct == nil {
		return nil, nil
	}

	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTJSONProtocol(transport)

	d := &thrift.TSerializer{Transport: transport, Protocol: protocol}
	serializedValue, err := d.Write(sourceStruct)
	return serializedValue, err
}

// Binary behavior
func BinaryDeserialize(targetStruct thrift.TStruct, sourceBytes []byte) (err error) {
	if len(sourceBytes) == 0 {
		return nil
	}
	d := thrift.NewTDeserializer()
	err = d.Read(targetStruct, sourceBytes)
	return err
}

func BinarySerialize(sourceStruct thrift.TStruct) (b []byte, err error) {
	if sourceStruct == nil {
		return nil, nil
	}
	d := thrift.NewTSerializer()
	serializedValue, err := d.Write(sourceStruct)
	return serializedValue, err
}
