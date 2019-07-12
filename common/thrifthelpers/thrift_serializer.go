// Utils for JSON and binary serialize/deserialize functions for Thrift structures
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
	// NB(dbentley): this seems to have pathological behavior on some strings. E.g.,
	// a random 34 bytes took 45 seconds to decode.
	// This was triggered in job_status_property_test, which tries to deserialize and
	// ~1/20th of random byte slices would take dozens of seconds (or more) to deserialize.
	// Seems to happen consistently on bad inputs, but I didn't dig in to figure out why.
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
