# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: daemon.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='daemon.proto',
  package='protocol',
  syntax='proto3',
  serialized_pb=_b('\n\x0c\x64\x61\x65mon.proto\x12\x08protocol\"\x1b\n\x0b\x45\x63hoRequest\x12\x0c\n\x04ping\x18\x01 \x01(\t\"\x19\n\tEchoReply\x12\x0c\n\x04pong\x18\x01 \x01(\t\"%\n\x15\x43reateSnapshotRequest\x12\x0c\n\x04path\x18\x01 \x01(\t\"9\n\x13\x43reateSnapshotReply\x12\r\n\x05\x65rror\x18\x01 \x01(\t\x12\x13\n\x0bsnapshot_id\x18\x02 \x01(\t\";\n\x17\x43heckoutSnapshotRequest\x12\x13\n\x0bsnapshot_id\x18\x01 \x01(\t\x12\x0b\n\x03\x64ir\x18\x02 \x01(\t\"&\n\x15\x43heckoutSnapshotReply\x12\r\n\x05\x65rror\x18\x01 \x01(\t\"\xac\x03\n\nRunRequest\x12\x13\n\x0bsnapshot_id\x18\x01 \x01(\t\x12)\n\x03\x63md\x18\x02 \x01(\x0b\x32\x1c.protocol.RunRequest.Command\x12-\n\x04plan\x18\x03 \x01(\x0b\x32\x1f.protocol.RunRequest.OutputPlan\x1a\x8b\x01\n\x07\x43ommand\x12\x0c\n\x04\x61rgv\x18\x01 \x03(\t\x12\x32\n\x03\x65nv\x18\x02 \x03(\x0b\x32%.protocol.RunRequest.Command.EnvEntry\x12\x12\n\ntimeout_ns\x18\x03 \x01(\x03\x1a*\n\x08\x45nvEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a\xa0\x01\n\nOutputPlan\x12W\n\x16src_paths_to_dest_dirs\x18\x03 \x03(\x0b\x32\x37.protocol.RunRequest.OutputPlan.SrcPathsToDestDirsEntry\x1a\x39\n\x17SrcPathsToDestDirsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\")\n\x08RunReply\x12\x0e\n\x06run_id\x18\x01 \x01(\t\x12\r\n\x05\x65rror\x18\x02 \x01(\t\"?\n\x0bPollRequest\x12\x0f\n\x07run_ids\x18\x01 \x03(\t\x12\x12\n\ntimeout_ns\x18\x02 \x01(\x03\x12\x0b\n\x03\x61ll\x18\x03 \x01(\x08\"\x94\x02\n\tPollReply\x12*\n\x06status\x18\x01 \x03(\x0b\x32\x1a.protocol.PollReply.Status\x1a\xda\x01\n\x06Status\x12\x0e\n\x06run_id\x18\x01 \x01(\t\x12/\n\x05state\x18\x02 \x01(\x0e\x32 .protocol.PollReply.Status.State\x12\x13\n\x0bsnapshot_id\x18\x03 \x01(\t\x12\x11\n\texit_code\x18\x04 \x01(\x05\x12\r\n\x05\x65rror\x18\x05 \x01(\t\"X\n\x05State\x12\x0b\n\x07UNKNOWN\x10\x00\x12\x0b\n\x07PENDING\x10\x01\x12\r\n\tPREPARING\x10\x02\x12\x0b\n\x07RUNNING\x10\x03\x12\r\n\tCOMPLETED\x10\x04\x12\n\n\x06\x46\x41ILED\x10\x05\x32\xda\x02\n\x0bScootDaemon\x12\x34\n\x04\x45\x63ho\x12\x15.protocol.EchoRequest\x1a\x13.protocol.EchoReply\"\x00\x12R\n\x0e\x43reateSnapshot\x12\x1f.protocol.CreateSnapshotRequest\x1a\x1d.protocol.CreateSnapshotReply\"\x00\x12X\n\x10\x43heckoutSnapshot\x12!.protocol.CheckoutSnapshotRequest\x1a\x1f.protocol.CheckoutSnapshotReply\"\x00\x12\x31\n\x03Run\x12\x14.protocol.RunRequest\x1a\x12.protocol.RunReply\"\x00\x12\x34\n\x04Poll\x12\x15.protocol.PollRequest\x1a\x13.protocol.PollReply\"\x00\x62\x06proto3')
)
_sym_db.RegisterFileDescriptor(DESCRIPTOR)



_POLLREPLY_STATUS_STATE = _descriptor.EnumDescriptor(
  name='State',
  full_name='protocol.PollReply.Status.State',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='UNKNOWN', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='PENDING', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='PREPARING', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='RUNNING', index=3, number=3,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='COMPLETED', index=4, number=4,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='FAILED', index=5, number=5,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=1009,
  serialized_end=1097,
)
_sym_db.RegisterEnumDescriptor(_POLLREPLY_STATUS_STATE)


_ECHOREQUEST = _descriptor.Descriptor(
  name='EchoRequest',
  full_name='protocol.EchoRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='ping', full_name='protocol.EchoRequest.ping', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=26,
  serialized_end=53,
)


_ECHOREPLY = _descriptor.Descriptor(
  name='EchoReply',
  full_name='protocol.EchoReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='pong', full_name='protocol.EchoReply.pong', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=55,
  serialized_end=80,
)


_CREATESNAPSHOTREQUEST = _descriptor.Descriptor(
  name='CreateSnapshotRequest',
  full_name='protocol.CreateSnapshotRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='path', full_name='protocol.CreateSnapshotRequest.path', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=82,
  serialized_end=119,
)


_CREATESNAPSHOTREPLY = _descriptor.Descriptor(
  name='CreateSnapshotReply',
  full_name='protocol.CreateSnapshotReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='error', full_name='protocol.CreateSnapshotReply.error', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='snapshot_id', full_name='protocol.CreateSnapshotReply.snapshot_id', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=121,
  serialized_end=178,
)


_CHECKOUTSNAPSHOTREQUEST = _descriptor.Descriptor(
  name='CheckoutSnapshotRequest',
  full_name='protocol.CheckoutSnapshotRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='snapshot_id', full_name='protocol.CheckoutSnapshotRequest.snapshot_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='dir', full_name='protocol.CheckoutSnapshotRequest.dir', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=180,
  serialized_end=239,
)


_CHECKOUTSNAPSHOTREPLY = _descriptor.Descriptor(
  name='CheckoutSnapshotReply',
  full_name='protocol.CheckoutSnapshotReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='error', full_name='protocol.CheckoutSnapshotReply.error', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=241,
  serialized_end=279,
)


_RUNREQUEST_COMMAND_ENVENTRY = _descriptor.Descriptor(
  name='EnvEntry',
  full_name='protocol.RunRequest.Command.EnvEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='protocol.RunRequest.Command.EnvEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='value', full_name='protocol.RunRequest.Command.EnvEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=_descriptor._ParseOptions(descriptor_pb2.MessageOptions(), _b('8\001')),
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=505,
  serialized_end=547,
)

_RUNREQUEST_COMMAND = _descriptor.Descriptor(
  name='Command',
  full_name='protocol.RunRequest.Command',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='argv', full_name='protocol.RunRequest.Command.argv', index=0,
      number=1, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='env', full_name='protocol.RunRequest.Command.env', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='timeout_ns', full_name='protocol.RunRequest.Command.timeout_ns', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_RUNREQUEST_COMMAND_ENVENTRY, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=408,
  serialized_end=547,
)

_RUNREQUEST_OUTPUTPLAN_SRCPATHSTODESTDIRSENTRY = _descriptor.Descriptor(
  name='SrcPathsToDestDirsEntry',
  full_name='protocol.RunRequest.OutputPlan.SrcPathsToDestDirsEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='protocol.RunRequest.OutputPlan.SrcPathsToDestDirsEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='value', full_name='protocol.RunRequest.OutputPlan.SrcPathsToDestDirsEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=_descriptor._ParseOptions(descriptor_pb2.MessageOptions(), _b('8\001')),
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=653,
  serialized_end=710,
)

_RUNREQUEST_OUTPUTPLAN = _descriptor.Descriptor(
  name='OutputPlan',
  full_name='protocol.RunRequest.OutputPlan',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='src_paths_to_dest_dirs', full_name='protocol.RunRequest.OutputPlan.src_paths_to_dest_dirs', index=0,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_RUNREQUEST_OUTPUTPLAN_SRCPATHSTODESTDIRSENTRY, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=550,
  serialized_end=710,
)

_RUNREQUEST = _descriptor.Descriptor(
  name='RunRequest',
  full_name='protocol.RunRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='snapshot_id', full_name='protocol.RunRequest.snapshot_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='cmd', full_name='protocol.RunRequest.cmd', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='plan', full_name='protocol.RunRequest.plan', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_RUNREQUEST_COMMAND, _RUNREQUEST_OUTPUTPLAN, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=282,
  serialized_end=710,
)


_RUNREPLY = _descriptor.Descriptor(
  name='RunReply',
  full_name='protocol.RunReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='run_id', full_name='protocol.RunReply.run_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='error', full_name='protocol.RunReply.error', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=712,
  serialized_end=753,
)


_POLLREQUEST = _descriptor.Descriptor(
  name='PollRequest',
  full_name='protocol.PollRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='run_ids', full_name='protocol.PollRequest.run_ids', index=0,
      number=1, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='timeout_ns', full_name='protocol.PollRequest.timeout_ns', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='all', full_name='protocol.PollRequest.all', index=2,
      number=3, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=755,
  serialized_end=818,
)


_POLLREPLY_STATUS = _descriptor.Descriptor(
  name='Status',
  full_name='protocol.PollReply.Status',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='run_id', full_name='protocol.PollReply.Status.run_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='state', full_name='protocol.PollReply.Status.state', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='snapshot_id', full_name='protocol.PollReply.Status.snapshot_id', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='exit_code', full_name='protocol.PollReply.Status.exit_code', index=3,
      number=4, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='error', full_name='protocol.PollReply.Status.error', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _POLLREPLY_STATUS_STATE,
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=879,
  serialized_end=1097,
)

_POLLREPLY = _descriptor.Descriptor(
  name='PollReply',
  full_name='protocol.PollReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='status', full_name='protocol.PollReply.status', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_POLLREPLY_STATUS, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=821,
  serialized_end=1097,
)

_RUNREQUEST_COMMAND_ENVENTRY.containing_type = _RUNREQUEST_COMMAND
_RUNREQUEST_COMMAND.fields_by_name['env'].message_type = _RUNREQUEST_COMMAND_ENVENTRY
_RUNREQUEST_COMMAND.containing_type = _RUNREQUEST
_RUNREQUEST_OUTPUTPLAN_SRCPATHSTODESTDIRSENTRY.containing_type = _RUNREQUEST_OUTPUTPLAN
_RUNREQUEST_OUTPUTPLAN.fields_by_name['src_paths_to_dest_dirs'].message_type = _RUNREQUEST_OUTPUTPLAN_SRCPATHSTODESTDIRSENTRY
_RUNREQUEST_OUTPUTPLAN.containing_type = _RUNREQUEST
_RUNREQUEST.fields_by_name['cmd'].message_type = _RUNREQUEST_COMMAND
_RUNREQUEST.fields_by_name['plan'].message_type = _RUNREQUEST_OUTPUTPLAN
_POLLREPLY_STATUS.fields_by_name['state'].enum_type = _POLLREPLY_STATUS_STATE
_POLLREPLY_STATUS.containing_type = _POLLREPLY
_POLLREPLY_STATUS_STATE.containing_type = _POLLREPLY_STATUS
_POLLREPLY.fields_by_name['status'].message_type = _POLLREPLY_STATUS
DESCRIPTOR.message_types_by_name['EchoRequest'] = _ECHOREQUEST
DESCRIPTOR.message_types_by_name['EchoReply'] = _ECHOREPLY
DESCRIPTOR.message_types_by_name['CreateSnapshotRequest'] = _CREATESNAPSHOTREQUEST
DESCRIPTOR.message_types_by_name['CreateSnapshotReply'] = _CREATESNAPSHOTREPLY
DESCRIPTOR.message_types_by_name['CheckoutSnapshotRequest'] = _CHECKOUTSNAPSHOTREQUEST
DESCRIPTOR.message_types_by_name['CheckoutSnapshotReply'] = _CHECKOUTSNAPSHOTREPLY
DESCRIPTOR.message_types_by_name['RunRequest'] = _RUNREQUEST
DESCRIPTOR.message_types_by_name['RunReply'] = _RUNREPLY
DESCRIPTOR.message_types_by_name['PollRequest'] = _POLLREQUEST
DESCRIPTOR.message_types_by_name['PollReply'] = _POLLREPLY

EchoRequest = _reflection.GeneratedProtocolMessageType('EchoRequest', (_message.Message,), dict(
  DESCRIPTOR = _ECHOREQUEST,
  __module__ = 'daemon_pb2'
  # @@protoc_insertion_point(class_scope:protocol.EchoRequest)
  ))
_sym_db.RegisterMessage(EchoRequest)

EchoReply = _reflection.GeneratedProtocolMessageType('EchoReply', (_message.Message,), dict(
  DESCRIPTOR = _ECHOREPLY,
  __module__ = 'daemon_pb2'
  # @@protoc_insertion_point(class_scope:protocol.EchoReply)
  ))
_sym_db.RegisterMessage(EchoReply)

CreateSnapshotRequest = _reflection.GeneratedProtocolMessageType('CreateSnapshotRequest', (_message.Message,), dict(
  DESCRIPTOR = _CREATESNAPSHOTREQUEST,
  __module__ = 'daemon_pb2'
  # @@protoc_insertion_point(class_scope:protocol.CreateSnapshotRequest)
  ))
_sym_db.RegisterMessage(CreateSnapshotRequest)

CreateSnapshotReply = _reflection.GeneratedProtocolMessageType('CreateSnapshotReply', (_message.Message,), dict(
  DESCRIPTOR = _CREATESNAPSHOTREPLY,
  __module__ = 'daemon_pb2'
  # @@protoc_insertion_point(class_scope:protocol.CreateSnapshotReply)
  ))
_sym_db.RegisterMessage(CreateSnapshotReply)

CheckoutSnapshotRequest = _reflection.GeneratedProtocolMessageType('CheckoutSnapshotRequest', (_message.Message,), dict(
  DESCRIPTOR = _CHECKOUTSNAPSHOTREQUEST,
  __module__ = 'daemon_pb2'
  # @@protoc_insertion_point(class_scope:protocol.CheckoutSnapshotRequest)
  ))
_sym_db.RegisterMessage(CheckoutSnapshotRequest)

CheckoutSnapshotReply = _reflection.GeneratedProtocolMessageType('CheckoutSnapshotReply', (_message.Message,), dict(
  DESCRIPTOR = _CHECKOUTSNAPSHOTREPLY,
  __module__ = 'daemon_pb2'
  # @@protoc_insertion_point(class_scope:protocol.CheckoutSnapshotReply)
  ))
_sym_db.RegisterMessage(CheckoutSnapshotReply)

RunRequest = _reflection.GeneratedProtocolMessageType('RunRequest', (_message.Message,), dict(

  Command = _reflection.GeneratedProtocolMessageType('Command', (_message.Message,), dict(

    EnvEntry = _reflection.GeneratedProtocolMessageType('EnvEntry', (_message.Message,), dict(
      DESCRIPTOR = _RUNREQUEST_COMMAND_ENVENTRY,
      __module__ = 'daemon_pb2'
      # @@protoc_insertion_point(class_scope:protocol.RunRequest.Command.EnvEntry)
      ))
    ,
    DESCRIPTOR = _RUNREQUEST_COMMAND,
    __module__ = 'daemon_pb2'
    # @@protoc_insertion_point(class_scope:protocol.RunRequest.Command)
    ))
  ,

  OutputPlan = _reflection.GeneratedProtocolMessageType('OutputPlan', (_message.Message,), dict(

    SrcPathsToDestDirsEntry = _reflection.GeneratedProtocolMessageType('SrcPathsToDestDirsEntry', (_message.Message,), dict(
      DESCRIPTOR = _RUNREQUEST_OUTPUTPLAN_SRCPATHSTODESTDIRSENTRY,
      __module__ = 'daemon_pb2'
      # @@protoc_insertion_point(class_scope:protocol.RunRequest.OutputPlan.SrcPathsToDestDirsEntry)
      ))
    ,
    DESCRIPTOR = _RUNREQUEST_OUTPUTPLAN,
    __module__ = 'daemon_pb2'
    # @@protoc_insertion_point(class_scope:protocol.RunRequest.OutputPlan)
    ))
  ,
  DESCRIPTOR = _RUNREQUEST,
  __module__ = 'daemon_pb2'
  # @@protoc_insertion_point(class_scope:protocol.RunRequest)
  ))
_sym_db.RegisterMessage(RunRequest)
_sym_db.RegisterMessage(RunRequest.Command)
_sym_db.RegisterMessage(RunRequest.Command.EnvEntry)
_sym_db.RegisterMessage(RunRequest.OutputPlan)
_sym_db.RegisterMessage(RunRequest.OutputPlan.SrcPathsToDestDirsEntry)

RunReply = _reflection.GeneratedProtocolMessageType('RunReply', (_message.Message,), dict(
  DESCRIPTOR = _RUNREPLY,
  __module__ = 'daemon_pb2'
  # @@protoc_insertion_point(class_scope:protocol.RunReply)
  ))
_sym_db.RegisterMessage(RunReply)

PollRequest = _reflection.GeneratedProtocolMessageType('PollRequest', (_message.Message,), dict(
  DESCRIPTOR = _POLLREQUEST,
  __module__ = 'daemon_pb2'
  # @@protoc_insertion_point(class_scope:protocol.PollRequest)
  ))
_sym_db.RegisterMessage(PollRequest)

PollReply = _reflection.GeneratedProtocolMessageType('PollReply', (_message.Message,), dict(

  Status = _reflection.GeneratedProtocolMessageType('Status', (_message.Message,), dict(
    DESCRIPTOR = _POLLREPLY_STATUS,
    __module__ = 'daemon_pb2'
    # @@protoc_insertion_point(class_scope:protocol.PollReply.Status)
    ))
  ,
  DESCRIPTOR = _POLLREPLY,
  __module__ = 'daemon_pb2'
  # @@protoc_insertion_point(class_scope:protocol.PollReply)
  ))
_sym_db.RegisterMessage(PollReply)
_sym_db.RegisterMessage(PollReply.Status)


_RUNREQUEST_COMMAND_ENVENTRY.has_options = True
_RUNREQUEST_COMMAND_ENVENTRY._options = _descriptor._ParseOptions(descriptor_pb2.MessageOptions(), _b('8\001'))
_RUNREQUEST_OUTPUTPLAN_SRCPATHSTODESTDIRSENTRY.has_options = True
_RUNREQUEST_OUTPUTPLAN_SRCPATHSTODESTDIRSENTRY._options = _descriptor._ParseOptions(descriptor_pb2.MessageOptions(), _b('8\001'))
import grpc
from grpc.beta import implementations as beta_implementations
from grpc.beta import interfaces as beta_interfaces
from grpc.framework.common import cardinality
from grpc.framework.interfaces.face import utilities as face_utilities


class ScootDaemonStub(object):

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.Echo = channel.unary_unary(
        '/protocol.ScootDaemon/Echo',
        request_serializer=EchoRequest.SerializeToString,
        response_deserializer=EchoReply.FromString,
        )
    self.CreateSnapshot = channel.unary_unary(
        '/protocol.ScootDaemon/CreateSnapshot',
        request_serializer=CreateSnapshotRequest.SerializeToString,
        response_deserializer=CreateSnapshotReply.FromString,
        )
    self.CheckoutSnapshot = channel.unary_unary(
        '/protocol.ScootDaemon/CheckoutSnapshot',
        request_serializer=CheckoutSnapshotRequest.SerializeToString,
        response_deserializer=CheckoutSnapshotReply.FromString,
        )
    self.Run = channel.unary_unary(
        '/protocol.ScootDaemon/Run',
        request_serializer=RunRequest.SerializeToString,
        response_deserializer=RunReply.FromString,
        )
    self.Poll = channel.unary_unary(
        '/protocol.ScootDaemon/Poll',
        request_serializer=PollRequest.SerializeToString,
        response_deserializer=PollReply.FromString,
        )


class ScootDaemonServicer(object):

  def Echo(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def CreateSnapshot(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def CheckoutSnapshot(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def Run(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def Poll(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_ScootDaemonServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'Echo': grpc.unary_unary_rpc_method_handler(
          servicer.Echo,
          request_deserializer=EchoRequest.FromString,
          response_serializer=EchoReply.SerializeToString,
      ),
      'CreateSnapshot': grpc.unary_unary_rpc_method_handler(
          servicer.CreateSnapshot,
          request_deserializer=CreateSnapshotRequest.FromString,
          response_serializer=CreateSnapshotReply.SerializeToString,
      ),
      'CheckoutSnapshot': grpc.unary_unary_rpc_method_handler(
          servicer.CheckoutSnapshot,
          request_deserializer=CheckoutSnapshotRequest.FromString,
          response_serializer=CheckoutSnapshotReply.SerializeToString,
      ),
      'Run': grpc.unary_unary_rpc_method_handler(
          servicer.Run,
          request_deserializer=RunRequest.FromString,
          response_serializer=RunReply.SerializeToString,
      ),
      'Poll': grpc.unary_unary_rpc_method_handler(
          servicer.Poll,
          request_deserializer=PollRequest.FromString,
          response_serializer=PollReply.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'protocol.ScootDaemon', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))


class BetaScootDaemonServicer(object):
  def Echo(self, request, context):
    context.code(beta_interfaces.StatusCode.UNIMPLEMENTED)
  def CreateSnapshot(self, request, context):
    context.code(beta_interfaces.StatusCode.UNIMPLEMENTED)
  def CheckoutSnapshot(self, request, context):
    context.code(beta_interfaces.StatusCode.UNIMPLEMENTED)
  def Run(self, request, context):
    context.code(beta_interfaces.StatusCode.UNIMPLEMENTED)
  def Poll(self, request, context):
    context.code(beta_interfaces.StatusCode.UNIMPLEMENTED)


class BetaScootDaemonStub(object):
  def Echo(self, request, timeout, metadata=None, with_call=False, protocol_options=None):
    raise NotImplementedError()
  Echo.future = None
  def CreateSnapshot(self, request, timeout, metadata=None, with_call=False, protocol_options=None):
    raise NotImplementedError()
  CreateSnapshot.future = None
  def CheckoutSnapshot(self, request, timeout, metadata=None, with_call=False, protocol_options=None):
    raise NotImplementedError()
  CheckoutSnapshot.future = None
  def Run(self, request, timeout, metadata=None, with_call=False, protocol_options=None):
    raise NotImplementedError()
  Run.future = None
  def Poll(self, request, timeout, metadata=None, with_call=False, protocol_options=None):
    raise NotImplementedError()
  Poll.future = None


def beta_create_ScootDaemon_server(servicer, pool=None, pool_size=None, default_timeout=None, maximum_timeout=None):
  request_deserializers = {
    ('protocol.ScootDaemon', 'CheckoutSnapshot'): CheckoutSnapshotRequest.FromString,
    ('protocol.ScootDaemon', 'CreateSnapshot'): CreateSnapshotRequest.FromString,
    ('protocol.ScootDaemon', 'Echo'): EchoRequest.FromString,
    ('protocol.ScootDaemon', 'Poll'): PollRequest.FromString,
    ('protocol.ScootDaemon', 'Run'): RunRequest.FromString,
  }
  response_serializers = {
    ('protocol.ScootDaemon', 'CheckoutSnapshot'): CheckoutSnapshotReply.SerializeToString,
    ('protocol.ScootDaemon', 'CreateSnapshot'): CreateSnapshotReply.SerializeToString,
    ('protocol.ScootDaemon', 'Echo'): EchoReply.SerializeToString,
    ('protocol.ScootDaemon', 'Poll'): PollReply.SerializeToString,
    ('protocol.ScootDaemon', 'Run'): RunReply.SerializeToString,
  }
  method_implementations = {
    ('protocol.ScootDaemon', 'CheckoutSnapshot'): face_utilities.unary_unary_inline(servicer.CheckoutSnapshot),
    ('protocol.ScootDaemon', 'CreateSnapshot'): face_utilities.unary_unary_inline(servicer.CreateSnapshot),
    ('protocol.ScootDaemon', 'Echo'): face_utilities.unary_unary_inline(servicer.Echo),
    ('protocol.ScootDaemon', 'Poll'): face_utilities.unary_unary_inline(servicer.Poll),
    ('protocol.ScootDaemon', 'Run'): face_utilities.unary_unary_inline(servicer.Run),
  }
  server_options = beta_implementations.server_options(request_deserializers=request_deserializers, response_serializers=response_serializers, thread_pool=pool, thread_pool_size=pool_size, default_timeout=default_timeout, maximum_timeout=maximum_timeout)
  return beta_implementations.server(method_implementations, options=server_options)


def beta_create_ScootDaemon_stub(channel, host=None, metadata_transformer=None, pool=None, pool_size=None):
  request_serializers = {
    ('protocol.ScootDaemon', 'CheckoutSnapshot'): CheckoutSnapshotRequest.SerializeToString,
    ('protocol.ScootDaemon', 'CreateSnapshot'): CreateSnapshotRequest.SerializeToString,
    ('protocol.ScootDaemon', 'Echo'): EchoRequest.SerializeToString,
    ('protocol.ScootDaemon', 'Poll'): PollRequest.SerializeToString,
    ('protocol.ScootDaemon', 'Run'): RunRequest.SerializeToString,
  }
  response_deserializers = {
    ('protocol.ScootDaemon', 'CheckoutSnapshot'): CheckoutSnapshotReply.FromString,
    ('protocol.ScootDaemon', 'CreateSnapshot'): CreateSnapshotReply.FromString,
    ('protocol.ScootDaemon', 'Echo'): EchoReply.FromString,
    ('protocol.ScootDaemon', 'Poll'): PollReply.FromString,
    ('protocol.ScootDaemon', 'Run'): RunReply.FromString,
  }
  cardinalities = {
    'CheckoutSnapshot': cardinality.Cardinality.UNARY_UNARY,
    'CreateSnapshot': cardinality.Cardinality.UNARY_UNARY,
    'Echo': cardinality.Cardinality.UNARY_UNARY,
    'Poll': cardinality.Cardinality.UNARY_UNARY,
    'Run': cardinality.Cardinality.UNARY_UNARY,
  }
  stub_options = beta_implementations.stub_options(host=host, metadata_transformer=metadata_transformer, request_serializers=request_serializers, response_deserializers=response_deserializers, thread_pool=pool, thread_pool_size=pool_size)
  return beta_implementations.dynamic_stub(channel, 'protocol.ScootDaemon', cardinalities, options=stub_options)
# @@protoc_insertion_point(module_scope)
