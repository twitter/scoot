#!/usr/bin/env python
#
#TODO: turn this into a class if we ever need more than one.
#TODO(this PR): fill in comment blocks.

import os
import sys

import daemon_pb2
import grpc


# Copied from daemon/protocol/locate.go
def _locate_scoot_dir():
  scootdir = os.environ.get("SCOOTDIR")
  if scootdir is not None:
    return scootdir
  homedir = os.environ.get("HOME")
  if homedir is not None:
    return os.path.join(homedir, ".scoot")
  return None

def _locate_socket():
  scootdir = _locate_scoot_dir()
  if scootdir is None:
    return None
  return os.path.join(scootdir, "socket")


# Singleton variables.
_domain_sock = _locate_socket()
_client = None


class ScootException(Exception):
  """
  """
  def __init__(self, caught):
    self.caught = caught

  def __repr__(self):
    return '{}({!r})'.format(self.__class__.__name__, self.caught)

  def __str__(self):
    return repr(self)


class ScootStatus(object):
  """
  """
  class State(object):
    UNKNOWN = 0
    PENDING = 1
    PREPARING = 2
    RUNNING = 3
    COMPLETED = 4
    FAILED = 5
  def __init__(self, run_id, state, snapshot_id, exit_code, error):
    self.run_id = run_id
    self.state = state
    self.snapshot_id = snapshot_id
    self.exit_code = exit_code
    self.error = error


def start():
  """
  """
  global _client, _domain_sock
  if _domain_sock is None:
    raise ScootException(Exception("Could not find domain socket path."))
  if is_started():
    raise ScootException(Exception("Already started."))
  channel = grpc.insecure_channel("unix://%s" % _domain_sock)
  #channel.subscribe(lambda x: sys.stdout.write(str(x)+"\n"), try_to_connect=True)
  _client = daemon_pb2.ScootDaemonStub(channel)


def stop():
  """
  """
  global _client
  if not is_started():
    raise ScootException(Exception("Already stopped."))
  _client = None


def is_started():
  """
  """
  global _client
  return _client is not None


def echo(ping):
  """
  """
  global _client
  if not is_started():
    raise ScootException(Exception("Not started."))
  req = daemon_pb2.EchoRequest(ping=ping)
  try:
    resp = _client.Echo(req)
  except Exception as e:
    raise ScootException(e)
  return resp.pong


def create_snapshot(path):
  """
  """
  global _client
  if not is_started():
    raise ScootException(Exception("Not started."))
  req = daemon_pb2.CreateSnapshotRequest(path=path)
  try:
    resp = _client.CreateSnapshot(req)
  except Exception as e:
    raise ScootException(e)
  if resp.error:
    raise ScootException(Exception(resp.error))
  return resp.snapshot_id


def checkout_snapshot(snapshot_id, dirpath):
  """
  """
  global _client
  if not is_started():
    raise ScootException(Exception("Not started."))
  req = daemon_pb2.CheckoutSnapshotRequest(snapshot_id=snapshot_id, dir=dirpath)
  try:
    resp = _client.CheckoutSnapshot(req)
  except Exception as e:
    raise ScootException(e)
  if resp.error:
    raise ScootException(Exception(resp.error))
  return


def run(snapshot_id, argv, env=None, timeout_ns=0):
  """
  """
  global _client
  if not is_started():
    raise ScootException(Exception("Not started."))
  if env == None:
    env = {}
  cmd = daemon_pb2.RunRequest.Command(snapshot_id=snapshot_id, timeout_ns=timeout_ns)
  cmd.argv.extend(argv)
#  for key, val in env.iteritems():
#    cmd.env[key] = val
  cmd.env.update(env)

  req = daemon_pb2.RunRequest(cmd=cmd)
  try:
    resp = _client.Run(req)
  except Exception as e:
    raise ScootException(e)
  if resp.error:
    raise ScootException(Exception(resp.error))
  return resp.run_id


def poll(run_ids, timeout_ns=0, return_all=False):
  """
  """
  global _client
  if not is_started():
    raise ScootException(Exception("Not started."))
  req = daemon_pb2.PollRequest(timeout_ns=timeout_ns, all=return_all)
  req.run_ids.extend(run_ids)
  try:
    resp = _client.Poll(req)
  except Exception as e:
    raise ScootException(e)

  def domain(status):
    return ScootStatus(run_id=status.run_id,
                 state=status.state,
                 snapshot_id=status.snapshot_id,
                 exit_code=status.exit_code,
                 error=status.error)
  return [domain(x) for x in resp.status]
