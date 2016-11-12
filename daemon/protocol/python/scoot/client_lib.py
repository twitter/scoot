#!/usr/bin/env python
#
#TODO: turn this into a class if we ever need more than one, or just do it because it's cleaner (can get rid of is_started() etc).

"""A library that python clients must use to talk to a Scoot Daemon Server.

Caller can start() and stop() the connection and has access to the minimal
API required to ingest or checkout filesystem snapshots, and to execute
commands against those snapshots.
"""


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
  """ All exceptions in this module are wrapped by this class and re-raised.
  """
  def __init__(self, caught):
    self.caught = caught

  def __repr__(self):
    return '{}({!r})'.format(self.__class__.__name__, self.caught)

  def __str__(self):
    return repr(self)


class ScootStatus(object):
  """ The current state of a job running on the Daemon server.
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
  """ Must be called before interacting with the Daemon server.
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
  """ Set the Daemon server connection to None.
  """
  global _client
  if not is_started():
    raise ScootException(Exception("Already stopped."))
  _client = None


def is_started():
  """ If the Daemon server connection is started.
  """
  global _client
  return _client is not None


def echo(ping):
  """ Test ping/pong command. Not useful outside of testing.

  @type ping: string
  @param ping: Text to send to and get back from the Daemon server.

  @rtype: string
  @return: The same ping text provided as input.
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
  """ Requests that Daemon server store and assign an id to the given file system path.

  @type path: string
  @param path: An absolute path on the local fs. Only directory paths are allowed at this time (ingesting contents not the dir itself).

  @rtype: string
  @return The id associated with the newly stored path.
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
  """ Requests that Daemon server populate a directory with the contents of a specific file system snapshot.

  @type snapshot_id: string
  @param snapshot_id: A snapshot id returned from an earlier call to create_snapshot().

  @type dirpath: string
  @param dirpath: The local absolute directory path in which to place snapshot contents.
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
  """ Requests that Daemon server run a command within a snapshot checkout directory.

  @type snapshot_id: string
  @param snapshot_id: A snapshot id returned from an earlier call to create_snapshot().

  @type argv: list of string
  @param argv: List of command name followed by args, ex: ("bash", "-c", "echo hello")

  @type env: dict<string, string>
  @param env: Mapping of environment variable names to values.

  @rtype: string
  @return A run id which is used to query the Daemon server for the command's status.
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
  """ Requests that Daemon server return status for any finished run ids.

  @type run_ids: list of string
  @param run_ids: The ids with which to poll for completed status.

  @type timeout_ns: int
  @param timeout_ns:
    <0 to block indefinitely waiting for at least one finished run.
    0 to return immediately with finished runs, if any.
    >0 to wait at most timeout_ns for at least one finished run.

  @type return_all: bool
  @param return_all: Return a status for all specified run ids whether finished or not.

  @rtype: list of ScootStatus
  @return Statuses for [a subset of] all the specified run ids.
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
