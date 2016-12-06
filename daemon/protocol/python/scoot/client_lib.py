#!/usr/bin/env python
#
#TODO: turn this into a class if we ever need more than one, or just do it because it's cleaner (can get rid of is_started() etc).

"""A library that python clients must use to talk to a Scoot Daemon Server.

Caller can start() and stop() the connection and has access to the minimal
API required to ingest or checkout filesystem snapshots, and to execute
commands against those snapshots.
"""

import daemon_pb2
import grpc
import os
import re
import subprocess
import time
import uuid


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

def display_state(val):
  if val == 0:
    return "UNKNONW"
  if val == 1:
    return "PENDING"
  if val == 2:
    return "PREPARING"
  if val == 3:
    return "RUNNING"
  if val == 4:
    return "COMPLETED"
  if val == 5:
    return "FAILED"
  raise ScootException("Invalid state value {0}.".format(val))

def start():
  """ Establish a connection to the Daemon Server.  Must be called before interacting with the Daemon server.
  
  This function starts a client connection and verified that the echo command works.  If echo doesn't work,
  then the function starts a daemon and verfies the connection to the new daemon with another echo command.
  
  If no connection is verified, a ScootException is raised.
  """
  global _client, _domain_sock
  if _domain_sock is None:
    raise ScootException(Exception("Could not find domain socket path."))
  if is_started():
    raise ScootException(Exception("Already started."))
  try:
    channel = grpc.insecure_channel("unix://%s" % _domain_sock)
  except Exception as e:
    raise ScootException("Error getting channel:'unix://{}'".format(_domain_sock))
  #channel.subscribe(lambda x: sys.stdout.write(str(x)+"\n"), try_to_connect=True)
  _client = daemon_pb2.ScootDaemonStub(channel)
  
  # verify the connection
  echoReturn = None
  if _client is not None:
    ping = str(uuid.uuid4())
    try:
      echoReturn = echo(ping=ping)
    except ScootException as e:
      if "UNAVAILABLE" in str(e):
        echoReturn = str(e)
      else:
        raise ScootException("ScootException verifying connection: echo:'{}', '{}'". format(ping,e))
    except Exception as e:
        raise ScootException("Exception verifying connection: echo:'{}', '{}'". format(ping,e))
      
          
  # if the connection was not verified, try starting the daemon and checking the connection again
  if _client is None or "UNAVAILABLE" in echoReturn:
    #the echo failed, try starting the daemon 
    gopath = re.split(":", os.environ['GOPATH'])[0]
    daemon = gopath + '/bin/daemon'
    subprocess.Popen([daemon,"-execer_type", "os"])
    start = time.time()
      
    # give the daemon up to 3 seconds to start
    while True:
      if _client is None: # try to create a connection
        _client = daemon_pb2.ScootDaemonStub(channel)
      # send an echo request
      ping = str(uuid.uuid4())
      echoReturn = echo(ping=ping)
      if ping in echoReturn:
        break
        
      now = time.time()
      if now - start > 3.0: #wait up to 3 seconds for the daemon to start
        raise ScootException(Exception("The daemon did not start within 3 seconds"))


def stop():
  """ Terminate the connection to the daemon server.
  """
  global _client
  if not is_started():
    raise ScootException(Exception("Already stopped."))
  _client = None


def is_started():
  """ Returns true if the Daemon server connection is started.
  """
  global _client
  return _client is not None


def echo(ping):
  """ Request that the Daemon echo the 'ping' value. Used to validate that the Daemon is running and connection has been established.

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
    raise ScootException("Echo returned error:'{}'".format(str(e)))
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
    raise ScootException("Calling create snapshot with path:'{}' returned error: '{}'".format(path, str(e)))
  if resp.error:
    raise ScootException("Create snapshot with path:'{}' returned error: '{}'".format(path, resp.error))
  
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
    raise ScootException("Calling checkout with snapshot id:'{0}' into '{1}' returned error: '{2}'".format(snapshot_id, dirpath, str(e)))
                       
  if resp.error:
    raise ScootException("Checkout snapshot id:'{0}' into '{1}' response contained error: '{2}'".format(snapshot_id, dirpath, resp.error))
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
    raise ScootException("Calling run with snapshotId:'{}', argv:'{}', env:'{}', timeout:'{}', returned error: '{}'".format(snapshot_id, argv, env, timeout_ns, str(e)))
  if resp.error:
    raise ScootException("Run with snapshotId:'{}', argv:'{}', env:'{}', timeout:'{}', returned error: '{}'".format(snapshot_id, argv, env, timeout_ns, resp.error))
  
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
    raise ScootException("Calling poll with runIds:'{}' and timeout:'{}' returned error: '{}'".format(run_ids, timeout_ns, str(e)))

  def domain(status):
    return ScootStatus(run_id=status.run_id,
                 state=status.state,
                 snapshot_id=status.snapshot_id,
                 exit_code=status.exit_code,
                 error=status.error)
  return [domain(x) for x in resp.status]


def stop_daemon():
  """ Request that the daemon be Stopped
  """
  global _client
  if not is_started():
    raise ScootException(Exception("Connection not started."))
  req = daemon_pb2.EmptyStruct()
  try:
    _client.StopDaemon(req)
    _client = None
  except Exception:
      pass  # the call stops the daemon, but the grpc connection returns an error: swallow the error
    