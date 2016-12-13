#!/usr/bin/env python

import os
import shutil
import stat
import sys
import tempfile
import time
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), '../scoot'))
import client_lib as proto


tmpdir = None
  
def setUp():
  """ Start a (verified) client connection.
  """
  global tmpdir
  tmpdir = tempfile.mkdtemp()

  proto.start()

  # wait up to 1 second for daemon to start 
  start = time.time()
  elapsedTime = 0
  started = False

  while not started and elapsedTime < 1.0:    
    try:
      echo = proto.echo("ping")
      if echo == "ping":
        started = True
    except proto.ScootException as e:
      elapsedTime = time.time() - start
  
  if not started:
    raise Exception("Connection to daemon couldn't be established in {0} seconds".format(elapsedTime))
                    
def tearDown():
  shutil.rmtree(tmpdir)
  proto.stop_daemon()
    
sleep_s_key = "sleep_s_id"
ls_s_key = "ls_s_id"
ls_fail_s_key = "ls_fail_s_id"

# test scenario
class CmdDef(object):
  """ Reusable command definitions for the tests:  
  cmd = the command,  
  snapshot_key = the snapshot key to use when requesting the command be run,   
  interm_states = states to allow while not all the tests have completed  
  final_state = the state to expect when all the runs have completed
  """
  def __init__(self, cmd, snapshot_key, interm_states, final_state):
    self.cmd = cmd
    self.snapshot_key = snapshot_key
    self.interm_states = interm_states
    self.final_state = final_state
    
# run the sleep script and expec final status:complete
sleepDef = CmdDef(cmd=["./sleep_script.sh"], 
                  snapshot_key="sleep_s_id", 
                  interm_states=[proto.ScootStatus.State.PENDING, 
                                 proto.ScootStatus.State.PREPARING,
                                 proto.ScootStatus.State.RUNNING, 
                                 proto.ScootStatus.State.COMPLETED], 
                  final_state=proto.ScootStatus.State.COMPLETED)
# run the ls script against a snapshot that has the target file and expect final status:complete  
lsDef = CmdDef(cmd=["./ls_script.sh"], 
               snapshot_key="ls_s_id", 
               interm_states=[proto.ScootStatus.State.PREPARING, 
                              proto.ScootStatus.State.PENDING, 
                              proto.ScootStatus.State.RUNNING, 
                              proto.ScootStatus.State.COMPLETED], 
               final_state=proto.ScootStatus.State.COMPLETED)  
# run the ls script against a snapshot that does not have the target file and expect final status:failed
failDef = CmdDef(cmd=["./ls_script.sh"], 
                 snapshot_key="ls_fail_s_id", 
                 interm_states=[proto.ScootStatus.State.PENDING,
                                proto.ScootStatus.State.PREPARING, 
                                proto.ScootStatus.State.RUNNING, 
                                proto.ScootStatus.State.COMPLETED], 
                 final_state=proto.ScootStatus.State.COMPLETED)  

def test_many():
  """ Submit ~50 run requests (5 sets of requests where each set of requests contain 10 requests that should complete and 1 request 
  that should fail).
  Wait for all the requests to complete and verify their final states.
  """
  global sleepDef, lsDef, failDef
  rpc_timeout_ns = int(1000 * 1e6)
  
  runs = {}
  
  # Make the snapshots.
  s_ids = make_snapshots()

  # Run the sleep script
  id = proto.run(argv=sleepDef.cmd, timeout_ns=rpc_timeout_ns, snapshot_id=s_ids[sleepDef.snapshot_key])
  runs[id] = "sleep"
  
  # run ~200 requests, 10 repetitions of 20 successful, 1 error
  for i in range(5):
    for j in range(10):
      try:
        id = proto.run(argv=lsDef.cmd, timeout_ns=rpc_timeout_ns, snapshot_id=s_ids[lsDef.snapshot_key])
        runs[id] = "ls"
      except proto.ScootException as e:
        if "Runner is busy" not in str(e):
            raise Exception("Run error, not resource limitation.{}".format(str(e)))
    
    try:
      id = proto.run(argv=failDef.cmd, timeout_ns=rpc_timeout_ns, snapshot_id=s_ids[failDef.snapshot_key])
    except proto.ScootException as e:
      if "Runner is busy" not in str(e):
        raise Exception("Run error, not resource limitation.{}".format(str(e)))
        
    runs[id] = "fail"

          
  # get all statuses
  statuses = proto.poll(run_ids=runs.keys(), timeout_ns=0, return_all=True)
  assertIntermediateStatuses(statuses, runs)
  
  # wait for all runs to finish and validate their status
  start = time.time()
  allDone = False
  elapsedTime = 0
  while not allDone and elapsedTime < 4.0:
      ids = runs.keys()
      statuses = proto.poll(run_ids=ids, timeout_ns=int(3000 * 1e6), return_all=False)
      if len(statuses) == len(ids):
          allDone = True
      elapsedTime = time.time() - start
      
  if len(ids) != len(statuses):
    raise Exception("runs did not finish.")
  assertCompleteStatuses(statuses, runs)
        

def make_snapshots():
  """ make the following snapshots  
  sleep snapshot: has a "sleep_script.sh" that sleeps for 0.5 seconds  
  ls snapshot: has an "ls_script.sh" that issues a successful ls command  
  ls fail snapshot: has an "ls_script.sh" that issues a failing ls command  
  Returns a dict of the snapshot ids
  """      
  sleep_script = os.path.join(tmpdir, "sleep_script.sh")
  open(sleep_script, 'w').write("#!/bin/sh\nsleep 0.5")
  os.chmod(sleep_script, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

  ls_script = os.path.join(tmpdir, "ls_script.sh")
  open(ls_script, 'w').write("#!/bin/sh\nls resource.txt")
  os.chmod(ls_script, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

  resource = os.path.join(tmpdir, "resource.txt")
  open(resource, 'w').write("content")

  # make the snapshots
  sleep_s_id = proto.create_snapshot(sleep_script)
  ls_s_id = proto.create_snapshot(os.path.join(tmpdir, "*"))
  fail_ls_s_id = proto.create_snapshot(ls_script)
  
  # return a dict of the snapshot ids
  return {sleep_s_key:sleep_s_id, ls_s_key:ls_s_id, ls_fail_s_key:fail_ls_s_id}

    
def assertIntermediateStatuses( statuses, runs):
  """ Verify that each run's state matches one of the states in its interm_states list
  """
  for status in statuses:
    state = status.state
    if runs[status.run_id] == "sleep":
      if not state in sleepDef.interm_states:
        raise Exception("run type:{0}, expected on of {1}, got {2}".format("sleep", sleepDef.interm_states, state))
    if runs[status.run_id] == "ls":
      if not state in lsDef.interm_states:
        raise Exception("run type:{0}, expected on of {1}, got {2}".format("ls", lsDef.interm_states, state))
    if runs[status.run_id] == "fail":
      if not state in failDef.interm_states:
        raise Exception("run type:{0}, expected on of {1}, got {2}".format("fail", failDef.interm_states, state))
      
  return True

def assertCompleteStatuses( statuses, runs):
  """ Verify that each run's state matches it's expected final_state 
  """
  for status in statuses:
    state = status.state
    if runs[status.run_id] == "sleep":
      if state != sleepDef.final_state:
        raise Exception("run type:{0}, expected {1}, got {2}".format("sleep", sleepDef.final_state, state))
    if runs[status.run_id] == "ls":
      if state != lsDef.final_state:
        raise Exception("run type:{0}, expected {1}, got {2}".format("ls", lsDef.final_state, state))
    if runs[status.run_id] != "fail":
      if state != failDef.final_state:
        raise Exception("run type:{0}, expected {1}, got {2}".format("fail", failDef.final_state, state))
    
  return True
    
        

if __name__ == '__main__':
  try:
    setUp()
    test_many()
  finally:
    tearDown()
  