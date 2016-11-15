#!/usr/bin/env python

import subprocess
import os
import re
import shutil
import sys
import tempfile
import time
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), '../../protocol'))
import client_lib as proto

# def StartDaemon():
# #     call(["$GOPATH/bin/daemon", "-execer_type", "os", "-q_len", "200"])
#     (["/Users/jbruno/workspace/bin/daemon", "-execer_type", "os", "-q_len", "200"])


class TestManyRunRequests(unittest.TestCase):
    daemonProcess = None
      
    def setUp(self):
        # Note: the following does not work - the process from the pool does not have GOPATH defined so it can't find the binary
        gopath = os.environ['GOPATH']
        print("keys {0}".format(os.environ))
        print("gopath:{0}",gopath)
        self.daemonProcess = subprocess.Popen(["{0}/bin/daemon".format(gopath), "-execer_type", "os", "-q_len", "200"])
        time.sleep(1.0)
        proto.start()
        
    def tearDown(self):
        self.daemonProcess.kill()
        unittest.TestCase.tearDown(self)
        
    sleep_ss_key = "sleep_s_id"
    ls_ss_key = "ls_s_id"
    ls_fail_ss_key = "ls_fail_s_id"
    
    # test scenario
    class CmdDef(object):
        def __init__(self, cmd, snapshot_key, interm_states, final_state):
            self.cmd = cmd
            self.snapshot_key = snapshot_key
            self.interm_states = interm_states
            self.final_state = final_state
        
    sleepDef = CmdDef(cmd=["sh", "./sleep.sh"], snapshot_key="sleep_s_id", interm_states=[proto.ScootStatus.State.PENDING, proto.ScootStatus.State.RUNNING, proto.ScootStatus.State.COMPLETED], final_state=proto.ScootStatus.State.COMPLETED)  # run sleep 1, expect complete or running, final status:complete
    lsDef = CmdDef(cmd=["sh", "./ls_script.sh"], snapshot_key="ls_s_id", interm_states=[proto.ScootStatus.State.PREPARING, proto.ScootStatus.State.PENDING, proto.ScootStatus.State.RUNNING, proto.ScootStatus.State.COMPLETED], final_state=proto.ScootStatus.State.COMPLETED)  # run ls script 5 times expect complete, running or queued, final status:complete
    failDef = CmdDef(cmd=["sh", "./ls_script.sh"], snapshot_key="ls_fail_s_id", interm_states=[proto.ScootStatus.State.PENDING, proto.ScootStatus.State.RUNNING, proto.ScootStatus.State.COMPLETED, proto.ScootStatus.State.FAILED], final_state=proto.ScootStatus.State.COMPLETED)  # run ls script 5 times expect complete, running or queued, final status:failed

    # TODO start the server    
    def test_many(self):
        tmpdir = tempfile.mkdtemp()
        rpc_timeout_ns = int(500 * 1e6)
        
        runs = {}
        
        try:
        # Make the snapshots.
            ss_ids = self.make_snapshots()
        
            # Run the sleep script
            id = proto.run(argv=self.sleepDef.cmd, timeout_ns=rpc_timeout_ns, snapshot_id=ss_ids[self.sleepDef.snapshot_key])
            runs[id] = "sleep"
            
            # run ~200 requests, 10 repetitions of 20 successful, 1 error
            for i in range (0, 10):
                for j in range(0, 19):
                    try:
                        id = proto.run(argv=self.lsDef.cmd, timeout_ns=rpc_timeout_ns, snapshot_id=ss_ids[self.lsDef.snapshot_key])
                        runs[id] = "ls"
                    except proto.ScootException as e:
                        if re.search("No resources available", str(e)) is None:
                            raise Exception("Run error, not resource limitation.")
                
                # TODO check that as is queue full
                try:
                    id = proto.run(argv=self.failDef.cmd, timeout_ns=rpc_timeout_ns, snapshot_id=ss_ids[self.failDef.snapshot_key])
                except proto.ScootException as e:
                    if re.search("No resources available", str(e)) is None:
                        raise Exception("Run error, not resource limitation.")
                    
                runs[id] = "fail"
        
                    
            # get all statuses
            statuses = proto.poll(run_ids=runs.keys(), timeout_ns=0, return_all=True)
            self.assertIntermediateStatuses(statuses, runs)
            
            # get status of every run that has completed so far
            statuses = proto.poll(run_ids=runs.keys(), timeout_ns=0, return_all=False)
            self.assertCompleteStatuses(statuses, runs)
            
            # wait for all runs to finish and validate their status
            statuses = proto.poll(run_ids=runs.keys(), timeout_ns=-1, return_all=False)
            self.assertCompleteStatuses(statuses, runs)
            
        finally:
            shutil.rmtree(tmpdir)
    
    # make the following snapshots
    # sleep snapshot: has a "sleep_script.sh" that sleeps for 0.5 seconds
    # ls snapshot: has an "ls_script.sh" that issues a successful ls command
    # ls fail snapshot: has an "ls_script.sh" that issues a failing ls command
    # will return a dict of the snapshot ids
    def make_snapshots(self):
        # make the files that will go into the snapshots
        tmpdir = tempfile.mkdtemp()
        
        sleep_script = os.path.join(tmpdir, "sleep_script.sh")
        open(sleep_script, 'w').write("#!/bin/sh\nsleep 0.5")
    
        ls_script = os.path.join(tmpdir, "ls_script.sh")
        open(ls_script, 'w').write("#!/bin/sh\nls resource.txt")
    
        resource = os.path.join(tmpdir, "resource.txt")
        open(resource, 'w').write("content")
    
        # make the snapshots
        sleep_ss_id = proto.create_snapshot(sleep_script)
        ls_ss_id = proto.create_snapshot(os.path.join(tmpdir, "*"))
        fail_ls_ss_id = proto.create_snapshot(ls_script)
        
        # return a dict of the snapshot ids
        return {self.sleep_ss_key:sleep_ss_id, self.ls_ss_key:ls_ss_id, self.ls_fail_ss_key:fail_ls_ss_id}
    
        
    def assertIntermediateStatuses(self, statuses, runs):
        for status in statuses:
            state = status.state
            if runs[status.run_id] == "sleep":
                self.assertTrue(state in self.sleepDef.interm_states, "run type:{0}, expected on of {1}, got {2}".format("sleep", self.sleepDef.interm_states, state))
            if runs[status.run_id] == "ls":
                self.assertTrue(state in self.lsDef.interm_states, "run type:{0}, expected on of {1}, got {2}".format("ls", self.lsDef.interm_states, state))
            if runs[status.run_id] == "fail":
                self.assertTrue(state in self.failDef.interm_states, "run type:{0}, expected on of {1}, got {2}".format("fail", self.failDef.interm_states, state))
            
        return True
    
    def assertCompleteStatuses(self, statuses, runs):
        for status in statuses:
            state = status.state
            if runs[status.run_id] == "sleep":
                self.assertTrue(state == self.sleepDef.final_state, "run type:{0}, expected {1}, got {2}".format("sleep", self.sleepDef.final_state, state))
            if runs[status.run_id] == "ls":
                self.assertTrue(state == self.lsDef.final_state, "run type:{0}, expected {1}, got {2}".format("ls", self.lsDef.final_state, state))
            if runs[status.run_id] != "fail":
                self.assertTrue(state == self.failDef.final_state, "run type:{0}, expected {1}, got {2}".format("fail", self.failDef.final_state, state))
          
        return True
    
        

if __name__ == '__main__':
    unittest.main()
  

