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


class TestManyRunRequests(unittest.TestCase):
    daemonProcess = None
    tmpdir = None
      
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

        proto.start()

        #wait up to 1 second for daemon to start 
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
            self.fail("Daemon didn't start within {0}".format(elapsedTime))
                        
    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        unittest.TestCase.tearDown(self)
        proto.stop()
        
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
        
    sleepDef = CmdDef(cmd=["./sleep_script.sh"], 
                      snapshot_key="sleep_s_id", 
                      interm_states=[proto.ScootStatus.State.PENDING, 
                                     proto.ScootStatus.State.PREPARING,
                                     proto.ScootStatus.State.RUNNING, 
                                     proto.ScootStatus.State.COMPLETED], 
                      final_state=proto.ScootStatus.State.COMPLETED)  # run sleep 1, expect complete or running, final status:complete
    lsDef = CmdDef(cmd=["./ls_script.sh"], 
                   snapshot_key="ls_s_id", 
                   interm_states=[proto.ScootStatus.State.PREPARING, 
                                  proto.ScootStatus.State.PENDING, 
                                  proto.ScootStatus.State.RUNNING, 
                                  proto.ScootStatus.State.COMPLETED], 
                   final_state=proto.ScootStatus.State.COMPLETED)  # run ls script 5 times expect complete, running or queued, final status:complete
    failDef = CmdDef(cmd=["./ls_script.sh"], 
                     snapshot_key="ls_fail_s_id", 
                     interm_states=[proto.ScootStatus.State.PENDING,
                                    proto.ScootStatus.State.PREPARING, 
                                    proto.ScootStatus.State.RUNNING, 
                                    proto.ScootStatus.State.COMPLETED], 
                     final_state=proto.ScootStatus.State.COMPLETED)  # run ls script 5 times expect complete, running or queued, final status:failed

    def test_many(self):
        rpc_timeout_ns = int(1000 * 1e6)
        
        runs = {}
        
        # Make the snapshots.
        ss_ids = self.make_snapshots()
    
        # Run the sleep script
        id = proto.run(argv=self.sleepDef.cmd, timeout_ns=rpc_timeout_ns, snapshot_id=ss_ids[self.sleepDef.snapshot_key])
        runs[id] = "sleep"
        
        # run ~200 requests, 10 repetitions of 20 successful, 1 error
        for i in range(5):
            for j in range(10):
                try:
                    id = proto.run(argv=self.lsDef.cmd, timeout_ns=rpc_timeout_ns, snapshot_id=ss_ids[self.lsDef.snapshot_key])
                    runs[id] = "ls"
                except proto.ScootException as e:
                    if "Runner is busy" not in str(e):
                        raise Exception("Run error, not resource limitation.{}".format(str(e)))
            
            try:
                id = proto.run(argv=self.failDef.cmd, timeout_ns=rpc_timeout_ns, snapshot_id=ss_ids[self.failDef.snapshot_key])
            except proto.ScootException as e:
                if "Runner is busy" not in str(e):
                    raise Exception("Run error, not resource limitation.{}".format(str(e)))
                
            runs[id] = "fail"
    
                
        # get all statuses
        statuses = proto.poll(run_ids=runs.keys(), timeout_ns=0, return_all=True)
        self.assertIntermediateStatuses(statuses, runs)
        
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
            print("len keys:{0}, len statuses:{1}".format(len(ids), len(statuses)))
            
        self.assertTrue(len(ids) == len(statuses), "runs did not finish.")
        self.assertCompleteStatuses(statuses, runs)
            
    
    # make the following snapshots
    # sleep snapshot: has a "sleep_script.sh" that sleeps for 0.5 seconds
    # ls snapshot: has an "ls_script.sh" that issues a successful ls command
    # ls fail snapshot: has an "ls_script.sh" that issues a failing ls command
    # will return a dict of the snapshot ids
    def make_snapshots(self):
        # make the files that will go into the snapshots        
        sleep_script = os.path.join(self.tmpdir, "sleep_script.sh")
        open(sleep_script, 'w').write("#!/bin/sh\nsleep 0.5")
        os.chmod(sleep_script, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    
        ls_script = os.path.join(self.tmpdir, "ls_script.sh")
        open(ls_script, 'w').write("#!/bin/sh\nls resource.txt")
        os.chmod(ls_script, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    
        resource = os.path.join(self.tmpdir, "resource.txt")
        open(resource, 'w').write("content")
    
        # make the snapshots
        sleep_ss_id = proto.create_snapshot(sleep_script)
        ls_ss_id = proto.create_snapshot(os.path.join(self.tmpdir, "*"))
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
  