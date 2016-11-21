#!/usr/bin/env python

import os
import re
import shutil
import subprocess
import tempfile
import time
import unittest



class TestCliCommands(unittest.TestCase):
  gopath = re.split(":", os.environ['GOPATH'])[0]
  pypath = gopath + '/src/github.com/scootdev/scoot/daemon/protocol/python/scoot'
  cliPath = ['python', pypath + '/scoot.py']
  createSReq = ['snapshot', 'create']
  checkoutSReq = ['snapshot', 'checkout']
  runEReq = ['exec', 'run']
  pollEReq = ['exec', 'poll']
  
  echoReq = ['echo']

  daemonProcess = None
  daemonStarted = False;
     
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()
    self.daemonStarted = False

    # Note: the following does not work - the process from the pool does not have GOPATH defined so it can't find the binary
    try:
      self.daemonProcess = subprocess.Popen(['{0}/bin/daemon'.format(self.gopath), '-execer_type', 'os'])
    except subprocess.CalledProcessError as e:
      self.fail('Fail:{0}'.format(e))  
    start = time.time()
    elapsedTime = 0
    cmd = self.cliPath + self.echoReq + ['ping']
    while not self.daemonStarted and elapsedTime < 4.0:
      try:    
        r = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        if self.verifyOut(r, 'ping', False):
          self.daemonStarted = True
      except subprocess.CalledProcessError as e:
        elapsedTime = time.time() - start
        time.sleep(0.25)
       
    if not self.daemonStarted:
      shutil.rmtree(self.tmpdir)
      self.fail("Daemon didn't start within '{0}'".format(elapsedTime))

  def tearDown(self):
    if self.daemonStarted:
      self.daemonProcess.kill()
      self.daemonStarted = False
    if tempfile._exists(self.tmpdir):
      shutil.rmtree(self.tmpdir)
    unittest.TestCase.tearDown(self)

  def verifyOut(self, out, expected, failImmediately):
    m = re.search(expected, out)
    if failImmediately:
      self.assertTrue(m != None, "expected to find {0} in '{1}'".format(expected, out))
           
    return m != None

  def test_happy_path(self):
    try:
      # issue createSnapshot validate that get a snapShot id 
      cmd = self.cliPath + self.createSReq + [self.tmpdir]
      r = subprocess.check_output(cmd)
      self.verifyOut(r, 'snapshot id = [0-9]+', True)
      m = re.findall(r'[0-9]+', r)
      sId = m[0]
      
      # issue createSnapshot without a path validate get error message and usage prompt 
      cmd = self.cliPath + self.checkoutSReq + [sId, self.tmpdir]
      r = subprocess.check_output(cmd).rstrip()
      self.assertTrue(r == '', "Error: expected '' from checkout, got '{0}'".format(r))

      # run with snapshot id and timeout        
      cmd = self.cliPath + self.runEReq + ['ls', '--snapshotId={0}'.format(sId), '--timeout={0}'.format('2')]
      r = subprocess.check_output(cmd).rstrip()
      self.verifyOut(r, 'run id = [0-9]+', True)
      r1 = self.get_run_id(r)
   
      # run with snapshot id         
      cmd = self.cliPath + self.runEReq + ['ls', '--snapshotId={0}'.format(sId), ]
      r = subprocess.check_output(cmd).rstrip()
      self.verifyOut(r, 'run id = [0-9]+', True)
      r2 = self.get_run_id(r)
      
      pollIds = [r1, r2]
      # poll with wait and all
      cmd = self.cliPath + self.pollEReq + pollIds + ['--wait=0', '--all']
      r = subprocess.check_output(cmd)
      self.verifyOut(r, 'COMPLETE', True)

      # poll with wait
      cmd = self.cliPath + self.pollEReq + pollIds + ['--wait=0']
      r = subprocess.check_output(cmd)
      self.verifyOut(r, 'COMPLETE', True)
  
      # poll with just ids
      cmd = self.cliPath + self.pollEReq + pollIds 
      r = subprocess.check_output(cmd)
      self.verifyOut(r, 'COMPLETE', True)
    except subprocess.CalledProcessError as e:
      self.tearDown()
      self.fail(str(e))

  def get_run_id(self, out):   
    m = re.findall(r'[0-9]+', out)
    return m[0]
       
       
  def assert_invalid_command(self, cmd):
    try:
      r = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
      if re.search("UNAVAILABLE", r) > 0:
        return
      self.tearDown()
      self.fail("'{0}' did not return an error. Instead got: {1}".format(cmd), r)
    except subprocess.CalledProcessError:
      return

        
  def test_cmd_errors(self):
  
    # issue createSnapshot without a path validate get error message and usage prompt 
    cmd = self.cliPath + self.createSReq
    self.assert_invalid_command(cmd)
      
    # checkout without snapshot nor dest
    cmd = self.cliPath + self.checkoutSReq
    self.assert_invalid_command(cmd)
  
    # checkout without dest
    cmd = self.cliPath + self.checkoutSReq + ['34']
    self.assert_invalid_command(cmd)
  
    # checkout with invalid snapshot
    cmd = self.cliPath + self.checkoutSReq + ['34', self.tmpdir + "/checkout"]
    self.assert_invalid_command(cmd)
     
    # run with no command
    cmd = self.cliPath + self.runEReq 
    self.assert_invalid_command(cmd)
     
    # run without snapshot id
    cmd = self.cliPath + self.runEReq + ['ls']
    self.assert_invalid_command(cmd)
     
    # run with invalid timeout
    cmd = self.cliPath + self.runEReq + ['ls', '--snapshotId=0', '--timeout=badValue']
    self.assert_invalid_command(cmd)
     
    # poll with no run ids
    cmd = self.cliPath + self.pollEReq
    self.assert_invalid_command(cmd)
        
      # poll with bad wait value
    cmd = self.cliPath + self.pollEReq +['--wait=notNumber']
    self.assert_invalid_command(cmd)

  
  def test_daemon_not_started(self):
    self.daemonProcess.kill()
    self.daemonStarted = False

    # issue createSnapshot validate that get a snapShot id 
    cmd = self.cliPath + self.createSReq + [self.tmpdir]
    self.assert_invalid_command(cmd)
    
    # issue createSnapshot without a path validate get error message and usage prompt 
    cmd = self.cliPath + self.checkoutSReq + ["1", self.tmpdir]
    self.assert_invalid_command(cmd)

    # run with snapshot id and timeout        
    cmd = self.cliPath + self.runEReq + ['ls', '--snapshotId=1', '--timeout=2']
    self.assert_invalid_command(cmd)
 
    # poll with wait and all
    cmd = self.cliPath + self.pollEReq + ['1', '--wait=0', '--all']
    self.assert_invalid_command(cmd)

    cmd = self.cliPath + self.echoReq + ['ping']
    self.assert_invalid_command(cmd)
        

    
if __name__ == '__main__':
  unittest.main()
  
