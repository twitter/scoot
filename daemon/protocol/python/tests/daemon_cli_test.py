#!/usr/bin/env python

import os
import re
import shutil
import subprocess
import tempfile
import unittest


gopath = re.split(":", os.environ['GOPATH'])[0]
pypath = gopath + '/src/github.com/scootdev/scoot/daemon/protocol/python/scoot'
cliPath = ['python', pypath + '/scoot.py']

class TestCliCommands(unittest.TestCase):
  createSReq = ['snapshot', 'create']
  checkoutSReq = ['snapshot', 'checkout']
  runEReq = ['exec', 'run']
  pollEReq = ['exec', 'poll']
  echoReq = ['echo']

  daemonProcess = None
  daemonStarted = False;
     
  def setUp(self):
    self.tmpdir = tempfile.mkdtemp()

  def tearDown(self):
    if tempfile._exists(self.tmpdir):
      shutil.rmtree(self.tmpdir)
    unittest.TestCase.tearDown(self)

  def verifyOut(self, stdout, expectedRE, stderr, expectedErr, failImmediately):
    stdoutOk = True
    stderrOk = True
    if expectedRE != '':
      found = re.search(expectedRE, stdout) >= 0
    if (expectedRE != '' and not found) or (expectedRE == '' and stdout != ''):
      stdoutOk = False
    if (expectedErr != '' and stderr not in expectedErr) or (expectedErr == '' and stderr != ''):
      stderrOk = False
    if (not stdoutOk or not stderrOk) and failImmediately: 
      self.fail("expected match:'{0}' in '{1}' and expected error: '{2}' got '{3}'".format(expectedRE, stdout, expectedErr, stderr))
           
    return True

  def test_happy_path(self):
    global cliPath
    try:
      # issue createSnapshot validate that get a snapShot id 
      cmd = cliPath + self.createSReq + [self.tmpdir]
      # this test only passes if the validation of create snapshot uses the following code instead of subprocess.check_output()
      out = self.usePopenForCommand(cmd)
      self.verifyOut(out[0], 'snapshot id = [0-9]+', out[1], '', True)
      m = re.findall(r'[0-9]+', out[0])
      sId = m[0]
      
      # issue checkoutSnapshot without a path validate get error message and usage prompt 
      cmd = cliPath + self.checkoutSReq + [sId, self.tmpdir]
      r = subprocess.check_output(cmd).rstrip()
      self.assertTrue(r == '', "Error: expected '' from checkout, got '{0}'".format(r))

      # run with snapshot id and timeout        
      cmd = cliPath + self.runEReq + ['ls', '--snapshotId={0}'.format(sId), '--timeout={0}'.format('2')]
      r = subprocess.check_output(cmd).rstrip()
      self.verifyOut(r, 'run id = [0-9]+',  out[1], '', True)
      r1 = self.get_run_id(r)
   
      # run with snapshot id         
      cmd = cliPath + self.runEReq + ['ls', '--snapshotId={0}'.format(sId), ]
      r = subprocess.check_output(cmd).rstrip()
      self.verifyOut(r, 'run id = [0-9]+',  out[1], '', True)
      r2 = self.get_run_id(r)
      
      pollIds = [r1, r2]
      # poll with wait and all
      cmd = cliPath + self.pollEReq + pollIds + ['--wait=0', '--all']
      r = subprocess.check_output(cmd)
      self.verifyOut(r, 'COMPLETE',  out[1], '', True)

      # poll with wait
      cmd = cliPath + self.pollEReq + pollIds + ['--wait=0']
      r = subprocess.check_output(cmd)
      self.verifyOut(r, 'COMPLETE',  out[1], '', True)
  
      # poll with just ids
      cmd = cliPath + self.pollEReq + pollIds 
      r = subprocess.check_output(cmd)
      self.verifyOut(r, 'COMPLETE',  out[1], '', True)
    except subprocess.CalledProcessError as e:
      self.tearDown()
      self.fail(str(e))

  def get_run_id(self, out):   
    m = re.findall(r'[0-9]+', out)
    return m[0]
       
  def usePopenForCommand(self, cmd):
      outDir = tempfile.mkdtemp()
      sOut = outDir+"/commandOut.txt"
      sOutFD = open(sOut, 'w')
      sErr = outDir+ "/commandErr.txt"
      sErrFD = open(sErr, 'w')
      r = subprocess.Popen(cmd, stdout=sOutFD, stderr=sErrFD)
      r.wait()
      sOutFDR = open(sOut, 'r')
      cmdOut = sOutFDR.read()
      sErrFDR = open(sErr, 'r')
      cmdErr = sErrFDR.read()

      shutil.rmtree(outDir)

      return [cmdOut, cmdErr]
       
  def assert_invalid_command(self, cmd, expectedOut, expectedErr):
    try:
      # this test only passes if the validation of create snapshot uses the following code instead of subprocess.check_output()
      out = self.usePopenForCommand(cmd)
      fail = False
      
      if (expectedOut != '' and expectedOut not in out[0]) or (expectedOut == '' and out[0] != ''):
        fail = True
      if (expectedErr != '' and expectedErr not in out[1]) or (expectedErr == '' and out[1] != ''):
        fail = True 
      if fail:
        self.fail("cmd:'{0}'\nexpected: '{1}', got '{2}' and expected error: '{3}' got '{4}'".format(cmd, expectedOut, out[0], expectedErr, out[1])) 
      return

    except subprocess.CalledProcessError as e:
      self.fail(str(e))
#       return

        
  def test_cmd_errors(self):
    global cliPath
  
    # issue createSnapshot without a path validate get error message and usage prompt 
    cmd = cliPath + self.createSReq
    self.assert_invalid_command(cmd, '', 'Usage')
      
    # checkout without snapshot nor dest
    cmd = cliPath + self.checkoutSReq
    self.assert_invalid_command(cmd, '', 'Usage')
  
    # checkout without dest
    cmd = cliPath + self.checkoutSReq + ['34']
    self.assert_invalid_command(cmd, '', 'Usage')
  
    # checkout with invalid snapshot
    cmd = cliPath + self.checkoutSReq + ['344444', self.tmpdir + "/checkout"]
    self.assert_invalid_command(cmd, '', 'No snapshot')
     
    # run with no command
    cmd = cliPath + self.runEReq 
    self.assert_invalid_command(cmd, '', 'Usage')
     
    # run without snapshot id
    cmd = cliPath + self.runEReq + ['ls']
    self.assert_invalid_command(cmd, '', 'Usage')
     
    # run with invalid timeout
    cmd = cliPath + self.runEReq + ['ls', '--snapshotId=0', '--timeout=badValue']
    self.assert_invalid_command(cmd, '', 'Invalid value for timeout')
     
    # poll with no run ids
    cmd = cliPath + self.pollEReq
    self.assert_invalid_command(cmd, '', 'Usage')
        
      # poll with bad wait value
    cmd = cliPath + self.pollEReq +['1', '--wait=notNumber']
    self.assert_invalid_command(cmd, '', 'Wait must be an integer')

  
if __name__ == '__main__':
  try:
    unittest.main()
  finally:
    stopReq = ['daemon', 'stop']
    cmd = cliPath + stopReq
    subprocess.call(cmd)
    
