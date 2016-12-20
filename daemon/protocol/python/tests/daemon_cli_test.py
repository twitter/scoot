#!/usr/bin/env python

import os
import re
import shutil
import subprocess
import tempfile


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

tmpdir = None
   
def setUp():
  """ make the directory for the temporary files needed by the test
  """
  global tmpdir
  tmpdir = tempfile.mkdtemp()

def tearDown():
  """ delete the directory created for the temporary files
  """
  if tempfile._exists(tmpdir):
    shutil.rmtree(tmpdir)

def verifyOut( stdout, expectedRE, stderr, expectedErr, failImmediately):
  """ Verify the contents of the stdout and stderr.  If failImmediately is set, raise an Exception if the 
  expected output is not found, otherwise return false if the expected output is not found.
  """
  stdoutOk = True
  stderrOk = True
  if expectedRE != '':
    found = re.search(expectedRE, stdout) >= 0
  if (expectedRE != '' and not found) or (expectedRE == '' and stdout != ''):
    stdoutOk = False
  if (expectedErr != '' and stderr not in expectedErr) or (expectedErr == '' and stderr != ''):
    stderrOk = False
  if (not stdoutOk or not stderrOk) and failImmediately: 
    raise Exception("expected match:'{0}' in '{1}' and expected error: '{2}' got '{3}'".format(expectedRE, stdout, expectedErr, stderr))
         
  return True

def test_happy_path():
  """ Test a set of command line commands that are expected to run successfully
  """
  global cliPath
  try:
    # issue createSnapshot validate that get a snapShot id 
    cmd = cliPath + createSReq + [tmpdir]
    # this test only passes if the validation of create snapshot uses the following code instead of subprocess.check_output()
    out = usePopenForCommand(cmd)
    verifyOut(out[0], 'snapshot id = [0-9]+', out[1], '', True)
    m = re.findall(r'[0-9]+', out[0])
    sId = m[0]
    
    # issue checkoutSnapshot without a path validate get error message and usage prompt 
    cmd = cliPath + checkoutSReq + [sId, tmpdir]
    r = subprocess.check_output(cmd).rstrip()
    if r != '':
      raise Exception("Error: expected '' from checkout, got '{0}'".format(r))

    # run with snapshot id and timeout        
    cmd = cliPath + runEReq + ['ls', '--snapshotId={0}'.format(sId), '--timeout={0}'.format('2')]
    r = subprocess.check_output(cmd).rstrip()
    verifyOut(r, 'run id = [0-9]+',  out[1], '', True)
    r1 = get_run_id(r)
 
    # run with snapshot id         
    cmd = cliPath + runEReq + ['ls', '--snapshotId={0}'.format(sId), ]
    r = subprocess.check_output(cmd).rstrip()
    verifyOut(r, 'run id = [0-9]+',  out[1], '', True)
    r2 = get_run_id(r)
    
    pollIds = [r1, r2]
    # poll with wait and all
    cmd = cliPath + pollEReq + pollIds + ['--wait=0', '--all']
    r = subprocess.check_output(cmd)
    verifyOut(r, 'COMPLETE',  out[1], '', True)

    # poll with wait
    cmd = cliPath + pollEReq + pollIds + ['--wait=0']
    r = subprocess.check_output(cmd)
    verifyOut(r, 'COMPLETE',  out[1], '', True)

    # poll with just ids
    cmd = cliPath + pollEReq + pollIds 
    r = subprocess.check_output(cmd)
    verifyOut(r, 'COMPLETE',  out[1], '', True)
  except subprocess.CalledProcessError as e:
    tearDown()
    raise Exception(str(e))

def get_run_id( out): 
  """ Extract the run id from the run request's output
  """  
  m = re.findall(r'[0-9]+', out)
  return m[0]
     
def usePopenForCommand( cmd):
  """ Use the Popen function to run the command and get its output (as opposed to the check_output()
  """
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
     
def assert_invalid_command( cmd, expectedErr):
  """ Run the command and verify that the stdout is empty and stderr contains the expected clause
  """
  try:
    # this test only passes if the validation of create snapshot uses the following code instead of subprocess.check_output()
    out = usePopenForCommand(cmd)
    fail = False
    
    if out[0] != '':
      fail = True
    if (expectedErr != '' and expectedErr not in out[1]) or (expectedErr == '' and out[1] != ''):
      fail = True 
    if fail:
      raise Exception("cmd:'{}'\nexpected: '', got '{}' and expected error: '{}' got '{}'".format(cmd, out[0], expectedErr, out[1])) 
    return

  except subprocess.CalledProcessError as e:
    raise Exception(str(e))

      
def test_cmd_errors():
  """ Test illegal command line commands
  """
  global cliPath

  # issue createSnapshot without a path validate get error message and usage prompt 
  cmd = cliPath + createSReq
  assert_invalid_command(cmd, 'Usage')
    
  # checkout without snapshot nor dest
  cmd = cliPath + checkoutSReq
  assert_invalid_command(cmd, 'Usage')

  # checkout without dest
  cmd = cliPath + checkoutSReq + ['34']
  assert_invalid_command(cmd, 'Usage')

  # checkout with invalid snapshot
  cmd = cliPath + checkoutSReq + ['344444', tmpdir + "/checkout"]
  assert_invalid_command(cmd, 'No snapshot')
   
  # run with no command
  cmd = cliPath + runEReq 
  assert_invalid_command(cmd, 'Usage')
   
  # run without snapshot id
  cmd = cliPath + runEReq + ['ls']
  assert_invalid_command(cmd, 'Usage')
   
  # run with invalid timeout
  cmd = cliPath + runEReq + ['ls', '--snapshotId=0', '--timeout=badValue']
  assert_invalid_command(cmd, 'Invalid value for timeout')
   
  # poll with no run ids
  cmd = cliPath + pollEReq
  assert_invalid_command(cmd, 'Usage')
      
    # poll with bad wait value
  cmd = cliPath + pollEReq +['1', '--wait=notNumber']
  assert_invalid_command(cmd, 'Wait must be an integer')

  
if __name__ == '__main__':
  try:
    setUp()
    test_happy_path()
    test_cmd_errors()
  finally:
    stopReq = ['daemon', 'stop']
    cmd = cliPath + stopReq
    subprocess.call(cmd)
    tearDown()
    
