#!/usr/bin/env python

import os
import re
import shutil
import subprocess
import tempfile
import time
import unittest
from Carbon.Aliases import false

gopath = os.environ['GOPATH']
pypath = gopath + '/src/github.com/scootdev/scoot/daemon/protocol/python/scoot'
cliRef = ['python', pypath+'/daemon_cli.py']
createS = ['snapshot', 'create']
checkoutS = ['snapshot', 'checkout']
runE = ['exec', 'run']
pollE = ['exec', 'poll']

echo = ['echo']

class TestCliCommands(unittest.TestCase):
    daemonProcess = None
    pypath = ''
      
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

        # Note: the following does not work - the process from the pool does not have GOPATH defined so it can't find the binary
        try:
            self.daemonProcess = subprocess.Popen(['{0}/bin/daemon'.format(gopath), '-execer_type', 'os'])
        except Exception as e:
            self.fail('Fail:{0}'.format(e))  
        started = false
        start = time.time()
        elapsedTime = 0
        cmd = cliRef + echo + ['ping']
        while not started and elapsedTime < 4.0:
            try:    
                r = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
                if self.verifyOut(r, 'ping', False):
                    started = True
            except Exception as e:
                elapsedTime = time.time() - start
                time.sleep(0.25)
        
        if not started:
            self.daemonProcess.kill()
            self.fail("Daemon didn't start within '{0}'".format(elapsedTime))

    def tearDown(self):
        self.daemonProcess.kill()
        shutil.rmtree(self.tmpdir)
        unittest.TestCase.tearDown(self)

    def verifyOut(self, out, expected, failImmediately):
        m = re.search(expected, out)
        if failImmediately:
            self.assertTrue(m != None, "expected to find {0} in '{1}'".format(expected, out))
            
        return m != None

    def test_happyPath(self):
        # issue createSnapshot validate that get a snapShot id 
        cmd = cliRef + createS + [self.tmpdir]
        r = subprocess.check_output(cmd)
        self.verifyOut(r, 'snapshot id = [0-9]+', True)
        m = re.findall(r'[0-9]+', r)
        sId = m[0]
        
        # issue createSnapshot without a path validate get error message and usage prompt 
        cmd = cliRef + checkoutS + [sId, self.tmpdir]
        r = subprocess.check_output(cmd).rstrip()
        self.assertTrue(r == '', "Error: expected '' from checkout, got '{0}'".format(r))

        # run with snapshot id and timeout        
        cmd = cliRef + runE + ['ls', '--snapshotId', sId, '--timeout', '2000000']
        r = subprocess.check_output(cmd).rstrip()
        self.verifyOut(r, 'run id = [0-9]+', True)
        r1 = self.get_run_id(r)
     
        # run with snapshot id         
        cmd = cliRef + runE + ['\"ls\"', '--snapshotId', sId]
        r = subprocess.check_output(cmd).rstrip()
        self.verifyOut(r, 'run id = [0-9]+', True)
        r2 = self.get_run_id(r)
        
        pollIds = [r1, r2]
        
        # TODO implement poll check

    
    def get_run_id(self, out):   
        m = re.findall(r'[0-9]+', out)
        return m[0]
        
        
    def assert_invalid_command(self, cmd):
        try:
            r = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except Exception as e:
            return
        self.fail('{0} did not return an error.'.format(*cmd))

         
    def test_cmdErrors(self):
        # issue createSnapshot without a path validate get error message and usage prompt 
        cmd = cliRef + createS
        self.assert_invalid_command(cmd)
         
        cmd = cliRef + checkoutS
        self.assert_invalid_command(cmd)
 
        cmd = cliRef + checkoutS + ['34']
        self.assert_invalid_command(cmd)

    
if __name__ == '__main__':
    unittest.main()
  
