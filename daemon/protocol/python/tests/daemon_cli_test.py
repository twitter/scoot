#!/usr/bin/env python

import os
import re
import subprocess
import time
import unittest
from Carbon.Aliases import false

gopath = os.environ['GOPATH']
pypath = gopath + "/src/github.com/scootdev/scoot/daemon/protocol/python/scoot"
cliRef = ["python", pypath+"/daemon_cli.py"]
createS = ["snapshot", "create"]
checkoutS = ["snapshot", "checkout"]
echo = ["echo"]

class TestCliCommands(unittest.TestCase):
    daemonProcess = None
    pypath = ""
      
    def setUp(self):
        # Note: the following does not work - the process from the pool does not have GOPATH defined so it can't find the binary
        try:
            self.daemonProcess = subprocess.Popen(["{0}/bin/daemon".format(gopath), "-execer_type", "os"])
        except Exception as e:
            self.fail("Fail:{0}".format(e))  
        started = false
        start = time.time()
        elapsedTime = 0
        cmd = cliRef + echo + ['ping']
        while not started and elapsedTime < 1.0:
            try:    
                r = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
                if self.verifyOut(r, 'ping', False):
                    started = True
            except Exception as e:
                elapsedTime = time.time() - start
        
        if not started:
            self.fail("Daemon didn't start within {0}".format(elapsedTime))
            
    def tearDown(self):
        self.daemonProcess.kill()
        unittest.TestCase.tearDown(self)

    def verifyOut(self, out, expected, failImmediately):
        m = re.search(expected, out)
        if failImmediately:
            self.assertTrue(m != None, "expected to find {0} in '{1}'".format(expected, out))
            
        return m != None

    def test_createSnapshot(self):
        # issue createSnapshot validate that get a snapShot id 
        cmd = cliRef + createS + ["./"]
        r = subprocess.check_output(cmd)
        self.verifyOut(r, 'snapshot id = [0-9]+', True)
        
        # issue createSnapshot without a path validate get error message and usage prompt 
        cmd = cliRef + createS
        invalidCmd = False
        try:
            r = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except Exception as e:
            invalidCmd = True
            
        self.assertTrue(invalidCmd , "snapshot create without srcDir did not return an error.")
        
         
        
    
if __name__ == '__main__':
    unittest.main()
  
