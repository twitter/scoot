"""
Usage:
  daemon_cli.py createSnapshot <srcDir>
  daemon_cli.py run <command> [--snapshotId] [--timeout]
  daemon_cli.py poll <runIds> [--wait] [--all]
  daemon_cli.py checkoutSnapshot <snapshotId> <destDir>
  
Submit commands to the Scoot daemon:
createSnapshot  creates a snapshot containing the files in the srcDir directory.  
This command returns the id for the snapshot.

run             copies the files from snapshotId (if a snapshotId is supplied), and runs the command.  
                if timeout is supplied, the daemon will wait timeout seconds for the command to complete.  
                Default timeout is 0.5 seconds.

poll            Get the status of runs.  RunIds must be a comma delimited list of run ids (1,3,5,6).
                wait < 0 : poll will return the status values as soon as one or more run ids status is finished.
                wait = 0 : poll will return the status values immediately
                wait > 0 : poll will wait up to <wait> seconds for one of the run ids to reach finished state
                If all is present, poll will return the status of all run ids, not just finished runs.

checkoutSnapshot    copies the contents of snapshotId to destDir.  Files from snapshotId will overwrite files in destDir.

Options:
  -h --help     Show this screen.
  --snapshot    Snapshot to install before running the command.
  --timeout     Maximum time(ms) to allow the command to run. Default 500ms
  --wait        <0:wait indefinitely for at least one run to complete. 
                0: return immediately with the status(s).  
                >0: wait up to this time(seconds) for at least one of the runs to finish.  
                Default: 0
  --all         Return the status of all the runs not just finished runs.

"""
import docopt
import os
import sys
import re

sys.path.append(os.path.join(os.path.dirname(__file__), '../../protocol'))
import client_lib as proto

def display_statuses(statuses):
    for status in statuses:
        print("\n\{RunId:{0},\n State:{1},\n ExitCode:{2},\n {Error:{3},\n Snapshot:{5}\n\}", status.runId, proto.ScootStatus.State(status.state), status.ExitCode, status.Error, status.Snapshot)


if __name__ == '__main__':
    cmd = docopt.docopt(__doc__)
    proto.start()
    try:
        if "createSnapshot" in cmd:
            path = cmd['<srcDir>']
            try:
                print("snapshot id = {0}".format(proto.create_snapshot(path)))
            except proto.ScootException as e:
                if re.search("Not started", str(e)) > 0:
                    raise docopt.DocoptExit("Scoot Daemon is not running!\n")
                raise docopt.DocoptExit(str(e))
            
            
        if "run" in cmd:
            if "--timeout" in cmd:
                timeout = cmd["--timeout"]
            else:
                timeout = 500
            sid = cmd["--snapshotId"]
            if sid == None:
                raise docopt.DocoptExit()
            try:
                id = proto.run(cmd["<command>"], timeout_ns=timeout, snapshot_id=sid)
            except proto.ScootException as e:
                if re.search("No resources available", str(e)) is None:
                    print(str(e))
                else:
                    print("run id = {0}".format(id))
            
        if "poll" in cmd:
            if "--all" in cmd:
                all = True
            else:
                all = False
            wait = cmd["--wait"]
            if wait == None:
                wait = 0
            runIds = cmd["<runIds>"]
            if runIds == None:
                raise docopt.DocoptExit()
            display_statuses(proto.poll(runIds, wait, all))
            
        if "checkoutSnapshot" in cmd:
            sid = cmd["<snapshotId>"]
            if sid == None:
                raise docopt.DocoptExit()
            destDir = cmd["<destDir>"]
            if destDir == None:
                raise docopt.DocoptExit()
            print("{0}", proto.checkout_snapshot(sid, destDir))  
             
    except Exception as e:
        raise docopt.DocoptExit(str(e))
    
        
