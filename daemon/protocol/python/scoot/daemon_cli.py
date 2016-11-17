"""
Usage:
  daemon_cli.py snapshot create <srcDir>
  daemon_cli.py snapshot checkout <snapshotId> <destDir>
  daemon_cli.py exec run <command> [--snapshotId] [--timeout]
  daemon_cli.py exec poll <runIds> [--wait] [--all]
  daemon_cli.py exec abort <runId>
  daemon_cli.py echo <ping>

  
Submit commands to the Scoot daemon:

snapshot commands:
create   Creates a snapshot containing the files in the srcDir directory.  
         This command returns the id for the snapshot.
         
checkout Copies the contents of snapshotId to destDir.  Files from snapshotId will overwrite files in destDir.


execution commands:
run             Copies the files from snapshotId (if a snapshotId is supplied), and runs the command.  
                if timeout is supplied, the daemon will wait timeout seconds for the command to complete.  
                Default timeout is 0.5 seconds.

poll            Get the status of runs.  RunIds must be a comma delimited list of run ids (1,3,5,6).
                wait < 0 : poll will return the status values as soon as one or more run ids status is finished.
                wait = 0 : poll will return the status values immediately
                wait > 0 : poll will wait up to <wait> seconds for one of the run ids to reach finished state
                If all is present, poll will return the status of all run ids, not just finished runs.

abort           Abort the specified run.

check daemon running:
echo            The Scoot daemon echo's <ping> back to the client.  Use this command to verify that the daemon
                is running.

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
        print("\n\{RunId:{0},\n State:{1},\n ExitCode:{2},\n {Error:{3},\n Snapshot:{5}\n\}".format(status.runId, proto.ScootStatus.State(status.state), status.ExitCode, status.Error, status.Snapshot))


if __name__ == '__main__':
    cmd = docopt.docopt(__doc__)
    
    try:
        proto.start()
    except proto.ScootException as e:
        if re.search("UNAVAILABLE", e) > 0:
            raise docopt.DocoptExit("Scoot Daemon is not running!\n")
        raise docopt.DocoptExit(e)          
        
    try:
        if cmd['snapshot'] and cmd['create']:
            if not cmd['<srcDir>']:
                raise docopt.DocoptExit("Must provide srcDir!")
            path = cmd['<srcDir>']
            try:
                print("snapshot id = {0}".format(proto.create_snapshot(path)))
            except proto.ScootException as e:
                if re.search("Not started", e) > 0 or re.search("UNAVAILABLE", e) > 0:
                    raise docopt.DocoptExit("Scoot Daemon is not running!\n")
                raise docopt.DocoptExit(e)          
            
        elif cmd['snapshot'] and cmd['checkout']:
            sid = cmd["<snapshotId>"]
            if not sid:
                raise docopt.DocoptExit()
            destDir = cmd["<destDir>"]
            if not destDir:
                raise docopt.DocoptExit()
            print("{0}".format(proto.checkout_snapshot(sid, destDir)))
            
        elif cmd['exec'] and cmd['run']:
            timeout = cmd['--timeout']
            if not timeout:
                timeout = 500
            sid = cmd["--snapshotId"]
            if not sid:
                raise docopt.DocoptExit("No SnapshotId provided!")

            try:
                id = proto.run(cmd["<command>"], timeout_ns=timeout, snapshot_id=sid)
                print("run id = {0}".format(id))
            except proto.ScootException as e:
                if re.search("No resources available", e) is None:
                    print(e)
                elif re.search("Not started", e) > 0 or re.search("UNAVAILABLE", e) > 0:
                    raise docopt.DocoptExit("Scoot Daemon is not running!\n")
                raise docopt.DocoptExit(e)
            
        elif cmd['exec'] and cmd['poll']:
            all = cmd['--all']
            wait = cmd["--wait"]
            if not wait:
                wait = 0
            runIds = cmd["<runIds>"]
            if not runIds:
                raise docopt.DocoptExit("RunId list is empty!")
            
            try:
                statuses = proto.poll(runIds, wait, all)
                display_statuses(statuses)
            except proto.ScootException as e:
                if re.search("Not started", e) > 0 or re.search("UNAVAILABLE", e) > 0:
                    raise docopt.DocoptExit("Scoot Daemon is not running!\n")
                raise docopt.DocoptExit(e)
            
        elif cmd['exec'] and cmd['abort']:
            raise docopt.DocoptExit("Abort not implemented yet.")
            
        elif cmd['echo']:
            ping = cmd['<ping>']
            if not ping:
                raise docopt.DocoptExit("Must supply <ping> value to echo.")
            try:
                echo = proto.echo(ping)
                print("{0}".format(echo)) 
            except proto.ScootException as e:
                if re.search("Not started", e) > 0 or re.search("UNAVAILABLE", e) > 0:
                    raise docopt.DocoptExit("Scoot Daemon is not running!\n")
                raise docopt.DocoptExit(e)
             
    except Exception as e:
        raise docopt.DocoptExit(str(e))
    