"""
Usage:
  daemon_cli.py snapshot create <srcDir>
  daemon_cli.py snapshot checkout <snapshotId> <destDir>
  daemon_cli.py exec run <command> [--snapshotId=<sid>] [--timeout=<ns>]
  daemon_cli.py exec poll <runIds> [--wait=<wait>] [--all]
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
  -h --help          Show this screen.
  --snapshotId=<sid>  Snapshot to install before running the command.
  --timeout=<ns>     Maximum time(ms) to allow the command to run. Default 500ms
  --wait=<wait>      <0:wait indefinitely for at least one run to complete. 
                     0: return immediately with the status(s).  
                     >0: wait up to this time(seconds) for at least one of the runs to finish.  
                     Default: 0
  --all              Return the status of all the runs not just finished runs.

"""
import docopt
import os
import sys
import re
from scoot.docopt import DocoptExit

sys.path.append(os.path.join(os.path.dirname(__file__), '../../protocol'))
import client_lib as proto

def display_statuses(statuses):
    for status in statuses:
        print("\n\{RunId:{0},\n State:{1},\n ExitCode:{2},\n {Error:{3},\n Snapshot:{5}\n\}".format(status.runId, proto.ScootStatus.State(status.state), status.ExitCode, status.Error, status.Snapshot))


if __name__ == '__main__':
    try:
        cmd = docopt.docopt(__doc__)
    except Exception as e:
        print("exception:{0}".format(e))
        raise docopt.DocoptExit(e)
    try:
#         print("starting client")
        proto.start()
    except proto.ScootException as e:
        if re.search("UNAVAILABLE", e) > 0:
            raise docopt.DocoptExit("Scoot Daemon is not running!\n")
        raise docopt.DocoptExit(e)          
        
    if cmd['snapshot'] and cmd['create']:
        if not cmd['<srcDir>']:
            raise docopt.DocoptExit("Must provide srcDir!")
        try:
            sid = proto.create_snapshot(cmd['<srcDir>'])
#             print("createSnapshot({0})".format(cmd['<srcDir>']))
            print("snapshot id = {0}".format(sid))
        except proto.ScootException as e:
            if re.search("Not started", e) > 0 or re.search("UNAVAILABLE", e) > 0:
                raise docopt.DocoptExit("Scoot Daemon is not running!\n")
            raise docopt.DocoptExit(e)          
        
    elif cmd['checkout']:
        if not cmd['<snapshotId>']:
            raise docopt.DocoptExit("Must supply a snapshot id.")
        if not cmd["<destDir>"]:
            raise docopt.DocoptExit("Must supply a destination directory.")
        try:
            r = proto.checkout_snapshot(snapshot_id=cmd['<snapshotId>'], dirpath=cmd["<destDir>"])
#             print("checkout({0}, {1})".format(cmd['<snapshotId>'], dirpath=cmd["<destDir>"]))
        except Exception as e:
            raise docopt.DocoptExit(e)
        
    elif cmd['run']:
        timeout = cmd['--timeout']
        if not timeout:
            timeout = 500000
        else:
            try:
                timeout = int(timeout)
            except Exception as e:
                raise docopt.DocoptExit("invalid value for timeout, must be an integer.")
        if not cmd["--snapshotId"]:
            raise docopt.DocoptExit("SnapshotId is missing")
        args = re.split(" ", cmd['<command>'])  #TODO - fix this - need to handle spaces in \"

        try:
            runId = proto.run(snapshot_id=cmd["--snapshotId"], argv=args, timeout_ns=timeout)
#             print("run({0}, {1}, {2})".format(["--snapshotId"], cmd["<command>"], timeout))
            print("run id = {0}".format(runId))
        except proto.ScootException as e:
            if re.search("No resources available", e) is None:
                print(e)
            elif re.search("Not started", e) > 0 or re.search("UNAVAILABLE", e) > 0:
                raise docopt.DocoptExit("Scoot Daemon is not running!\n")
            raise docopt.DocoptExit(e)
        except Exception as e1:
            raise docopt.DocoptExit(e1)
        
    elif cmd['poll']:
        wait = cmd["--wait"]
        if not wait:
            wait = 0
        runIds = cmd["<runIds>"]
        if not cmd["<runIds>"]:
            raise docopt.DocoptExit("RunId list is empty!")
        
        try:
            statuses = proto.poll(runIds=cmd["<runIds>"], timeout_ns=wait, return_all=cmd['--all'])
#             print("poll({0}, {1}, {2}".format(cmd["<runIds>"], wait, ['--all']))
            display_statuses(statuses)
        except proto.ScootException as e:
            if re.search("Not started", e) > 0 or re.search("UNAVAILABLE", e) > 0:
                raise docopt.DocoptExit("Scoot Daemon is not running!\n")
            raise docopt.DocoptExit(e)
        
    elif cmd['abort']:
        raise docopt.DocoptExit("Abort not implemented yet.")
        
    elif cmd['echo']:
        if not cmd['<ping>']:
            raise docopt.DocoptExit("Must supply <ping> value to echo.")
        try:
            echo = proto.echo(ping=cmd['<ping>'])
#             print("echo({0})".format(cmd['<ping>']))
            print("{0}".format(echo)) 
        except proto.ScootException as e:
            if re.search("Not started", str(e)) > 0 or re.search("UNAVAILABLE", str(e)) > 0:
                raise docopt.DocoptExit("Scoot Daemon is not running!\n")
            raise docopt.DocoptExit(e)
             
