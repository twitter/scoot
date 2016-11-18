#!/usr/bin/env python
#This is the command line entry to the scoot daemon.
#This code uses docopt to parse the command line.  docopt uses the  __doc__ string below 
#for both parsing the command line into a dict and for displaying help.
"""
Usage:
  scoot snapshot create <srcDir>
  scoot snapshot checkout <snapshotId> <destDir>
  scoot exec run <command> ... --snapshotId=<sid> [--timeout=<ns>]
  scoot exec poll <runId> ... [--wait=<wait>] [--all]
  scoot exec abort <runId>
  scoot echo <ping>

  
Submit commands to the Scoot daemon:

snapshot commands:
create   Creates a snapshot containing the files in the srcDir directory.  
         This command returns the id for the snapshot.
         
checkout Copies the contents of snapshotId to destDir.  Files from snapshotId will overwrite files in destDir.


execution commands:
run             Copies the files from snapshotId (if a snapshotId is supplied), and runs the command.  
                if timeout_ns is supplied, the daemon will wait timeout (seconds) for the command to complete.  
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
  -h --help               Show this screen.
  --snapshotId=<sid>      Snapshot to install before running the command.
  --timeout=<seconds>     Maximum time(ms) to allow the command to run. Default 500ms
  --wait=<wait>           <0:wait indefinitely for at least one run to complete. 
                          0: return immediately with the status(s).  
                          >0: wait up to this time(seconds) for at least one of the runs to finish.  
                          Default: 0
  --all                   Return the status of all the runs not just finished runs.

"""
import docopt
import os
import sys
import re

sys.path.append(os.path.join(os.path.dirname(__file__), '../../protocol'))
import client_lib as proto


def display_statuses(statuses):
  for status in statuses:
    print("\nRunId:{0},\n\tState:{1},\n\tExitCode:{2},\n\tError:{3},\n\tSnapshot:{4}\n".format(status.run_id, proto.display_state(status.state), status.exit_code, status.error, status.snapshot_id))


def snapshot_create(cmd):
    #verify args
  if not cmd['<srcDir>']:
    raise docopt.DocoptExit("Must provide srcDir!")
  
  #run the command
  try:
    sid = proto.create_snapshot(cmd['<srcDir>'])
    # print the output
    print("snapshot id = {0}".format(sid))
  #handle errors running the command
  except proto.ScootException as e:
    if re.search("Not started", e) > 0 or re.search("UNAVAILABLE", e) > 0:
      raise docopt.DocoptExit("Scoot Daemon is not running!\n")
    raise docopt.DocoptExit("create snapshot error: '{0}'.".format(str(e)))       


def snapshot_checkout(cmd):
    #verify args
  if not cmd['<snapshotId>']:
    raise docopt.DocoptExit("Must supply a snapshot id.")
  if not cmd["<destDir>"]:
    raise docopt.DocoptExit("Must supply a destination directory.")
  #run the command
  try:
    r = proto.checkout_snapshot(snapshot_id=cmd['<snapshotId>'], dirpath=cmd["<destDir>"])
  #handle errors running the command
  except Exception as e:
    raise docopt.DocoptExit("snapshot checkout error: '{0}'.".format(str(e)))
  
  
def run(cmd):
  #verfiy args
  timeout = cmd['--timeout']
  if not timeout:
    timeout_ns = int(1e9)
  else:
    try:
      timeout_ns = int(int(timeout) * 1e9)
    except Exception as e:
      raise docopt.DocoptExit("invalid value for timeout, must be an integer or decimal number.")
  snapshotId = cmd['--snapshotId']
  args = cmd['<command>']

  #run the command
  try:
    runId = proto.run(snapshot_id=snapshotId, argv=args, timeout_ns=timeout_ns)
    #print the output
    print("run id = {0}".format(runId))
  #handle errors running the command
  except proto.ScootException as e:
    if re.search("No resources available", e) is None:
      print(e)
    elif re.search("Not started", e) > 0 or re.search("UNAVAILABLE", e) > 0:
      raise docopt.DocoptExit("Scoot Daemon is not running!\n")
    else:
      raise docopt.DocoptExit("run request error: '{0}'".format(str(e)))
  except Exception as e1:
    raise docopt.DocoptExit("run request error:'{0}'".format(str(e1)))


def poll(cmd):
  #verify args
  wait = cmd["--wait"]
  if not wait:
    wait = 0
  try:
    wait = int(wait)
  except Exception as e:
    raise docopt.DocoptExit("Wait must be an integer.")
  runIds = cmd["<runId>"]
  if not cmd["<runId>"]:
    raise docopt.DocoptExit("RunId list is empty!")
  
  #run the command
  try:
    statuses = proto.poll(run_ids=cmd["<runId>"], timeout_ns=wait, return_all=cmd['--all'])
    # print the output
    display_statuses(statuses)
  #handle errors running the command
  except proto.ScootException as e:
    if re.search("Not started", e) > 0 or re.search("UNAVAILABLE", e) > 0:
      raise docopt.DocoptExit("Scoot Daemon is not running!\n")
    raise docopt.DocoptExit("poll request error:'{0}'.".format(str(e)))


def echo(cmd):
  #verify args
  if not cmd['<ping>']:
    raise docopt.DocoptExit("Must supply <ping> value to echo.")
  
  #run the command
  try:
    echo = proto.echo(ping=cmd['<ping>'])
    #print the output
    print("{0}".format(echo)) 
  #handle errors running the command
  except proto.ScootException as e:
    if re.search("Not started", str(e)) > 0 or re.search("UNAVAILABLE", str(e)) > 0:
      raise docopt.DocoptExit("Scoot Daemon is not running!\n")
    raise docopt.DocoptExit("echo request error:'{0}'".format(str(e)))


if __name__ == '__main__':
  # parse the command line
  try:
    cmd = docopt.docopt(__doc__)
  except Exception as e:
    raise docopt.DocoptExit("error parsing command: '{0}'.".format(str(e)))
  
  # make the client connection
  try:
    proto.start()
  #handle errors making the client connection
  except proto.ScootException as e:
    if re.search("UNAVAILABLE", e) > 0:
      raise docopt.DocoptExit("Scoot Daemon is not running!\n")
    raise docopt.DocoptExit("connecting to daemon error: '{0}' (make sure you have started the daemon).".format(str(e)))       
   
  #process the command     
  if cmd['create']:
    snapshot_create(cmd)
  elif cmd['checkout']:
    snapshot_checkout(cmd)
  elif cmd['run']:
    run(cmd)
  elif cmd['poll']:
    poll(cmd)
  elif cmd['abort']:
    raise docopt.DocoptExit("Abort not implemented yet.")
  elif cmd['echo']:
    echo(cmd)
  else:
    raise docopt.DocoptExit("Unknown command:'{0}'".format(cmd))
