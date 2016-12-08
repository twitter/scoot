#!/usr/bin/env python
#This is the command line entry to the scoot daemon.
#This code uses docopt to parse the command line.  docopt uses the  __doc__ string below 
#for both parsing the command line into a dict and for displaying help.
"""
Usage:
  scoot.py snapshot create <srcDir>
  scoot.py snapshot checkout <snapshotId> <destDir>
  scoot.py exec run <command> ... --snapshotId=<sid> [--timeout=<seconds>]
  scoot.py exec poll <runId> ... [--wait=<waitSeconds>] [--all]
  scoot.py exec abort <runId>
  scoot.py echo <ping>
  scoot.py daemon stop

  
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

miscellaneous commands:
echo            The Scoot daemon echo's <ping> back to the client.  Use this command to verify that the daemon
                is running.
daemon stop     Stops the daemon

Options:
  -h --help               Show this screen.
  --snapshotId=<sid>      Install the snapshot with <sid> before running the command.
  --timeout=<seconds>     Maximum time(seconds) to allow the command to run. Default 1 second.
  --wait=<waitSeconds>    <0: wait indefinitely for at least one run to complete. 
                          0:  return immediately with the status(s) of the runs.  
                          >0: wait up to <waitSeconds> for at least one of the runs to finish.  
                          Default: 0.
  --all                   Return the status of all the runs not just finished runs.

"""
import sys
try:
  import docopt
except ImportError as e:
  sys.exit("Error importing docopt.\nMake sure you have docopt installed: 'pip install docopt'")

import client_lib as proto


def display_statuses(statuses):
  """ Formatted display of the status object
  """
  for status in statuses:
    print("\nRunId:{},\n\tState:{},\n\tExitCode:{},\n\tError:{},\n\tSnapshot:{}\n".format(status.run_id, proto.display_state(status.state), status.exit_code, status.error, status.snapshot_id))


def snapshot_create_cli(cmd):
  """ Run the create snapshot command - start a connection and run the command.  This processing will start a daemon if one
  has not been started yet.
  """
  startConnection()
  try:
    sid = proto.create_snapshot(cmd['<srcDir>'])
    # print the output
    print("snapshot id = {0}".format(sid))
  # handle errors running the command
  except proto.ScootException as e:
    if "Not started" in str(e) or "UNAVAILABLE" in str(e):
      sys.exit("Create snapshot failed. Scoot Daemon is not running!\n")
    sys.exit("create snapshot error: '{0}'.".format(str(e)))  # TODO: should be 'contact scoot support'?


def snapshot_checkout_cli(cmd):
  """ Run the checkout snapshot command - start a connection and run the command.  This processing will start a daemon if one
  has not been started yet.
  """
  startConnection()
  try:
    r = proto.checkout_snapshot(snapshot_id=cmd['<snapshotId>'], dirpath=cmd["<destDir>"])
  # handle errors running the command
  except proto.ScootException as e:
    if "Not started" in str(e) or "UNAVAILABLE" in str(e):
      sys.exit("Checkout snapshot failed. Scoot Daemon is not running!\n")
    else:
      sys.exit("Snapshot checkout error: '{0}'".format(str(e)))  # TODO: should be 'contact scoot support'?
  except Exception as e:
    sys.exit("Snapshot checkout error: '{0}'.".format(str(e)))  # TODO: should be 'contact scoot support'?
  
  
def run_cli(cmd):
  """ Verify the command arguments, start a connection and issue the Run command.
  This processing will start a daemon if one has not been started yet.
  """
  # verfiy the command arguments
  timeout = cmd['--timeout']
  if not timeout:
    timeout_ns = int(1e9)
  else:
    try:
      timeout_ns = int(int(timeout) * 1e9)
    except Exception as e:
      sys.exit("Invalid value for timeout, must be an integer or decimal number.%s".format(str(e)))

  startConnection()
  # run the command
  try:
    runId = proto.run(snapshot_id=cmd['--snapshotId'], argv=cmd['<command>'], timeout_ns=timeout_ns)
    # print the output
    print("run id = {0}".format(runId))
  # handle errors running the command
  except proto.ScootException as e:
    if "No resources available" in str(e):
      print(str(e))
    elif "Not started" in str(e) or "UNAVAILABLE" in str(e):
      sys.exit("Run failed. Scoot Daemon is not running!\n")
    else:
      sys.exit("run request error: '{0}'".format(str(e))) #TODO: should be 'contact scoot support'?
  except Exception as e1:
    sys.exit("run request error:'{0}'".format(str(e1))) #TODO: should be 'contact scoot support'?


def poll_cli(cmd):
  """ Verify the command arguments, start a connection and issue the Poll command.
  This processing will start a daemon if one has not been started yet.
  """
  # verify args
  wait = cmd["--wait"]
  if not wait:
    wait = 0
  try:
    wait = int(wait)
  except Exception as e:
    sys.exit("Wait must be an integer. {0}".format(str(e)))
  
  startConnection()
  # run the command
  try:
    statuses = proto.poll(run_ids=cmd["<runId>"], timeout_ns=wait, return_all=cmd['--all'])
    # print the output
    display_statuses(statuses)
  # handle errors running the command
  except proto.ScootException as e:
    if "Not started" in str(e) or "UNAVAILABLE" in str(e):
      sys.exit("Poll failed. Scoot Daemon is not running!\n")  # TODO: should be 'contact scoot support'?
    sys.exit("poll request error:'{0}'.".format(str(e)))  # TODO: should be 'contact scoot support'?


def echo_cli(cmd):
  """ Run the echo command - start a connection and run the command.  This processing will start a daemon if one
  has not been started yet.
  """
  startConnection()
  # run the command
  try:
    echo = proto.echo(ping=cmd['<ping>'])
    # print the output
    print("{0}".format(echo)) 
  # handle errors running the command
  except proto.ScootException as e:
    if "Not started" in str(e) or "UNAVAILABLE" in str(e):
      sys.exit("Echo failed. Scoot Daemon is not running!\n")
    sys.exit("echo request error:'{0}'".format(str(e)))  # TODO: should be 'contact scoot support'?

def startConnection():
  """ Create a verified client connection to the deamon.  (Start the daemon if necessary.)
  """
  try:
    proto.start()
  # handle errors making the client connection
  except proto.ScootException as e:
    if "UNAVAILABLE" in str(e):
      sys.exit("Cannot establish connection. Is Scoot Daemon running?\n")
    sys.exit("ScootException connecting to daemon error: '{0}'.".format(str(e)))  # TODO: should be 'contact scoot support'?
  except Exception as e:
    sys.exit("Exception connecting to daemon error: '{0}'.".format(str(e)))

def stop_daemon_cli():
  """ Stop the daemon
  """
  startConnection()
  proto.stop_daemon()

if __name__ == '__main__':
  # parse the command line
  cmd = docopt.docopt(__doc__)
  
  # process the command     
  if cmd['create']:
    snapshot_create_cli(cmd)
  elif cmd['checkout']:
    snapshot_checkout_cli(cmd)
  elif cmd['run']:
    run_cli(cmd)
  elif cmd['poll']:
    poll_cli(cmd)
  elif cmd['abort']:
    sys.exit("Abort not implemented yet.")
  elif cmd['daemon']:
    stop_daemon_cli()
  else:
    echo_cli(cmd)
