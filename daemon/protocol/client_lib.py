#!/usr/bin/env python

import os
import re
import tempfile
import shutil

from grpc.beta import implementations
#import grpc
import daemon_pb2


# Copied from daemon/protocol/locate.go
def _locate_scoot_dir():
    scootdir = os.environ.get("SCOOTDIR")
    if scootdir is not None:
        return scootdir
    homedir = os.environ.get("HOME")
    if homedir is not None:
        return os.path.join(homedir, ".scoot")
    return None

def _locate_socket():
    scootdir = _locate_scoot_dir()
    if scootdir is None:
        return None
    return os.path.join(scootdir, "socket")


# Singleton variables.
_domain_sock = _locate_socket()
_client = None
_context = None

class ScootException(Exception):
    """
    """
    def __init__(self, caught):
        self.caught = caught



class ScootStatus:
    """
    """
    class State:
        UNKNOWN = 0
        PENDING = 1
        PREPARING = 2
        RUNNING = 3
        COMPLETED = 4
        FAILED = 5
    def __init__(self, run_id, state, snapshot_id, exit_code, error):
        self.run_id = run_id
        self.state = state
        self.snapshot_id = snapshot_id
        self.exit_code = exit_code
        self.error = error


def start():
    """
    """
    global _client, _domain_sock
    if _domain_sock is None:
        raise ScootExceptoin("Could not find domain socket path.")
    if is_started():
        raise ScootException("Already started.")
    #channel = grpc.insecure_channel("unix://%s:0" % _domain_sock)
    #_client = daemon_pb2.ScootDaemonStub(channel)
    channel = implementations.insecure_channel(host=_domain_sock, port=None)
    _client = daemon_pb2.beta_create_ScootDaemon_stub(channel)



def stop():
    """
    """
    global _client
    if not is_started():
        raise ScootException("Already stopped.")
    _client = None
    pass



def is_started():
    """
    """
    global _client
    return _client is not None


def echo(ping):
    """
    """
    global _client
    if not is_started():
        raise ScootException("Not started.")
    req = daemon_pb2.EchoRequest(ping=ping)
    try: resp = _client.Echo(req)
    except Exception as e: raise ScootException(e)
    return resp.pong


def create_snapshot(path):
    """
    """
    global _client, _context
    if not is_started():
        raise ScootException("Not started.")
    req = daemon_pb2.CreateSnapshotRequest(path=path)
    try: resp = _client.CreateSnapshot(req, _context)
    except Exception as e: raise #ScootException(e)
    if resp.error != "":
        raise ScootException(resp.error)
    return resp.snapshot_id


def checkout_snapshot(snapshot_id, dirpath):
    """
    """
    global _client
    if not is_started():
        raise ScootExceptoin("Not started.")
    req = daemon_pb2.CheckoutSnapshotRequest(snapshot_id=snapshot_id, dir=dirpath)
    try: resp = _client.CheckoutSnapshot(req)
    except Exception as e: raise ScootException(e)
    if resp.error != "":
        raise ScootException(resp.error)
    return


def run(snapshot_id, argv, env=None, timeout_ns=0, src_paths_to_dest_dirs=None, ):
    """
    """
    global _client
    if not is_started():
        raise ScootExceptoin("Not started.")
    cmd = daemon_pb2.RunRequest.Command(timeout_ns=timeout_ns)
    cmd.argv.extend(argv)
    for key, val in env.iteritems():
        cmd.env[key] = val

    plan = daemon_pb2.RunRequest.OutputPlan()
    for key, val in src_paths_to_dest_dirs.iteritems():
        plan.src_paths_to_dest_dirs[key] = val

    req = daemon_pb2.RunRequest(snapshot_id=snapshot_id, cmd=cmd, plan=plan)
    try: resp = _client.Run(req)
    except Exception as e: raise ScootException(e)
    if resp.error != "":
        raise ScootException(resp.error)
    return resp.run_id


def poll(run_ids, timeout_ns=0, return_all=False):
    """
    """
    global _client
    if not is_started():
        raise ScootExceptoin("Not started.")
    req = daemon_pb2.PollRequest(timeout_ns=timeout_ns, all=return_all)
    req.run_ids.extend(run_ids)
    try: resp = _client.Poll(req)
    except Exception as e: raise ScootException(e)

    def domain(status):
        return ScootStatus(run_id=status.run_id,
                           state=status.state,
                           snapshot_id=status.snapshot_id,
                           exit_code=status.exit_code,
                           error=status.error)
    return [domain(x) for x in resp.status]


if __name__ == '__main__':
    # Adapted from daemon/server/handler_test.go
    # TODO: make this a proper unit test.
    # TODO: use a tmp domain socket instead of the default.
    start()
    tmpdir = tempfile.mkdtemp()
    try:
        # Populate the paths we want to ingest.
	script = os.path.join(tmpdir, "script.sh")
	resource = os.path.join(tmpdir, "resource.txt")
        open(script, 'w').write("#!/bin/sh\nls resource.txt")
        open(resource, 'w').write("content")

	# Ingest scripts into their own snapshots. The 'fail' snapshot will be missing resource.txt.
        ok_id = create_snapshot(os.path.join(tmpdir, "*"))
        fail_id = create_snapshot(script)

	# Run scripts serially in their respective snapshots. Block until each run finishes.
	ok_run_id = run(argv=["./script.sh"], timeout_ns=500*1e6, snapshotId=ok_id)
        ok_statuses = poll(run_ids=[ok_run_id], timeout_ns=500*1e6)
        if len(ok_statuses) != 1:
            raise ScootException("expected one poll result for ok_run_id.")

	fail_run_id = run(argv=["./script.sh"], timeout_ns=500*1e6, snapshotId=fail_id)
        fail_statuses = poll(run_ids=[fail_run_id], timeout_ns=500*1e6)
        if len(fail_statuses) != 1:
            raise ScootException("expected one poll result for fail_run_id.")

	# Make sure 'ok' and 'fail' returned the correct exit code.
	if ok_statuses[0].exit_code != 0:
	    raise ScootException("failure checking exit code of 'ok' run." + ok_statuses[0].exit_code)
	if fail_statuses[0].exit_code == 0:
	    raise ScootException("failure checking exit code of 'fail' run." + fail_statuses[0].exit_code)

	# Checkout result snapshots for both runs.
	ok_dir = os.path.join(tmpdir, "okco")
	fail_dir = os.path.join(tmpdir, "failco")
	checkoutSnapshot(snapshot_id=ok_statuses[0].snapshot_id, dirpath=ok_dir)
	checkoutSnapshot(snapshot_id=fail_statuses[0].snapshot_id, dirpath=fail_dir)

	# Check that 'ok' and 'fail' populated only STDOUT or STDERR respectively.
        def assert_file_contains(filepath, contents, msg):
            text = open(filepath, 'r').read()
            if re.search(contents, text) is None:
                raise ScootException("%s: [%s] bad file contents: %s" % (msg, filepath, text))
	assert_file_contains(os.path.join(ok_dir, "STDOUT"), "resource.txt\n", "ok")
	assert_file_contains(os.path.join(ok_dir, "STDERR"), "", "ok")
	assert_file_contains(os.path.join(fail_dir, "STDOUT"), "", "fail")
	assert_file_contains(os.path.join(fail_dir, "STDERR"), "No such file or directory\n", "fail")

    finally:
        shutil.rmtree(tmpdir)
