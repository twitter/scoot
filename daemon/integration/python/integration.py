#!/usr/bin/env python


import os
import re
import shutil
import sys
import tempfile

sys.path.append('../../protocol')
import client_lib as proto


# TODO: use a tmp domain socket instead of the default.
# Adapted from daemon/server/handler_test.go
def example_test():
  proto.start()
  tmpdir = tempfile.mkdtemp()
  rpc_timeout_ns = int(500*1e6)
  try:
    # Populate the paths we want to ingest.
    script = os.path.join(tmpdir, "script.sh")
    resource = os.path.join(tmpdir, "resource.txt")
    open(script, 'w').write("#!/bin/sh\nls resource.txt")
    open(resource, 'w').write("content")

    # Ingest scripts into their own snapshots. The 'fail' snapshot will be missing resource.txt.
    ok_id = proto.create_snapshot(os.path.join(tmpdir, "*"))
    fail_id = proto.create_snapshot(script)

    # Run scripts serially in their respective snapshots. Block until each run finishes.
    ok_run_id = proto.run(argv=["sh", "./script.sh"], timeout_ns=rpc_timeout_ns, snapshot_id=ok_id)
    ok_statuses = proto.poll(run_ids=[ok_run_id], timeout_ns=rpc_timeout_ns)
    if len(ok_statuses) != 1:
      raise proto.ScootException(Exception("expected one poll result for ok_run_id."))

    fail_run_id = proto.run(argv=["sh", "./script.sh"], timeout_ns=rpc_timeout_ns, snapshot_id=fail_id)
    fail_statuses = proto.poll(run_ids=[fail_run_id], timeout_ns=rpc_timeout_ns)
    if len(fail_statuses) != 1:
      raise proto.ScootException(Exception("expected one poll result for fail_run_id."))

    # Make sure 'ok' and 'fail' returned the correct exit code.
    if ok_statuses[0].exit_code != 0:
      raise proto.ScootException(Exception("failure checking exit code of 'ok' run: " + str(ok_statuses[0].exit_code)))
    if fail_statuses[0].exit_code == 0:
      raise proto.ScootException(Exception("failure checking exit code of 'fail' run: " + str(fail_statuses[0].exit_code)))

    # Checkout result snapshots for both runs.
    ok_dir = os.path.join(tmpdir, "okco")
    fail_dir = os.path.join(tmpdir, "failco")
    proto.checkout_snapshot(snapshot_id=ok_statuses[0].snapshot_id, dirpath=ok_dir)
    proto.checkout_snapshot(snapshot_id=fail_statuses[0].snapshot_id, dirpath=fail_dir)

    # Check that 'ok' and 'fail' populated only STDOUT or STDERR respectively.
    def assert_file_contains(filepath, contents, msg):
      text = open(filepath, 'r').read()
      if re.search(contents, text) is None:
        raise proto.ScootException(Exception("%s: [%s] bad file contents: %s" % (msg, filepath, text)))
    assert_file_contains(os.path.join(ok_dir, "STDOUT"), "resource.txt\n", "ok")
    assert_file_contains(os.path.join(ok_dir, "STDERR"), "", "ok")
    assert_file_contains(os.path.join(fail_dir, "STDOUT"), "", "fail")
    assert_file_contains(os.path.join(fail_dir, "STDERR"), "No such file or directory\n", "fail")

  finally:
    shutil.rmtree(tmpdir)


if __name__ == '__main__':
  example_test()
