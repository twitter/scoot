Runner is a library for running Scoot processes.

The top-level interface is Runner. It's a system that runs processes against snapshots and lets you query their status.

There's an implementation in local simpleRunner. This will run at most process at a time.

Underneath Runner is Execer. It is just to run a Unix process locally in a directory. It should be free of Scoot abstractions.

There is a SimExecer that simulates behavior based on the args passed to it. This lets a caller of a Runner script the behavior of the Execer.

There will be an Execer that calls os/exec to exec.