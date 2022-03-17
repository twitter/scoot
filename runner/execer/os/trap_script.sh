#!/bin/bash
trap ':' SIGTERM

while :
do
	python3 -c 'import time; exec("x=[]\nfor i in range(1):\n x.append(\" \" * 1024*1024)\n time.sleep(.1)")'
done
