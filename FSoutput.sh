#!/bin/bash

rm -rf fsout/
mkdir fsout
hadoop fs -get /user/cloudera/wordcount/output/* ./fsout/
#cat ./fsout/*part* > ./fsout/index.txt
