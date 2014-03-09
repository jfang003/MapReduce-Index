#!/bin/bash

#run job
cd classes/artifacts/MRIndex/ 
hadoop jar MRIndex.jar /user/cloudera/wordcount/input /user/cloudera/wordcount/output
#print the output
cd ../../..
hadoop fs -cat /user/cloudera/wordcount/output/* > out.txt
cat out.txt
#clean up
hadoop fs -rm -r /user/cloudera/wordcount/output
