#!/bin/bash

#clean input
hadoop fs -rm -r /user/cloudera/wordcount/input

#make input folder
hadoop fs -mkdir /user/cloudera/wordcount/input

#add in new files to the input
hadoop fs -put ./temp/* /user/cloudera/wordcount/input
