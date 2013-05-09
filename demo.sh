#!/bin/bash

####
# a simple demo of the javanicus webhdfs fuse driver
#
# * mounts the cluster's hdfs store
# * copies in a text file
# * runs wordcount on it
# * heads the output
# * unmounts the hdfs store
####

# echo the commands we're running
set -x

# create the mount point
mkdir -p /tmp/hdfs

# mount it
./javanicus.py localhost 50070 /tmp/hdfs

# copy in a text file to analyze
cp -f Principia.txt /tmp/hdfs/user/cloudera

# make sure we don't have the output from a previous run left over
test -e /tmp/hdfs/user/cloudera/demo.out && rm -rf /tmp/hdfs/user/cloudera/demo.out

# run mapreduce
hadoop jar \
	/usr/lib/hadoop-0.20-mapreduce/hadoop-2.0.0-mr1-cdh4.2.0-examples.jar \
	wordcount /user/cloudera/Principia.txt /user/cloudera/demo.out

# verify the output
head /tmp/hdfs/user/cloudera/demo.out/part-r-00000

# unmount it
fusermount -u /tmp/hdfs
