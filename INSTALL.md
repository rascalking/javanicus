## PREREQUISITES

Javanicus is a FUSE driver, so you'll need FUSE installed.

    $ sudo yum install fuse

If you don't want to have to use sudo to mount a WebHDFS server, you'll want to make sure your user is a member of the fuse group.  Fine the line in your /etc/groups file for the fuse group, and add your user to it.

    $ sudo vigr

Javanicus is written in python, so you'll need a copy of python.  It's been tested with 2.6 and 2.7.

    $ sudo yum install python

It depends on the fusepy and requests libraries.

    $ sudo pip install fusepy requests

Javanicus is a WebHDFS FUSE driver, so you'll need an HDFS server with WebHDFS enabled on it.  It was primarily tested with a cluster running on a [Cloudera CDH 4.2 quickstart demo VM](https://ccp.cloudera.com/display/SUPPORT/Cloudera+QuickStart+VM).

If you want to run Javanicus from a machine besides the namenode or a datanode, you'll need to enable listening to the wildcard address (0.0.0.0) for that service on that machine.  If you're using the Cloudera quickstart VM, you can use the Cloudera Manager webapp to do that.  Select `hdfs1->Configuration->NameNode (Base)->Ports and Addresses->Bind NameNode to Wildcard Address` and `hdfs1->Configuration->DataNode(Base)->Ports and Addresses->Bind NameNode to Wildcard Address`.  Then restart the hdfs1 service.

## HOW TO RUN

First, you'll need to create a mountpoint.

    $ mkdir /tmp/hdfs

Then, you can to start javanicus.

    $ javanicus.py namenode.mydomain.org 50070 /tmp/hdfs

And from there, it's just another mounted filesystem.  You can copy files in and out with a file manager or with cp on the command line.  You can edit files with vim, or with Eclipse.

## SAMPLE USE WITH A WORDCOUNT JOB

    [cloudera@localhost javanicus]$ ./demo.sh 
    + mkdir -p /tmp/hdfs
    + ./javanicus.py localhost 50070 /tmp/hdfs
    + cp -f Principia.txt /tmp/hdfs/user/cloudera
    + test -e /tmp/hdfs/user/cloudera/demo.out
    + rm -rf /tmp/hdfs/user/cloudera/demo.out
    + hadoop jar /usr/lib/hadoop-0.20-mapreduce/hadoop-2.0.0-mr1-cdh4.2.0-examples.jar wordcount /user/cloudera/Principia.txt /user/cloudera/demo.out
    13/05/09 11:34:20 WARN mapred.JobClient: Use GenericOptionsParser for parsing the arguments. Applications should implement Tool for the same.
    13/05/09 11:34:21 INFO input.FileInputFormat: Total input paths to process : 1
    13/05/09 11:34:21 INFO mapred.JobClient: Running job: job_201305091054_0006
    13/05/09 11:34:22 INFO mapred.JobClient:  map 0% reduce 0%
    13/05/09 11:34:33 INFO mapred.JobClient:  map 100% reduce 0%
    13/05/09 11:34:41 INFO mapred.JobClient:  map 100% reduce 100%
    13/05/09 11:34:44 INFO mapred.JobClient: Job complete: job_201305091054_0006
    13/05/09 11:34:44 INFO mapred.JobClient: Counters: 32
    13/05/09 11:34:44 INFO mapred.JobClient:   File System Counters
    13/05/09 11:34:44 INFO mapred.JobClient:     FILE: Number of bytes read=54138
    13/05/09 11:34:44 INFO mapred.JobClient:     FILE: Number of bytes written=414553
    13/05/09 11:34:44 INFO mapred.JobClient:     FILE: Number of read operations=0
    13/05/09 11:34:44 INFO mapred.JobClient:     FILE: Number of large read operations=0
    13/05/09 11:34:44 INFO mapred.JobClient:     FILE: Number of write operations=0
    13/05/09 11:34:44 INFO mapred.JobClient:     HDFS: Number of bytes read=130927
    13/05/09 11:34:44 INFO mapred.JobClient:     HDFS: Number of bytes written=67637
    13/05/09 11:34:44 INFO mapred.JobClient:     HDFS: Number of read operations=2
    13/05/09 11:34:44 INFO mapred.JobClient:     HDFS: Number of large read operations=0
    13/05/09 11:34:44 INFO mapred.JobClient:     HDFS: Number of write operations=1
    13/05/09 11:34:44 INFO mapred.JobClient:   Job Counters 
    13/05/09 11:34:44 INFO mapred.JobClient:     Launched map tasks=1
    13/05/09 11:34:44 INFO mapred.JobClient:     Launched reduce tasks=1
    13/05/09 11:34:44 INFO mapred.JobClient:     Data-local map tasks=1
    13/05/09 11:34:44 INFO mapred.JobClient:     Total time spent by all maps in occupied slots (ms)=13066
    13/05/09 11:34:44 INFO mapred.JobClient:     Total time spent by all reduces in occupied slots (ms)=6912
    13/05/09 11:34:44 INFO mapred.JobClient:     Total time spent by all maps waiting after reserving slots (ms)=0
    13/05/09 11:34:44 INFO mapred.JobClient:     Total time spent by all reduces waiting after reserving slots (ms)=0
    13/05/09 11:34:44 INFO mapred.JobClient:   Map-Reduce Framework
    13/05/09 11:34:44 INFO mapred.JobClient:     Map input records=3095
    13/05/09 11:34:44 INFO mapred.JobClient:     Map output records=21068
    13/05/09 11:34:44 INFO mapred.JobClient:     Map output bytes=210581
    13/05/09 11:34:44 INFO mapred.JobClient:     Input split bytes=126
    13/05/09 11:34:44 INFO mapred.JobClient:     Combine input records=21068
    13/05/09 11:34:44 INFO mapred.JobClient:     Combine output records=6829
    13/05/09 11:34:44 INFO mapred.JobClient:     Reduce input groups=6829
    13/05/09 11:34:44 INFO mapred.JobClient:     Reduce shuffle bytes=54134
    13/05/09 11:34:44 INFO mapred.JobClient:     Reduce input records=6829
    13/05/09 11:34:44 INFO mapred.JobClient:     Reduce output records=6829
    13/05/09 11:34:44 INFO mapred.JobClient:     Spilled Records=13658
    13/05/09 11:34:44 INFO mapred.JobClient:     CPU time spent (ms)=3540
    13/05/09 11:34:44 INFO mapred.JobClient:     Physical memory (bytes) snapshot=258781184
    13/05/09 11:34:44 INFO mapred.JobClient:     Virtual memory (bytes) snapshot=1408012288
    13/05/09 11:34:44 INFO mapred.JobClient:     Total committed heap usage (bytes)=175312896
    + head /tmp/hdfs/user/cloudera/demo.out/part-r-00000
    "	2
    "A	1
    "ANERISTIC"	1
    "And	1
    "And,	1
    "But	4
    "COSMOGONY"	1
    "COSMOLOGY"	1
    "COSMOLOGY"*	1
    "Courtesy	1
    + fusermount -u /tmp/hdfs
