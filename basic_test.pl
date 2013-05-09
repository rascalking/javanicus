#!/usr/bin/env perl

####
# this is the sanity check i used when developing javanicus.  it runs
# a series of tests designed to exercise fuse filesystems, and reports
# on any erors at the end.
#
# to run it, you need perl installed, and you need the Test::Virtual::Filesystem
# module installed as well.
#
# $ sudo yum install perl perl-CPANPLUS
# $ sudo cpanp install Test::Virtual::Filesystem
# $ mkdir -p /tmp/hdfs
# $ javanicus.py localhost 50070 /tmp/hdfs
# $ mkdir -p /tmp/hdfs/tmp/testme
# $ basic_test.pl /tmp/hdfs/tmp/testme
# $ fusermount -u /tmp/hdfs
####

use Test::Virtual::Filesystem;

my $tester = Test::Virtual::Filesystem->new({mountdir => $ARGV[0]});

# hdfs doesn't really support symlinks correctly
$tester->enable_test_symlink(0);

# hdfs doesn't have a concept of ctime, at least not one they expose
$tester->enable_test_ctime(0);

# xattr support isn't implemented yet
$tester->enable_test_xattr(0);

$tester->runtests;
