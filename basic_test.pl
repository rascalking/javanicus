#!/usr/bin/env perl

use Test::Virtual::Filesystem;

my $tester = Test::Virtual::Filesystem->new({mountdir => $ARGV[0]});

# hdfs doesn't really support symlinks correctly
$tester->enable_test_symlink(0);

# hdfs doesn't have a concept of ctime they expose
$tester->enable_test_ctime(0);

$tester->runtests;
