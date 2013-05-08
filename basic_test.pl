#!/usr/bin/env perl

use Test::Virtual::Filesystem;

my $tester = Test::Virtual::Filesystem->new({mountdir => $ARGV[0]});
$tester->enable_test_symlink(0);
$tester->runtests;
