#!/usr/bin/env perl

use Test::Virtual::Filesystem;
Test::Virtual::Filesystem->new({mountdir => $ARGV[0]})->runtests;
exit 0;
