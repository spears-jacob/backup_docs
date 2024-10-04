#! /usr/bin/perl
# This script output the oldest date from filenames that contain YYYY-MM-DD dates and end in gz
use strict;
use warnings;
use Cwd;

#-- get current directory
my $pwd = cwd();
if ($ARGV[0]) {chdir($ARGV[0]);}

#http://perldoc.perl.org/perlfaq4.html#How-can-I-remove-duplicate-elements-from-a-list-or-array%3f
sub uniq {
    my %seen;
    grep !$seen{$_}++, @_;
}

#get list of files from current directory and put them in an array
my @files =
   grep { -f }
   glob("*gz");

#replace filename with date string extracted from file name over every element of the array
s/.+\D(\d\d\d\d)-(\d\d)-(\d\d).+/$1-$2-$3/g for @files;

#sort the date-filled array and remove duplicates
my @unique_files = uniq(@files);
my @sorted_files = sort (@unique_files);

print $sorted_files[0];
