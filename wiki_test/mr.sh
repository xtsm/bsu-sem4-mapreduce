#!/usr/bin/env bash
./build/mapreduce map ./build/wiki_url_map urls.txt medium.txt
./build/mapreduce reduce ./build/wiki_reduce <(cat medium.txt wordfilter.txt) output.txt
rm medium.txt
