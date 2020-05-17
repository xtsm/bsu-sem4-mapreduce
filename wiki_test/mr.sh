#!/usr/bin/env bash
set -e
./build/mapreduce map ./build/wiki_url_map "$1" medium.txt -s 1
./build/mapreduce reduce ./build/wiki_reduce <(cat medium.txt "$2") "$3"
rm medium.txt
