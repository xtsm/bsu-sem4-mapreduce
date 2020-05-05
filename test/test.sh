#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
mkdir -p build/
cd build/
cmake ../../
make
cd ../

i=1
while [ -r data/input$i.txt ] && [ -r data/medium$i.txt ] && [ -r data/output$i.txt ]
do
  echo " === $i ==="
  ./build/mapreduce map ./build/wordcount_map data/input$i.txt medium.txt
  ./build/mapreduce reduce ./build/wordcount_reduce medium.txt output.txt
  diff <(sort medium.txt) <(sort data/medium$i.txt)
  diff <(sort output.txt) <(sort data/output$i.txt)
  let i+=1
done
