#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
mkdir -p build/
cd build/
cmake ../../
make
cd ../

while [ 1 = 1 ]
do
  cat nouns.txt | shuf | head -50 | sed "s/\(.*\)/https:\/\/en.wikipedia.org\/wiki\/\1\t/" > urls.txt
  cat words.txt | shuf | head -30 | sed "s/\(.*\)/\1\t/" > wordfilter.txt
  echo -n "running MR... "
  ./mr.sh
  echo "done, press any key to continue or q to quit"
  read -n 1 -r -s
  rm urls.txt wordfilter.txt
  [ "$REPLY" = "q" ] && break
done
