#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
mkdir -p build/
cd build/
cmake ../../
make
cd ../

generate_urls() {
  cat nouns.txt | shuf | head -$1 | sed "s/\(.*\)/https:\/\/en.wikipedia.org\/wiki\/\1\t/" > $2
}

generate_wordfilter() {
  cat words.txt | shuf | head -$1 | sed "s/\(.*\)/\1\t/" > $2
}

while [ 1 = 1 ]
do
  generate_urls 50 urls.txt
  generate_wordfilter 30 wordfilter.txt
  echo -n "running MR... "
  ./mr.sh
  echo "done, press any key to continue or q to quit"
  read -n 1 -r -s
  rm urls.txt wordfilter.txt
  [ "$REPLY" = "q" ] && break
done
