#!/bin/bash -eu

RED=$(tput setaf 1)
GREEN=$(tput setaf 2)
BLUE=$(tput setaf 4)
NONE=$(tput op)

function wait_for_modified(){
  inotifywait --quiet --recursive --event modify,move,create,delete .
}
function build(){
  go test -v -timeout 5s ./... |
    sed -e "s/PASS/${GREEN}PASS${NONE}/g" -e "s/FAIL/${RED}FAIL${NONE}/g"
}
build
while wait_for_modified
do
  echo
  echo "###### RUN #####"
  echo
  build
done
