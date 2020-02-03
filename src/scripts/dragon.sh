#!/usr/bin/env bash

DIR=`realpath "$0" | sed 's|\(.*\)/.*|\1|'`
export DRAGON_HOME=$DIR/..
export RUN_JAVA_OPTS=-javaagent:$DRAGON_HOME/lib/dragon.jar
$DIR/run-java.sh dragon.Run "$@"