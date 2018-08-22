#!/usr/bin/env bash

ROOT=$(cd $(dirname $0)/..; pwd)
SAMPLE_ROOT=$ROOT/sample
LOCAL_MAVEN_REPO=$ROOT/build/repo

(
  cd $SAMPLE_ROOT

  ## to remove cache
  rm -rfv .digdag

  ## run
  digdag run plugin.dig -p repos=${LOCAL_MAVEN_REPO} --no-save
)
