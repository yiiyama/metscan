#!/bin/bash

KILL=false

while [ $# -gt 0 ]
do
  case $1 in
    -k)
      KILL=true
      shift
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

source /cvmfs/cms.cern.ch/cmsset_default.sh
source /cvmfs/cms.cern.ch/crab3/crab.sh

CMSSW_BASE=/local/metscan/cmssw/CMSSW_7_4_12_scanningHalo

cd $CMSSW_BASE
eval `scram runtime -sh`

JOBDIR=/local/metscan/jobs

for TIMESTAMP in $(ls $JOBDIR)
do
  cd $JOBDIR/$TIMESTAMP
  for CRABJOB in $(ls -d crab_*)
  do
    if $KILL
    then
      crab kill -d $CRABJOB
    else
      STATUS=$(crab status -d $CRABJOB | awk '/^Jobs status/ {print $3}')
      if [ "$STATUS" = "finished" ] || [ "$STATUS" = "failed" ]
      then
        echo "rm -rf $JOBDIR/$TIMESTAMP/$CRABJOB"
        rm -rf $JOBDIR/$TIMESTAMP/$CRABJOB
      fi
    fi
  done
  if [ $(ls -d $JOBDIR/$TIMESTAMP/crab_* 2>/dev/null | wc -l) -eq 0 ]
  then
    cd $JOBDIR
    rm -rf $TIMESTAMP
  fi
done
