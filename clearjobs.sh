#!/bin/bash

source /cvmfs/cms.cern.ch/cmsset_default.sh
source /cvmfs/cms.cern.ch/crab3/crab.sh

CMSSW_BASE=/local/metscan/cmssw/CMSSW_7_4_12_scanningHalo

cd $CMSSW_BASE
eval `scram runtime -sh`

JOBDIR=/local/metscan/jobs

for TIMESTAMP in $(ls $JOBDIR)
do
  cd $JOBDIR/$TIMESTAMP
  for CRABJOB in $(ls -d $JOBDIR/$TIMESTAMP/crab_*)
  do
    STATUS=$(crab status -d $CRABJOB | awk '/^Jobs status/ {print $3}')
    if [ "$STATUS" = "finished" ]
    then
      echo "rm -rf $JOBDIR/$TIMESTAMP/$CRABJOB"
      rm -rf $JOBDIR/$TIMESTAMP/$CRABJOB
    fi
  done
  if [ $(ls -d $JOBDIR/$TIMESTAMP/crab_* | wc -l) -eq 0 ]
  then
    cd $JOBDIR
    rm $TIMESTAMP
  fi
done
