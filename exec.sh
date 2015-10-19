#!/bin/bash

SCRIPT=$1
shift
ARGS="$@"

WORKDIR=/local/metscan

date

if [ -e $WORKDIR/locks/$SCRIPT.lock ]
then
  echo "Lock for $SCRIPT exists"
  exit 0
fi

touch $WORKDIR/locks/$SCRIPT.lock

SCRIPTDIR=$WORKDIR/scripts

source /cvmfs/cms.cern.ch/cmsset_default.sh
source /cvmfs/cms.cern.ch/crab3/crab.sh
unalias eos
source $SCRIPTDIR/eos.sh

CMSSW_BASE=$WORKDIR/cmssw/CMSSW_7_4_12_scanningHalo

cd $CMSSW_BASE
eval `scram runtime -sh`

if [[ $SCRIPT =~ \.py$ ]]
then
  python $SCRIPTDIR/$SCRIPT $ARGS
else
  bash $SCRIPTDIR/$SCRIPT $ARGS
fi

rm $WORKDIR/locks/$SCRIPT.lock
