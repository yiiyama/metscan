#!/bin/bash

SCRIPT=$1

SCRIPTDIR=/local/metscan/scripts

source /cvmfs/cms.cern.ch/cmsset_default.sh
source /cvmfs/cms.cern.ch/crab3/crab.sh
unalias eos
source $SCRIPTDIR/eos.sh

CMSSW_BASE=/local/metscan/cmssw/CMSSW_7_4_12_scanningHalo

cd $CMSSW_BASE
eval `scram runtime -sh`

python $SCRIPTDIR/$SCRIPT
