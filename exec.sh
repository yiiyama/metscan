#!/bin/bash

SCRIPT=$1

source /cvmfs/cms.cern.ch/cmsset_default.sh

CMSSW_BASE=/local/metscan/cmssw/CMSSW_7_4_12_scanningHalo

cd $CMSSW_BASE
eval `scram runtime -sh`

python /local/metscan/scripts/$SCRIPT
