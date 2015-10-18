#!/bin/bash

RELEASE=$1
TARBALL=$2
RECO=$3
DATASET=$4
SOURCE=$5
FILEID=$6

source /cvmfs/cms.cern.ch/cmsset_default.sh

cd $TMPDIR
scram p CMSSW $RELEASE
cd $RELEASE
eval `scram runtime -sh`
cd -

USER=$(id -un)
export X509_USER_PROXY=/afs/cern.ch/user/${USER:0:1}/${USER}/x509up_u$(id -u)

xrdcp root://eoscms.cern.ch//eos/cms/store/caf/user/yiiyama/metscan/$TARBALL.tar.gz $PWD/$TARBALL.tar.gz
tar xzf $TARBALL.tar.gz -C $CMSSW_BASE

OUTPUTNAME=$(basename $SOURCE)

cmsRun $CMSSW_BASE/src/ntuplize.py inputFiles=$SOURCE outputFile=$OUTPUTNAME

if [ $? -ne 0 ]
then
  echo "cmsRun failed."
  exit 255
fi

xrdcp $PWD/$OUTPUTNAME root://eoscms.cern.ch//eos/cms/store/caf/user/yiiyama/metscan/$RECO/$DATASET/$OUTPUTNAME

if [ $? -ne 0 ]
then
  echo "copy failed."
  exit 1
fi

echo 'UPDATE `files` SET `status` = '"'ntuplized'"' WHERE `fileid` = '$FILEID';' | mysql -h cms-metscan.cern.ch -u cmsmet -pFindBSM -D metscan
