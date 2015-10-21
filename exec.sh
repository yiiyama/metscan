#!/bin/bash

CRON=false
USECRAB=false

while true
do
  case $1 in
    -C)
      CRON=true
      shift
      ;;
    -R)
      USECRAB=true
      shift
      ;;
    *)
      break
      ;;
  esac
done

SCRIPT=$1

shift
ARGS="$@"

WORKDIR=/local/metscan

date

if $CRON && [ -e $WORKDIR/locks/cron.lock ]
then
  echo "Lock for cron job exists"
  exit 0
fi

if [ -e $WORKDIR/locks/$SCRIPT.lock ]
then
  echo "Lock for $SCRIPT exists"
  exit 0
fi

touch $WORKDIR/locks/$SCRIPT.lock

SCRIPTDIR=$WORKDIR/scripts

source /cvmfs/cms.cern.ch/cmsset_default.sh

if $USECRAB
then
  CMSSW_BASE=$WORKDIR/cmssw/CMSSW_7_4_12_scanningHalo

  cd $CMSSW_BASE
  eval `scram runtime -sh`

  source /cvmfs/cms.cern.ch/crab3/crab.sh
else
  EXTERNALS="external/gcc/4.9.1-cms external/glibc/2.12-1.149.el6 external/python/2.7.6-kpegke lcg/root/6.02.10-kpegke2 external/xz/5.2.1 external/libjpg/8b-cms external/libpng/1.6.16-eccfad external/boost/1.57.0-kpegke"
  for EXT in $EXTERNALS
  do
    source /cvmfs/cms.cern.ch/slc6_amd64_gcc491/$EXT/etc/profile.d/init.sh
  done
fi

unalias eos
source $SCRIPTDIR/eos.sh

cd $SCRIPTDIR

if [[ $SCRIPT =~ \.py$ ]]
then
  python $SCRIPT $ARGS
else
  bash $SCRIPT $ARGS
fi

rm $WORKDIR/locks/$SCRIPT.lock
