ROOTDIR=/store/user/yiiyama/metscan
LOGDIR=/afs/cern.ch/work/y/yiiyama/metscan/logs
SCRIPT=/afs/cern.ch/user/y/yiiyama/public/metscan/analyze.py

for TIMESTAMP in $(eos ls $ROOTDIR)
do
  mkdir -p $LOGDIR/$TIMESTAMP
  for PD in $(eos ls $ROOTDIR/$TIMESTAMP)
  do
    for CRABDIR in $(eos ls $ROOTDIR/$TIMESTAMP/$PD)
    do
      echo "bsub -q 8nh -J $CRABDIR -o $LOGDIR/$TIMESTAMP/$CRABDIR 'python $SCRIPT $ROOTDIR/$TIMESTAMP/$PD'"
      ssh -oStrictHostKeyChecking=no -oLogLevel=quiet lxplus.cern.ch "bsub -q 8nh -J $CRABDIR -o $LOGDIR/$TIMESTAMP/$CRABDIR 'python $SCRIPT $ROOTDIR/$TIMESTAMP/$PD'"
      break
    done
    break
  done
  break
done
