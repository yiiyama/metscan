import sys
import os
import re

if 'CMSSW_BASE' not in os.environ:
    print 'CMSSW environment must be set'
    sys.exit(1)

sys.path.append('/cvmfs/cms.cern.ch/' + os.environ['SCRAM_ARCH'] + '/cms/cmssw/' + os.environ['CMSSW_BASE'] + '/external/' + os.environ['SCRAM_ARCH'] + '/bin')
import das_client

import config

def dasQuery(query, limit = 0):
    global das_client
    result = das_client.get_data('https://cmsweb.cern.ch', query, 0, limit, False, 300, '', '')
    return result['data']

def datasetList():
    datasets = []

    for reco in config.reconstructions:
        for row in dasQuery('dataset dataset=/*/' + reco + '-*/RECO'):
            dsdata = row['dataset'][0]
            pd = dsdata['primary_dataset']['name']
            for excl in config.datasetExcludePatterns:
                if re.match(excl + '$', pd):
                    break
            else:
                datasets.append((pd, dsdata['processed_ds_name']))
    
    return datasets
