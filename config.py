dbuser = 'cmsmet'
dbpass = 'FindBSM'
dbhost = '127.0.0.1'
dbname = 'metscan'

installdir = '/local/metscan'
scratchdir = '/data/scratch'

reconstructions = ['Run2015D-PromptReco']

datasets = [
    'BTagCSV',
    'BTagMu',
    'Charmonium',
    'DisplacedJet',
    'DoubleEG',
    'DoubleMuon',
    'DoubleMuonLowMass',
    'HTMHT',
    'JetHT',
    'MET',
    'MinimumBias',
    'MuOnia',
    'MuonEG',
    'NoBPTX',
    'SingleElectron',
    'SingleMuon',
    'SinglePhoton',
    'Tau'
]

cmsswbases = {
    'Run2015D-PromptReco': ('CMSSW_7_4_12', 'CMSSW_7_4_12_scanningHalo')
}

dcsJsons = [
    '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions15/13TeV/DCSOnly/json_DCSONLY.txt',
    '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions15/13TeV/DCSOnly/json_DCSONLY_0T.txt',
    '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions15/13TeV/DCSOnly/json_DCSONLY_2.8T.txt'
]

goldenJson = '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions15/13TeV/Cert_246908-260627_13TeV_PromptReco_Collisions15_25ns_JSON.txt'
silverJson = '/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions15/13TeV/Cert_246908-260627_13TeV_PromptReco_Collisions15_25ns_JSON_Silver.txt'

eosdir = '/eos/cms/store/user/yiiyama/metscan'
