dbuser = 'cmsmet'
dbpass = 'FindBSM'
dbhost = '127.0.0.1'
dbname = 'metscan'

installdir = '/local/metscan'

reconstructions = ['Run2015D-PromptReco']

datasetExcludePatterns = ['HLT.*', '.*_0T', 'Commissioning', 'Cosmics', 'TOTEM.*', 'ToTOTEM.*', 'L1MinimumBias.*']

cmsswbases = {
    'Run2015D-PromptReco': ('CMSSW_7_4_12', 'CMSSW_7_4_12_scanningHalo')
}

filters = [
    "tracking_letmc",
    "tracking_letms",
    "tracking_msc",
    "tracking_tmsc",
    "csc",
    "csc2015",
    "cscTMU",
    "halo",
    "hbher1",
    "hbher1nozeros",
    "hbher2l",
    "hbher2t",
    "hbheiso",
    "ecaltp",
    "ecalbe",
    "ecalsc"
]

eosdir = '/store/user/yiiyama/metscan'

submitMax = 1000 # submissions per run
