dbuser = 'cmsmet'
dbpass = 'FindBSM'
dbhost = '127.0.0.1'
dbname = 'metscan'

reconstructions = ['Run2015D-PromptReco']

datasetExcludePatterns = ['HLT.*', '.*_0T', 'Commissioning', 'Cosmics']

cmsswbases = {
    'Run2015D-PromptReco': ('CMSSW_7_4_12', 'CMSSW_7_4_12_scanningHalo')
}

eosdir = 'root://eoscms.cern.ch//eos/cms/store/caf/user/yiiyama/metscan'

submitMax = 1000 # submissions per run
