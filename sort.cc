#include "TFile.h"
#include "TTree.h"
#include "TString.h"

#include <algorithm>
#include <iostream>
#include <bitset>

void
sort()
{
  auto* outputFile(TFile::Open("/data/scratch/sorted.root", "recreate"));
  auto* output(new TTree("metscan", "metscan"));

  unsigned run;
  unsigned lumi;
  unsigned event;
  TString pdNames[] = {
    "BTagCSV",
    "BTagMu",
    "Charmonium",
    "DisplacedJet",
    "DoubleEG",
    "DoubleMuon",
    "DoubleMuonLowMass",
    "HTMHT",
    "JetHT",
    "MET",
    "MinimumBias",
    "MuOnia",
    "MuonEG",
    "NoBPTX",
    "SingleElectron",
    "SingleMuon",
    "SinglePhoton",
    "Tau"
  };
  unsigned const nD(sizeof(pdNames) / sizeof(TString));
  unsigned const pdIds[] = {
    1, //BTagCSV           |
    2, //BTagMu            |
    3, //Charmonium        |
    6, //DisplacedJet      |
    7, //DoubleEG          |
    8, //DoubleMuon        |
    9, //DoubleMuonLowMass |
    10, //HTMHT             |
    13, //JetHT             |
    14, //MET               |
    15, //MinimumBias       |
    16, //MuOnia            |
    17, //MuonEG            |
    18, //NoBPTX            |
    20, //SingleElectron    |
    21, //SingleMuon        |
    22, //SinglePhoton      |
    23 //Tau               |
  };
  std::vector<unsigned> pdIdMap(pdIds[nD - 1] + 1, -1);
  unsigned iP(0);
  for (unsigned pdId : pdIds)
    pdIdMap[pdId] = iP++;

  bool pds[nD];
  TString filterNames[] = {
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
    "ecalsc",
    "ecalscn1023023",
    "ecalscp1048098",
    "ecalscn1078063",
    "ecalscn1043093",
    "badTrack",
    "badMuonTrack"
  };
  unsigned const nF(sizeof(filterNames) / sizeof(TString));
  bool tags[nF];

  output->Branch("run", &run, "run/i");
  output->Branch("lumi", &lumi, "lumi/i");
  output->Branch("event", &event, "event/i");
  for (unsigned iD(0); iD != nD; ++iD)
    output->Branch("pd_" + pdNames[iD], pds + iD, "pd_" + pdNames[iD] + "/O");
  for (unsigned iF(0); iF != nF; ++iF)
    output->Branch("filter_" + filterNames[iF], tags + iF, "filter_" + filterNames[iF] + "/O");

  std::vector<TString> fileNames{
    "256630.root",  "256734.root",  "256926.root",  "257399.root",  "257614.root",  "257804.root",  "257969.root",  "258177.root",  "258320.root",  "258434.root",  "258656.root",  "258714.root",  "259636.root",  "259809.root",  "259822.root",  "260425.root",  "260536.root",  "260627.root",
    "256673.root",  "256801.root",  "256936.root",  "257400.root",  "257645.root",  "257805.root",  "258129.root",  "258211.root",  "258335.root",  "258440.root",  "258694.root",  "258741.root",  "259637.root",  "259810.root",  "259861.root",  "260426.root",  "260538.root",
    "256674.root",  "256842.root",  "256941.root",  "257461.root",  "257682.root",  "257816.root",  "258136.root",  "258213.root",  "258403.root",  "258443.root",  "258702.root",  "258742.root",  "259681.root",  "259811.root",  "259862.root",  "260427.root",  "260540.root",
    "256675.root",  "256843.root",  "257394.root",  "257487.root",  "257722.root",  "257819.root",  "258157.root",  "258214.root",  "258425.root",  "258444.root",  "258703.root",  "258745.root",  "259682.root",  "259813.root",  "259884.root",  "260431.root",  "260541.root",
    "256676.root",  "256866.root",  "257395.root",  "257490.root",  "257723.root",  "257821.root",  "258158.root",  "258215.root",  "258426.root",  "258445.root",  "258705.root",  "258749.root",  "259683.root",  "259817.root",  "259890.root",  "260528.root",  "260575.root",
    "256677.root",  "256867.root",  "257396.root",  "257531.root",  "257732.root",  "257822.root",  "258159.root",  "258287.root",  "258427.root",  "258446.root",  "258706.root",  "258750.root",  "259685.root",  "259818.root",  "259891.root",  "260532.root",  "260576.root",
    "256728.root",  "256868.root",  "257397.root",  "257599.root",  "257735.root",  "257823.root",  "258174.root",  "258312.root",  "258428.root",  "258448.root",  "258712.root",  "259464.root",  "259686.root",  "259820.root",  "260373.root",  "260533.root",  "260577.root",
    "256729.root",  "256869.root",  "257398.root",  "257613.root",  "257751.root",  "257968.root",  "258175.root",  "258313.root",  "258432.root",  "258655.root",  "258713.root",  "259626.root",  "259721.root",  "259821.root",  "260424.root",  "260534.root",  "260593.root"
  };

  std::sort(fileNames.begin(), fileNames.end());

  for (auto& fileName : fileNames) {
    std::cout << fileName << std::endl;

    auto* tagSource(TFile::Open("/data/scratch/eventtags/" + fileName));
    auto* dataSource(TFile::Open("/data/scratch/eventdata/" + fileName));
    auto* relSource(TFile::Open("/data/scratch/datasetrel/" + fileName));

    auto* tagTree(static_cast<TTree*>(tagSource->Get("eventtags")));
    auto* dataTree(static_cast<TTree*>(dataSource->Get("eventdata")));
    auto* relTree(static_cast<TTree*>(relSource->Get("datasetrel")));

    std::map<unsigned long long, std::bitset<nF>> results;
    std::map<unsigned long long, unsigned> lumis;
    std::map<unsigned long long, std::bitset<nD>> datasets;

    tagTree->SetBranchAddress("run", &run);
    tagTree->SetBranchAddress("event", &event);
    dataTree->SetBranchAddress("run", &run);
    dataTree->SetBranchAddress("event", &event);
    relTree->SetBranchAddress("run", &run);
    relTree->SetBranchAddress("event", &event);

    unsigned filterid;
    unsigned datasetid;
    tagTree->SetBranchAddress("filterid", &filterid);
    dataTree->SetBranchAddress("lumi", &lumi);
    relTree->SetBranchAddress("datasetid", &datasetid);

    long iEntry(0);
    while (tagTree->GetEntry(iEntry++) > 0)
      results[((unsigned long long)(run) << 32) + event].set(filterid - 1);
    
    iEntry = 0;
    while (dataTree->GetEntry(iEntry++) > 0)
      lumis.emplace(((unsigned long long)(run) << 32) + event, lumi);

    iEntry = 0;
    while (relTree->GetEntry(iEntry++) > 0)
      datasets[((unsigned long long)(run) << 32) + event].set(pdIdMap[datasetid]);

    iEntry = 0;
    for (auto&& result : results) {
      if ((iEntry++ % 10000) == 1)
	std::cout << iEntry << std::endl;

      run = result.first >> 32;
      lumi = lumis[result.first];
      event = result.first & 0x00000000ffffffff;

      for (unsigned iF(0); iF != nF; ++iF)
	tags[iF] = result.second[iF];

      for (unsigned iD(0); iD != nD; ++iD)
	pds[iD] = datasets[result.first][iD];

      output->Fill();
    }

    delete tagSource;
    delete dataSource;
    delete relSource;
  }

  outputFile->cd();
  outputFile->Write();
  delete outputFile;
}
