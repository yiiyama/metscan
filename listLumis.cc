#include "TFile.h"
#include "TTree.h"

#include <fstream>

void
listLumis()
{
  auto* source(TFile::Open("/data/scratch/sorted.root"));
  auto* tree(static_cast<TTree*>(source->Get("metscan")));

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


  unsigned run(0);
  unsigned lumi(0);
  bool pds[nD]{};
  tree->SetBranchStatus("*", 0);
  tree->SetBranchStatus("run", 1);
  tree->SetBranchStatus("lumi", 1);
  tree->SetBranchStatus("pd_*", 1);
  tree->SetBranchAddress("run", &run);
  tree->SetBranchAddress("lumi", &lumi);
  for (unsigned iD(0); iD != nD; ++iD)
    tree->SetBranchAddress("pd_" + pdNames[iD], pds + iD);

  unsigned currentRun(0);
  unsigned currentLumi(0);
  bool hasPD[nD]{};

  std::ofstream output("/data/scratch/lumiList_sorted.txt");

  long iEntry(0);
  while (tree->GetEntry(iEntry++) > 0) {
    if (run != currentRun || lumi != currentLumi) {
      if (currentRun != 0) {
	for (unsigned iD(0); iD != nD; ++iD) {
	  if (hasPD[iD])
	    output << run << " " << lumi << " " << pdIds[iD] << std::endl;
	}
      }
      currentRun = run;
      currentLumi = lumi;
      for (bool& has : hasPD)
	has = false;
    }
    for (unsigned iD(0); iD != nD; ++iD) {
      if (pds[iD])
	hasPD[iD] = true;
    }
  }

  delete source;
}
