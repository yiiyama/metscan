#include "connection.h"
#include "query.h"
#include "result.h"
#include "row.h"

#include "TFile.h"
#include "TTree.h"
#include "TString.h"

#include <iostream>

enum Table {
  kEventTags,
  kEventData,
  kDatasetRel
};

void
dumpToROOT(Table table)
{
  mysqlpp::Connection connection("metscan", "localhost", "cmsmet", "FindBSM");

  TString queryStr;
  TString outputDir;
  TString treeName;
  TString treeTitle;
  TString dataBranch;

  switch (table) {
  case kEventTags:
    queryStr = "SELECT `run`, `event`, `filterid` FROM `eventtags` WHERE `recoid` = 1";
    outputDir = "/data/scratch/eventtags";
    treeName = "eventtags";
    treeTitle = "tag info";
    dataBranch = "filterid";
    break;
  case kEventData:
    queryStr = "SELECT `run`, `event`, `lumi` FROM `eventdata` WHERE `recoid` = 1";
    outputDir = "/data/scratch/eventdata";
    treeName = "eventdata";
    treeTitle = "lumi info";
    dataBranch = "lumi";
    break;
  case kDatasetRel:
    queryStr = "SELECT `run`, `event`, `datasetid` FROM `datasetrel`";
    outputDir = "/data/scratch/datasetrel";
    treeName = "datasetrel";
    treeTitle = "dataset info";
    dataBranch = "datasetid";
    break;
  };

  mysqlpp::Query query(connection.query(queryStr));

  auto result(query.use());
  mysqlpp::Row row;

  unsigned run(0);
  unsigned event(0);
  unsigned data(0);

  unsigned currentRun(0);
  TTree* tree(0);

  unsigned long long iRow(0);
  while ((row = result.fetch_row())) {
    if ((iRow++) % 100000 == 0)
      std::cout << iRow << std::endl;

    run = int(row[0]);
    event = int(row[1]);
    data = int(row[2]);

    if (run != currentRun) {
      if (tree) {
	auto* currentFile(tree->GetCurrentFile());
	currentFile->cd();
	tree->Write();
	delete currentFile;
      }

      auto* newFile(TFile::Open(outputDir + TString::Format("/%d.root", run), "update"));
      tree = static_cast<TTree*>(newFile->Get(treeName));

      if (tree) {
	tree->SetBranchAddress("run", &run);
	tree->SetBranchAddress("event", &event);
	tree->SetBranchAddress(dataBranch, &data);
      }
      else {
	tree = new TTree(treeName, treeTitle);
	tree->Branch("run", &run, "run/i");
	tree->Branch("event", &event, "event/i");
	tree->Branch(dataBranch, &data, dataBranch + "/i");
      }

      currentRun = run;
    }

    tree->Fill();
  }

  if (tree) {
    auto* currentFile(tree->GetCurrentFile());
    currentFile->cd();
    tree->Write();
    delete currentFile;
  }
}
