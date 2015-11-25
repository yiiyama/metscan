#include "TFile.h"
#include "TTree.h"
#include "TString.h"

#include <fstream>
#include <iostream>
#include <algorithm>
#include <vector>
#include <map>
#include <set>

class TreeWriter {
public:
  TreeWriter(char const* outputDir);
  ~TreeWriter() {}

  void addFilter(unsigned idx, char const* name) { filterIndices_.push_back(idx); filterNames_.push_back(name); }
  void addLumiMask(unsigned long long run, unsigned long long lumi) { lumiMask_.insert((run << 32) | lumi); }
  void clearLumiMask() { lumiMask_.clear(); }

  bool dump(char const* inputPath, unsigned datasetid);

  std::vector<unsigned> getAnalyzedRuns() const;
  std::vector<unsigned> getAnalyzedLumis(unsigned run) { return analyzedLumis_[run]; }

  void resetRuns() { analyzedLumis_.clear(); nLumis_ = 0; }

private:
  bool openOutput_(unsigned run);
  bool closeOutput_();

  TString const outputDir_;
  std::vector<TString> filterNames_{};
  std::vector<unsigned> filterIndices_{};
  std::set<unsigned long long> lumiMask_{};

  TTree* currentOutput_{0};

  std::map<unsigned, std::vector<unsigned>> analyzedLumis_{};
};

TreeWriter::TreeWriter(char const* _outputDir) :
  outputDir_(_outputDir)
{
}  

bool
TreeWriter::dump(char const* _inputPath, unsigned _datasetid)
{
  auto* source(TFile::Open(_inputPath));
  if (!source || source->IsZombie())
    return false;

  unsigned nFilters(filterNames_.size());

  auto* events(static_cast<TTree*>(source->Get("ntuples/metfilters")));
  auto* lumis(static_cast<TTree*>(source->Get("ntuples/lumis")));

  unsigned run;
  unsigned lumi;
  unsigned event;
  float pfMET;
  bool* results(new bool[nFilters]);
  std::fill_n(results, nFilters, true);

  events->SetBranchAddress("run", &run);
  events->SetBranchAddress("lumi", &lumi);
  events->SetBranchAddress("event", &event);
  events->SetBranchAddress("pfMET", &pfMET);
  auto* branches(events->GetListOfBranches());
  for (auto* br : *branches) {
    TString bName(br->GetName());
    if (bName.Index("filter_") == 0) {
      TString fName(bName(7, bName.Length()));
      auto fItr(std::find(filterNames_.begin(), filterNames_.end(), fName));
      if (fItr == filterNames_.end())
	continue;

      events->SetBranchAddress(bName, results + (fItr - filterNames_.begin()));
    }
  }

  lumis->SetBranchAddress("run", &run);
  lumis->SetBranchAddress("lumi", &lumi);

  auto mEnd(lumiMask_.end());
  bool testLumi(lumiMask_.size() != 0);

  unsigned currentRun_(0);

  long iEntry(0);
  while (events->GetEntry(iEntry++) > 0) {
    if (testLumi) {
     unsigned long long test(run);
     test = (test << 32) | lumi;
     if (lumiMask_.find(test) != mEnd)
       continue;
    }

    if (run_ != currentRun_) {
      if (currentOutput_) {
	auto* outputFile(currentFile_->GetCurrentFile());
	outputFile->cd();
	currentFile_->Write();
	delete outputFile;
      }
      auto* outputFile(TFile::Open(outputDir_ + "/" + TString::Format("%d.root", run), "update"));
      if (!outputFile || outputFile->IsZombie())
	return false;

      currentOutput_ = static_cast<TTree*>(outputFile->Get("events"));
      if (currentOutput_) {
	currentOutput_->SetBranchAddress("datasetid", &_datasetid);
	currentOutput_->SetBranchAddress("run", &run);
	currentOutput_->SetBranchAddress("lumi", &lumi);
	currentOutput_->SetBranchAddress("event", &event);
	currentOutput_->SetBranchAddress("pfMET", &pfMET);
	for (auto* br : *branches) {
	  TString bName(br->GetName());
	  if (bName.Index("filter_") == 0) {
	    TString fName(bName(7, bName.Length()));
	    auto fItr(std::find(filterNames_.begin(), filterNames_.end(), fName));
	    if (fItr == filterNames_.end())
	      continue;

	    currentOutput_->SetBranchAddress(bName, results + (fItr - filterNames_.begin()));
	  }
	}
      }
      else {
	currentOutput_ = new TTree("events", "MET filtered events");
	currentOutput_->Branch("datasetid", &_datasetid, "datasetid/i");
	currentOutput_->Branch("run", &run, "run/i");
	currentOutput_->Branch("lumi", &lumi, "lumi/i");
	currentOutput_->Branch("event", &event, "event/i");
	currentOutput_->Branch("pfMET", &pfMET, "pfMET/F");
	for (auto* br : *branches) {
	  TString bName(br->GetName());
	  if (bName.Index("filter_") == 0) {
	    TString fName(bName(7, bName.Length()));
	    auto fItr(std::find(filterNames_.begin(), filterNames_.end(), fName));
	    if (fItr == filterNames_.end())
	      continue;

	    currentOutput_->Branch(bName, results + (fItr - filterNames_.begin()), bName + "/O");
	  }
	}
      }
    }

    currentOutput_->Fill();
  }

  iEntry = 0;
  while (lumis->GetEntry(iEntry++) > 0) {
    unsigned long long test(run);
    test = (test << 32) | lumi;
    if (lumiMask_.find(test) != mEnd)
      continue;

    analyzedLumis_[run].push_back(lumi);
  }

  delete source;
  delete [] results;
  delete branches;

  return true;
}

std::vector<unsigned>
TreeWriter::getAnalyzedRuns() const
{
  std::vector<unsigned> runs;
  for (auto& r : analyzedLumis_)
    runs.push_back(r.first);

  return runs;
}
