#include "TFile.h"
#include "TTree.h"
#include "TString.h"

#include <fstream>
#include <iostream>
#include <algorithm>
#include <vector>
#include <map>

class ASCIIDumper {
public:
  ASCIIDumper(char const* outputDir);
  ~ASCIIDumper() {}

  void addFilter(unsigned idx, char const* name) { filterIndices_.push_back(idx); filterNames_.push_back(name); }

  bool dump(char const* inputPath, unsigned recoid, unsigned datasetid);

  unsigned getNTags() const { return nTags_; }
  unsigned getNData() const { return nData_; }
  unsigned getNLumis() const { return nLumis_; }
  std::vector<unsigned> getAnalyzedRuns() const;
  std::vector<unsigned> getAnalyzedLumis(unsigned run) { return analyzedLumis_[run]; }

  void closeTags() { tagsOut_.close(); }
  void closeData() { dataOut_.close(); relOut_.close(); }

  void resetNTags() { nTags_ = 0; tagsOut_.open((outputDir_ + "/eventtags.txt").Data()); }
  void resetNData() { nData_ = 0; dataOut_.open((outputDir_ + "/eventdata.txt").Data()); relOut_.open((outputDir_ + "/datasetrel.txt").Data()); }
  void resetRuns() { analyzedLumis_.clear(); nLumis_ = 0; }

private:
  TString const outputDir_;
  ofstream tagsOut_;
  ofstream dataOut_;
  ofstream relOut_;
  std::vector<TString> filterNames_{};
  std::vector<unsigned> filterIndices_{};
  unsigned nTags_{0};
  unsigned nData_{0};
  unsigned nLumis_{0};
  std::map<unsigned, std::vector<unsigned>> analyzedLumis_{};
};

ASCIIDumper::ASCIIDumper(char const* _outputDir) :
  outputDir_(_outputDir),
  tagsOut_((outputDir_ + "/eventtags.txt").Data()),
  dataOut_((outputDir_ + "/eventdata.txt").Data()),
  relOut_((outputDir_ + "/datasetrel.txt").Data())
{
}  

bool
ASCIIDumper::dump(char const* _inputPath, unsigned _recoid, unsigned _datasetid)
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

  long iEntry(0);
  while (events->GetEntry(iEntry++) > 0) {
    for (unsigned iF(0); iF != nFilters; ++iF) {
      if (!results[iF]) {
	tagsOut_ << _recoid << "," << run << "," << event << "," << filterIndices_[iF] << std::endl;
	++nTags_;
      }
    }

    dataOut_ << _recoid << "," << run << "," << event << "," << lumi << "," << pfMET << std::endl;
    relOut_ << _datasetid << "," << run << "," << event << std::endl;
    ++nData_;
  }

  iEntry = 0;
  while (lumis->GetEntry(iEntry++) > 0)
    analyzedLumis_[run].push_back(lumi);

  nLumis_ += iEntry;

  delete source;
  delete [] results;

  return true;
}

std::vector<unsigned>
ASCIIDumper::getAnalyzedRuns() const
{
  std::vector<unsigned> runs;
  for (auto& r : analyzedLumis_)
    runs.push_back(r.first);

  return runs;
}
