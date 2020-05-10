#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
#include "include/process.h"
#include "include/tmpdir.h"

void SplitReduceInput(const std::filesystem::path& inpath,
    const std::filesystem::path& dirpath,
    std::unordered_map<size_t, std::vector<std::string>>& hashmap) {
  std::ifstream fin(inpath);
  if (!fin.is_open()) {
    std::ostringstream ss;
    ss << "failed to open " << inpath;
    throw std::runtime_error(ss.str());
  }
  std::hash<std::string> hasher;
  std::string line;
  while (getline(fin, line)) {
    size_t tab_pos = line.find('\t');
    if (tab_pos == std::string::npos) {
      continue;
    }
    std::string key = line.substr(0, tab_pos);
    size_t hash = hasher(key);
    auto& colls = hashmap[hash];
    size_t coll_num = colls.size();
    auto it = std::find(colls.begin(), colls.end(), key);
    if (it == colls.end()) {
      colls.push_back(key);
    } else {
      coll_num = it - colls.begin();
    }
    std::string fname_in = dirpath / (std::to_string(hash) + "_"
        + std::to_string(coll_num) + "_in");
    std::ofstream fout(fname_in, std::ios_base::out | std::ios_base::app);
    if (!fout.is_open()) {
      std::ostringstream ss;
      ss << "failed to open " << fname_in;
      throw std::runtime_error(ss.str());
    }
    fout << line << std::endl;
    fout.close();
  }
  fin.close();
}

void MergeReduceOutput(const std::filesystem::path& outpath,
    const std::filesystem::path& dirpath,
    std::unordered_map<size_t, std::vector<std::string>>& hashmap) {
  std::ofstream fout(outpath);
  if (!fout.is_open()) {
    std::ostringstream ss;
    ss << "failed to open " << outpath;
    throw std::runtime_error(ss.str());
  }
  for (const auto& kv : hashmap) {
    for (size_t i = 0; i < kv.second.size(); i++) {
      std::ifstream fin(dirpath / (std::to_string(kv.first) + "_"
          + std::to_string(i) + "_out"));
      std::string line;
      while (getline(fin, line)) {
        fout << line << std::endl;
      }
      fin.close();
    }
  }
  fout.close();
}

int main(int argc, char** argv) {
  if (argc != 5) {
    std::cerr << "Usage: " << argv[0] << " <map|reduce> <exec> <input> <output>"
        << std::endl;
    return 1;
  }
  if (!strcmp(argv[1], "map")) {
    auto p = Process::Create(argv[2]);
    p->SetInput(argv[3]);
    p->SetOutput(argv[4]);
    p->Run();
    if (p->Wait() == 0) {
      return 0;
    } else {
      std::cerr << "mapper has failed" << std::endl;
      return 1;
    }
  } else if (!strcmp(argv[1], "reduce")) {
    TmpDir tmpdir("mr_tmp");
    std::unordered_map<size_t, std::vector<std::string>> key_hashmap;
    SplitReduceInput(argv[3], tmpdir.GetPath(), key_hashmap);

    std::vector<std::unique_ptr<Process>> reducers;
    for (const auto& kv : key_hashmap) {
      for (size_t i = 0; i < kv.second.size(); i++) {
        std::string fname_base = tmpdir.GetPath() / (std::to_string(kv.first)
            + "_" + std::to_string(i) + "_");
        reducers.push_back(Process::Create(argv[2]));
        reducers.back()->Run(fname_base + "in", fname_base + "out");
      }
    }
    bool is_success_for_all = true;
    for (auto& reducer : reducers) {
      if (reducer->Wait() != 0) {
        is_success_for_all = false;
      }
    }
    if (!is_success_for_all) {
      std::cerr << "at least one reducer has failed" << std::endl;
      return 1;
    }

    MergeReduceOutput(argv[4], tmpdir.GetPath(), key_hashmap);
    return 0;
  } else {
    std::cerr << "unknown mode: " << argv[1] << std::endl;
    return 1;
  }

  return 0;
}
