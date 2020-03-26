#include <cstring>
#include <string>
#include <thread>
#include <chrono>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <unordered_map>
#include "include/process.h"
#include "include/tmpdir.h"

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
    std::ifstream fin(argv[3]);
    if (!fin.is_open()) {
      std::cerr << "failed to open " << argv[3] << std::endl;
      return 1;
    }
    std::hash<std::string> hasher;
    std::unordered_map<size_t, std::vector<std::string>> hashmap;
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
      std::string fname_in = tmpdir.GetPath() / (std::to_string(hash) + "_"
          + std::to_string(coll_num) + "_in");
      std::ofstream fout(fname_in, std::ios_base::out | std::ios_base::app);
      if (!fout.is_open()) {
        std::cerr << "failed to open " << fname_in << std::endl;
        return 1;
      }
      fout << line << std::endl;
      fout.close();
    }
    fin.close();

    std::vector<std::unique_ptr<Process>> reducers;
    for (const auto& kv : hashmap) {
      for (size_t i = 0; i < kv.second.size(); i++) {
        std::string fname_base = tmpdir.GetPath() / (std::to_string(kv.first)
            + "_" + std::to_string(i) + "_");
        reducers.push_back(Process::Create(argv[2]));
        reducers.back()->SetInput(fname_base + "in");
        reducers.back()->SetOutput(fname_base + "out");
        reducers.back()->Run();
      }
    }
    bool good = true;
    for (auto& reducer : reducers) {
      if (reducer->Wait() != 0) {
        good = false;
      }
    }
    if (!good) {
      std::cerr << "at least one reducer has failed" << std::endl;
      return 1;
    }
    std::ofstream fout(argv[4]);
    if (!fout.is_open()) {
      std::cerr << "failed to open " << argv[4] << std::endl;
      return 1;
    }
    for (const auto& kv : hashmap) {
      for (size_t i = 0; i < kv.second.size(); i++) {
        fin.open(tmpdir.GetPath() / (std::to_string(kv.first) + "_"
            + std::to_string(i) + "_out"));
        while (getline(fin, line)) {
          fout << line << std::endl;
        }
        fin.close();
      }
    }
    fout.close();
    return 0;
  } else {
    std::cerr << "unknown mode: " << argv[1] << std::endl;
    return 1;
  }

  return 0;
}
