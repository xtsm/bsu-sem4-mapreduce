#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <queue>
#include "include/process.h"
#include "include/tmpdir.h"
#include "include/key_value.h"

struct ExtSortElement {
  size_t chunk_number;
  TsvKeyValue data;
  ExtSortElement() : chunk_number(-1), data() {}
  ExtSortElement(size_t chunk_number, TsvKeyValue data) :
      chunk_number(chunk_number),
      data(data) {}
  bool operator<(const ExtSortElement& oth) const {
    return data.key > oth.data.key;
  }
};

// Reads `infile` and splits it into `outdir` with size limit of `size`
// per chunk.
// Returns the number of resulting chunks.
size_t SplitBySize(
    const std::filesystem::path& infile,
    const std::filesystem::path& outdir,
    size_t size) {
  std::ifstream fin(infile);
  if (!fin.is_open()) {
    std::ostringstream err;
    err << "failed to open " << infile << " for splitting";
    throw std::runtime_error(err.str());
  }
  TsvKeyValue kv;
  std::vector<TsvKeyValue> current_entries;
  size_t current_size = 0;
  size_t chunk_count = 0;
  while (fin >> kv) {
    current_size += kv.GetSize();
    current_entries.push_back(std::move(kv));
    if (current_size >= size) {
      std::ofstream chunk_file(outdir / std::to_string(chunk_count));
      for (const auto& kv : current_entries) {
        chunk_file << kv << std::endl;
      }
      current_entries.clear();
      chunk_file.close();
      ++chunk_count;
    }
  }
  fin.close();
  if (!current_entries.empty()) {
    std::ofstream chunk_file(outdir / std::to_string(chunk_count));
    for (const auto& kv : current_entries) {
      chunk_file << kv << std::endl;
    }
    current_entries.clear();
    chunk_file.close();
    ++chunk_count;
  }
  return chunk_count;
}

// Reads `infile` and performs an external sort of its contents.
// Writes results to `outfile`.
// May create temporary entries in the `workdir`.
void ExternalSortByKey(
    const std::filesystem::path& infile,
    const std::filesystem::path& outfile,
    const std::filesystem::path& workdir) {
  // TODO(xtsm): change to something adaptive later
  size_t chunk_size_limit = 64 << 20;
  TmpDir chunks_dir(workdir / "sorted_chunks");
  TsvKeyValue kv;

  // step 1: split
  size_t chunk_count = SplitBySize(infile, chunks_dir.GetPath(),
      chunk_size_limit);
  for (size_t chunk_num = 0; chunk_num < chunk_count; ++chunk_num) {
    std::vector<TsvKeyValue> entries;
    auto chunk_path = chunks_dir.GetPath() / std::to_string(chunk_num);
    std::ifstream fin(chunk_path);
    while (fin >> kv) {
      entries.push_back(std::move(kv));
    }
    fin.close();
    std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
      return a.key < b.key;
    });
    std::ofstream fout(chunk_path);
    for (const auto& entry : entries) {
      fout << entry << std::endl;
    }
    fout.close();
  }

  // step 2: merge
  std::vector<std::ifstream> chunk_files;
  std::priority_queue<ExtSortElement> heap;
  for (size_t chunk_num = 0; chunk_num < chunk_count; ++chunk_num) {
    chunk_files.emplace_back(chunks_dir.GetPath() / std::to_string(chunk_num));
    if (chunk_files.back() >> kv) {
      heap.emplace(chunk_num, std::move(kv));
    }
  }

  std::ofstream fout(outfile);
  while (!heap.empty()) {
    auto elem = heap.top();
    heap.pop();
    fout << elem.data << std::endl;
    if (chunk_files[elem.chunk_number] >> elem.data) {
      heap.push(std::move(elem));
    }
  }
  fout.close();
}

// Reads `infile` and splits it into `outdir` by key.
// Returns the number of resulting chunks.
size_t SplitByKey(
    const std::filesystem::path& infile,
    const std::filesystem::path& outdir) {
  std::ifstream fin(infile);
  if (!fin.is_open()) {
    std::ostringstream err;
    err << "failed to open " << infile << " for splitting by key";
    throw std::runtime_error(err.str());
  }

  TsvKeyValue kv;
  std::optional<std::string> current_key;
  size_t chunk_count = 0;
  std::ofstream fout;
  while (fin >> kv) {
    if (!current_key.has_value() || current_key != kv.key) {
      current_key = kv.key;
      if (fout.is_open()) {
        fout.close();
      }
      fout.open(outdir / std::to_string(chunk_count));
      chunk_count++;
    }
    fout << kv << std::endl;
  }
  fin.close();
  if (fout.is_open()) {
    fout.close();
  }
  return chunk_count;
}

// Runs `exec` processes for all `count` chunks from `indir`.
// Writes corresponding chunks to `outdir`.
void RunForAllChunks(
    const std::string& exec,
    const std::filesystem::path& indir,
    const std::filesystem::path& outdir,
    size_t count) {
  std::vector<std::unique_ptr<Process>> processes;
  for (size_t i = 0; i < count; i++) {
    processes.push_back(Process::Create(exec));
    processes.back()->Run(
        indir / std::to_string(i),
        outdir / std::to_string(i));
  }
  for (auto& process : processes) {
    if (process->Wait() != 0) {
      throw std::runtime_error("one of workers did not exit normally");
    }
  }
}

// Merges all `count` chunks from `indir` into `outfile`.
void MergeChunks(
    const std::filesystem::path& indir,
    const std::filesystem::path& outfile,
    size_t count) {
  std::ofstream fout(outfile);
  TsvKeyValue kv;
  for (size_t i = 0; i < count; i++) {
    std::ifstream fin(indir / std::to_string(i));
    while (fin >> kv) {
      fout << kv << std::endl;
    }
    fin.close();
  }
  fout.close();
}


void DoMap(const std::filesystem::path& infile,
    const std::filesystem::path& outfile,
    const std::string& exec) {
  // TODO(xtsm): change to something adaptive later
  size_t chunk_size_limit = 64 << 20;
  TmpDir workdir("mr_tmp");
  TmpDir input_chunks(workdir.GetPath() / "input_chunks");
  size_t key_count = SplitBySize(infile, input_chunks.GetPath(),
      chunk_size_limit);
  TmpDir output_chunks(workdir.GetPath() / "output_chunks");
  RunForAllChunks(exec,
      input_chunks.GetPath(),
      output_chunks.GetPath(),
      key_count);
  MergeChunks(output_chunks.GetPath(), outfile, key_count);
}

void DoReduce(const std::filesystem::path& infile,
    const std::filesystem::path& outfile,
    const std::string& exec) {
  TmpDir workdir("mr_tmp");
  auto sorted_infile = workdir.GetPath() / "sorted_infile";
  ExternalSortByKey(infile, sorted_infile, workdir.GetPath());
  TmpDir input_chunks(workdir.GetPath() / "input_chunks");
  size_t key_count = SplitByKey(sorted_infile, input_chunks.GetPath());
  TmpDir output_chunks(workdir.GetPath() / "output_chunks");
  RunForAllChunks(exec,
      input_chunks.GetPath(),
      output_chunks.GetPath(),
      key_count);
  MergeChunks(output_chunks.GetPath(), outfile, key_count);
}

int main(int argc, char** argv) {
  if (argc != 5) {
    std::cerr << "Usage: " << argv[0] << " <map|reduce> <exec> <input> <output>"
        << std::endl;
    return 1;
  }
  std::string mr_mode(argv[1]);
  std::string mr_exec(argv[2]);
  std::filesystem::path infile(argv[3]);
  std::filesystem::path outfile(argv[4]);
  try {
    if (mr_mode == "map") {
      DoMap(infile, outfile, mr_exec);
    } else if (mr_mode == "reduce") {
      DoReduce(infile, outfile, mr_exec);
    } else {
      throw std::runtime_error("unknown mode: " + mr_mode);
    }
  } catch (const std::exception& e) {
    std::cerr << "mapreduce failed" << std::endl << e.what() << std::endl;
    return 1;
  }
  return 0;
}
