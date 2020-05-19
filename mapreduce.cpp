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
#include "include/thread_pool.h"

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
// Splits data into chunks of `chunk_size_limit` in process and sorts them,
// creates temporary entries in the `workdir` for that purpose.
void ExternalSortByKey(
    const std::filesystem::path& infile,
    const std::filesystem::path& outfile,
    const std::filesystem::path& workdir,
    size_t chunk_size_limit) {
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
// Runs at most `process_count` worker processes at a time.
void RunForAllChunks(
    const std::string& exec,
    const std::filesystem::path& indir,
    const std::filesystem::path& outdir,
    size_t count,
    size_t process_count) {
  ThreadPool pool(process_count);
  bool all_exited_normally = true;
  std::mutex mutex;
  for (size_t i = 0; i < count; i++) {
    pool.Run([&, i]() {
      auto process = Process::Create(exec);
      process->Run(
          indir / std::to_string(i),
          outdir / std::to_string(i));
      auto retcode = process->Wait();
      std::lock_guard<std::mutex> lock(mutex);
      all_exited_normally &= retcode == 0;
    });
  }
  pool.WaitForAll();
  if (!all_exited_normally) {
    throw std::runtime_error("one of workers did not exit normally");
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
    const std::string& exec,
    size_t block_size,
    size_t process_count) {
  TmpDir workdir("mr_tmp");
  TmpDir input_chunks(workdir.GetPath() / "input_chunks");
  size_t key_count = SplitBySize(infile, input_chunks.GetPath(),
      block_size);
  TmpDir output_chunks(workdir.GetPath() / "output_chunks");
  RunForAllChunks(exec,
      input_chunks.GetPath(),
      output_chunks.GetPath(),
      key_count,
      process_count);
  MergeChunks(output_chunks.GetPath(), outfile, key_count);
}

void DoReduce(const std::filesystem::path& infile,
    const std::filesystem::path& outfile,
    const std::string& exec,
    size_t block_size,
    size_t process_count) {
  TmpDir workdir("mr_tmp");
  auto sorted_infile = workdir.GetPath() / "sorted_infile";
  ExternalSortByKey(infile, sorted_infile, workdir.GetPath(), block_size);
  TmpDir input_chunks(workdir.GetPath() / "input_chunks");
  size_t key_count = SplitByKey(sorted_infile, input_chunks.GetPath());
  TmpDir output_chunks(workdir.GetPath() / "output_chunks");
  RunForAllChunks(exec,
      input_chunks.GetPath(),
      output_chunks.GetPath(),
      key_count,
      process_count);
  MergeChunks(output_chunks.GetPath(), outfile, key_count);
}

void PrintUsageAndExit(const char* program_name) {
  std::cerr << "Usage: " << program_name
      << " <map|reduce> <exec> <input> <output>"
      << " [-p COUNT] [-s SIZE]" << std::endl
      << "  -p COUNT   use at most COUNT parallel processes" << std::endl
      << "  -s SIZE    split input into blocks of SIZE bytes" << std::endl;
  exit(1);
}

int main(int argc, char** argv) {
  if (argc < 5) {
    PrintUsageAndExit(argv[0]);
  }
  std::string mr_mode(argv[1]);
  std::string mr_exec(argv[2]);
  std::filesystem::path infile(argv[3]);
  std::filesystem::path outfile(argv[4]);
  size_t process_count = std::thread::hardware_concurrency();
  size_t block_size = 64 << 20;
  for (int i = 5; i < argc; i++) {
    if (!strcmp(argv[i], "-p")) {
      ++i;
      if (i == argc) {
        PrintUsageAndExit(argv[0]);
      }
      char* err;
      process_count = strtoul(argv[i], &err, 0);
      if (*err) {
        PrintUsageAndExit(argv[0]);
      }
    } else if (!strcmp(argv[i], "-s")) {
      ++i;
      if (i == argc) {
        PrintUsageAndExit(argv[0]);
      }
      char* err;
      block_size = strtoul(argv[i], &err, 0);
      if (*err) {
        PrintUsageAndExit(argv[0]);
      }
    } else {
      PrintUsageAndExit(argv[0]);
    }
  }
  try {
    if (mr_mode == "map") {
      DoMap(infile, outfile, mr_exec, block_size, process_count);
    } else if (mr_mode == "reduce") {
      DoReduce(infile, outfile, mr_exec, block_size, process_count);
    } else {
      throw std::runtime_error("unknown mode: " + mr_mode);
    }
  } catch (const std::exception& e) {
    std::cerr << mr_mode << " failed" << std::endl << e.what() << std::endl;
    return 1;
  }
  return 0;
}
