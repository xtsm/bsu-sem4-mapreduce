#include "include/tmpdir.h"

TmpDir::TmpDir(const std::filesystem::path& prefix) : path_() {
  auto tmpdir_path = prefix;
  while (std::filesystem::exists(tmpdir_path)) {
    tmpdir_path += '_';
  }
  path_ = tmpdir_path;
  if (!std::filesystem::create_directory(path_)) {
    throw std::runtime_error("failed to create directory");
  }
}

std::filesystem::path TmpDir::GetPath() const {
  return path_;
}

TmpDir::~TmpDir() {
  std::filesystem::remove_all(path_);
}
