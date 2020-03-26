#include "include/tmpdir.h"

TmpDir::TmpDir(const std::string& prefix) : path_() {
  std::string tmpdir_name = prefix;
  while (std::filesystem::exists(tmpdir_name)) {
    tmpdir_name += '_';
  }
  path_ = tmpdir_name;
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
