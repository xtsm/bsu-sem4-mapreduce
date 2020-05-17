#pragma once
#include <filesystem>
#include <string>

class TmpDir {
 public:
  explicit TmpDir(const std::filesystem::path& prefix);
  std::filesystem::path GetPath() const;
  ~TmpDir();
 private:
  std::filesystem::path path_;
};
