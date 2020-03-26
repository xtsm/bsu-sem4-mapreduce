#pragma once
#include <filesystem>
#include <memory>
#include <vector>

class Process {
 public:
  Process() {}

  virtual void Run() = 0;

  virtual void SetArguments(const std::vector<std::string>& args) = 0;

  virtual void SetInput(const std::filesystem::path& path) = 0;

  virtual void SetOutput(const std::filesystem::path& path) = 0;

  virtual int Wait() = 0;

  virtual ~Process() {}

  static std::unique_ptr<Process> Create(const std::filesystem::path& path);

  Process& operator=(const Process& p) = delete;
  
  Process(const Process& p) = delete;
};
