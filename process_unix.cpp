#include "include/process.h"
#include <vector>
#include <optional>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <signal.h>

class ProcessUnix : public Process {
 public:
  ProcessUnix(const std::string& path) : exec_path_(path), args_(), input_(), output_(), pid_(0), state_(0) {}

  void Run() override {
    if (state_ != 0) {
      throw std::runtime_error("can't run twice");
    }
    pid_t res = fork();
    if (res < 0) {
      throw std::runtime_error("fork() failed");
    }
    if (res == 0) {
      // TODO is this safe?
      std::vector<char*> argv;
      argv.push_back(exec_path_.data());
      for (auto& arg : args_) {
        argv.push_back(arg.data());
      }
      argv.push_back(nullptr);

      if (input_.has_value()) {
        int fd = open(input_->c_str(), O_RDONLY);
        if (fd < 0) {
          std::ostringstream ss;
          ss << "opening \"" << *input_ << "\" for read failed";
          throw std::runtime_error(ss.str());
        }
        if (dup2(fd, STDIN_FILENO) < 0) {
          throw std::runtime_error("dup2() for stdin failed");
        }
      }

      if (output_.has_value()) {
        int fd = open(output_->c_str(), O_WRONLY | O_CREAT, 0644);
        if (fd < 0) {
          std::ostringstream ss;
          ss << "opening \"" << *output_ << "\" for write failed";
          throw std::runtime_error(ss.str());
        }
        if (dup2(fd, STDOUT_FILENO) < 0) {
          throw std::runtime_error("dup2() for stdout failed");
        }
      }

      execv(exec_path_.c_str(), argv.data());
      _exit(1);
    } else {
      pid_ = res;
      state_ = 1;
    }
  }
  void SetArguments(const std::vector<std::string>& args) override {
    if (state_ != 0) {
      throw std::runtime_error("can't change args after start");
    }
    args_ = args;
  }
  void SetInput(const std::filesystem::path& path) override {
    if (state_ != 0) {
      throw std::runtime_error("can't change input after start");
    }
    input_ = path;
  }
  void SetOutput(const std::filesystem::path& path) override {
    if (state_ != 0) {
      throw std::runtime_error("can't change output after start");
    }
    output_ = path;
  }
  int Wait() override {
    if (state_ == 0) {
      throw std::runtime_error("process wasn't started");
    }
    // TODO is it ok?
    state_ = 2;
    int status;
    return (waitpid(pid_, &status, 0) >= 0 && WIFEXITED(status) ? WEXITSTATUS(status) : -1);
  }

  ~ProcessUnix() {
    if (state_ == 1) {
      kill(pid_, SIGKILL);
    }
  }
 private:
  std::string exec_path_;
  std::vector<std::string> args_;
  std::optional<std::string> input_;
  std::optional<std::string> output_;
  pid_t pid_;
  uint8_t state_;
};

std::unique_ptr<Process> Process::Create(const std::filesystem::path& path) {
  return std::make_unique<ProcessUnix>(path);
}
