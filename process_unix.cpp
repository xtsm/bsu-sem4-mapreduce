#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <signal.h>
#include <cstring>
#include <vector>
#include <optional>
#include "include/process.h"

enum class ProcessState {
  CREATED,
  RUNNING,
  TERMINATED,
};

class ProcessUnix : public Process {
 public:
  explicit ProcessUnix(const std::string& exec) :
      exec_(exec), args_(), input_(), output_(), pid_(0), state_(ProcessState::CREATED) {}

  void Run() override {
    if (state_ != ProcessState::CREATED) {
      throw std::runtime_error("can't run twice");
    }
    pid_t res = fork();
    if (res < 0) {
      throw std::runtime_error("fork() failed");
    }
    if (res == 0) {
      std::vector<char*> argv;
      argv.push_back(exec_.data());
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

      execvp(exec_.c_str(), argv.data());
      _exit(1);
    } else {
      pid_ = res;
      state_ = ProcessState::RUNNING;
    }
  }
  void SetArguments(const std::vector<std::string>& args) override {
    if (state_ != ProcessState::CREATED) {
      throw std::runtime_error("can't change args after start");
    }
    args_ = args;
  }
  void SetInput(const std::filesystem::path& path) override {
    if (state_ != ProcessState::CREATED) {
      throw std::runtime_error("can't change input after start");
    }
    input_ = path;
  }
  void SetOutput(const std::filesystem::path& path) override {
    if (state_ != ProcessState::CREATED) {
      throw std::runtime_error("can't change output after start");
    }
    output_ = path;
  }
  int Wait() override {
    if (state_ != ProcessState::RUNNING) {
      throw std::runtime_error("process isn't running");
    }
    // TODO(xtsm) is it ok?
    state_ = ProcessState::TERMINATED;
    int status;
    if (waitpid(pid_, &status, 0) >= 0 && WIFEXITED(status)) {
      return WEXITSTATUS(status);
    } else {
      return -1;
    }
  }

  ~ProcessUnix() {
    if (state_ == ProcessState::RUNNING) {
      kill(pid_, SIGTERM);
    }
  }

 private:
  std::string exec_;
  std::vector<std::string> args_;
  std::optional<std::string> input_;
  std::optional<std::string> output_;
  pid_t pid_;
  ProcessState state_;
};

std::unique_ptr<Process> Process::Create(const std::filesystem::path& path) {
  return std::make_unique<ProcessUnix>(path);
}
