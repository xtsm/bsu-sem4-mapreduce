#include "include/process.h"

void Process::Run(const std::filesystem::path& input,
    const std::filesystem::path& output) {
  SetInput(input);
  SetOutput(output);
  Run();
}
