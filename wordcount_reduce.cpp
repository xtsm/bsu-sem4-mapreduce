#include <iostream>
#include <string>
#include <optional>

int main() {
  std::string line;
  std::optional<std::string> key;
  uint64_t sum = 0;
  while (std::getline(std::cin, line)) {
    size_t tab_pos = line.find('\t');
    uint64_t count = std::stoull(line.substr(tab_pos + 1));
    if (!key.has_value()) {
      key = line.substr(0, tab_pos);
    }
    // TODO should I add key equality assert?
    sum += count;
  }
  if (key.has_value()) {
    std::cout << *key << '\t' << sum << std::endl;
  }
  return 0;
}
