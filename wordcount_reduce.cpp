#include <iostream>
#include <sstream>
#include <string>
#include <optional>

int main() {
  std::string line;
  std::optional<std::string> key;
  uint64_t sum = 0;
  while (std::getline(std::cin, line)) {
    size_t tab_pos = line.find('\t');
    uint64_t count = std::stoull(line.substr(tab_pos + 1));
    std::string new_key = line.substr(0, tab_pos);
    if (!key.has_value()) {
      key = new_key;
    } else if (*key != new_key) {
      std::ostringstream ss;
      ss << "got different keys in input: \"" << *key <<
          "\", \"" << new_key << "\"";
      throw std::runtime_error(ss.str());
    }
    sum += count;
  }
  if (key.has_value()) {
    std::cout << *key << '\t' << sum << std::endl;
  } else {
    std::cerr << "warning: input is empty" << std::endl;
  }
  return 0;
}
