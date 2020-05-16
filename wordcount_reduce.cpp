#include <iostream>
#include <sstream>
#include <string>
#include <optional>
#include "include/key_value.h"

int main() {
  std::string line;
  std::optional<std::string> key;
  TsvKeyValue kv;
  uint64_t sum = 0;
  while (std::cin >> kv) {
    uint64_t count = std::stoull(kv.value);
    if (!key.has_value()) {
      key = kv.key;
    } else if (*key != kv.key) {
      std::ostringstream ss;
      ss << "got different keys in input: \"" << *key <<
          "\", \"" << kv.key << "\"";
      throw std::runtime_error(ss.str());
    }
    sum += count;
  }
  if (key.has_value()) {
    TsvKeyValue result(*key, std::to_string(sum));
    std::cout << result << std::endl;
  } else {
    std::cerr << "warning: input is empty" << std::endl;
  }
  return 0;
}
