#include <iostream>
#include <sstream>
#include <string>
#include <unordered_set>
#include "include/key_value.h"

int main() {
  try {
    TsvKeyValue kv, result;
    std::unordered_set<std::string> titles;
    bool had_empty_value = false;
    while (std::cin >> kv) {
      if (kv.value.empty()) {
        had_empty_value = true;
        result.key = kv.key;
      } else {
        titles.insert(kv.value);
      }
    }
    if (!had_empty_value) {
      return 0;
    }
    for (const auto& title : titles) {
      result.value.append(title);
      result.value.push_back('#');
    }
    if (!titles.empty()) {
      result.value.pop_back();
    }
    std::cout << result << std::endl;
    return 0;
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
}
