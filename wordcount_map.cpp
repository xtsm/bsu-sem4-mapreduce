#include <iostream>
#include <sstream>
#include <string>
#include "include/key_value.h"

int main() {
  std::string line;
  TsvKeyValue kv;
  while (std::cin >> kv) {
    kv.key.clear();
    std::istringstream new_keys_stream(std::move(kv.value));
    std::string new_key;
    while (new_keys_stream >> new_key) {
      TsvKeyValue new_kv(new_key, "1");
      std::cout << new_kv << std::endl;
    }
  }
  return 0;
}
