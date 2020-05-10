#include <iostream>
#include <sstream>
#include <string>

int main() {
  std::string line;
  while (std::getline(std::cin, line)) {
    line.erase(line.begin(), line.begin() + line.find('\t') + 1);
    std::istringstream keys_stream(line);
    std::string key;
    while (keys_stream >> key) {
        std::cout << key << "\t1" << std::endl;
    }
  }
  return 0;
}
