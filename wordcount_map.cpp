#include <iostream>
#include <string>

int main() {
  std::string line;
  while (std::getline(std::cin, line)) {
    size_t cur_space, prev_space = line.find('\t');
    while (prev_space != std::string::npos) {
      cur_space = line.find(' ', prev_space + 1);
      if (prev_space + 1 != cur_space && prev_space + 1 != line.size()) {
        std::cout << line.substr(prev_space + 1, cur_space - prev_space - 1)
            << "\t1" << std::endl;
      }
      prev_space = cur_space;
    }
  }
  return 0;
}
