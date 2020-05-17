#include "include/key_value.h"

KeyValue::KeyValue() : key(), value() {}

KeyValue::KeyValue(std::string key, std::string value) :
    key(key), value(value) {}

size_t KeyValue::GetSize() const {
  return key.size() + value.size();
}

TsvKeyValue::TsvKeyValue() : KeyValue() {}

TsvKeyValue::TsvKeyValue(std::string key, std::string value) :
    KeyValue(std::move(key), std::move(value)) {}

std::istream& operator>>(std::istream& s, TsvKeyValue& key_value) {
  std::string line, key_str, value_str;
  std::getline(s, line);
  if (s) {
    size_t first_tab_pos = line.find('\t');
    size_t second_tab_pos = line.find('\t', first_tab_pos + 1);
    if (first_tab_pos == std::string::npos
        || second_tab_pos != std::string::npos) {
      throw std::runtime_error("TSV key-value must contain exactly two fields");
    }
    key_value.key = line.substr(0, first_tab_pos);
    key_value.value = line.substr(first_tab_pos + 1);
  }
  return s;
}

std::ostream& operator<<(std::ostream& s, const TsvKeyValue& key_value) {
  return s << key_value.key << '\t' << key_value.value;
}
