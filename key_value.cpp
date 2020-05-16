#include "include/key_value.h"

KeyValue::KeyValue() : key(), value() {}

KeyValue::KeyValue(std::string key, std::string value) : key(key), value(value) {}

size_t KeyValue::GetSize() const {
  return key.size() + value.size();
}

TsvKeyValue::TsvKeyValue() : KeyValue() {}

TsvKeyValue::TsvKeyValue(std::string key, std::string value) : KeyValue(std::move(key), std::move(value)) {}

std::istream& operator>>(std::istream& s, TsvKeyValue& key_value) {
  std::string line, key_str, value_str;
  std::getline(s, line);
  if (s) {
    std::istringstream sstream(std::move(line));
    std::getline(sstream, key_value.key, '\t');
    if (!sstream) {
      throw std::runtime_error("could not extract key from TSV line");
    }
    std::getline(sstream, key_value.value, '\t');
    if (!sstream) {
      throw std::runtime_error("could not extract value from TSV line");
    }
    if (!sstream.eof()) {
      throw std::runtime_error("excess TSV data after value");
    }
  }
  return s;
}

std::ostream& operator<<(std::ostream& s, const TsvKeyValue& key_value) {
  return s << key_value.key << '\t' << key_value.value;
}
