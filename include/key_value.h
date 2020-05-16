#pragma once
#include <string>
#include <sstream>

struct KeyValue {
  std::string key;
  std::string value;
  KeyValue();
  KeyValue(std::string key, std::string value);
  size_t GetSize() const;
};

struct TsvKeyValue : public KeyValue {
  TsvKeyValue();
  TsvKeyValue(std::string key, std::string value);
};

std::istream& operator>>(std::istream& s, TsvKeyValue& key_value);

std::ostream& operator<<(std::ostream& s, const TsvKeyValue& key_value);
