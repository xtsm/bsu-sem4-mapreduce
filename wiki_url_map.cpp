#include <json/value.h>
#include <json/reader.h>
#include <curl/curl.h>
#include <iostream>
#include <regex>
#include "include/key_value.h"
using Json::operator>>;

size_t CurlWriteCallback(char* data, size_t, size_t size, void* stream_ptr) {
  std::ostream& stream = *static_cast<std::ostream*>(stream_ptr);
  stream.write(data, size);
  return size;
}

const std::vector<std::regex> remove_sequence = {
  std::regex("<!--.*?-->"),
  std::regex("<style.*?>.*?</style>"),
  std::regex("<.*?>"),
  std::regex("[.,!?:;()\\[\\]\"^]"),
  std::regex("&#\\d+"),
};

int main() {
  CURL* session = curl_easy_init();
  try {
    TsvKeyValue kv;
    while (std::cin >> kv) {
      std::ostringstream url_stream;
      std::regex_replace(std::ostreambuf_iterator(url_stream),
          kv.key.begin(),
          kv.key.end(),
          std::regex("/wiki/"),
          "/w/api.php?action=parse&redirects=true&prop=text|displaytitle"
              "&format=json&page=");
      try {
        std::stringstream stream;
        curl_easy_setopt(session, CURLOPT_URL, url_stream.str().data());
        curl_easy_setopt(session, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
        curl_easy_setopt(session, CURLOPT_WRITEDATA, &stream);
        CURLcode result_code = curl_easy_perform(session);
        if (result_code != CURLE_OK) {
          throw std::runtime_error(curl_easy_strerror(result_code));
        }
        curl_easy_reset(session);

        Json::Value value;
        stream >> value;
        std::string page_text = value["parse"]["text"]["*"].asString();
        std::string page_title = value["parse"]["displaytitle"].asString();
        std::replace(page_text.begin(), page_text.end(), '\n', ' ');
        std::transform(page_text.begin(), page_text.end(), page_text.begin(),
            [](auto c) {
              return std::tolower(c);
            });

        for (const auto& expr : remove_sequence) {
          stream.str("");
          stream.clear();
          std::regex_replace(std::ostreambuf_iterator(stream),
              page_text.begin(),
              page_text.end(),
              expr,
              " ");
          page_text = stream.str();
        }
        stream.str(page_text);
        kv.value = page_title;
        while (stream >> kv.key) {
          if (kv.key.size() >= 3) {
            std::cout << kv << std::endl;
          }
        }
      } catch (const std::exception& e) {
        throw std::runtime_error("could not process URL: " + url_stream.str()
            + "\n" + e.what());
      }
    }
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    curl_easy_cleanup(session);
    return 1;
  }
  curl_easy_cleanup(session);
  return 0;
}
