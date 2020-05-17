project(mapreduce)
cmake_minimum_required(VERSION 3.16)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED true)

find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
pkg_search_module(JSONCPP REQUIRED IMPORTED_TARGET jsoncpp)
pkg_search_module(CURL REQUIRED IMPORTED_TARGET libcurl)

# TODO add conditional compilation of Windoes process version when it is available
add_compile_options(-Wall -Wextra -Weffc++ -Werror)
add_executable(mapreduce mapreduce.cpp process_unix.cpp tmpdir.cpp process.cpp key_value.cpp thread_pool.cpp)
add_executable(wordcount_map wordcount_map.cpp key_value.cpp)
add_executable(wordcount_reduce wordcount_reduce.cpp key_value.cpp)
add_executable(wiki_url_map wiki_url_map.cpp key_value.cpp)
add_executable(wiki_reduce wiki_reduce.cpp key_value.cpp)
target_link_libraries(wiki_url_map PRIVATE PkgConfig::JSONCPP PkgConfig::CURL)
target_link_libraries(mapreduce PRIVATE Threads::Threads)
