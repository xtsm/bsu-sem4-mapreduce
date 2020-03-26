project(mapreduce)
cmake_minimum_required(VERSION 3.16)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED true)
# TODO add conditional compilation of win version when it is available
add_executable(mapreduce mapreduce.cpp process_unix.cpp tmpdir.cpp)
add_executable(wordcount_map wordcount_map.cpp)
add_executable(wordcount_reduce wordcount_reduce.cpp)
target_compile_options(mapreduce PRIVATE -Wall -Wextra -Weffc++ -Werror)
