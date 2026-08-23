cmake_minimum_required(VERSION 3.10)
project(skeletron CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

enable_testing()

add_executable(pubsub_test test/pubsub_test.cpp)
target_link_libraries(pubsub_test PRIVATE Threads::Threads)

add_test(NAME pubsub_test COMMAND pubsub_test)
