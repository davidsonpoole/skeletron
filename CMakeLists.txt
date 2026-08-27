cmake_minimum_required(VERSION 3.10)
project(skeletron CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)
find_package(GTest CONFIG REQUIRED)
enable_testing()
include(GoogleTest)

add_executable(pubsub_test test/pubsub_test.cpp)
target_link_libraries(pubsub_test PRIVATE Threads::Threads)
add_test(NAME pubsub_test COMMAND pubsub_test)

add_executable(pubsub_test_concurrency test/pubsub_test_concurrency.cpp)
target_link_libraries(pubsub_test_concurrency PRIVATE GTest::gtest GTest::gtest_main Threads::Threads)
gtest_discover_tests(pubsub_test_concurrency)

