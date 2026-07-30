# card-sentry native app: five CEP detectors built against the installed
# clink package, the way any downstream consumer builds.
#
#   cmake -S app -B app/build -DCMAKE_PREFIX_PATH=$PWD/.clink/prefix
#   cmake --build app/build --parallel
#   ./app/build/card_sentry --in data/events.ndjson --out out/alerts.ndjson
#   ctest --test-dir app/build      # runs the clink::test_support tests

cmake_minimum_required(VERSION 3.24)
project(card_sentry_app LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

find_package(clink REQUIRED COMPONENTS core test_support)

add_executable(card_sentry src/card_sentry.cpp)
target_link_libraries(card_sentry PRIVATE clink::core)

enable_testing()
add_executable(patterns_test tests/patterns_test.cpp)
target_include_directories(patterns_test PRIVATE src)
target_link_libraries(patterns_test PRIVATE clink::core clink::test_support)
add_test(NAME patterns_test COMMAND patterns_test)
