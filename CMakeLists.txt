cmake_minimum_required(VERSION 3.14)
project(pedasus)

find_package(Boost 1.67.0 REQUIRED)

set(CMAKE_CXX_STANDARD 17)

add_executable(pedasus
        main.cpp
        )

target_link_libraries(
        pedasus
        LINK_PUBLIC
        libpedasus
)

add_library(libpedasus
        STATIC
        database.cc
        page.cpp
        page_pool.cpp)

########################################
## Testing
########################################

include(ExternalProject)
enable_testing()
ExternalProject_Add(
  gtest
  GIT_REPOSITORY https://github.com/google/googletest
  GIT_TAG master
  SOURCE_DIR ${PROJECT_BINARY_DIR}/third_party/gtest
  BINARY_DIR ${PROJECT_BINARY_DIR}/gtest
  INSTALL_COMMAND ""
)
add_library(libgtest IMPORTED STATIC GLOBAL)
add_dependencies(libgtest gtest)
set_target_properties(libgtest PROPERTIES
    "IMPORTED_LOCATION" "${PROJECT_BINARY_DIR}/gtest/lib/libgtest.a"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
)
add_library(libgtest_main IMPORTED STATIC GLOBAL)
add_dependencies(libgtest_main gtest)
set_target_properties(libgtest_main PROPERTIES
    "IMPORTED_LOCATION" "${PROJECT_BINARY_DIR}/gtest/lib/libgtest_main.a"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
)
set(GTEST_INCLUDE ${PROJECT_BINARY_DIR}/third_party/gtest/googletest/include)

function (add_simple_test test_name)
  set(target_name "${test_name}")
  add_executable(${target_name} ${test_name})
  add_test(
    NAME ${target_name}
    COMMAND ${target_name}
  )
  add_dependencies(${target_name} libgtest libgtest_main gtest)
  target_link_libraries(${target_name} libgtest libgtest_main pthread libpedasus)
  target_include_directories(${target_name}
    PRIVATE
      ${PROJECT_SOURCE_DIR}/include
      ${GTEST_INCLUDE}
  )
endfunction()

add_simple_test(page_pool_test)
