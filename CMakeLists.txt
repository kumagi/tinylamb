cmake_minimum_required(VERSION 3.14)
project(tinylamb)

set(CMAKE_CXX_STANDARD 17)

add_executable(tinylamb
        main.cpp
        )

target_link_libraries(
        tinylamb
        LINK_PUBLIC
        libtinylamb
)

add_library(libtinylamb
        STATIC
        database.cpp
        page.cpp
        page_pool.cpp catalog.hpp schema_params.hpp catalog_test.cpp page_manager.hpp macro.hpp page_manager_test.cpp database_test.cpp logging_test.cpp log_record.hpp lock_manager.hpp row_position.hpp transaction.hpp transaction_manager.hpp logger.hpp logger_test.cpp)

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
  target_link_libraries(${target_name} libgtest libgtest_main pthread libtinylamb)
  target_include_directories(${target_name}
    PRIVATE
      ${PROJECT_SOURCE_DIR}/include
      ${GTEST_INCLUDE}
  )
endfunction()

add_simple_test(page_pool_test)
add_simple_test(page_manager_test)
add_simple_test(catalog_test)
add_simple_test(logging_test)
add_simple_test(database_test)
