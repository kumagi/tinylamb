cmake_minimum_required(VERSION 3.14)
project(tinylamb)

find_package (Threads)
set(CMAKE_CXX_STANDARD 17)

add_executable(tinylamb
        main.cpp
        )

target_link_libraries(
        tinylamb
        LINK_PUBLIC
        Threads::Threads
        libtinylamb
)

target_include_directories(
        tinylamb
        PUBLIC
        ${PROJECT_SOURCE_DIR}
)

add_library(libtinylamb
        STATIC
        database.cpp
        page/page.cpp
        transaction/lock_manager.cpp
        page/page_pool.cpp type/catalog.hpp type/catalog_test.cpp page/page_manager.hpp log_message.hpp page/page_manager_test.cpp database_test.cpp logging_test.cpp recovery/log_record.hpp transaction/lock_manager.hpp page/row_position.hpp transaction/transaction.hpp transaction/transaction_manager.hpp recovery/logger.hpp recovery/logger_test.cpp type/value.hpp type/schema.hpp type/value_type.hpp page/catalog_page.hpp recovery/log_record_test.cpp page/meta_page.hpp page/row_page.hpp page/row_page.cpp page/row_page_test.cpp page/page_manager.cpp recovery/logger.cpp type/catalog.cpp type/row.cpp type/schema.cpp transaction/transaction.cpp page/catalog_page.cpp recovery/log_record.cpp page/meta_page.cpp page/free_page.hpp recovery/recovery.cpp recovery/recovery.hpp transaction/transaction_manager.cpp recovery/recovery_test.cpp type/column.cpp log_message.cpp page/catalog_page_test.cpp)

target_include_directories(
        libtinylamb
        PUBLIC
        ${PROJECT_SOURCE_DIR}
)

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
add_library(libgtest IMPORTED STATIC GLOBAL cmake-build-debug/fixed_length_page.hpp type/schema_test.cpp transaction/lock_manager.cpp)
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

function (add_simple_test file_path target_name)
  add_executable(${target_name} ${file_path})
  add_test(
    NAME ${target_name}
    COMMAND ./${target_name}
  )
  add_dependencies(${target_name} libgtest libgtest_main gtest)
  target_link_libraries(${target_name} libgtest libgtest_main pthread libtinylamb)
  target_include_directories(${target_name}
    PRIVATE
      ${PROJECT_SOURCE_DIR}/include
      ${GTEST_INCLUDE}
  )
endfunction()

add_simple_test(page/catalog_page_test.cpp catalog_page_test)
add_simple_test(page/page_pool_test.cpp page_pool_test)
add_simple_test(page/page_manager_test.cpp page_manager_test)
add_simple_test(page/row_page_test.cpp row_page_test)
add_simple_test(type/schema_test.cpp schema_test)
add_simple_test(type/catalog_test.cpp catalog_test)
add_simple_test(recovery/logger_test.cpp logger_test)
add_simple_test(recovery/recovery_test.cpp recovery_test)
add_simple_test(database_test.cpp database_test)
