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
  PRIVATE
  ${PROJECT_SOURCE_DIR}
  )

add_library(libtinylamb
  STATIC
  database.cpp page/page.cpp transaction/lock_manager.cpp page/page_pool.cpp type/catalog.hpp page/page_manager.hpp log_message.hpp recovery/log_record.hpp transaction/lock_manager.hpp page/row_position.hpp transaction/transaction.hpp transaction/transaction_manager.hpp recovery/logger.hpp type/value.hpp type/schema.hpp type/value_type.hpp page/catalog_page.hpp page/meta_page.hpp page/row_page.hpp page/row_page.cpp page/page_manager.cpp recovery/logger.cpp type/catalog.cpp type/row.cpp type/schema.cpp transaction/transaction.cpp page/catalog_page.cpp recovery/log_record.cpp page/meta_page.cpp page/free_page.hpp recovery/recovery.cpp recovery/recovery.hpp transaction/transaction_manager.cpp type/column.cpp log_message.cpp
  )

target_include_directories(
  libtinylamb
  PUBLIC
  ${PROJECT_SOURCE_DIR}
  )

########################################
## Testing
########################################
enable_testing()

include(FetchContent)
FetchContent_Declare(
  googletest
  # Specify the commit you depend on and update it regularly.
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)

function (add_simple_test file_path)
  get_filename_component(filename ${file_path} NAME)
  string(REPLACE ".cpp" "" target_name ${filename})
  add_executable(${target_name} ${file_path})
  target_link_libraries(${target_name}
    gtest_main
    libtinylamb
    )
  add_test(
    NAME ${target_name}
    COMMAND ${target_name}
    )
  gtest_discover_tests(${target_name})
endfunction()

add_simple_test(page/catalog_page_test.cpp)
add_simple_test(page/page_pool_test.cpp)
add_simple_test(page/page_manager_test.cpp)
add_simple_test(page/row_page_test.cpp)
add_simple_test(type/schema_test.cpp)
add_simple_test(type/catalog_test.cpp)
add_simple_test(recovery/logger_test.cpp)
add_simple_test(recovery/recovery_test.cpp)
add_simple_test(database_test.cpp)
