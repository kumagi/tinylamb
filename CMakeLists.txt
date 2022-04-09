cmake_minimum_required(VERSION 3.14)
project(tinylamb)

find_package(Threads)
set(CMAKE_CXX_STANDARD 17)

add_executable(tinylamb
        main.cpp
        )
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fsanitize=bounds -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ggdb -Wall -Wextra")

set(CMAKE_C_COMPILER "/usr/bin/clang" CACHE STRING "clang compiler" FORCE)
set(CMAKE_CXX_COMPILER "/usr/bin/clang++" CACHE STRING "clang++ compiler" FORCE)

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
        database/database.cpp page/page.cpp transaction/lock_manager.cpp
        page/page_pool.cpp page/row_page.cpp page/page_manager.cpp
        recovery/logger.cpp type/row.cpp type/schema.cpp
        transaction/transaction.cpp recovery/log_record.cpp page/meta_page.cpp
        recovery/recovery_manager.cpp recovery/checkpoint_manager.cpp
        transaction/transaction_manager.cpp page/page_type.cpp type/column.cpp
        common/serdes.cpp common/log_message.cpp page/page_ref.cpp
        page/leaf_page.cpp page/branch_page.cpp index/b_plus_tree.cpp
        type/value.cpp type/constraint.cpp type/constraint.hpp table/table.cpp
        table/table.hpp index/index.cpp common/debug.cpp common/encoder.cpp
        table/full_scan_iterator.cpp index/b_plus_tree_iterator.cpp
        index/index_scan_iterator.cpp index/index_scan_iterator.hpp
        database/catalog.cpp executor/full_scan.cpp executor/projection.cpp
        executor/selection.cpp expression/binary_expression.cpp
        expression/column_value.cpp executor/hash_join.cpp common/decoder.cpp
        table/fake_table.cpp plan/full_scan_plan.cpp
        plan/projection_plan.cpp plan/selection_plan.cpp plan/product_plan.cpp
        plan/optimizer.cpp
        executor/cross_join.cpp table/table_statistics.cpp plan/plan.cpp expression/expression.cpp)

add_library(libtinylamb_test_util
        STATIC
        table/fake_table.cpp type/row.cpp)

target_include_directories(
        libtinylamb
        PUBLIC
        ${PROJECT_SOURCE_DIR}
)

target_include_directories(
        libtinylamb_test_util
        PUBLIC
        ${PROJECT_SOURCE_DIR})

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

function(add_simple_test file_path)
    get_filename_component(filename ${file_path} NAME)
    string(REPLACE ".cpp" "" target_name ${filename})
    add_executable(${target_name} ${file_path})
    target_link_libraries(${target_name}
            gtest_main
            libtinylamb
            libtinylamb_test_util
            )
    add_test(
            NAME ${target_name}
            COMMAND ${target_name}
    )
    gtest_discover_tests(${target_name})
endfunction()

add_simple_test(database/database_test.cpp)
add_simple_test(type/value_test.cpp)
add_simple_test(type/row_test.cpp)
add_simple_test(type/constraint_test.cpp)
add_simple_test(type/column_test.cpp)
add_simple_test(type/schema_test.cpp)
add_simple_test(page/leaf_page_test.cpp)
add_simple_test(page/branch_page_test.cpp)
add_simple_test(page/page_pool_test.cpp)
add_simple_test(page/page_manager_test.cpp)
add_simple_test(page/row_page_test.cpp)
add_simple_test(recovery/logger_test.cpp)
add_simple_test(recovery/log_record_test.cpp)
add_simple_test(recovery/recovery_manager_test.cpp)
add_simple_test(recovery/checkpoint_manager_test.cpp)
add_simple_test(index/b_plus_tree_test.cpp)
add_simple_test(index/b_plus_tree_iterator_test.cpp)
add_simple_test(table/table_test.cpp)
add_simple_test(table/full_scan_iterator_test.cpp)
add_simple_test(index/index_scan_iterator_test.cpp)
add_simple_test(transaction/transaction_test.cpp)
add_simple_test(expression/expression_test.cpp)
add_simple_test(executor/executor_test.cpp)
add_simple_test(database/catalog_test.cpp)
add_simple_test(plan/plan_test.cpp)
add_simple_test(plan/optimizer_test.cpp)
add_simple_test(table/table_statistics_test.cpp)

# Concurrency tests.
add_simple_test(page/row_page_concurrent_test.cpp)


########################################
## Fuzzing
########################################
function(add_fuzzer_test file_path)
    set(fuzzer_option "-fsanitize=address,fuzzer,undefined,bounds")
    get_filename_component(filename ${file_path} NAME)
    string(REPLACE ".cpp" "" target_name ${filename})
    add_executable(${target_name} ${file_path})

    target_compile_options(${target_name} PRIVATE -g ${fuzzer_option})
    target_link_options(${target_name} PRIVATE ${fuzzer_option})

    target_link_libraries(${target_name}
            libtinylamb
            libtinylamb_test_util
            )
endfunction()

add_fuzzer_test(type/value_fuzzer.cpp)
add_fuzzer_test(page/row_page_fuzzer.cpp)
add_fuzzer_test(index/b_plus_tree_fuzzer.cpp)

########################################
## Fuzzing Replay
########################################
function(add_fuzzer_test file_path)
    set(fuzzer_option "-fsanitize=address")
    get_filename_component(filename ${file_path} NAME)
    string(REPLACE ".cpp" "" target_name ${filename})
    add_executable(${target_name} ${file_path})

    target_compile_options(${target_name} PRIVATE -g ${fuzzer_option})
    target_link_options(${target_name} PRIVATE ${fuzzer_option})

    target_link_libraries(${target_name}
            gtest_main
            libtinylamb
            libtinylamb_test_util
            )
endfunction()

add_fuzzer_test(page/row_page_fuzzer_replay.cpp)
add_fuzzer_test(index/b_plus_tree_fuzzer_replay.cpp)