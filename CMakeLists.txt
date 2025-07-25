cmake_minimum_required(VERSION 3.14)
project(tinylamb)

find_package(Threads)
set(CMAKE_CXX_STANDARD 20)

add_executable(tinylamb
        main.cpp
)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined,bounds,address -fsanitize-address-use-after-scope")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ggdb -Wall -Wextra")

#set(CMAKE_C_COMPILER "/usr/bin/clang" CACHE STRING "clang compiler" FORCE)
#set(CMAKE_CXX_COMPILER "/usr/bin/clang++" CACHE STRING "clang++ compiler" FORCE)

target_link_libraries(
        tinylamb
        LINK_PUBLIC
        Threads::Threads
        libtinylamb
        libtinylamb_parser
)

target_include_directories(
        tinylamb
        PRIVATE
        ${PROJECT_SOURCE_DIR}
)

add_library(libtinylamb_parser
        STATIC
        parser/tokenizer.cpp
        parser/parser.cpp
        parser/pratt_parser.cpp
)

add_library(libtinylamb
        STATIC
        page/page.cpp transaction/lock_manager.cpp
        page/page_pool.cpp page/row_page.cpp page/page_manager.cpp
        recovery/logger.cpp type/row.cpp type/schema.cpp
        transaction/transaction.cpp recovery/log_record.cpp page/meta_page.cpp
        recovery/recovery_manager.cpp recovery/checkpoint_manager.cpp
        transaction/transaction_manager.cpp page/page_type.cpp type/column.cpp
        common/serdes.cpp common/log_message.cpp page/page_ref.cpp
        page/leaf_page.cpp page/branch_page.cpp index/b_plus_tree.cpp
        type/value.cpp type/constraint.cpp table/table.cpp
        index/index.cpp common/debug.cpp common/encoder.cpp
        table/full_scan_iterator.cpp index/b_plus_tree_iterator.cpp
        index/index_scan_iterator.cpp
        database/database.cpp executor/full_scan.cpp executor/projection.cpp
        executor/aggregation.cpp
        executor/selection.cpp expression/binary_expression.cpp
        expression/unary_expression.cpp
        expression/aggregate_expression.cpp
        expression/case_expression.cpp
        expression/in_expression.cpp
        expression/function_call_expression.cpp
        expression/column_value.cpp executor/hash_join.cpp common/decoder.cpp
        plan/full_scan_plan.cpp plan/projection_plan.cpp plan/selection_plan.cpp
        plan/product_plan.cpp plan/optimizer.cpp plan/index_only_scan_plan.cpp
        plan/aggregation_plan.cpp plan/plan.cpp
        executor/cross_join.cpp table/table_statistics.cpp expression/expression.cpp
        database/page_storage.cpp index/index_schema.cpp
        database/transaction_context.cpp executor/insert.cpp
        executor/update.cpp plan/index_scan_plan.cpp executor/index_scan.cpp
        executor/index_only_scan.cpp type/column_name.cpp query/query_data.cpp
                expression/named_expression.cpp executor/index_join.cpp
        executor/constant_executor.cpp
        index/lsm_tree.cpp
        index/lsm_detail/lsm_view.cpp
        index/lsm_detail/blob_file.cpp
        index/lsm_detail/cache.cpp index/lsm_detail/sorted_run.cpp
        common/ring_buffer.hpp
        common/vm_cache_impl.cpp
        common/vm_cache_impl.hpp
        type/function.cpp
        type/type.cpp
        expression/expression.cpp
)


add_library(libtinylamb_test_util
        STATIC
        type/row.cpp)

target_include_directories(
        libtinylamb
        PUBLIC
        ${PROJECT_SOURCE_DIR}
)

target_include_directories(
        libtinylamb_parser
        PUBLIC
        ${PROJECT_SOURCE_DIR}
)

target_include_directories(
        libtinylamb_test_util
        PUBLIC
        ${PROJECT_SOURCE_DIR})

########################################
## Bench
########################################

function(add_benchmark file_path)
    get_filename_component(filename ${file_path} NAME)
    string(REPLACE ".cpp" "" target_name ${filename})
    add_executable(${target_name} ${file_path})
    target_compile_options(${target_name} PRIVATE "-O2")
    set_property(TARGET ${target_name} PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    target_link_libraries(${target_name}
            libtinylamb
            leveldb
            rocksdb
    )
endfunction()

#add_benchmark("index/lsm_detail/lsm_tree_bench.cpp")

########################################
## Testing
########################################
enable_testing()

include(FetchContent)
FetchContent_Declare(
        googletest
        # Specify the commit you depend on and update it regularly.
        URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
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
            libtinylamb_parser
            libtinylamb
            libtinylamb_test_util
    )
    gtest_discover_tests(${target_name})
endfunction()

add_simple_test(common/vm_cache_test.cpp)
add_simple_test(common/log_message_test.cpp)
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
add_simple_test(index/lsm_tree_test.cpp)
add_simple_test(table/table_test.cpp)
add_simple_test(table/index_test.cpp)
add_simple_test(table/full_scan_iterator_test.cpp)
add_simple_test(index/index_scan_iterator_test.cpp)
add_simple_test(transaction/transaction_test.cpp)
add_simple_test(expression/expression_test.cpp)
add_simple_test(executor/executor_test.cpp)
add_simple_test(database/catalog_test.cpp)
add_simple_test(plan/plan_test.cpp)
add_simple_test(type/column_name_test.cpp)
add_simple_test(plan/optimizer_test.cpp)
add_simple_test(table/table_statistics_test.cpp)
add_simple_test(index/lsm_detail/blob_file_test.cpp)
add_simple_test(index/lsm_detail/lsm_view_test.cpp)
add_simple_test(index/lsm_detail/cache_test.cpp)
add_simple_test(index/lsm_detail/sorted_run_test.cpp)
add_simple_test(parser/parser_test.cpp)
add_simple_test(parser/tokenizer_test.cpp)
add_simple_test(parser/pratt_parser_test.cpp)
add_simple_test(query/query_test.cpp)

# Concurrency tests.
add_simple_test(page/row_page_concurrent_test.cpp)
add_simple_test(index/b_plus_tree_concurrent_test.cpp)
add_simple_test(table/table_concurrent_test.cpp)
add_simple_test(index/lsm_detail/cache_concurrent_test.cpp)


########################################
## Fuzzing
########################################
#function(add_fuzzer_test file_path)
#    set(fuzzer_option "-fsanitize=fuzzer,undefined,bounds")
#    get_filename_component(filename ${file_path} NAME)
#    string(REPLACE ".cpp" "" target_name ${filename})
#    add_executable(${target_name} ${file_path})
#
#    target_compile_options(${target_name} PRIVATE -g ${fuzzer_option})
#    target_link_options(${target_name} PRIVATE ${fuzzer_option})
#
#    target_link_libraries(${target_name}
#            libtinylamb
#            libtinylamb_test_util
#    )
#endfunction()
#
#add_fuzzer_test(recovery/logger_fuzzer.cpp)
#add_fuzzer_test(type/value_fuzzer.cpp)
#add_fuzzer_test(page/row_page_fuzzer.cpp)
#add_fuzzer_test(page/leaf_page_fuzzer.cpp)
#add_fuzzer_test(index/lsm_detail/lsm_view_fuzzer.cpp)
#add_fuzzer_test(index/lsm_detail/cache_fuzzer.cpp)
#add_fuzzer_test(index/lsm_tree_fuzzer.cpp)
#add_fuzzer_test(index/b_plus_tree_fuzzer.cpp)
#add_fuzzer_test(table/table_fuzzer.cpp)

########################################
## Fuzzing Replay
########################################
#function(add_replay_test file_path)
#    set(fuzzer_option "-fsanitize-trap=undefined,bounds")
#    get_filename_component(filename ${file_path} NAME)
#    string(REPLACE ".cpp" "" target_name ${filename})
#    add_executable(${target_name} ${file_path})
#
#    target_compile_options(${target_name} PRIVATE -g ${fuzzer_option})
#    target_link_options(${target_name} PRIVATE ${fuzzer_option})
#
#    target_link_libraries(${target_name}
#            gtest_main
#            libtinylamb
#            libtinylamb_test_util
#    )
#endfunction()
#
#add_replay_test(recovery/logger_fuzzer_replay.cpp)
#add_replay_test(page/row_page_fuzzer_replay.cpp)
#add_replay_test(page/leaf_page_fuzzer_replay.cpp)
#add_replay_test(index/lsm_detail/lsm_view_fuzzer_replay.cpp)
#add_replay_test(index/lsm_detail/cache_fuzzer_replay.cpp)
#add_replay_test(index/lsm_tree_fuzzer_replay.cpp)
#add_replay_test(index/b_plus_tree_fuzzer_replay.cpp)
#
#add_replay_test(table/table_fuzzer_replay.cpp)