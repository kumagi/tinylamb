cmake_minimum_required(VERSION 3.14)
project(tinylamb LANGUAGES C CXX)

if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

include(CTest)

option(TINYLAMB_ENABLE_WARNINGS "Enable compiler warnings" ON)
option(TINYLAMB_ENABLE_PEDANTIC_WARNINGS
       "Enable additional strict compiler warnings" ON)
option(TINYLAMB_ENABLE_CONVERSION_WARNINGS
       "Enable noisy implicit numeric conversion warnings" OFF)
option(TINYLAMB_ENABLE_SANITIZERS "Build with AddressSanitizer and UBSan" OFF)
option(TINYLAMB_ENABLE_COVERAGE "Build with coverage instrumentation" OFF)
option(TINYLAMB_ENABLE_GOOGLESQL
       "Build the direct GoogleSQL AST frontend" ON)
option(TINYLAMB_ENABLE_LLVM_JIT "Build LLVM ORC expression kernels" ON)

set(TINYLAMB_GOOGLESQL_VERSION "2026.7.2" CACHE STRING
    "Pinned GoogleSQL release used by the SQL frontend")

if(TINYLAMB_ENABLE_GOOGLESQL)
    set(_googlesql_download_dir "${CMAKE_BINARY_DIR}/_deps/googlesql")
    set(_googlesql_executable "${_googlesql_download_dir}/execute_query")
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
       CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
        set(_googlesql_asset "execute_query_linux")
        set(_googlesql_sha256
            "d362b2469f7077db5c8029f594dd4c435efb162fce0f50c51c9b85695fe98041")
    elseif(APPLE)
        set(_googlesql_asset "execute_query_macos")
        set(_googlesql_sha256
            "f0e9532a6a6285f97fc684461825fb481c07879f08dd28c25e2322d52f99ac29")
    else()
        message(WARNING
                "No prebuilt GoogleSQL parser is available for this platform; "
                "the SQL frontend will be unavailable")
        set(TINYLAMB_ENABLE_GOOGLESQL OFF)
    endif()

    if(TINYLAMB_ENABLE_GOOGLESQL AND NOT EXISTS "${_googlesql_executable}")
        file(MAKE_DIRECTORY "${_googlesql_download_dir}")
        file(DOWNLOAD
             "https://github.com/google/googlesql/releases/download/${TINYLAMB_GOOGLESQL_VERSION}/${_googlesql_asset}"
             "${_googlesql_executable}"
             EXPECTED_HASH "SHA256=${_googlesql_sha256}"
             SHOW_PROGRESS
             STATUS _googlesql_download_status)
        list(GET _googlesql_download_status 0 _googlesql_download_code)
        if(NOT _googlesql_download_code EQUAL 0)
            list(GET _googlesql_download_status 1 _googlesql_download_error)
            message(FATAL_ERROR
                    "Failed to download GoogleSQL ${TINYLAMB_GOOGLESQL_VERSION}: "
                    "${_googlesql_download_error}")
        endif()
        file(CHMOD "${_googlesql_executable}"
             PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                         GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
    endif()
endif()

find_package(Threads REQUIRED)
if(TINYLAMB_ENABLE_LLVM_JIT)
    find_package(LLVM CONFIG QUIET)
endif()

if(TINYLAMB_ENABLE_SANITIZERS)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        message(FATAL_ERROR
                "TINYLAMB_ENABLE_SANITIZERS requires GCC or Clang")
    endif()

    add_library(tinylamb_sanitizers INTERFACE)
    target_compile_options(tinylamb_sanitizers INTERFACE
            -fsanitize=address,undefined,bounds
            -fsanitize-address-use-after-scope
            -fno-omit-frame-pointer)
    target_link_options(tinylamb_sanitizers INTERFACE
            -fsanitize=address,undefined,bounds)
endif()

if(TINYLAMB_ENABLE_COVERAGE)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
        message(FATAL_ERROR
                "TINYLAMB_ENABLE_COVERAGE requires GCC or Clang")
    endif()

    add_library(tinylamb_coverage INTERFACE)
    target_compile_options(tinylamb_coverage INTERFACE --coverage)
    target_link_options(tinylamb_coverage INTERFACE --coverage)
endif()

function(tinylamb_apply_options target)
    target_compile_features(${target} PRIVATE cxx_std_20)

    if(TINYLAMB_ENABLE_WARNINGS)
        if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
            target_compile_options(${target} PRIVATE -Wall -Wextra)
            if(TINYLAMB_ENABLE_PEDANTIC_WARNINGS)
                target_compile_options(${target} PRIVATE
                        -Wpedantic
                        -Wshadow
                        -Wformat=2
                        -Wundef
                        -Wextra-semi
                        -Wimplicit-fallthrough)
                if(TINYLAMB_ENABLE_CONVERSION_WARNINGS)
                    target_compile_options(${target} PRIVATE
                            -Wconversion
                            -Wsign-conversion)
                endif()
            endif()
        elseif(MSVC)
            target_compile_options(${target} PRIVATE /W4)
        endif()
    endif()

    if(TINYLAMB_ENABLE_SANITIZERS)
        target_link_libraries(${target} PRIVATE tinylamb_sanitizers)
    endif()

    if(TINYLAMB_ENABLE_COVERAGE)
        target_link_libraries(${target} PRIVATE tinylamb_coverage)
    endif()
endfunction()

add_library(tinylamb_parser
        STATIC
        parser/tokenizer.cpp
        parser/parser.cpp
        parser/pratt_parser.cpp
)
add_library(tinylamb::parser ALIAS tinylamb_parser)
target_compile_features(tinylamb_parser PUBLIC cxx_std_20)
tinylamb_apply_options(tinylamb_parser)

add_library(tinylamb_core
        STATIC
        page/page.cpp transaction/lock_manager.cpp
        page/page_pool.cpp page/row_page.cpp page/pax_block.cpp page/page_manager.cpp
        recovery/logger.cpp type/row.cpp type/schema.cpp type/date.cpp
        transaction/transaction.cpp recovery/log_record.cpp page/meta_page.cpp
        recovery/recovery_manager.cpp recovery/checkpoint_manager.cpp
        transaction/transaction_manager.cpp page/page_type.cpp type/column.cpp
        common/serdes.cpp common/log_message.cpp page/page_ref.cpp
        page/leaf_page.cpp page/branch_page.cpp index/b_plus_tree.cpp
        type/value.cpp type/constraint.cpp table/table.cpp
        index/index.cpp common/debug.cpp common/encoder.cpp
        table/full_scan_iterator.cpp index/b_plus_tree_iterator.cpp
        index/index_scan_iterator.cpp
        database/database.cpp executor/full_scan.cpp executor/parallel_scan.cpp
        executor/projection.cpp
        executor/aggregation.cpp executor/parallel_aggregation.cpp
        executor/zone_map.cpp
        executor/query_scheduler.cpp
        executor/data_chunk.cpp
        executor/executor_base.cpp
        executor/selection.cpp expression/binary_expression.cpp
        expression/unary_expression.cpp
        expression/aggregate_expression.cpp
        expression/case_expression.cpp
        expression/in_expression.cpp
        expression/function_call_expression.cpp
        expression/query_expression.cpp
        expression/interval_expression.cpp
        expression/rewrite.cpp expression/bytecode.cpp expression/jit.cpp
        expression/column_value.cpp executor/hash_join.cpp common/decoder.cpp
        plan/full_scan_plan.cpp plan/projection_plan.cpp plan/selection_plan.cpp
        plan/product_plan.cpp plan/optimizer.cpp plan/cascades.cpp
        plan/index_only_scan_plan.cpp
        plan/aggregation_plan.cpp plan/plan.cpp
        executor/cross_join.cpp table/table_statistics.cpp expression/expression.cpp
        database/page_storage.cpp index/index_schema.cpp
        database/transaction_context.cpp executor/insert.cpp
        executor/update.cpp executor/delete.cpp executor/sort.cpp
        executor/limit.cpp executor/distinct.cpp
        executor/relational.cpp
        plan/index_scan_plan.cpp executor/index_scan.cpp
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
)
add_library(tinylamb::core ALIAS tinylamb_core)
target_compile_features(tinylamb_core PUBLIC cxx_std_20)
tinylamb_apply_options(tinylamb_core)
target_link_libraries(tinylamb_core PUBLIC Threads::Threads)
if(LLVM_FOUND AND TINYLAMB_ENABLE_LLVM_JIT)
    target_compile_definitions(tinylamb_core PRIVATE TINYLAMB_HAS_LLVM=1)
    target_include_directories(tinylamb_core SYSTEM PRIVATE ${LLVM_INCLUDE_DIRS})
    llvm_map_components_to_libnames(TINYLAMB_LLVM_LIBS
        Core OrcJIT ExecutionEngine RuntimeDyld Support native)
    target_link_libraries(tinylamb_core PUBLIC ${TINYLAMB_LLVM_LIBS})
endif()

add_library(tinylamb_sql
        STATIC
        query/googlesql_frontend.cpp
        query/googlesql_ast.cpp
        query/googlesql_ast_visitor.cpp
        query/sql_template.cpp
        query/sql_engine.cpp
)
add_library(tinylamb::sql ALIAS tinylamb_sql)
target_compile_features(tinylamb_sql PUBLIC cxx_std_20)
tinylamb_apply_options(tinylamb_sql)
target_link_libraries(tinylamb_sql PUBLIC tinylamb::core)
if(TINYLAMB_ENABLE_GOOGLESQL)
    target_compile_definitions(tinylamb_sql PRIVATE
            TINYLAMB_GOOGLESQL_EXECUTABLE="${_googlesql_executable}")
endif()

add_library(tinylamb_postgres_server
        STATIC
        server/postgres_protocol.cpp
        server/postgres_server.cpp
)
add_library(tinylamb::postgres_server ALIAS tinylamb_postgres_server)
target_compile_features(tinylamb_postgres_server PUBLIC cxx_std_20)
tinylamb_apply_options(tinylamb_postgres_server)
target_include_directories(tinylamb_postgres_server
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(tinylamb_postgres_server PUBLIC tinylamb::sql)

add_library(tinylamb_tpcc STATIC benchmark/tpcc_workload.cpp)
add_library(tinylamb::tpcc ALIAS tinylamb_tpcc)
target_compile_features(tinylamb_tpcc PUBLIC cxx_std_20)
tinylamb_apply_options(tinylamb_tpcc)
target_include_directories(tinylamb_tpcc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(tinylamb_tpcc PUBLIC tinylamb::sql)


add_library(tinylamb_test_util
        STATIC
        type/row.cpp)
add_library(tinylamb::test_util ALIAS tinylamb_test_util)
target_compile_features(tinylamb_test_util PUBLIC cxx_std_20)
tinylamb_apply_options(tinylamb_test_util)

target_include_directories(
        tinylamb_parser
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_include_directories(
        tinylamb_core
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_include_directories(
        tinylamb_sql
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_include_directories(
        tinylamb_test_util
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(tinylamb
        main.cpp
)
tinylamb_apply_options(tinylamb)
target_link_libraries(
        tinylamb
        PRIVATE
        tinylamb::sql
)
target_include_directories(
        tinylamb
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(tinylamb_server server/postgres_server_main.cpp)
tinylamb_apply_options(tinylamb_server)
target_link_libraries(tinylamb_server PRIVATE tinylamb::postgres_server)

add_executable(tinylamb_tpcc_benchmark benchmark/tpcc_benchmark.cpp)
tinylamb_apply_options(tinylamb_tpcc_benchmark)
target_link_libraries(tinylamb_tpcc_benchmark PRIVATE tinylamb::tpcc)

# TPC-H DBGEN is an executable build dependency of the benchmark only.  It is
# deliberately not linked into tinylamb and is not downloaded when building
# the database, server, or unit-test targets.
include(ExternalProject)
set(TINYLAMB_TPCH_DBGEN_COMMIT
    "32f1c1b92d1664dba542e927d23d86ffa57aa253"
    CACHE STRING "Pinned TPC-H DBGEN commit")
set(_tpch_dbgen_source_dir "${CMAKE_BINARY_DIR}/_deps/tpch-dbgen-src")
ExternalProject_Add(tinylamb_tpch_dbgen
    PREFIX "${CMAKE_BINARY_DIR}/_deps/tpch-dbgen"
    GIT_REPOSITORY https://github.com/electrum/tpch-dbgen.git
    GIT_TAG "${TINYLAMB_TPCH_DBGEN_COMMIT}"
    GIT_SHALLOW FALSE
    UPDATE_DISCONNECTED TRUE
    SOURCE_DIR "${_tpch_dbgen_source_dir}"
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE TRUE
    BUILD_COMMAND make "CC=cc -std=gnu17 -D_DEFAULT_SOURCE" dbgen
    INSTALL_COMMAND ""
    EXCLUDE_FROM_ALL TRUE)

add_executable(tinylamb_tpch_benchmark EXCLUDE_FROM_ALL
        benchmark/tpch_benchmark.cpp)
tinylamb_apply_options(tinylamb_tpch_benchmark)
target_link_libraries(tinylamb_tpch_benchmark PRIVATE tinylamb::sql)
target_compile_definitions(tinylamb_tpch_benchmark PRIVATE
    TINYLAMB_TPCH_DBGEN_EXECUTABLE="${_tpch_dbgen_source_dir}/dbgen"
    TINYLAMB_TPCH_DBGEN_CONFIG="${_tpch_dbgen_source_dir}"
    TINYLAMB_TPCH_DBGEN_COMMIT="${TINYLAMB_TPCH_DBGEN_COMMIT}")
add_dependencies(tinylamb_tpch_benchmark tinylamb_tpch_dbgen)

add_executable(tinylamb_pg_read_benchmark
        benchmark/pg_read_benchmark.cpp)
tinylamb_apply_options(tinylamb_pg_read_benchmark)
target_link_libraries(tinylamb_pg_read_benchmark PRIVATE Threads::Threads)

add_executable(tinylamb_expression_jit_benchmark EXCLUDE_FROM_ALL
        benchmark/expression_jit_benchmark.cpp)
tinylamb_apply_options(tinylamb_expression_jit_benchmark)
target_link_libraries(tinylamb_expression_jit_benchmark PRIVATE tinylamb::core)

########################################
## Bench
########################################

function(add_benchmark file_path)
    get_filename_component(filename ${file_path} NAME)
    get_filename_component(target_name ${filename} NAME_WE)
    add_executable(${target_name} ${file_path})
    tinylamb_apply_options(${target_name})
    set_property(TARGET ${target_name} PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    target_link_libraries(${target_name}
            PRIVATE
            tinylamb::core
            leveldb
            rocksdb
    )
endfunction()

#add_benchmark("index/lsm_detail/lsm_tree_bench.cpp")

########################################
## Testing
########################################
if(BUILD_TESTING)
    include(FetchContent)
    FetchContent_Declare(
            googletest
            URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
            URL_HASH SHA256=1f357c27ca988c3f7c6b4bf68a9395005ac6761f034046e9dde0896e3aba00e4
    )

    # Keep the dependency focused on the test library used here.
    set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    set(INSTALL_GMOCK OFF CACHE BOOL "" FORCE)
    set(gtest_build_tests OFF CACHE BOOL "" FORCE)
    if(WIN32)
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    endif()

    FetchContent_MakeAvailable(googletest)
    include(GoogleTest)

    function(add_simple_test file_path)
        get_filename_component(filename ${file_path} NAME)
        get_filename_component(target_name ${filename} NAME_WE)
        add_executable(${target_name} ${file_path})
        tinylamb_apply_options(${target_name})
        target_link_libraries(${target_name}
                PRIVATE
                GTest::gtest_main
                tinylamb::parser
                tinylamb::sql
                tinylamb::test_util
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
add_simple_test(page/pax_layout_test.cpp)
add_simple_test(page/pax_block_test.cpp)
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
add_simple_test(expression/rewrite_test.cpp)
add_simple_test(expression/bytecode_test.cpp)
add_simple_test(expression/jit_test.cpp)
add_simple_test(executor/executor_test.cpp)
add_simple_test(executor/data_chunk_test.cpp)
add_simple_test(executor/query_scheduler_test.cpp)
add_simple_test(database/catalog_test.cpp)
add_simple_test(plan/plan_test.cpp)
add_simple_test(type/column_name_test.cpp)
add_simple_test(plan/optimizer_test.cpp)
add_simple_test(plan/cascades_test.cpp)
add_simple_test(table/table_statistics_test.cpp)
add_simple_test(index/lsm_detail/blob_file_test.cpp)
add_simple_test(index/lsm_detail/lsm_view_test.cpp)
add_simple_test(index/lsm_detail/cache_test.cpp)
add_simple_test(index/lsm_detail/sorted_run_test.cpp)
add_simple_test(parser/parser_test.cpp)
add_simple_test(parser/tokenizer_test.cpp)
add_simple_test(parser/pratt_parser_test.cpp)
add_simple_test(query/query_test.cpp)
add_simple_test(query/googlesql_frontend_test.cpp)
add_simple_test(query/googlesql_ast_test.cpp)
add_simple_test(query/sql_template_test.cpp)
add_simple_test(query/sql_engine_tpcc_test.cpp)
add_simple_test(query/sql_engine_tpch_test.cpp)
add_simple_test(server/postgres_protocol_test.cpp)
target_link_libraries(postgres_protocol_test PRIVATE tinylamb::postgres_server)
add_simple_test(server/postgres_server_test.cpp)
target_link_libraries(postgres_server_test PRIVATE tinylamb::postgres_server)
add_simple_test(benchmark/tpcc_workload_test.cpp)
target_link_libraries(tpcc_workload_test PRIVATE tinylamb::tpcc)

# Concurrency tests.
add_simple_test(page/row_page_concurrent_test.cpp)
add_simple_test(index/b_plus_tree_concurrent_test.cpp)
add_simple_test(table/table_concurrent_test.cpp)
add_simple_test(index/lsm_detail/cache_concurrent_test.cpp)

endif()

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
