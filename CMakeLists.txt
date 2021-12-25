cmake_minimum_required(VERSION 3.14)
project(tinylamb)

find_package(Threads)
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
        database.cpp page/page.cpp transaction/lock_manager.cpp
        page/page_pool.cpp page/row_page.cpp page/page_manager.cpp
        recovery/logger.cpp type/row.cpp type/schema.cpp
        transaction/transaction.cpp recovery/log_record.cpp page/meta_page.cpp
        recovery/recovery.cpp transaction/transaction_manager.cpp page/page_type.cpp
        type/column.cpp log_message.cpp page/page_ref.cpp)

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

function(add_simple_test file_path)
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

#add_simple_test(page/catalog_page_test.cpp)
add_simple_test(page/page_pool_test.cpp)
add_simple_test(page/page_manager_test.cpp)
add_simple_test(page/row_page_test.cpp)
add_simple_test(type/schema_test.cpp)
#add_simple_test(type/catalog_test.cpp)
add_simple_test(recovery/logger_test.cpp)
add_simple_test(recovery/log_record_test.cpp)
add_simple_test(recovery/recovery_test.cpp)
add_simple_test(database_test.cpp)
