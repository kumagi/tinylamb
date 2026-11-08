# Cross toolchain for Windows 11 (x86_64-w64-mingw32, UCRT64).
#
# Uses the host clang + lld with a sysroot mirrored from MSYS2 UCRT64 on the
# target Windows host (C:/msys64/ucrt64/{include,lib,x86_64-w64-mingw32} ->
# TINYLAMB_WIN_SYSROOT).
#
# Example:
#   cmake -S . -B build-win -G Ninja -DCMAKE_BUILD_TYPE=Release \
#     -DCMAKE_TOOLCHAIN_FILE=cmake/win-ucrt64.toolchain.cmake \
#     -DTINYLAMB_WIN_SYSROOT=$HOME/sysroots/win-ucrt64 \
#     -DTINYLAMB_ENABLE_LLVM_JIT=OFF -DBUILD_TESTING=OFF

set(CMAKE_SYSTEM_NAME Windows)
set(CMAKE_SYSTEM_PROCESSOR x86_64)

set(CMAKE_TRY_COMPILE_PLATFORM_VARIABLES TINYLAMB_WIN_SYSROOT)

if(NOT DEFINED TINYLAMB_WIN_SYSROOT)
    if(DEFINED ENV{TINYLAMB_WIN_SYSROOT})
        set(TINYLAMB_WIN_SYSROOT "$ENV{TINYLAMB_WIN_SYSROOT}")
    else()
        message(FATAL_ERROR
            "Set TINYLAMB_WIN_SYSROOT to the UCRT64 sysroot directory "
            "(mirrored from the target Windows machine)")
    endif()
endif()
set(CMAKE_SYSROOT "${TINYLAMB_WIN_SYSROOT}")

set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)

set(_tinylamb_win_triple x86_64-w64-windows-gnu)
set(_tinylamb_win_flags "--target=${_tinylamb_win_triple}")
set(CMAKE_C_FLAGS_INIT "${_tinylamb_win_flags}")
set(CMAKE_CXX_FLAGS_INIT "${_tinylamb_win_flags}")
set(CMAKE_EXE_LINKER_FLAGS_INIT "-fuse-ld=lld -static-libgcc -static-libstdc++ -Wl,-Bstatic -lwinpthread -Wl,-Bdynamic")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-fuse-ld=lld")

set(CMAKE_FIND_ROOT_PATH "${TINYLAMB_WIN_SYSROOT}")
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
