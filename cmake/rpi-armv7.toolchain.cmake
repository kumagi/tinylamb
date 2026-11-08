# Cross toolchain for Raspberry Pi OS (32-bit armv7l, hard-float).
#
# Uses the host clang + lld with a sysroot mirrored from the target Pi
# (rsync /usr/include, /usr/lib/arm-linux-gnueabihf and /usr/lib/gcc into
# TINYLAMB_RPI_SYSROOT). Mirroring the target's own glibc/libstdc++ keeps the
# produced binaries linked against the exact runtime on the device.
#
# Example:
#   cmake -S . -B build-pi -G Ninja -DCMAKE_BUILD_TYPE=Release \
#     -DCMAKE_TOOLCHAIN_FILE=cmake/rpi-armv7.toolchain.cmake \
#     -DTINYLAMB_RPI_SYSROOT=$HOME/sysroots/rpi-armv7 \
#     -DTINYLAMB_ENABLE_LLVM_JIT=OFF -DBUILD_TESTING=OFF

set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR armv7l)

# Propagate into try_compile scratch projects, which re-include this file.
set(CMAKE_TRY_COMPILE_PLATFORM_VARIABLES TINYLAMB_RPI_SYSROOT)

if(NOT DEFINED TINYLAMB_RPI_SYSROOT)
    if(DEFINED ENV{TINYLAMB_RPI_SYSROOT})
        set(TINYLAMB_RPI_SYSROOT "$ENV{TINYLAMB_RPI_SYSROOT}")
    else()
        message(FATAL_ERROR
            "Set TINYLAMB_RPI_SYSROOT to the armv7 sysroot directory "
            "(mirrored from the target Pi)")
    endif()
endif()
set(CMAKE_SYSROOT "${TINYLAMB_RPI_SYSROOT}")

set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)

# Raspberry Pi 3 (BCM2837, Cortex-A53) in AArch32 state. The hard-float ABI
# matches arm-linux-gnueabihf; --gcc-toolchain lets clang find the target's
# gcc-14 runtime (crt*.o, libgcc, libstdc++ headers) inside the sysroot.
set(_tinylamb_rpi_triple arm-linux-gnueabihf)
# -fforce-enable-int128: the codebase relies on __int128 (PAX codecs,
# aggregation, extended floats). armv7 has no hardware int128, but clang can
# still synthesize it: all integer ops lower inline, and the handful of
# float<->int128 conversion builtins are provided by
# common/platform/int128_rt.cpp.
set(_tinylamb_rpi_flags
    "--target=${_tinylamb_rpi_triple} -mcpu=cortex-a53 -fforce-enable-int128 --gcc-toolchain=${TINYLAMB_RPI_SYSROOT}/usr")
set(CMAKE_C_FLAGS_INIT "${_tinylamb_rpi_flags}")
set(CMAKE_CXX_FLAGS_INIT "${_tinylamb_rpi_flags}")
set(CMAKE_EXE_LINKER_FLAGS_INIT "-fuse-ld=lld")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-fuse-ld=lld")

set(CMAKE_FIND_ROOT_PATH "${TINYLAMB_RPI_SYSROOT}")
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
