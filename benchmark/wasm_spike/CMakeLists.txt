# Optional experiment, isolated from the SQL engine and its dependencies.
cmake_minimum_required(VERSION 3.14)
project(tinylamb_wasm_spike LANGUAGES CXX)
add_library(tinylamb_wasm_spike_native SHARED EXCLUDE_FROM_ALL kernels.cpp)
target_compile_features(tinylamb_wasm_spike_native PRIVATE cxx_std_20)
target_compile_options(tinylamb_wasm_spike_native PRIVATE -O2)
