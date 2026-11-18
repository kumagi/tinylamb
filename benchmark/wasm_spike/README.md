# WASM fused-loop execution spike (S0)

This experiment compares three bounded, non-null kernels using the **same
input bytes**: integer filter+SUM, 17-bit FOR decoding+filter+SUM, and bloom
prefilter+two open-addressed hash probes+256 local groups. The probe reads
two independent keys and a stored INT64 measure from each 24-byte fact. It is not yet a SQL
executor, a bytecode compiler, or a test of SQL overflow/NULL semantics.

```sh
python3 -m venv build/wasm-spike-venv
build/wasm-spike-venv/bin/pip install -r benchmark/wasm_spike/requirements.txt
build/wasm-spike-venv/bin/python benchmark/wasm_spike/run.py \
  --rows 10000000 --repeats 7 --cpus 0,1,2,3,4,5,6,7 \
  --output build/benchmarks/wasm-scalar
build/wasm-spike-venv/bin/python benchmark/wasm_spike/run.py \
  --simd --output build/benchmarks/wasm-simd
```

Requires clang++ with wasm64, wasm-ld, and WABT wasm2wat with memory64/threads.
Dependencies are optional and outside the normal build. A standalone CMake
project supplies an EXCLUDE_FROM_ALL native target as well:

```sh
cmake -S benchmark/wasm_spike -B build/wasm-spike -DCMAKE_CXX_COMPILER=clang++
cmake --build build/wasm-spike --target tinylamb_wasm_spike_native
```

The C++ loop source is compiled at `-O2` for both targets; the emitted `.wasm`
and disassembled `.wat` are retained. These are **compiler-generated**, not
the handwritten templates planned for production. `--simd` compares native
AVX2 with WASM SIMD128; otherwise native baseline vectorization is permitted
and no WASM SIMD is requested. Timing includes Python orchestration, thread
pool dispatch, and FFI entry, so very short samples are not pure kernel cost.
Compilation samples cover all three functions in one Wasmtime module, without
text conversion or clang. They do not establish compilation cost for a complete
SQL pipeline. Initialization, validation, and allocation are outside timing.

Wasmtime owns one shared 8 GiB virtual reservation; touched input starts above
4 GiB. Native code receives its actual address; WASM receives offsets into the
same allocation. Workers have distinct stores, instances and output regions.
This proves no per-call input copy in this experiment. It **does not** prove
host-reserved M1 allocation, mmap adoption, or the existing PaxChunk/LSTable ABI.
The Python binding's extra pointer indirection in `_as_extern` is corrected
locally for pinned wasmtime-py 50.0.1. The reservation is explicitly set to the
module's maximum: the default 4 GiB reservation trapped for this import above
4 GiB on this host, even though `memory.size` reported the larger allocation.
Bounds violations are checked to trap; an independent probe oracle and all
per-worker group arrays are checked, as is packed-vs-unpacked SUM equivalence.

See `docs/adr/0002-wasm-fused-execution.md` for measured results and outstanding
acceptance gates. Do not interpret these microbenchmarks as ClickBench results.
