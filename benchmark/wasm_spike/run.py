#!/usr/bin/env python3
"""S0: same bounded kernels, same memory, native vs Wasmtime memory64.

This is a microbenchmark, not a SQL-engine or PaxChunk integration benchmark.
The Python orchestration/FFI cost is included equally in wall-clock samples;
compilation, allocation, initialization and result checking are outside them.
"""
import argparse
import concurrent.futures
import ctypes as C
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import time
import wasmtime as W
from wasmtime import _ffi


class SharedMemory(W.SharedMemory):
    # wasmtime-py 50.0.1 adds a pointer indirection in _as_extern. Pass the
    # owned C API handle directly; keep the installed package unchanged.
    def _as_extern(self):
        return _ffi.wasmtime_extern_t(
            _ffi.WASMTIME_EXTERN_SHAREDMEMORY,
            _ffi.wasmtime_extern_union(sharedmemory=self.ptr()))


def measure(call, repeats):
    call()
    times, answers = [], []
    for _ in range(repeats):
        start = time.perf_counter_ns()
        answers.append(call())
        times.append((time.perf_counter_ns() - start) / 1e6)
    return times, answers


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rows', type=int, default=10_000_000)
    parser.add_argument('--repeats', type=int, default=7)
    parser.add_argument('--cpus', default='0,1,2,3,4,5,6,7')
    parser.add_argument('--clang', default='clang++')
    parser.add_argument('--simd', action='store_true', help='WASM SIMD128 vs native AVX2')
    args = parser.parse_args()
    if not 0 < args.rows <= 100_000_000 or args.repeats < 1:
        parser.error('rows must be 1..100000000 and repeats positive')
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    if hasattr(os, 'sched_setaffinity'):
        os.sched_setaffinity(0, {int(c) for c in args.cpus.split(',')})
    src = Path(__file__).with_name('kernels.cpp').resolve()
    for source in (src, Path(__file__).resolve(), Path(__file__).with_name('requirements.txt')):
        shutil.copy2(source, out / source.name)
    native = out / 'native.so'
    wasm = out / 'kernels.wasm'
    flags = [args.clang, '-std=c++20', '-O2']
    subprocess.run(flags + (['-mavx2'] if args.simd else []) + ['-shared', '-fPIC', str(src), '-o', str(native)], check=True)
    command = flags + ['--target=wasm64', '-nostdlib', '-matomics', '-mbulk-memory',
                      '-Wl,--no-entry', '-Wl,--import-memory', '-Wl,--shared-memory',
                      '-Wl,--max-memory=8589934592']
    names = ['filter_sum', 'packed_sum', 'probe_aggregate']
    command += (['-msimd128'] if args.simd else [])
    command += ['-Wl,--export=' + name for name in names]
    subprocess.run(command + [str(src), '-o', str(wasm)], check=True)
    subprocess.run(['wasm2wat', '--enable-threads', '--enable-memory64', str(wasm), '-o', str(out / 'kernels.wat')], check=True)
    lib = C.CDLL(str(native))
    for name in names:
        fn = getattr(lib, name)
        fn.argtypes = [C.c_uint64] * 4
        fn.restype = C.c_int64
    lib.initialize.argtypes = [C.c_uint64] * 5
    lib.initialize.restype = None
    config = W.Config()
    config.wasm_memory64 = True
    config.wasm_threads = True
    config.shared_memory = True
    # Reserve the declared maximum; the default 4 GiB reservation is too
    # small for imported shared-memory64 loads above that boundary.
    config.memory_reservation = 8 << 30
    config.cranelift_opt_level = 'speed'
    engine = W.Engine(config)
    binary = wasm.read_bytes()
    compile_times, modules = measure(lambda: W.Module(engine, binary), args.repeats)
    module = modules[-1]
    n = args.rows
    # Start above 4 GiB so every benchmark proves real memory64 addressing.
    column = (1 << 32) + 65536
    packed = column + n * 8
    keys = (packed + (n * 17 + 7) // 8 + 15) & ~7
    auxiliary = keys + n * 24
    output = auxiliary + 2 * 65536 * 16 + (1 << 20) // 8
    pages = (output + 8 * 256 * 8 + 65535) // 65536
    memory = SharedMemory(engine, W.MemoryType(W.Limits(pages, 131072), is_64=True, shared=True))
    base = C.addressof(memory.data_ptr().contents)
    lib.initialize(base + column, base + packed, base + keys, n, base + auxiliary)
    linker = W.Linker(engine)
    owner = W.Store(engine)
    linker.define(owner, 'env', 'memory', memory)
    stores = [W.Store(engine) for _ in range(8)]
    instances = [linker.instantiate(store, module) for store in stores]
    functions = [{name: instance.exports(store)[name] for name in names}
                 for store, instance in zip(stores, instances)]
    records = []
    # Bounds failures must remain catchable, and a healthy call must still work.
    try:
        functions[0]['filter_sum'](stores[0], memory.data_len(), 1, 100, 0)
    except W.Trap:
        bounds_trap = True
    else:
        raise AssertionError('out-of-bounds load did not trap')
    filter_reference = None
    # An independently computed aggregate validates the hash/bloom result;
    # native-vs-WASM also compares all 256 groups for every worker.
    probe_count = min(n, 1024)
    fact = (C.c_int64 * (probe_count * 3)).from_address(base + keys)
    probe_expected = sum(fact[i * 3 + 2] for i in range(probe_count)
                         if fact[i * 3] <= 30000 and fact[i * 3 + 1] <= 30000
                         and fact[i * 3 + 1] % 3 == 0)
    for backend in ('native', 'wasm'):
        params = (base + keys, probe_count, base + auxiliary, base + output) if backend == 'native' else (keys, probe_count, auxiliary, output)
        value = lib.probe_aggregate(*params) if backend == 'native' else functions[0]['probe_aggregate'](stores[0], *params)
        if value != probe_expected:
            raise AssertionError('independent probe oracle disagrees')
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        for name in names:
            for workers in (1, 2, 8):
                chunks = [(n * w // workers, n * (w + 1) // workers) for w in range(workers)]
                # Packed chunk beginnings must be byte-aligned (8 rows).
                chunks = [(a // 8 * 8, b if w == workers - 1 else b // 8 * 8)
                          for w, (a, b) in enumerate(chunks)]
                def call(backend):
                    def work(w):
                        a, b = chunks[w]
                        p = (packed + a * 17 // 8 if name == 'packed_sum' else
                             keys + a * 24 if name == 'probe_aggregate' else column + a * 8)
                        aux = auxiliary if name == 'probe_aggregate' else 100
                        dest = output + w * 256 * 8
                        if backend == 'native':
                            return getattr(lib, name)(base + p, b - a,
                                                      base + aux if name == 'probe_aggregate' else aux,
                                                      base + dest)
                        return functions[w][name](stores[w], p, b - a, aux, dest)
                    return sum(pool.map(work, range(workers)))
                results = {}
                groups = {}
                # Alternate order with worker count; retain every sample.
                for backend in (('native', 'wasm') if workers != 2 else ('wasm', 'native')):
                    samples, answers = measure(lambda: call(backend), args.repeats)
                    results[backend] = {'samples_ms': samples, 'median_ms': statistics.median(samples), 'answers': answers}
                    if name == 'probe_aggregate':
                        groups[backend] = bytes((C.c_ubyte * (workers * 256 * 8)).from_address(base + output))
                expected = results['native']['answers'][0]
                if name == 'filter_sum':
                    if filter_reference is not None and expected != filter_reference:
                        raise AssertionError('parallel filter changed the answer')
                    filter_reference = expected
                if name == 'packed_sum' and expected != filter_reference:
                    raise AssertionError('packed decoder differs from unpacked input')
                if any(answer != expected for r in results.values() for answer in r['answers']):
                    raise AssertionError('kernel results differ')
                if groups and groups['native'] != groups['wasm']:
                    raise AssertionError('per-worker groups differ')
                record = {'kernel': name, 'workers': workers, 'verified': True, **results}
                records.append(record)
                print(json.dumps({k: v for k, v in record.items() if k not in ('native', 'wasm')} |
                                 {b: results[b]['median_ms'] for b in results}), flush=True)
    report = {'platform': platform.platform(), 'cpu_affinity': sorted(os.sched_getaffinity(0)),
              'wasmtime': importlib.metadata.version('wasmtime'), 'rows': n,
              'simd': args.simd, 'bounds_trap_verified': bounds_trap,
              'clang': subprocess.check_output([args.clang, '--version'], text=True).splitlines()[0],
              'source_sha256': hashlib.sha256(src.read_bytes()).hexdigest(),
              'wasm_sha256': hashlib.sha256(binary).hexdigest(),
              'compile_samples_ms': compile_times, 'compile_median_ms': statistics.median(compile_times),
              'memory': {'owner': 'Wasmtime', 'shared': True, 'memory64': True, 'input_offset': column,
                         'virtual_bytes': memory.data_len(), 'per_query_copy_bytes': 0,
                         'host_reserved_memory_proven': False, 'pax_integration_proven': False},
              'records': records}
    (out / 'results.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
