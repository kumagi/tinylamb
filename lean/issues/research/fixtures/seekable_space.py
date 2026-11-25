"""Measure a temporary seekable container around an immutable TLCS (Python 3.14+).

Files passed as input are read-only. The temporary encoded file is written in
--scratch, verified from its on-disk directory, then unlinked on close.
"""
import argparse
from compression import zstd
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import struct
import tempfile
import zlib

MAGIC = b"TLZIP001"
HEADER = struct.Struct("<8sQQQ")  # raw length, chunk size, directory offset
ENTRY = struct.Struct("<QIIB")    # absolute offset, length, raw CRC32, codec


def measure(path, scratch, chunk, level, workers):
    if not 0 < chunk < 2**32 or workers < 1:
        raise ValueError("chunk must fit uint32 and workers must be positive")
    with path.open("rb") as source, tempfile.TemporaryFile(dir=scratch) as packet, ThreadPoolExecutor(workers) as pool:
        size = os.fstat(source.fileno()).st_size
        count = (size + chunk - 1) // chunk

        def encode(i):
            raw = os.pread(source.fileno(), chunk, i * chunk)
            compressed = zstd.compress(raw, level=level)
            tag = int(len(compressed) < len(raw))
            return tag, zlib.crc32(raw), compressed if tag else raw

        packet.write(bytes(HEADER.size))
        directory = bytearray()
        raw_chunks = 0
        for tag, crc, body in pool.map(encode, range(count), buffersize=workers * 2):
            directory.extend(ENTRY.pack(packet.tell(), len(body), crc, tag))
            packet.write(body)
            raw_chunks += int(tag == 0)
        directory_offset = packet.tell()
        packet.write(directory)
        packet.seek(0)
        packet.write(HEADER.pack(MAGIC, size, chunk, directory_offset))
        packet.flush()
        packed_size = os.fstat(packet.fileno()).st_size

        packet.seek(0)
        magic, original, stride, index = HEADER.unpack(packet.read(HEADER.size))
        assert (magic, original, stride) == (MAGIC, size, chunk)
        packet.seek(index)
        entries = list(ENTRY.iter_unpack(packet.read()))
        assert len(entries) == count and packed_size == index + count * ENTRY.size
        assert (entries[0][0] if entries else index) == HEADER.size

        def verify(i):
            offset, length, crc, tag = entries[i]
            end = entries[i+1][0] if i+1 < count else index
            assert offset + length == end and tag in (0, 1)
            encoded = os.pread(packet.fileno(), length, offset)
            decoded = zstd.decompress(encoded) if tag else encoded
            assert len(decoded) == min(stride, original - i*stride) and zlib.crc32(decoded) == crc
            if decoded != os.pread(source.fileno(), stride, i*stride):
                raise ValueError(f"round-trip failed at chunk {i}")
        for _ in pool.map(verify, range(count), buffersize=workers * 2):
            pass
        return dict(original=size, packed=packed_size, header_and_index=HEADER.size + count*ENTRY.size,
                    chunk=chunk, level=level, chunks=count, raw_chunks=raw_chunks,
                    verified_bytes=size, zstd=zstd.zstd_version)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--chunk", type=int, default=1 << 20)
    parser.add_argument("--level", type=int, default=6)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    print(json.dumps(measure(args.input, args.scratch, args.chunk, args.level, args.workers)))
