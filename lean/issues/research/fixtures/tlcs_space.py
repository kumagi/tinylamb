"""Read a benchmark TLCS v3 file and compare lossless payloads on fixed sample blocks."""
import argparse
import json
import re
from functools import lru_cache
import struct
import zlib
from collections import Counter, defaultdict
from pathlib import Path

HEADER = struct.Struct("<8sIIQQQIIQQII")
GROUP = struct.Struct("<QIIQIQQQQB")
# encoding, FOR width, flags, 3 integer + 2 double statistics, 3 codes,
# dictionary mask, (offset, size) pairs for validity, data and auxiliary.
COLUMN = struct.Struct("<BBBqqqddIII4Q6Q")
FRAME = struct.Struct("<BII")  # format tag, row count, encoded body bytes


def varint(n):
    out = bytearray()
    while n >= 128:
        out.append(128 | (n & 127))
        n >>= 7
    out.append(n)
    return out


def number(buf, pos):
    total = shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        total |= (byte & 127) << shift
        if byte < 128:
            return total, pos
        shift += 7


def pack_codes(codes, width):
    out = bytearray()
    acc = bits = 0
    for code in codes:
        acc |= code << bits
        bits += width
        while bits >= 8:
            out.append(acc & 255)
            acc >>= 8
            bits -= 8
    if bits:
        out.append(acc)
    return out


def front_body(words):
    dictionary = sorted(set(words))
    ids = {word: i for i, word in enumerate(dictionary)}
    body = varint(len(dictionary))
    prev = b""
    for word in dictionary:
        prefix = 0
        while prefix < min(len(prev), len(word)) and prev[prefix] == word[prefix]:
            prefix += 1
        body.extend(varint(prefix))
        body.extend(varint(len(word) - prefix))
        body.extend(word[prefix:])
        prev = word
    body.extend(pack_codes((ids[w] for w in words), (len(dictionary) - 1).bit_length()))
    return body


def decode(packet):
    tag, n, size = FRAME.unpack_from(packet)
    body = memoryview(packet)[FRAME.size:]
    assert len(body) == size
    words = []
    pos = 0
    if tag == 1:
        d, pos = number(body, pos)
        prev = b""
        for _ in range(d):
            prefix, pos = number(body, pos)
            length, pos = number(body, pos)
            assert prefix <= len(prev) and pos + length <= len(body)
            prev = prev[:prefix] + bytes(body[pos:pos + length])
            pos += length
            words.append(prev)
        width = (d - 1).bit_length()
        assert len(body) - pos == (n * width + 7) // 8
        acc = bits = 0
        rows = []
        for _ in range(n):
            while bits < width:
                acc |= body[pos] << bits
                pos += 1
                bits += 8
            rows.append(words[acc & ((1 << width) - 1)])
            acc >>= width
            bits -= width
        return rows
    assert tag == 2
    plain = zlib.decompress(body)
    for _ in range(n):
        length, pos = number(plain, pos)
        words.append(plain[pos:pos + length])
        pos += length
    assert pos == len(plain)
    return words


def compare(source, count, fields):
    _, ns, data_off, data_size, aux_off, aux_size = fields
    assert ns == 0 and aux_size == 4 * count
    source.seek(aux_off)
    ends = struct.unpack(f"<{count}I", source.read(aux_size))
    source.seek(data_off)
    data = source.read(data_size)
    assert len(data) == data_size and ends[-1] == data_size
    assert all(a <= b for a, b in zip(ends, ends[1:]))
    words = [data[a:b] for a, b in zip((0,) + ends, ends)]
    original = aux_size + data_size
    candidates = [original]
    front = front_body(words)
    lengths = b"".join(varint(len(w)) + w for w in words)
    for tag, body in [(1, front), (2, zlib.compress(lengths, 6))]:
        packet = FRAME.pack(tag, count, len(body)) + body
        assert decode(packet) == words
        candidates.append(len(packet))
    return original, len(front) + FRAME.size, candidates[2], min(candidates)


STAMP = rb"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[+]00"
STAMP_BLOCK = re.compile(b"(?:" + STAMP + b")*")
TIME_COLUMNS = {"EventTime", "ClientEventTime", "LocalEventTime"}


@lru_cache(maxsize=8)
def fixed_ends(count):
    return struct.pack(f"<{count}I", *range(22, 22 * count + 1, 22))


def matches_timestamp_block(src, count, fields):
    _, null_size, data_offset, data_size, aux_offset, aux_size = fields
    if null_size or data_size != 22 * count or aux_size != 4 * count:
        return False
    src.seek(aux_offset)
    if src.read(aux_size) != fixed_ends(count):
        return False
    src.seek(data_offset)
    data = src.read(data_size)
    return len(data) == data_size and STAMP_BLOCK.fullmatch(data) is not None


def timestamp_sample(src, count, data_offset):
    src.seek(data_offset)
    data = src.read(22 * count)
    def ordinals():
        for i in range(0, len(data), 22):
            w = data[i:i + 22]
            yield int(w[:4] + w[5:7] + w[8:10] + w[11:13] + w[14:16] + w[17:19])

    body = pack_codes(ordinals(), 47)
    packet = FRAME.pack(3, count, len(body)) + body
    tag, rows, size = FRAME.unpack_from(packet)
    assert tag == 3 and rows == count and len(packet) == FRAME.size + size
    for i in range(rows):
        pos, shift = divmod(47 * i, 8)
        start = FRAME.size + pos
        v = (int.from_bytes(packet[start:start + 7], "little") >> shift) % (1 << 47)
        d = b"%014d" % v
        w = d[:4] + b"-" + d[4:6] + b"-" + d[6:8] + b" " + d[8:10] + b":" + d[10:12] + b":" + d[12:] + b"+00"
        assert w == data[22 * i:22 * (i + 1)]
    return len(packet)


def inspect(file, names, targets, timestamp_census=False):
    with file.open("rb") as src:
        magic, version, _, _, _, rows, blocks, cols, offset, length, _, _ = HEADER.unpack(src.read(HEADER.size))
        assert (magic, version, cols) == (b"TLCS0001", 3, len(names))
        src.seek(offset)
        directory = src.read(length)
        pos = 2 * cols
        for _ in range(cols):
            kind, typ, _, _, _, n = struct.unpack_from("<5BI", directory, pos)
            pos += 9
            if typ == 2 and kind:
                for _ in range(n):
                    size, = struct.unpack_from("<I", directory, pos)
                    pos += 4 + size
            else:
                pos += 8 * n
        group_width, groups = struct.unpack_from("<II", directory, pos)
        pos += 8
        assert group_width == 128 and groups == (blocks + group_width - 1) // group_width
        by_name = defaultdict(Counter)
        by_kind = Counter()
        sample = defaultdict(Counter)
        temporal = defaultdict(Counter)
        n_blocks = n_rows = 0
        for g in range(groups):
            start, count, _, size, *_ = GROUP.unpack_from(directory, pos + g * GROUP.size)
            assert size == count * (5 + COLUMN.size * cols)
            src.seek(start + 8 * count)
            body = src.read(size)
            i = 0
            for block in range(count):
                row_count, = struct.unpack_from("<I", body, i)
                i += 5
                n_rows += row_count
                n_blocks += 1
                for name in names:
                    meta = COLUMN.unpack_from(body, i)
                    i += COLUMN.size
                    ns, ds, aux = meta[-5], meta[-3], meta[-1]
                    by_name[name].update(nulls=ns, data=ds, aux=aux)
                    by_kind[meta[0]] += ns + ds + aux
                    if timestamp_census and name in TIME_COLUMNS:
                        fits = meta[0] == 4 and matches_timestamp_block(src, row_count, meta[-6:])
                        temporal[name].update(blocks=1, matched_blocks=int(fits),
                                              matched_rows=row_count if fits else 0)
                        if fits:
                            predicted = FRAME.size + (47 * row_count + 7) // 8
                            temporal[name].update(model_frame_bytes=predicted)
                            if block == 0:
                                measured = timestamp_sample(src, row_count, meta[-4])
                                assert predicted == measured
                                temporal[name].update(sample_frame_bytes=measured,
                                                      roundtrip_sample_rows=row_count)
                    if block == 0 and name in targets and meta[0] == 4 and ns == 0:
                        raw, front, entropy, best = compare(src, row_count, meta[-6:])
                        sample[name].update(blocks=1, rows=row_count, raw=raw, front=front,
                                            deflate=entropy, best=best)
            assert i == len(body)
        assert (n_blocks, n_rows) == (blocks, rows)
        return dict(rows=rows, blocks=blocks, bytes=file.stat().st_size, payload=sum(by_kind.values()),
                    by_encoding=by_kind, top=sorted(by_name.items(), key=lambda x: -sum(x[1].values()))[:10],
                    sample=sample, timestamp_census=temporal)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tlcs", type=Path)
    parser.add_argument("metadata", type=Path, help="ClickBench dataset/metadata.json")
    parser.add_argument("--columns", nargs="*", default=["URL", "Referer", "Title"])
    parser.add_argument("--timestamp-census", action="store_true",
                        help="Check exact byte layout and every end offset in the three time columns")
    args = parser.parse_args()
    print(json.dumps(inspect(args.tlcs, json.loads(args.metadata.read_text())["columns"],
                             set(args.columns), args.timestamp_census), indent=2))
