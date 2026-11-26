"""Minimal PostgreSQL v3 Extended Query + Binary Format client for benchmarks.

Uses Parse + Bind (result_formats=[1]) + Execute + Sync pipelining to avoid
text formatting and parsing overhead on large analytical result sets while
remaining 100% compliant with PostgreSQL v3 wire protocol.
"""
from __future__ import annotations

import datetime
import socket
import struct


def _pg_frame(tag: bytes, body: bytes) -> bytes:
    return tag + struct.pack("!I", 4 + len(body)) + body


_PG_DATE_EPOCH = datetime.date(2000, 1, 1)


def is_single_select_sql(sql: str) -> bool:
    """Return True if *sql* looks like a single SELECT/WITH/EXPLAIN query."""
    stripped = sql.strip()
    if not stripped:
        return False
    body = stripped[:-1].rstrip() if stripped.endswith(";") else stripped
    if ";" in body:
        return False
    head = body.split(None, 1)[0].upper()
    return head in ("SELECT", "WITH", "EXPLAIN")


class PgExtendedBinaryClient:
    """PostgreSQL v3 Extended Query + Binary Format (`format_code=1`) client."""

    def __init__(self, host: str, port: int, user: str, dbname: str,
                 timeout: float = 60.0):
        self.sock = socket.create_connection((host, int(port)), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        params = (
            b"user\x00" + user.encode("utf-8") +
            b"\x00database\x00" + dbname.encode("utf-8") + b"\x00\x00"
        )
        self.sock.sendall(struct.pack("!II", 8 + len(params), 196608) + params)
        self._buf = bytearray()
        self._pos = 0
        while True:
            tag, _, _ = self._next_msg()
            if tag == 90:  # 'Z' ReadyForQuery
                break
            if tag == 69:  # 'E' ErrorResponse
                raise RuntimeError("PostgreSQL startup failed")
        self._buf.clear()
        self._pos = 0

    def _fill(self, need: int):
        while len(self._buf) - self._pos < need:
            if self._pos > 0 and self._pos == len(self._buf):
                self._buf.clear()
                self._pos = 0
            chunk = self.sock.recv(524288)
            if not chunk:
                raise EOFError("PostgreSQL server closed connection")
            self._buf.extend(chunk)

    def _next_msg(self):
        self._fill(5)
        tag = self._buf[self._pos]
        length = struct.unpack_from("!I", self._buf, self._pos + 1)[0]
        total = 1 + length
        self._fill(total)
        start = self._pos + 5
        end = self._pos + total
        self._pos = end
        return tag, start, end

    def query(self, sql: str):
        # Pipeline Parse("") + Bind("", "", result_formats=[1]) + Execute("", 0) + Sync
        p_body = b"\x00" + sql.encode("utf-8") + b"\x00\x00\x00"
        b_body = b"\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01"
        e_body = b"\x00\x00\x00\x00\x00"
        self.sock.sendall(
            _pg_frame(b"P", p_body) +
            _pg_frame(b"B", b_body) +
            _pg_frame(b"E", e_body) +
            _pg_frame(b"S", b"")
        )
        col_oids = []
        rows = []
        fixed_unpacker = None
        fixed_stride = 0
        fixed_body_len = 0
        error_msg = None

        while True:
            # Fast-path batch unpack when consecutive fixed-width binary DataRows
            # are already buffered in self._buf.
            if fixed_unpacker is not None:
                buf = self._buf
                pos = self._pos
                avail = len(buf)
                if avail - pos >= fixed_stride and buf[pos] == 68:
                    cur = pos
                    exp_len = fixed_body_len + 4
                    while cur + fixed_stride <= avail and buf[cur] == 68:
                        if struct.unpack_from("!I", buf, cur + 1)[0] != exp_len:
                            break
                        cur += fixed_stride
                    if cur > pos:
                        rows.extend(fixed_unpacker(memoryview(buf)[pos:cur]))
                        self._pos = cur
                        continue

            tag, start, end = self._next_msg()
            if tag == 84:  # 'T' RowDescription
                buf = self._buf
                ncols = struct.unpack_from("!H", buf, start)[0]
                off = start + 2
                col_oids = []
                for _ in range(ncols):
                    off = buf.index(0, off, end) + 1
                    oid = struct.unpack_from("!I", buf, off + 6)[0]
                    off += 18
                    col_oids.append(oid)
                if col_oids and all(o in (20, 701) for o in col_oids):
                    fmt = "!7x" + "".join("4x" + ("q" if o == 20 else "d") for o in col_oids)
                    fixed_unpacker = struct.Struct(fmt).iter_unpack
                    fixed_body_len = 2 + 12 * len(col_oids)
                    fixed_stride = 5 + fixed_body_len
                else:
                    fixed_unpacker = None
            elif tag == 68:  # 'D' DataRow (generic binary / NULL / text)
                buf = self._buf
                ncols = struct.unpack_from("!H", buf, start)[0]
                off = start + 2
                vals = []
                for c in range(ncols):
                    clen = struct.unpack_from("!i", buf, off)[0]
                    off += 4
                    if clen == -1:
                        vals.append(None)
                    else:
                        oid = col_oids[c]
                        if oid == 20 and clen == 8:
                            vals.append(struct.unpack_from("!q", buf, off)[0])
                        elif oid == 701 and clen == 8:
                            vals.append(struct.unpack_from("!d", buf, off)[0])
                        elif oid == 1082 and clen == 4:
                            days = struct.unpack_from("!i", buf, off)[0]
                            vals.append(_PG_DATE_EPOCH + datetime.timedelta(days=days))
                        else:
                            vals.append(bytes(buf[off:off + clen]).decode("utf-8"))
                        off += clen
                rows.append(tuple(vals))
            elif tag == 69:  # 'E' ErrorResponse
                error_msg = bytes(self._buf[start:end]).decode("utf-8", errors="replace")
            elif tag == 90:  # 'Z' ReadyForQuery
                break

        if self._pos == len(self._buf):
            self._buf.clear()
            self._pos = 0
        if error_msg is not None:
            raise RuntimeError(error_msg)
        return rows

    def close(self):
        try:
            self.sock.sendall(_pg_frame(b"X", b""))
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass
