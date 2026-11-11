/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_PAX_CHUNK_HPP
#define TINYLAMB_PAX_CHUNK_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "table/sorted_run/sorted_run.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Row-group-major layout (1024 rows per row group). Each column is stored as
// a contiguous array of fixed-width or length-prefixed varchar values within
// the row group. An inline footer exposes column offsets, row-group count,
// and schema_version so that a WASM module (or native iterator) can traverse
// the chunk without copying. This is the WASM pushdown surface: the chunk is
// self-describing and random-accessible from WebAssembly.

struct Predicate;
struct AggTerm;
struct AggPlan;
struct AggResult;
class WorkloadTracker;

// One posting of the inverted secondary index: all row ids in a row group
// holding `value` (sorted ascending). Materialized from whichever container
// (array / bitmap / run) the builder selected for size.
struct InvertedPosting {
  int64_t value = 0;
  std::vector<uint16_t> rows;
};

// All postings of one indexed (row-group, column) cell, sorted by value.
struct InvertedCell {
  uint32_t group = 0;
  slot_t column = 0;
  std::vector<InvertedPosting> postings;
};

// One Xor8 pre-filter cell: probabilistic membership over the distinct raw
// values of a high-cardinality (row-group, int64 column) cell that the exact
// inverted index refuses. `table` holds m fingerprint bytes.
struct XorCell {
  uint32_t group = 0;
  slot_t column = 0;
  uint32_t seed = 0;
  std::vector<uint8_t> table;
};

// One PGM learned-index segment over the (ascending) int64 key column:
// rows with keys in [key, next_key) sit at pos + slope * (k - key),
// within eps rows. Positions are dense global row indexes.
struct PgmSegment {
  int64_t key = 0;
  uint64_t pos = 0;
  double slope = 0.0;
};

struct FsstDecodeMeta {
  uint32_t symbol_index = 0;
  uint32_t offset_table = 0;
  uint32_t stream = 0;
  uint32_t stream_len = 0;
  uint16_t symbol_count = 0;
  uint16_t row_count = 0;
  bool valid = false;
};

struct FsstDecodeView {
  const FsstDecodeMeta* meta = nullptr;
  const uint32_t* symbol_offsets = nullptr;
  const uint8_t* symbol_lengths = nullptr;
};

class PaxChunk final {
 public:
  static constexpr size_t kRowGroupSize = 1024;

  // Lightweight per-(row-group, column) encodings (footer v4). Every region
  // is lossless; the builder tries applicable candidates and keeps the
  // smallest encoding, so PLAIN remains the fallback.
  enum class Scheme : uint8_t {
    kPlain = 0,
    kConst = 1,    // single value for the whole region
    kRle = 2,      // [runs u32][{value 8B}{len u32}]x      param=runs
    kForBp = 3,    // [base 8B][bpv-byte LE offsets]      param=bpv(1..8)
                   // (scheme ID reuses the historical "FOR-BP" name; the
                   // layout is byte-aligned FOR-BA, not bit-packed)
    kDict = 4,     // int: [n u16][codes w bits][dict 8B*n]
                   // str: [n u16][codes w bits][len-prefixed strings]
                   //                                             param=n
    kDeltaFp = 5,  // sorted ints: [first 8B][checkpoints][deltas w bits]
                   //                                      param=w
    kBitmap = 6,  // two-valued: [true 8B][false 8B][bitmap ceil(n/8) MSB-first]
                  //                                      param=unused(0)
    kXorFor = 7,  // doubles: [first f64 bits 8B][bitpacked xors w]
                  //                                      param=w
    kBitWeave = 8,   // ints: [base 8B][w u8][bitpacked v-base, w bits] (true
                     // column bit-packing; bpv need not be a whole byte)
                     //                                      param=w
    kEliasFano = 9,  // monotonic ints: [base 8B][maxd 8B][l u8]
                     // [high bitmap (n+maxd>>l) bits][low bits (n*l bits)]
                     //                                      param=l
    kWavelet = 10,   // ints: wavelet tree over (val-min), W levels of bitmaps;
                     // rank is recomputed by popcount at decode time (no
                     // stored prefix arrays);          param=n
    kDeltaFor =
        11,  // composite: delta-encode then FOR-BA the deltas.
             // [ncp u16][checkpoints ncp*BE64][FOR-BA(deltas)]  param=bpv
             // checkpoints[b]=value[min(b*C,n-1)], C=128; random-access via
             // value[i]=checkpoints[i/C]+sum(deltas[i/C*C+1..i]).
    kDeltaBitweave = 12,  // composite: delta-encode then BitWeave the deltas.
                          // [ncp u16][checkpoints ncp*BE64][BITWEAVE(deltas)]
                          // param=w
    kByteSlice =
        13,         // bit-sliced int64: [w u8][slice_0..slice_{w-1}], each
                    // slice ceil(n/8) bytes LSB-first. Enables SUM via
                    // popcount over slices (no value materialization). param=w
    kGorilla = 14,  // doubles (bitcast u64): ClickHouse "Gorilla" float codec.
                    // [first u64 BE][bitpacked XOR-stream]. Each subsequent
                    // value stores XOR-with-previous with leading/trailing-zero
                    // elision. param unused (n known from region).
    kFsst = 15,     // varchar: DuckDB's FSST (Fast Static Symbol Table).
                    // [nsym u16][symbols][n u32 offsets][code stream]; code 0
                    // = 1 literal byte, codes 1..nsym = symbol strings.
                    // param=nsym.
    kPfor = 16,     // ints: patched FOR. [base 8B BE][w u8][nexc u16 BE]
                    // [packed low-w offsets: n*w bits MSB-first]
                    // [exc_pos u16 BE x nexc][exc_val 8B BE x nexc].
                    // Packed part holds (v-base) & ((1<<w)-1); exception
                    // positions overwrite with full values. param=w.
    kDdRice = 17,   // ints: delta-of-delta + Rice (timestamps). [ncp u16]
                    // [checkpoints ncp x (val 8B + d1 8B)][bitoffs ncp x u32]
                    // [k u8][Rice bitstream of zigzag dd, dd[1] = d1[1]].
                    // Unary quotients are encoder-capped at 65535 (decoder
                    // enforces the same bound, so hostile streams terminate).
                    // param=k.
    kChimp = 18,    // doubles (bitcast u64): Chimp float codec. [first u64]
                    // [bitstream]: '0' identical, '10' reuses previous
                    // (lz,tz) window for the XOR center bits, '11' + 5-bit
                    // lz + 6-bit tz + center bits. param unused (0).
    kDeltaVarint =
        19,     // ints: checkpointed LEB128 deltas (byte-stream varint family).
                // [ncp u16][checkpoints ncp*8B][byteoffs ncp*u32 BE][LEB128
                // zigzag deltas, deltas[0]=0]. Random access via checkpoints;
                // at most 10 bytes per delta (decoder-enforced). param=0.
    kAlp = 20,  // doubles: ALP (Adaptive Lossless floating-Point, SIGMOD'24).
                // Exact decimal factorization v == (double)m / 10^e with m an
                // int64; the mantissa vector is compressed by a composed
                // integer backend. Layout:
                // [exp u8][backend u8][nexc u16]
                // [exc (pos u16 + val 8B) x nexc][inner region]
                // backend = inner scheme ID (1 CONST / 2 RLE / 4 DICT /
                // 8 BitWeave); the inner region is that scheme's ordinary
                // self-contained region. param unused (inner headers rule).
                // Values that do not factor exactly (incl. -0.0 / NaN /
                // huge magnitudes) become PFOR-style exceptions carrying
                // full 8B values.
    kFrontCode = 21,  // varchar: front coding (incremental/prefix encoding;
                      // Parquet DELTA_BYTE_ARRAY, LevelDB restart points).
                      // [nrestart u16 BE][stream_len u32 BE]
                      // [restart u32 BE x nrestart][entry stream]
                      // Each entry is [plen u16 BE][slen u16 BE][suffix],
                      // value_i = prev_value[0..plen) + suffix; every
                      // restart_every-th entry restarts the chain (plen=0,
                      // full string) and its stream offset sits in the
                      // restart table. param = restart_every, bit 15 set
                      // when the sequence is non-decreasing -- the flag
                      // lets the predicate kernels answer EQ/order counts
                      // by compressed-domain binary search without
                      // materializing any row.
    kOpDict = 22,     // order-preserving dictionary: byte-identical layout to
                      // kDict ([n u16][codes w bits][entries]) with the
                      // dictionary stored ASCENDING (int64 numeric order;
                      // varchar unsigned-byte lexicographic order) and each
                      // code = the rank of its value in that order. Value order
                      // therefore equals code order, so equality AND range
                      // predicates evaluate directly on the packed code stream
                      // as a code window [lo, hi) -- no per-query dictionary
                      // evaluation, pass table, or decode. param=n.
    kRleDict = 23,    // int64: run-length over dictionary codes (Parquet
                      // RLE_DICTIONARY hybrid). [d u16 BE][dict d x 8B BE]
                      // [runs u32 BE][{code ceil(cw/8) B BE}{len u32 BE}] x
                      // runs, cw = BitsNeeded(d-1). The predicate is evaluated
                      // ONCE per dictionary entry into a pass table, then
                      // folds per RUN: count = sum(pass[c]*len), SUM =
                      // sum(dict[c]*pass[c]*len) -- no per-row decode, and
                      // 5-6B per run instead of kRle's 12B. param=runs.
    kFreq = 24,       // frequency encoding (DB2 BLU / BtrBlocks "FREQUENCY"):
                      // one dominant value plus exceptions.
                      // [top i64 BE][inner_scheme u8][inner_param u32 BE]
                      // [nexc u16 BE][bitmap ceil(n/8) B MSB-first,
                      //   bit i = 1 iff row i == top]
                      // [inner region: the nexc exception values in row
                      //   order, any leaf scheme]
                      // Equality COUNT against `top` is a popcount; the
                      // exception substream is itself a self-contained
                      // region, so decode cost scales with nexc not n.
                      // param = nexc.
    kRleX = 25,       // cascaded RLE (BtrBlocks recursive compression): the
                      // value list and run-length list are each stored as a
                      // self-contained inner int64 region instead of raw
                      // pairs, so e.g. values -> DICT and counts -> FOR-BA.
                      // [vscheme u8][cscheme u8][runs u32 BE]
                      // [vparam u32 BE][cparam u32 BE][vlen u32 BE]
                      // [values region: runs entries][counts region: runs
                      //   entries]
                      // Inner schemes are restricted to the leaf whitelist
                      // (PLAIN/CONST/RLE/FORBA/DICT/BITWEAVE): depth-1
                      // cascade, no recursion. param = runs.
    kDictX = 26,      // cascaded dictionary (BtrBlocks Dict+FSST family):
                      // the code stream is a self-contained inner int64
                      // region and the dictionary itself is pluggable.
                      // [d u16 BE][code_scheme u8][code_param u32 BE]
                      // [clen u32 BE][codes region: n entries][pool_scheme
                      //   u8][dictionary]
                      // int64: pool_scheme=0, dictionary = d x 8B BE.
                      // varchar: pool_scheme=0 -> d x [len u32][bytes]
                      // (same entry list as kDict); pool_scheme=15 -> an
                      // FSST region whose `d` "rows" decode to the
                      // dictionary entries.
                      // Codes >= d decode as 0/"" exactly like kDict.
                      // param = d.
    kArith = 27,      // exact arithmetic-progression model (LECO-family
                      // linear model with exact-model enforcement): the row
                      // values follow v_i = a + i*d (two's-complement wrap
                      // arithmetic) except for a u16-capped exception list.
                      // [a i64 BE][d i64 BE][inner_scheme u8][inner_param
                      //   u32 BE][nexc u16 BE]
                      // [bitmap ceil(n/8) B MSB-first, bit i = 1 iff row i
                      //   is ON the line (v_i == a + i*d)]
                      // [inner region: the nexc exception values in row
                      //   order, any leaf scheme; omitted when nexc == 0]
                      // On-line rows answer predicates as a closed-form
                      // INDEX WINDOW (monotone in i), EQ as a single
                      // candidate index, and SUM as n*a + d*n(n-1)/2 minus
                      // exception patches -- no per-row decode. d == 0 is
                      // rejected (kFreq/kConst territory, and kArith would
                      // be 8B larger for the same bytes). param = nexc.
    kSparse = 28,     // sparse null-majority encoding: only the non-null
                      // rows are stored, as a position list plus a dense
                      // inner-encoded value list; unlisted rows decode as
                      // raw 0 exactly like the null-as-0 convention.
                      // [vscheme u8][vparam u32 BE][vlen u32 BE]
                      // [pos_bpv u8][nnz u32 BE]
                      // [positions region: FOR-BA layout, nnz entries
                      //   (base i64 BE + nnz x pos_bpv LE offsets),
                      //   strictly increasing row indices]
                      // [values region: vlen bytes, a leaf-scheme inner
                      //   region of nnz entries in position order]
                      // Row i is non-null iff i is in the position list;
                      // its value is values[rank(i)]. Point access is a
                      // binary search (O(log nnz)), batch decode and SUM
                      // are O(nnz). param = nnz.
    kAnsFor = 29,     // entropy-coded FOR offsets: rANS (byte-oriented
                      // range ANS) over the little-endian byte stream of
                      // the FOR-BA offset array. Byte symbols are priced
                      // at their marginal frequency, so skewed offset
                      // distributions compress below the fixed bpv bound
                      // and a constant high byte costs ~0 bits. One
                      // checkpoint per 64 rows bounds random access to
                      // O(64*bpv) symbol decodes.
                      // [base i64 BE][bpv u8][prec u8][nsym u16 BE]
                      // [nsym x (sym u8, freq u16 BE)]  (sum == 1<<prec)
                      // [ncp u16 BE][ncp x (state u32 BE, pos u32 BE)]
                      // [slen u32 BE][rANS byte stream, read backwards]
                      // param = bpv; ncp = ceil(n/64); ckpt[k] is the
                      // decoder state + remaining-byte cursor immediately
                      // before symbol 64k*bpv (row-major LE offset bytes).
    kOpRle = 30,      // int64: kRleDict's byte layout with a SORTED
                      // (rank-ordered) dictionary -- the order-preserving
                      // run/dictionary hybrid. Codes order like values, so
                      // range predicates fold to a code window and the run
                      // stream still compresses sorted/low-cardinality
                      // columns; predicate and SUM paths are O(runs)
                      // without a pass table.
                      // [d u16][dict d x 8B BE, ascending][runs u32]
                      // [{code cb B BE}{len u32}] x runs; param = runs.
    kXorForC = 31,    // doubles (bitcast u64): checkpointed XOR-FOR.
                      // [ncp u16][ckpts ncp x u64 BE][w u8]
                      // [xors (n-1)*w bits MSB-first]; ckpts[b] = v[b*128],
                      // ncp = ceil(n/128). Random access jumps to the block
                      // checkpoint then XORs <= 127 deltas (vs the scheme-7
                      // from-row-0 walk). param = w (1..64).
    kGorillaC = 32,   // doubles: checkpointed Gorilla.
                      // [ncp u16][ckpts ncp x u64 BE][bitoffs ncp x u32 BE]
                      // [first u64 BE][Gorilla bitstream]. ckpts[b] =
                      // v[b*128]; bitoffs[b] = stream bit position of row
                      // b*128+1's code (bitoffs[0] = 0). param = 0.
    kChimpC = 33,     // doubles: checkpointed Chimp; same envelope as
                      // kGorillaC. The encoder emits a full '11' code for
                      // every row at a block-start boundary + 1, because
                      // the decoder resumes without the previous row's
                      // (lz, tz) window. param = 0.
    kPBitS = 34,
    kInterp = 35,  // int64: binary interpolative coding (Moffat & Stuiver
                   // 2000; the posting-list codec of Lucene/PISA-style
                   // engines) over NON-DECREASING sequences. Each
                   // 128-row checkpoint block is a middle-first
                   // recursion: the middle element is stored as its
                   // offset within the bounds [lo, hi] closed by its
                   // neighbors, then the left half (bounds [lo, mid])
                   // and the right half (bounds [mid, hi]) recurse.
                   // Code widths shrink toward the leaves, so clustered
                   // gaps cost ~log(cluster span) bits while one huge
                   // gap is paid a few times at the top -- Elias-Fano /
                   // DELTA-FP pad EVERY row to the max-gap width
                   // instead.
                   // [ncp u16][ckpts ncp x (first u64, last u64)]
                   // [bitoffs ncp x u32 BE][BIC bitstream MSB-first]
                   // ckpt[b] brackets block b with the row values
                   // v[b*128] and v[min(n,(b+1)*128)-1]; bitoffs[b] is
                   // the bit position of block b's stream (bitoffs[0] =
                   // 0); param = ncp. The checkpoint brackets double as
                   // an in-region zone map: a range predicate skips
                   // blocks whose bracket misses the window and answers
                   // fully-inside blocks from the header alone, so only
                   // boundary blocks walk their bitstream.
    kRleBp = 36,
    kDeltaPfor = 37,
    kWaveletR = 38,
    kQpp = 39,
    kBp128 = 40,
    kRle8 = 41,
    kRleDelta = 42,
    kChimp128 = 43,
    kAlpRdTans = 44,
    kPatas = 45,
    kWah = 46,  // two-valued int64: Word-Aligned Hybrid run-compressed bitmap
                // (Bell/Moffat/Nevill-Manning; the FastBit/SciDB family).
                // [true 8B BE][false 8B BE][words prm x u64 BE]; param = word
                // count. Word bit63 == 0 -> literal: bits 62..0 are row bits
                // MSB-first (bit62 = oldest row of the 63-row span). bit63 ==
                // 1 -> fill: bit62 = fill bit, bits 61..0 = L >= 1 covering
                // 63*L rows. A trailing partial span is always a literal
                // zero-padded; words must cover exactly n rows. Predicates
                // build the selection mask by copying literal words verbatim
                // (no value decode) and COUNT/SUM fold as popcounts over the
                // same words.
    kPla = 47,  // int64: piecewise-linear exact model -- variable-length
                // segmentation, one arithmetic line per segment (the
                // multi-segment generalization of kArith's single global
                // line; the LECO piecewise family). Every ON-LINE row of
                // segment j satisfies v_i = first_j + (i - start_j) * d_j
                // exactly (two's-complement wrap); rows off every line fall
                // to a u16-capped exception list (kArith's bitmap + inner
                // region, reused verbatim). Segmentation = greedy maximal
                // equal-step runs, runs shorter than 3 rows absorbed into
                // the preceding segment as exceptions, adjacent collinear
                // segments merged -- without the merge a noisy ramp splits
                // at every outlier and nseg explodes. d == 0 segments are
                // allowed (plateaus); nseg == 1 is rejected (kArith's
                // niche), so the two never race on the same input.
                // [nseg u16 BE][nseg x (first i64 BE, d i64 BE, end u32 BE)]
                // [ischeme u8][iparam u32 BE][nexc u16 BE]
                // [bitmap ceil(n/8) B MSB-first, bit i = 1 iff row i is
                //   on-line]
                // [inner region: nexc exception values in row order;
                //   omitted at nexc == 0]
                // param = nseg; ends are exclusive, strictly increasing and
                // tile [0, n] (end covers exception rows too -- the bitmap,
                // not the segment table, decides on-line).
                // Per-segment closed forms: a range predicate folds to ONE
                // index window per segment plus <= u16 exception patches
                // (O(nseg + nexc), no per-row evaluation), unpredicated SUM
                // folds as sum_j len*first + d*len*(len-1)/2 minus exception
                // patches, and random access is a binary search over `end`
                // + one FMA.
    kSimple8b = 48,  // ints: simple-8b word-adaptive bit packing (Anh &
                     // Moffat 2005, Information Retrieval 8(1):151-166; the
                     // Lucene posting-list codec in its 64-bit-word form).
                     // [base 8B BE][words param x u64 BE]; each word carries
                     // a 4-bit selector (BE bits 63..60) + a 60-bit payload
                     // of `base` offsets, first value in the high payload
                     // bits. Selector table (bits x count): 0x240, 1x120,
                     // 2x60, 3x30, 4x20, 5x15, 6x12, 7x10, 7x8, 8x7, 10x6,
                     // 12x5, 15x4, 20x3, 30x2, 60x1 -- so the word width
                     // adapts every <= 240 rows instead of every region
                     // (BitWeave) or every 128-row block (BP128), and a
                     // 60-bit offset cap replaces PFOR's per-exception
                     // (pos+8B) patches. Offsets must fit 60 bits (range
                     // gate). The selector implies the per-word max offset,
                     // so predicate folds skip words whose value range
                     // cannot match without decoding, and zero-width words
                     // (sel 0/1 = 240/120 copies of `base`) fold as O(1)
                     // range sets -- long equal-value runs cost 0.27 bits
                     // per row. param = word count.
    kRleIdx = 49,    // ints: MONOTONE run-length with a 16-run restart index.
                     // The run family (kRle/kRleBp/kRle8/kRleDict/kRleX) is
                     // value-agnostic and locates a row only by walking every
                     // run; this scheme gates on NON-DECREASING input, which
                     // sorts the run values, and adds a restart table, so a
                     // query becomes a bisection plus a <= 15-run walk:
                     // [runs u32 BE][nidx u32 BE]
                     // [values:  FOR-BA region over the run values, `runs`
                     //           entries: base 8B BE, bpv u8, then `runs`
                     //           little-endian bpv-byte offsets]
                     // [lengths: FOR-BA region over the run lengths, `runs`
                     //           entries: the same layout]
                     // [restart: nidx x u32 BE]  nidx == ceil(runs/16) and
                     //           entry e is the row index of run 16*e, so
                     //           row -> run is a binary search over the table
                     //           plus a bounded walk, and row_before(run) is
                     //           O(1). So RegionLowerBound is O(log runs)
                     //           rather than the O(runs) walk of every other
                     //           run scheme, and a value window maps to ONE
                     //           run window by bisecting the sorted run
                     //           values: range COUNT is the index difference
                     //           row_before(hi_run) - row_before(lo_run) and
                     //           range SUM folds sum(v*len) over that window
                     //           alone (the TPC-H "sorted date column, SUM
                     //           over a date range" shape, no row
                     //           materialized). Costs 8 + 4*ceil(runs/16)
                     //           bytes over the equivalent kRleBp layout,
                     //           which the cost model prices against the
                     //           run-walk decode tier. param = runs.
    kStreamVByte = 50,  // ints: byte-aligned variable-byte ("group varint",
                        // StreamVByte) over 1..4-byte values -- the engine's
                        // first BYTE-ADDRESSABLE int64 codec. Every other
                        // 2-4-byte int codec (kForBp, kBitWeave, kEliasFano,
                        // kSimple8b, kRle8, kPfor) bit-packs, so one value
                        // straddles byte boundaries and every reader must
                        // reproduce the engine's exact bit order; here each
                        // value is whole little-endian bytes and a value read
                        // is a plain unaligned load plus a zigzag, which is
                        // the shape the WASM pushdown surface and any
                        // byte-oriented consumer wants.
                        // [stream_len u32 BE][nblk u16 BE]
                        // [block_off: nblk x u16 BE]
                        // [ctrl: ceil(n/4) bytes][payload: stream_len bytes]
                        // ctrl byte c carries the 2-bit width selectors of
                        // values 4c..4c+3, MSB-first (selector s => (s+1)
                        // bytes); the payload is the values back to back,
                        // each its width little-endian. A value is
                        // zigzag(value) truncated to its width, so a column
                        // with any sign works and the encoder bails when some
                        // value falls outside the int32 range (the exact
                        // zigzag fit, INT32_MIN included: zigzag(INT32_MIN) is
                        // 2^32-1) -- past which a fixed-width 8B codec is no
                        // longer competitive anyway.
                        // block_off[b] is the payload offset of value
                        // b*16, nblk == ceil(n/16): the control plane alone
                        // cannot place a value (its offset is the prefix sum of
                        // ALL preceding widths, so reading it is O(n)), and
                        // this table is what turns a probe into one lookup plus
                        // a <= 15-width walk. That is the difference from
                        // kSimple8b, the nearest rival in size, whose probe
                        // replays up to 240 rows of words from the region head;
                        // it costs 2 bytes per 16 values. Stream len is also
                        // the exact cell extent (the width sum is a function of
                        // n alone), so ValidateWalk pins it -- and every
                        // block_off -- against the walked total and the WASM
                        // parse_footer derives the same number. It wins the
                        // size race against kForBp / kBitWeave exactly when the
                        // widths are MIXED, since those pad every row to the
                        // maximum width while this pays the sum of the actual
                        // widths; on uniform widths kForBp is cheaper and wins.
                        // param = stream_len (payload bytes).
    kHuffDict = 51,  // int64: canonical-Huffman entropy-coded ORDER-PRESERVING
                     // dictionary with an exact inline frequency table. The
                     // dictionary is kOpDict's (ascending, code = rank), but
                     // the fixed-width rank stream is replaced by a canonical
                     // MSB-first Huffman bitstream, so MULTI-MODAL skewed
                     // categoricals (a few hot values plus a long tail -- the
                     // shape kFreq's single-dominant model and kOpDict's fixed
                     // rank width both miss) pay the symbol ENTROPY instead of
                     // log2(d) bits per row. The stored per-symbol frequency
                     // table turns the region into an exact histogram, which
                     // is the compressed-domain headline: ANY CompareOp
                     // predicate answers COUNT and SUM in closed form over the
                     // d symbols -- sum(freq[s]*pass[s]) /
                     // sum(dict[s]*freq[s]*pass[s]) -- with ZERO bitstream
                     // reads and zero row decode, regardless of row order (no
                     // run structure like kRleDict, no progression like
                     // kArith/kPla); EQ COUNT is one binary search plus one
                     // table read, and the ascending dictionary makes MIN/MAX
                     // the first/last entries. Decode is a canonical bit-walk
                     // (complete tree: every bit pattern decodes to a valid
                     // symbol); random access jumps to a 64-row checkpoint and
                     // replays <= 64 symbols.
                     // [d u16][dict d x 8B BE, ascending]
                     // [lens ceil(d/2) B, nibbles: byte j = len(2j)<<4 |
                     //   len(2j+1), each length in [1,15]]
                     // [freq d x u16 BE, exact row counts, sum(freq) == n]
                     // [ncp u16][ckpt ncp x u32 BE bit offsets of rows 64k,
                     //   ckpt[0] == 0, non-decreasing]
                     // [stream_len u32][stream: canonical MSB-first codes in
                     //   row order]
                     // param = d (echoed in the header, DICT-style). n <= 1024
                     // bounds the natural Huffman depth at 13 (Fibonacci
                     // worst case), so the 15-bit length cap is validation
                     // headroom, not an encoder repair path; the validator
                     // checks Kraft sum == 2^15 exactly AND fully decodes the
                     // stream once, histogramming it against `freq`, because
                     // the closed-form kernels trust the table. Unhandled by
                     // the WASM module (gated with the other post-15 schemes
                     // behind TINYLAMB_NO_NEW_SCHEMES for stale readers).
    // kSimdFor (scheme 53): int64 FRAME-OF-REFERENCE bit-packing in a
    // four-lane INTERLEAVED layout, so one 8-byte load yields four values.
    // The layout is the "Umbra" one: a group is exactly 32 bytes, split into
    // four 8-byte lanes; value i of the group sits in lane (i mod 4) at bit
    // offset ((i div 4) * w) counting from that lane's LSB. A lane holds
    // floor(64/w) values, so ANY w in 1..64 is representable and each of the
    // four lane reads is an independent ALIGNED 8-byte load. When w does not
    // divide 64 the lane's high bits go unused -- a few bits per group, which
    // is the price of keeping the four loads aligned.
    //
    // It is a LEAF (no inner region) and order-preserving (plain FOR over the
    // raw values), so RegionLowerBound may bisect instead of scanning.
    // [base 8B BE][group: 4 lanes x 8B BE, 4*(64/w) values]
    // param = w (1..64). Group count is ceil(n / (4*(64/w))), so the region is
    // exactly 8 + 32*groups bytes and ValidateWalk pins it to that.
    //
    // Why it exists, and -- stated plainly, because the adaptive selector
    // compares real encoded sizes and will simply pick something else when
    // this loses -- exactly where it wins and where it does NOT. Measured over
    // 2048 values (bytes, kSimdFor / kForBp / kBitWeave):
    //
    //   spread        1     7    31   255   1023  65535  2^20-1  2^32-1
    //   kSimdFor    264   808  1384  2056   2760   4104   5480   8200
    //   kForBp     2056  2056  2056  2056   4104   4104   6152   8200
    //   kBitWeave   265   777  1289  2057   2569   4105   5129   6921
    //
    //   * vs kForBp: kForBp spends one BYTE per value on its offset, so its
    //     payload is n bytes whatever the width. kSimdFor has no offset table
    //     and never loses to it -- a decisive win below 8 bits, a 1-byte edge
    //     at the widths that divide 64, and a tie at 8/16/32.
    //   * vs kBitWeave: kBitWeave packs continuously at w bits/value, while
    //     this scheme rounds every group to a whole 32 bytes AND leaves
    //     64 mod w bits of every lane unused. That costs up to ~1.8 bits per
    //     value at an awkward width (w = 11), so kBitWeave is usually the
    //     smaller of the two and the selector will normally pick it. The two
    //     are level only where w divides 64.
    //   * WIDTH. kForBp caps at 8 bits because its offset is a byte, so a
    //     column needing 9..64 bits has no frame-of-reference option at all;
    //     this is the only FOR-layout codec that covers them.
    //   * DECODE. Four values share one aligned 8-byte lane load and one
    //     shift, so the layout is shaped for a 4-lane SIMD gather.
    //
    // So the honest summary: this is a DECODE-SHAPED frame-of-reference codec
    // that beats the byte-offset FOR codec outright and is the only one of its
    // family above 8 bits, but it is not the size winner against kBitWeave,
    // and the cost model says so rather than pretending otherwise.
    kSimdFor = 53,
    kD2Pfor = 52,  // int64: signed DELTA-OF-DELTA + PFOR, the empty cell of
                   // the derivative x inner-codec table. kDeltaFor (11) is
                   // delta + FOR-BA, kDeltaBitweave (12) is delta +
                   // BitWeave, kDeltaPfor (37) is delta + PFor, and
                   // kDdRice (17) is delta-of-delta + Rice; nothing paired
                   // the SECOND difference with a bit-packing inner codec.
                   //
                   // The win condition is narrow and measured, not assumed:
                   // dd must be much NARROWER than sd. A large-magnitude
                   // column with a large step and small per-row variation --
                   // `base + i*997 + jitter`, the shape a measured counter or
                   // an accumulating ledger has -- is the case: sd spans a
                   // wide band, so every first-difference scheme pays a wide
    // FOR-BA/BitWeave byte or a PFor patch list, while dd stays
    // at a handful of bits. On a 1024-row column of exactly
    // that shape this scheme is 0.59 B/row against 0.64
    // (kDeltaPfor), 0.65 (kDdRice), 1.33 (kDeltaBitweave), 2.08
    // (kDeltaFor) and 4.38 (kStreamVByte), and the adaptive
    // selector picks it. It is NOT a universal dd win and the
    // cost model does not pretend otherwise: on a clean
    // quadratic kQpp is better (0.205 vs 0.216 -- QPP's niche
    // is exactly that shape), and when sd is already narrow
    // (a jittered line, where the jitter is on the VALUE not
    // the step) kDeltaBitweave and kRleDelta win and this
    // scheme is merely competitive. The size comparison
    // decides in every case; nothing here is special-cased.
    // [ncp u16 BE]
    // [ckpt: ncp x (val 8B BE, sdp 8B BE)]
    // [inner: a complete kPfor region over the n-2 second
    //   differences]
    // ncp == ceil(n/256)+1 and checkpoint b sits on row
    // min(b*256, n-1). The 256, not the 128 every
    // first-difference composite shares, is this scheme's own
    // choice: the 16 B checkpoint pair is a far bigger share of
    // a narrow dd region than of a wide sd one, so halving the
    // entry count (9 x 16 = 144 B -> 5 x 16 = 80 B over 1024
    // rows) buys more than the shorter probe would. ValidateWalk
    // pins the count to that formula so a hand-built footer
    // cannot under-count the seek table. `val` is v[row] and
    // `sdp` is the first difference that PRECEDES it
    // (sd[row-1]); row 0 stores sd[0] instead, since it has no
    // predecessor. A probe is therefore one checkpoint load
    // plus at most 256 inner reads, and the forward decode is
    // two prefix sums.
    // Unlike kDeltaPfor this carries NO per-block brackets:
    // the second-difference stream is what the size win rests
    // on, and adding min/max/sum blocks back would put the
    // 20 B/row-index overhead of the first-difference schemes
    // onto the very rows it is meant to shrink. param = the
    // inner PFor width w (0..64), like kPfor's own param.
    kDeltaSimd = 54,  // int64: signed FIRST difference over kSimdFor's
                      // four-lane interleaved frame-of-reference layout --
                      // the one empty cell left in the derivative x
                      // inner-codec table. That table reads: kDeltaFor (11)
                      // is delta + FOR-BA, kDeltaBitweave (12) is delta +
                      // BitWeave, kDeltaPfor (37) is delta + PFor, kD2Pfor
                      // (52) is delta-of-delta + PFor. Nothing paired the
                      // first difference with the 4-lane layout, and
                      // nothing paired the SECOND difference with it either
                      // (kD2Pfor deliberately took PFor, where the win is
                      // measured on the dd stream).
                      //
                      // Structurally it is a sibling of kDeltaFor, not a
                      // new shape: the same [ncp u16][ckpt ncp x 8B BE]
                      // seek table, so a probe is one checkpoint load plus at
                      // most kDeltaCheckpointEvery (128) inner decodes. The
                      // delta vector (length n, deltas[0]=0) is then packed
                      // by kSimdFor's own layout -- four 8-byte BE lanes per
                      // 32-byte group, value t at lane (t mod 4), bit offset
                      // (t div 4)*w -- and the whole thing is DERIVED from
                      // (n, w) rather than declared, so there is no count
                      // field for a hostile footer to under-state.
                      //
                      // Why the pairing is worth having: kSimdFor packs a
                      // plain frame of reference, so on a column with a large
                      // magnitude AND a steady step it pays that magnitude in
                      // width, while the first differences are narrow. This is
                      // the same trade kDeltaFor makes against plain kForBp,
                      // one layout up. It is NOT a universal win: when the
                      // first differences are already narrow, kDeltaBitweave
                      // and the plain kSimdFor tie it, and the size
                      // comparison in the cost model decides -- nothing is
                      // special-cased.
                      // [ncp u16 BE]
                      // [ckpt: ncp x (value 8B BE)]
                      // [inner: a complete kSimdFor region over the n deltas]
                      // param = the inner kSimdFor width w (1..64), the same
                      // meaning kSimdFor's own param has.
    kS4 = 55,  // int64: per-block frame of reference on FIRST DIFFERENCES
               // ("S4"; Simonson, Suzuki, Absebassi & Merrill, SIGMOD 2014,
               // integer column). nblk = ceil(n/64), one block per 64 rows:
               // [base 8B BE][nblk u8]
               // [w x nblk, one byte each]
               // [lo x nblk, 8B BE each -- the block's minimum delta]
               // [ck x nblk-1, 8B BE each -- ck[b] = v[64b-1], the value of
               //  the row BEFORE block b]
               // [payload: block b's rows at w[b] bits, each block
               //  byte-aligned]
               // with d[0] = 0, d[i] = v[i] - v[i-1] for i > 0, and row i of
               // block b stored as (d[i] - lo[b]) in w[b] bits. The delta sum
               // is UNSIGNED-wrapping, so a column visiting both int64
               // extremes round-trips bit for bit. param = 0: no single width
               // parameterizes this scheme, the widths live in the region.
               //
               // The per-BLOCK frame and width are the whole point. One shared
               // width is set by its worst block, which on a 1024-row column
               // that is piecewise linear with a slope kink every 64 rows
               // measures 712 bytes for kDeltaFp against 401 here (1.8x, both
               // measured). The header is the price: 9 + nblk + 8*nblk +
               // 8*(nblk-1) bytes, 273 at a full 1024-row group (~2.1
               // bits/row), which is why kDeltaFp still wins a CLEAN ramp
               // (328 vs 409) and the cost model decides there.
               //
               // The second thing it buys is that it does not require monotone
               // input. kDeltaFp refuses to encode at all on a single
               // negative delta (`if (static_cast<int64_t>(d) < 0) return {}`);
               // framing each block against its own minimum absorbs dips, so
               // a column that trends upward and stutters is encodable here and
               // not there. Measured on a 1024-row column with a -20 step every
               // 10th row: 913 bytes here, kDeltaFp declines.
               //
               // Seek cost is a 64-row block: jump to ck[b] and replay at most
               // kS4Block-1 (63) deltas, against kDeltaFp's
               // kDeltaCheckpointEvery (128). It is NOT O(1) and does not claim
               // to be -- the stored words are deltas from the PREVIOUS ROW,
               // which is what makes them narrow, so the block's seek word
               // pins no row inside the block. An earlier draft stored offsets
               // from the block's first row, which WOULD have been O(1), and
               // was measured and dropped: 1297 bytes on a clean ramp against
               // 409 here, because a within-block delta shrinks with the step
               // size while an offset from the block head does not.
               //
               // It is honestly NOT a universal win, and the cost model does
               // not pretend otherwise. Against the FULL candidate set on the
               // shapes measured it does not win the adaptive race even once:
               // kArith and kPla compress a 1024-row trending column with one
               // big jump to 159 bytes (this scheme: 561), kBitWeave takes the
               // blocky-volatility walks, and kDeltaBitweave takes smooth
               // non-monotone data. What it beats is kDeltaFp -- the one scheme
               // with a region-wide width on the same transform -- plus the
               // non-monotone inputs kDeltaFp cannot encode at all. So today it
               // is reachable by TINYLAMB_FORCE_SCHEME=S4 and by the
               // encoder/validator agreement sweep, and it earns its place as a
               // correct, cheaper-to-seek alternative to kDeltaFp rather than
               // as a new adaptive winner.
               //
               // nblk is DERIVED from n and pinned by ValidateWalk, so there is
               // no count field to under-state. ValidateWalk also replays the
               // region and pins every seek word to the running value -- the
               // property the O(1)-ish probe depends on -- and re-derives each
               // block's widest offset to require the stored width to be
               // canonical, the same shape as kHuffDict's Kraft check.
    kTopk = 56,  // int64: TOP-K FREQUENCY encoding -- kFreq generalized from
                 // ONE dominant value to the K=15 most frequent (BtrBlocks
                 // "Frequency" proper: the two most frequent values take a
                 // 1-bit code, the next eight a 3-bit code, the next sixteen
                 // a 4-bit code and the rest are exceptions, SIGMOD'23; this
                 // region keeps the flat 4-bit form -- one NIBBLE per row,
                 // byte-aligned, no cross-row bit carry -- and drops the
                 // per-row exception flag bit by reserving code 15 as the
                 // exception SENTINEL, which is also why the hot set is 15
                 // and not 16). The niche is the shape every other
                 // dictionary-family scheme misses: DISTINCT counts far too
                 // high for any d-entry dictionary (kDict/kOpDict/kHuffDict
                 // pay 8B + 2B per entry, so d >= ~100 already costs more
                 // than the whole PLAIN column) with a HOT head -- the top-15
                 // of a zipf(s >= 1.5) tail with d in the hundreds covers
                 // 90%+ of the rows, and those rows cost 4 bits each while
                 // the cold tail lives in a self-contained inner region of
                 // nexc values. kFreq is the K=1 special case and loses here:
                 // its single dominant value leaves the whole mid-skew head
                 // as exceptions at 8B each. There is NO exception bitmap:
                 // code 15 marks the exception rows, so the nibble stream
                 // alone decides which rows are hot -- the one O(1)-ish
                 // random access the scheme needs (an exception's rank is a
                 // SWAR popcount of sentinel nibbles) and the thing that
                 // makes the per-row flag of the original bit-packed form
                 // redundant.
                 //
                 // The compressed-domain headline is the inline EXACT
                 // frequency table (kHuffDict's trick at dict size): hot
                 // values are stored ASCENDING with code = rank, so any
                 // CompareOp predicate folds over the 15 hot entries as ONE
                 // code window -- COUNT = sum(freq[c] in window) and filtered
                 // SUM = sum(hot[c]*freq[c] in window) -- with ZERO nibble
                 // reads, and the unpredicated SUM / MIN / MAX fold the same
                 // way plus one inner-region pass over the nexc tail.
                 // [d u16 BE][nexc u16 BE]
                 // [hot: 15 x 8B BE ascending, slots >= d zero]
                 // [freq: 15 x u16 BE, freq[c] >= 1 for c < d, 0 above]
                 // [inner scheme u8][inner param u32 BE]
                 // [nibbles ceil(n/2) B: byte j = code(2j)<<4 |
                 //   code(2j+1), codes 0..d-1 = hot[d], 15 = exception,
                 //   d..14 never emitted]
                 // [inner region: nexc exception values in row order, leaf
                 //   whitelist; omitted when nexc == 0]
                 // param = nexc (echoed in the header, kFreq-style).
                 // Applicability gate mirrors the family: d == 1 is kConst's
                 // (rejected), and the encoder bails when the top-15 head
                 // covers less than TWO THIRDS of the block -- a uniform-30
                 // column sits near 52% and would emit dead nibbles plus a
                 // tail the range folds must walk row by row, i.e. exception
                 // payload with a dead hot table; the dictionary family wins
                 // size AND range on every such column.
    kSfor = 57,  // doubles, NON-NEGATIVE + NON-DECREASING only: SFOR --
                 // "sorted float frame of reference". A monotone run of
                 // non-negative doubles bitcasts to a monotone u64 run
                 // (the sign bit is clear, so IEEE-754 bit order IS value
                 // order), and that monotone u64 vector is stored as a
                 // prefix-group FOR: split each value at `s` bits into
                 // msb = v >> s and lsbits = v & ((1<<s)-1); rows sharing a
                 // msb form a GROUP, groups are strictly ascending and
                 // disjoint, and within a group the lsbits are ascending
                 // too, so each group stores its first lsbits as a base
                 // and the (lsbits - base) deltas at w = BitsNeeded(last
                 // delta) bits. It is Elias-Fano's high/low split
                 // generalized to data-dependent groups with per-group
                 // widths -- the sorted-stream binary-fraction shape of
                 // the Elf line (Li et al., PVLDB'23) -- and the group
                 // directory is what buys the compressed-domain queries:
                 // the per-group bracket [msb<<s, msb<<s | (2^s-1)] lets
                 // ANY range predicate fold to whole-group SvSetRanges,
                 // and because the values ALSO ascend inside a group, the
                 // (at most two) boundary groups resolve by two in-group
                 // bisects into one contiguous range set -- the answer
                 // costs O(ng + log rows) unpacks, no value array and not
                 // even a boundary scan; EQ reads a single group. This is
                 // the FIRST
                 // double scheme in the file with a predicate kernel:
                 // kGorilla/kChimp/kPatas/kAlp are sequential-decode
                 // codecs, and nothing covered a monotone double column
                 // (a double-keyed chunk's key column is exactly that --
                 // the merge emits keys in key order). The niche is
                 // measured, not assumed: wide-magnitude ascending
                 // doubles whose low bits move in a narrow band
                 // (timestamps ~1.7e9, prices, sorted sensor readings)
                 // where the per-group delta width collapses; the encoder
                 // tries every split 1..63 and the byte race against
                 // kGorilla/kChimp/kAlp/kPatAs decides, so a dense
                 // integer-like ramp is NOT automatically its win (the
                 // directory is 19B/group). The NON-NEGATIVE gate is
                 // structural, not a shortcut: with the sign bit set the
                 // bitcast order inverts, and remapping through an
                 // order-preserving transform would break the bit-exact
                 // identity with the generic int64 predicate path (the
                 // rest of the file compares double columns as their raw
                 // bits). Encoder gate: prof.mono && prof.emin >= 0 on the
                 // bitcast cells and no nulls (raw 0 would silently make
                 // an unsorted column look monotone).
                 // [s u8][ng u16 BE]
                 // [dir: ng x (msb u64 BE][base u64 BE][row_start u16 BE][w
                 //   u8]) -- 19B per group, msb strictly ascending,
                 //   row_start[0] == 0 strictly ascending, base < 2^s,
                 //   w <= s and w == BitsNeeded(largest delta in the
                 //   group) (0 = every lsbits equals base); the group's
                 //   row count is row_start[g+1] - row_start[g] (n for
                 //   the last group)]
                 // [payload: per group, byte-aligned, (lsbits - base) at
                 //   w bits each, the file's MSB-first bit stream]
                 // param = s (echoed in the footer and pinned by
                 // ValidateWalk, kFreq-style). Random access bisects
                 // row_start (O(log ng)) and unpacks one width; it is NOT
                 // wired into RegionLowerBound -- that API is int64-key
                 // (double-keyed lookups resolve through the hash index),
                 // and raw-bit order only equals value order under the
                 // non-negative gate anyway. ValidateWalk replays the
                 // whole region and re-derives each group's widest delta
                 // (the kS4 canonicality rule) plus base+maxdelta < 2^s
                 // (the reconstruction must not carry into the msb).
    kMod = 58,   // int64: MODULAR AFFINE model -- the wrapped generalization
                 // of kArith. Every on-model row of an n-row region satisfies
                 // v_i = (a + b*i) mod m exactly, with a,b < m <= 2^63 and
                 // every value non-negative; rows off the model fall to the
                 // kArith exception envelope (bitmap + inner region, reused
                 // verbatim). This completes the exact-model family along the
                 // axis the linear models cannot express: sequences that
                 // WRAP -- i % N round-robin keys, (c*i) mod N synthetic
                 // fillers, clock/timestamp fields (second-of-minute,
                 // day-of-week), wrapped sequence counters -- where kArith
                 // pays one exception per wrap and kPla splits into
                 // per-wrap segments. The stored (a, b, m) make the region
                 // itself an exact arithmetic progression in Z/mZ, which is
                 // what the compressed-domain kernels exploit: EQ folds to
                 // the residue class of k -- i = (k-a)/g * (b/g)^-1 mod q,
                 // g = gcd(b,m), q = m/g -- an arithmetic progression
                 // stepping q, patched per exception (range predicates have
                 // no closed-form POSITION set, so they ride the generic
                 // path); an unpredicated SUM folds as the un-modded
                 // arithmetic sum minus m * floor sum
                 // S(n,a,b,m) = sum floor((a+b*i)/m), computed by the
                 // Euclidean descent (O(log), no per-row evaluation, no
                 // decode, no memory traffic beyond the 31B header), patched
                 // per exception. Full decode is one running add-and-wrap per
                 // row (b < m, so a single conditional subtract steps the
                 // model).
                 // [a u64 BE][b u64 BE][m u64 BE]
                 // [ischeme u8][iparam u32 BE][nexc u16 BE]
                 // [bitmap ceil(n/8) B MSB-first, bit i = 1 iff row i is
                 //   on-model]
                 // [inner region: nexc exception values in row order;
                 //   omitted at nexc == 0]
                 // param = nexc (kArith convention). Encoder gate: n >= 8,
                 // all values non-negative, a candidate modulus m in
                 // [2, 2^63] derived from the first wrap-consistent break
                 // pair (m divides b0 - d for the pair's signed delta d, and
                 // the actual difference of two residues is b or b - m, so
                 // the first break yields |m| exactly on either sign),
                 // verification with the fewest exceptions wins, nexc*2 < n
                 // (kArith's majority-on-model gate), and at least one
                 // ON-MODEL wrap inside the window -- a wrap-free window is
                 // exactly kArith's affine sequence and must stay its niche.
    kTileDict = 59,  // int64: BLOCK-LOCAL dictionary -- kDict re-scoped to a
                     // 64-row tile, so the dictionary cost curve that kills
                     // every region-wide member of the family (8B per
                     // DISTINCT value of the WHOLE region, dead past d ~
                     // 1500 and past the u16 cap not encodable at all) is
                     // paid per tile instead. The niche is a column whose
                     // global cardinality is far beyond any dictionary's
                     // reach but whose values repeat LOCALLY -- rows arrive
                     // grouped by the entity the values name (session-
                     // grouped user ids, time-bucketed sensor/source ids,
                     // FK-side pre-aggregation inputs), so each tile draws
                     // from a handful of wide-magnitude values while
                     // neighboring tiles draw from different ones. On that
                     // shape kDict/kOpDict/kHuffDict decline on the global
                     // count, kFreq/kTopk's global hot head cannot track a
                     // per-tile pool (every row falls to an 8B exception),
                     // and the frame/delta/bit-slice families pay
                     // log2(global span) bits per row; a tile dictionary
                     // pays log2(per-tile d) bits + d*8/64 bytes instead,
                     // and the size race prices that directly.
                     //
                     // The compressed-domain headline: each tile's
                     // dictionary is stored ASCENDING (the kOpDict
                     // convention), so any CompareOp predicate maps to ONE
                     // code window per tile by two binary searches; a tile
                     // whose bracket [dict[0], dict[d-1]] sits wholly inside
                     // or outside the window folds from the bracket alone
                     // with ZERO code reads, and only a straddling tile
                     // walks its codes -- at <= 6 bits per row of memory
                     // traffic instead of 8B values. COUNT / filtered SUM /
                     // the scan mask all fold this way; random access is a
                     // tile walk (<= 16 tiles in a full row group) plus one
                     // bit extraction plus one dictionary load.
                     // [tiles tile [0, n) at 64 rows; NO header]
                     //   per tile: [d u8]        1 <= d <= 64
                     //             [dict d x 8B BE, strictly ascending]
                     //             [codes ceil(rows_t * w / 8) B MSB-first,
                     //              w = BitsNeeded(d-1) (0..6), code = the
                     //              rank of the row's value in dict]
                     // param = 0: every extent is derived from n (the S4
                     // rule -- no count field for a hostile footer to
                     // under-state). Encoder gates: n > 64 (a single-tile
                     // region is kDict's exact layout minus one header byte;
                     // that must stay kDict's niche), every tile must
                     // hold d <= 64 -- the single-u64 pass-mask bound the
                     // kernels lean on -- and NOT every tile may be
                     // single-valued (an all-constant-tile region is runs
                     // of length >= 32, the kRle/kRleX family's home turf).
                     // ValidateWalk replays the whole
                     // region: tiles must cover n exactly, each dictionary
                     // strictly ascending, every code < d (the clamp bucket
                     // never fires on encoder output). Not handled by the
                     // WASM module (gated with the other post-15 schemes
                     // behind TINYLAMB_NO_NEW_SCHEMES for stale readers).
    kExpFor = 60,    // doubles, ANY finite values -- EXP-FOR, the sign+exponent
                     // grouped frame of reference. Every finite double bitcasts
    // to (sign, biased exponent, 52-bit mantissa); the IEEE-754
    // total-order transform key = bits ^ (bits >> 63 ? ~0 :
    // 2^63) makes key order EQUAL value order across signs, so
    // grouping consecutive rows that share the top 12 key bits
    // (sign + magnitude band) partitions the column into RUNS
    // that each pay only their own mantissa spread: a run
    // stores its minimum mantissa as a FOR base and the
    // (mantissa - base) offsets at w bits (the run's widest).
    // This is kSfor's prefix-group idea generalized from
    // "monotone non-negative" to EVERY finite double -- the
    // generalization that buys the compressed-domain headline:
    // each run's bracket [(gkey<<52)|base, +2^w) is a ZONE MAP
    // in value space, so ANY range predicate folds per run --
    // brackets outside the window cost ZERO reads, brackets
    // inside collapse to one range set, and only straddling
    // runs walk their rows at w bits instead of 8B values.
    // Brackets of runs sharing a band OVERLAP, so the fold
    // scans the directory linearly (O(ng), no bisect -- the
    // kSfor in-group binary search is gone with the sortedness
    // gate; random access still bisects row_start, which stays
    // ascending by construction). kSfor keeps the monotone
    // non-negative niche (its split search refines the same
    // grouping further); this scheme covers the columns kSfor
    // structurally cannot touch -- unsorted measurements,
    // signed oscillations, mixed magnitudes -- where today
    // every other double codec (Gorilla/Chimp/Patas/ALP/
    // XorFor) decodes sequentially and predicate evaluation
    // materializes all n rows. The niche is MEASURED, not
    // assumed, and bounded by the kExpForMaxRuns encoder gate:
    // bands that alternate faster than ~1 run per 16 rows
    // (oscillation ng=83, sawtooth noise ng=340, log-uniform
    // spread ng=440 per group -- bench_expfor, 65,536 rows)
    // burn directory bytes AND lengthen the linear fold until
    // a 2%-smaller cell runs its range COUNT 6.8x PLAIN, so
    // the gate declines them and the flat packers / sequential
    // codecs keep those columns. What fires is LOCALLY
    // CLUSTERED magnitude (batch/sensor columns that dwell in
    // one band then jump: measured ng = 8-16 at 128-row
    // blocks two decades apart with a 1e-4 relative wobble):
    // 5.1B/row = 18% under CHIMP128 and 37% under Gorilla on
    // the same bytes, decode at the kBitWeave tier, range
    // COUNT 1.7x PLAIN with the brackets pruning every dwell
    // block the window misses, and the adaptive selector picks
    // it unforced. Single-band columns lose by six header
    // bytes to kBitWeave's identical flat packing; smooth
    // series stay Gorilla's on size. Random access
    // bisects row_start (O(log ng)) and unpacks one width.
    // Encoder gates: n >= 2, no nulls (the raw-0 family
    // convention) and every cell FINITE -- NaN/Inf exponent
    // bits 0x7ff have no meaningful order bracket and decline
    // to the sequential codecs.
    // [ng u16 BE]
    // [dir: ng x (gkey u16 BE][base u64 BE][row_start u16 BE][w
    //   u8]) -- 13B per RUN, IN ROW ORDER (row_start[0] == 0,
    //   strictly ascending, tiles [0, n); gkey <= 0xfff, base
    //   < 2^52, w <= 52 and w == BitsNeeded(largest mantissa
    //   offset in the run) (0 == constant run); the run's row
    //   count is row_start[g+1] - row_start[g] (n for the last
    //   run)]
    // [payload: per run, byte-aligned, (lo52 - base) at w bits
    //   each, the file's MSB-first bit stream]
    // param = 0: the split is structural (top 12 bits), every
    // extent derives from (n, ng, the directory) -- the kS4
    // rule, no count field for a hostile footer to under-state.
    // ValidateWalk replays the region and re-derives each
    // run's widest offset (the kS4/kSfor canonicality rule).
    // Reconstruction inverts the transform (key >> 63 ? key ^
    // 2^63 : ~key), so a decoded cell is the RAW bitcast bits
    // exactly like every other double codec -- the int64
    // machinery compares them signed, and the predicate kernel
    // maps the signed window to <= 2 key intervals (one per
    // sign side of zero) to keep bit-for-bit agreement. Not
    // handled by the WASM module (gated with the other post-15
    // schemes behind TINYLAMB_NO_NEW_SCHEMES for stale
    // readers).
    kSortDict = 61,  // int64, MOSTLY-DISTINCT + UNORDERED -- SORT-DICT, the
    // sorted-view permutation layout. The column is re-sorted
    // WITHIN the row group and stored as a 64-row-block delta
    // stream (each block: first value + non-negative gaps at
    // the block's own width, the kS4 wire discipline); a
    // ceil(log2 n)-bit RANK plane maps row -> sorted index, so
    // the row order is one gather away. This is the
    // dictionary family pushed past its code cap: kDict/kOpDict
    // gate at 1024 codes and kTileDict at 64 per tile, so a
    // column where MOST values are unique (event ids, sparse
    // foreign keys, wide measurements) has no dictionary home
    // and today falls to the flat bit-packers, which decode to
    // answer ANY predicate. The sorted view is what buys the
    // compressed-domain kernels no unordered-column scheme
    // offers: the block firsts are an ASCENDING bracket table,
    // so an EQ/RANGE predicate resolves to one sorted window
    // [lo, hi) by bracket bisect + a <= 63-gap walk (the whole
    // row group answers from O(log nb + 63) unpacks); each
    // block header also carries the WRAPPED prefix sum of its
    // sorted values, so an unpredicated SUM is zero-read over
    // the 16-entry table and a predicated SUM/COUNT over the
    // window replays <= 2 x 63 values (the first closed-form
    // SUM for unordered high-cardinality columns). MIN/MAX are
    // the first/last brackets. The mask materializes by one
    // rank extraction + window test per row (about PLAIN's
    // compare cost at a fraction of the bytes touched), with
    // whole-window fast cases answering in O(log nb). The
    // price is the rank plane (~10 bits/row) plus per-block
    // headers: on uniform columns that loses the byte race to
    // PBitS/BitWeave by ~2 bits/row, but per-BLOCK widths
    // absorb the outlier rows that inflate a flat packer's
    // global width, so heavy-tailed / wide-magnitude columns
    // encode SMALLER (measured in bench_sortdict), and the
    // query kernels win at every size. Encoder gates: n >= 2,
    // int64 only, and MOSTLY-DISTINCT -- at least half the
    // rows unique (d * 2 >= n); below that the dictionary
    // family stores the same rows smaller and keeps its niche
    // (the kTileDict wrap-free-window rule). Duplicates are
    // legal: the rank plane stays a bijection (ties assigned
    // arbitrarily), zero gaps pack at w = 0 blocks.
    // [nb u8 BE]  -- block count == ceil(n/64) <= 16; param pins it
    // [rw u8 BE]  -- rank width == BitsNeeded(n-1) (canonical)
    // [widths: nb u8 BE] -- w_b == BitsNeeded(largest gap in the block)
    //   (0 == all-equal block, no gap bytes)
    // [firsts: nb x i64 BE] -- first_b == sorted[64b] (ascending,
    //   non-strict across duplicate-spanning blocks)
    // [psums: (nb+1) x u64 BE] -- psum[b] == wrapped sum of sorted
    //   [0 .. 64b), psum[0] == 0, psum[nb] == the wrapped total (the word
    //   the unpredicated SUM kernel loads)
    // [rank plane: ceil(n * rw / 8) B MSB-first; a permutation
    //   of [0, n) -- row i's sorted index]
    // [per block: byte-aligned gap plane, rows_b - 1 gaps at w
    //   bits, the file's MSB-first bit stream]
    // param = nb: pinned by the parse and ValidateWalk against
    // the header byte (a hostile footer must not relocate the
    // gap planes). ValidateWalk replays the gaps (canonicality:
    // stored width == re-derived widest gap), pins every first
    // / psum to the walk, and checks the rank plane IS a
    // permutation. Not handled by the WASM module (gated with
    // the other post-15 schemes behind TINYLAMB_NO_NEW_SCHEMES
    // for stale readers).
    kLane = 62,  // int64: K INTERLEAVED ARITHMETIC LANES -- the striping
    // member of the exact-model family (kArith's a + i*d, kPla's segment
    // lines, kMod's Z/mZ affine). The column is the row-major INTERLEAVING
    // of K independent arithmetic progressions: v(i) = base[i % K] +
    // step[i % K] * (i / K), all arithmetic wrapped (bit-exact for every
    // int64 input, the kArith/kMod contract). The shape this owns is the
    // SHARDED SEQUENCE / round-robin multi-stream measurement: K sensors,
    // shards or logical clocks sampled in rotation produce exactly this
    // column, and nothing else in the file fits it -- kArith/kPla need the
    // ramp contiguous in row order (interleaving breaks their line), kMod
    // needs ONE global modulus (K lanes are K independent phases), and the
    // delta family stores the shape small but pays the cumulative-codec
    // penalties (a serial replay from a checkpoint for ANY predicate or
    // SUM). The lane model is what buys the compressed-domain kernels: a
    // range predicate folds to one closed-form index window PER LANE (K
    // windows, the kPla fold), an unpredicated SUM is the sum of K
    // closed-form lane sums minus exception patches (O(K + nexc), no row
    // decode), and a point probe is one lane model eval plus a popcount
    // exception rank -- the same envelope discipline as kMod (bitmap set =
    // on-model row, exceptions live in an inner region). Wire (all extents
    // derived from (n, nd)):
    // [nd u8 BE]  -- lane count, 2 <= nd <= 16; nd == 1 is kArith's niche
    //   and is never emitted; nd > 16 cannot win the header race.
    // [nd x (base 8B BE][step 8B BE)] -- wrapped lane params; lane l's
    //   first row is l and its value there is base[l].
    // [ischeme u8][iparam u32 BE][nexc u16 BE] -- the kArith exception
    //   envelope header (param pins nexc).
    // [bitmap ceil(n/8) B MSB-first] -- set = on-model row (the kMod
    //   convention), ALWAYS present; a bitmap whose popcount disagrees
    //   with n - nexc fails the walk.
    // [inner region: nexc exception values, no bytes at nexc == 0].
    // Encoder gates: n >= 16, nd <= 16, nd <= n / 4 (every lane carries at
    // least 4 rows so the 16-bytes-per-lane header amortizes), and
    // nexc * 2 < n (kArith's majority-on-model rule; below that the flat
    // bit-packers own the column). Candidate lane counts 2..min(16, n/4)
    // are each verified by one wrapped walk with an early abort once the
    // live exception count can no longer win; the smallest WIRE wins.
    // ValidateWalk case 62 pins nd, the param echo, and the bitmap
    // consistency door check. Not handled by the WASM module (gated with
    // the other post-15 schemes behind TINYLAMB_NO_NEW_SCHEMES for stale
    // readers).
    kPostings = 63,  // int64: INVERTED DICTIONARY -- the dictionary
    // transposed. Instead of a code per row, each distinct value owns a
    // sorted posting list of its row ids (Rice-coded gaps), so the whole
    // query surface works on the compressed form: the dictionary plus its
    // count table is an exact histogram, ANY predicate folds to COUNT/SUM
    // in closed form over the codes it passes (O(log d) for EQ, O(window)
    // for a range -- zero row reads, zero bitstream reads), and a selective
    // predicate materializes its selection mask at O(matched rows) by
    // jumping (bitoffs table) straight to the posting lists that pass.
    // Random access is the honest weak spot -- the lists interleave in row
    // space, so a probe sorts the d candidates by first posting and walks
    // (the run family's poor-random-access tier). The footer's inverted
    // secondary index carries UNCOMPRESSED postings and declines above 64
    // distinct; this is the same idea folded into the column encoding
    // itself, compressed, and wired into every query path.
    // Wire (every extent derived from (n, d); d echoed in the footer
    // param):
    // [d u16 BE]  -- distinct count, 1 <= d <= n; the encoder gates
    //   4 <= d <= 64 -- the footer inverted index's own coverage band --
    //   and d <= n/8 (below 4 the two-value/kFreq family owns the column;
    //   above n/8 the average list is under 8 rows and the per-list
    //   tables stop amortizing).
    // [d x 8B BE dict]  -- strictly ascending distinct values.
    // [d x u16 BE counts]  -- rows per list, each >= 1, sum == n: the
    //   exact histogram the closed-form folds trust.
    // [ceil(d/2) x u8 k-nibbles]  -- Rice parameter (0..15) per list.
    // [d x u16 BE firsts]  -- each list's first (lowest) row id.
    // [d x u16 BE bitoffs]  -- each list's sub-stream offset in BITS;
    //   sub-streams are byte-aligned, so a cursor can jump straight to a
    //   passing list instead of decoding the skipped ones' gaps (what
    //   makes a selective mask O(matched), not O(rows)).
    // [bitstream MSB-first]  -- per list, (count-1) Rice(2^k) codes of
    //   (gap - 1) between consecutive postings; gaps are >= 1 because a
    //   row id appears in exactly one list. Lists are stored in dict
    //   order, NOT row order, so the stream is a concatenation of d
    //   independent byte-aligned sub-streams.
    // No economic gate beyond the structural ones: on a uniform column the
    // flat log2(d) code width wins the byte race and the cost model keeps
    // kDict/kOpDict's niche at every hint tier; skew is where per-list Rice
    // beats the code width and the closed forms earn their bytes. The
    // SYSTEM-level accounting is what the band is for: a d <= 64 int64 cell
    // otherwise carries the footer's UNCOMPRESSED inverted index (~2 B/row)
    // ON TOP of its codec, so where this scheme is chosen the chunk build
    // skips that footer copy -- the codec replaces the index outright (the
    // Inverted* footer APIs fall back to the scan paths, which hit the
    // closed forms here). ValidateWalk case 63
    // fully replays the stream (row bijection + consumption == extent),
    // which is what entitles the closed-form kernels to trust the count
    // table. Not handled by the WASM module (gated with the other
    // post-15 schemes behind TINYLAMB_NO_NEW_SCHEMES for stale readers).
    kTemplate = 64,  // varchar: TEMPLATE SPLIT -- structured strings stored as
    // a row-invariant literal skeleton plus fixed-width DIGIT RUNS stored as
    // ordinary int64 leaf regions. The shape nothing else in the file can
    // hold: strings that share a template ("GET /api/v2/users/000042 200",
    // "2026-10-07T12:34:56Z", "SKU-0042") but whose FULL-STRING cardinality
    // is far past any dictionary's reach (kDict/kOpDict cap at 1024 codes of
    // 8B+/entry; the varchar dict caps at 256) and whose repeated substrings
    // are longer than FSST's symbol table can price -- today those columns
    // fall to FSST or PLAIN, and EVERY predicate decodes strings row by row.
    // The template is what buys the compressed-domain processing: a row is
    // reconstructed as lit[0] + digits(v_0) + lit[1] + ... + digits(v_{nseg-1})
    // + lit[nseg], so an EQ predicate that parses against the template folds
    // to one int64 equality PER SEGMENT over the digit planes -- the count
    // never materializes a single string byte (and the literals are stored
    // once, not per row), while a needle whose structure misses the template
    // (a different literal, the wrong digit width) eliminates every
    // conforming row without reading a digit. Other ops (LT/LE/GT/GE and the
    // range form) decline to the generic decode path -- with equal digit
    // widths byte order equals numeric order per segment, but the composite
    // lexicographic fold across segments is left as future work rather than
    // shipped half-correct.
    // Digit runs must be ASCII '0'..'9' runs of a FIXED width 1..18 (18
    // bounds the int64 value domain; leading zeros are preserved by the
    // stored width, so "0042" and "42" are different rows' honest bytes).
    // Variable-width ids ("u0".."u1023") decline: per-row widths would cost
    // a byte per segment per row and erase the win. Rows that deviate from
    // the template in any byte (including NULL cells, which arrive as "")
    // fall to a u16-capped exception list stored as plain length-prefixed
    // strings in row order -- the kArith envelope with the exceptions as
    // strings. Wire (all extents derived from (n, the header); nexc echoed
    // in the footer param, the kArith convention):
    // [nseg u8]  -- 1 <= nseg <= 8
    // [nexc u16 BE]  -- exception count, param must equal it
    // [widths: nseg x u8]  -- digit width per segment, 1..18
    // [literals: (nseg+1) x (u16 len BE + bytes)]  -- the skeleton, stored
    //   ONCE; a zero-length literal is legal at any slot
    // [bmp ceil(n/8) B MSB-first]  -- bit i set = row i conforms
    //   (the kArith on-model convention); popcount(bmp) == n - nexc
    // [per segment: ischeme u8][iparam u32 BE][vlen u32 BE][inner region:
    //   vlen bytes, the n - nexc conforming values in row order, any
    //   whitelisted int64 leaf scheme (EncodeInnerI64's best-of)]
    // [exceptions: nexc x (u32 len BE + bytes)]  -- row order, the kPlain
    //   varchar layout
    // Encoder gates: n >= 8; the template inferred from row 0 has 1..8 digit
    // runs each of width 1..18; at least THREE QUARTERS of the rows conform
    // (nexc * 4 <= n, kArith's majority rule); and the column's full-string
    // distinct count is >= 97 -- below that the dictionary family stores the
    // same strings smaller AND already answers EQ from its pass table, so
    // the fence keeps kDict/kOpDict's home turf (the same pin-protection
    // role as kTileDict's all-constant-tile decline). ValidateWalk case 64
    // walks the whole region: widths, literal bounds, the bitmap popcount,
    // every inner extent (the shared inner_extent helper, so a hostile
    // footer cannot relocate a segment's bytes), and the exception chain
    // must tile [0, region_end) exactly. Not handled by the WASM module
    // (gated with the other post-15 schemes behind TINYLAMB_NO_NEW_SCHEMES
    // for stale readers).
    kDStream = 65,  // varchar: D-STREAM -- mined symbol dictionary (single
    // bytes through 8-byte substrings, the FSST candidate pool plus single
    // bytes) with greedy longest-match codes stored as LENGTH-DELIMITED
    // records instead of FSST's 4B-per-row offset table: each conforming row
    // costs one u8 code length + its code bytes, with a 16-row u32 restart
    // table for bounded replay. Code 0 is the escape (raw byte follows), so
    // every string encodes; a row whose code length passes 255 (u8 cap)
    // falls to a plain-string exception list behind the kArith conformance
    // bitmap. The shape this owns over FSST: HIGH-ENTROPY short/mid strings
    // -- UUIDs, hashes-as-strings, random tokens -- where symbol mining has
    // no multi-byte grip and the per-row index bytes dominate the race, and
    // every EQ predicate on FSST materializes and expands the row while this
    // scheme's EQ/NE count is a memcmp over the code records: the needle is
    // encoded once through the stored table (greedy longest match is a
    // deterministic function of (table, bytes), so equal strings always
    // produce equal code sequences and the memcmp is exact), then each row
    // costs one u8 load and a first-byte-rejected compare. The needle never
    // materializes a conforming string byte; only exception rows are compared
    // as strings. Ordered comparisons (LT/LE/GT/GE, the range form) and any
    // future prefix op decline to the generic decode path: codes are not
    // order-preserving, and a byte-prefix of a string may encode differently
    // than the prefix encoded standalone (a symbol may cross the boundary),
    // so a sound prefix fold needs its own op first. Wire (nexc echoed in
    // the footer param, the kArith convention):
    // [nsym u8]  -- 1 <= nsym <= 255; code 0 is the escape, codes 1..nsym
    //   address the symbol table in stored order
    // [symbols: nsym x (u8 len + bytes)]  -- each 1..8 bytes, distinct
    // [ndir u16 BE]  -- restart count, == (n - nexc + 15) / 16
    // [stream_len u32 BE]
    // [restarts: ndir x u32 BE]  -- stream offsets; [0] == 0, strictly
    //   increasing, each < stream_len (records end the last block there)
    // [stream: (n - nexc) x (u8 clen + clen code bytes)]  -- conforming rows
    //   in row order; clen 0 is the legal empty string
    // [bmp ceil(n/8) B MSB-first]  -- bit i set = row i conforms
    //   (the kArith on-model convention); popcount(bmp) == n - nexc
    // [exceptions: nexc x (u32 len BE + bytes)]  -- row order, the kPlain
    //   varchar layout, tiling the region tail exactly
    // Encoder gates: n >= 8; full-string distinct count >= 97 (the dictionary
    // family stores repeated strings smaller AND answers EQ from its pass
    // table -- the same home-turf fence as kTemplate's); and fewer than one
    // quarter of the rows may overflow the 255-code-length cap (nexc * 4 <=
    // n, kArith's majority rule -- only pathological >255-code-length rows
    // can land there, so the gate rarely bites). ValidateWalk case 65 walks
    // the whole region byte-independently: symbol bounds, the restart table
    // shape, every record of every block replayed to the recorded next
    // offset, the bitmap popcount pinned to n - nexc, and the exception
    // chain tiling the tail -- so the memcmp fold can trust clen and the
    // restarts without re-checking them per query. Not handled by the WASM
    // module (gated with the other post-15 schemes behind
    // TINYLAMB_NO_NEW_SCHEMES for stale readers).
    kFastPfor = 66,  // int64: SIMD PATCHED FRAME-OF-REFERENCE -- the empty
    // cell of the (locality x patching) table. kPfor (16) patches but its
    // width/base are GLOBAL -- one outlier per row group inflates every row;
    // kBp128 (40) localizes the frame to 128-row blocks but has NO patching
    // -- one outlier per block inflates the whole block's width. This scheme
    // pairs the two: per 128-row block, a frame-of-reference base plus the
    // narrowest width w whose REGULAR offsets all fit, with the offsets that
    // overflow 2^w stored as exceptions (position bitmap + full values). A
    // sensor/ledger column whose per-block range is a few bits but which
    // carries a handful of spikes is the shape: kPfor pays the global width
    // on every row, kBp128 pays the spike on every row of its block, this
    // pays w bits + ~8B per spike.
    //
    // Wire (every extent derived from the metadata walk -- the kS4/Bp128
    // convention, no count field a hostile footer can under-state; the block
    // count is echoed in the footer param):
    // per block b (nblk == ceil(n / 128), rows = min(128, n - b*128)):
    //   [min 8B BE]    block minimum (the FOR base; offsets are v - min)
    //   [max 8B BE]    block maximum
    //   [sum 16B BE]   exact block sum (unsigned __int128 as two BE words)
    //   [w u8]         packed width 0..64: REGULAR offsets are < 2^w
    //   [nexc u16 BE]  exception count, == popcount of this block's bitmap
    // then the payloads, blocks in order, each byte-aligned:
    //   [bmp ceil(rows/8) B MSB-first]  bit set = exception row
    //   [packed (rows*w+7)/8 B]  low w bits of (v - min) for ALL rows
    //     (exception rows carry their low bits too -- garbage for value
    //     decode, overwritten by the exception list; keeping the lanes dense
    //     is what lets the bulk decode reuse kBp128's AVX2 unpack kernels)
    //   [exceptions: nexc x 8B BE]  full values, row order
    //
    // Compressed-domain surface: the block min/max brackets prune whole
    // blocks on any CompareOp (zero lane reads), the stored block sums answer
    // a fully-selected block's SUM without decoding (the kBp128 metadata
    // arm), and only boundary blocks pay a lane decode plus a bitmap-aware
    // exception correction (an in-lane match on an exception row's low bits
    // is NOT a value match, so the mask kernel excludes bitmap bits and
    // evaluates exception values separately). Random access is one metadata
    // walk (<= 8 blocks per row group), one lane extraction, and a popcount
    // rank into the exception list. int64 only; NULL cells are raw 0 (the
    // region family convention). ValidateWalk case 66 replays every block:
    // bitmap popcount == nexc, regular offsets < 2^w, exception values exact,
    // and min/max/sum metadata matching the replay -- the entitlement behind
    // the bracket kernels. Not handled by the WASM module (native-only, the
    // post-55 convention; TINYLAMB_NO_NEW_SCHEMES=1 also stops the encoder
    // from emitting it).
    //
    // kDeltaFastPfor (67) fills the one cell this scheme leaves in the
    // (transform x locality x patching) table: the frame-of-reference runs
    // over FIRST DIFFERENCES, not raw values. On a cumulative series (a
    // counter climbing a few units per row, telemetries' default shape) the
    // raw offsets this scheme packs span the block's whole climb while the
    // first differences stay one spike-width wide -- kS4 (55) already
    // transformed, but its width has NO patch, so one huge increment (a
    // stall, a retry burst, an outage gap) re-widens every delta of its
    // 64-row block, and it carries no brackets or sums for the predicate
    // kernels. This pairs the transform with kPfor-style patching AND the
    // kBp128/FastPFOR envelope: block-local delta frames, exceptions as
    // (position bitmap + full delta), block value min/max/sum brackets.
    //
    // Wire (every extent derived from the metadata walk -- the kS4/Bp128/
    // FastPFOR convention, no count field a hostile footer can under-state;
    // the block count is echoed in the footer param):
    // per block b (nblk == ceil(n / 128), rows = min(128, n - b*128),
    // deltas = rows - 1 first differences within the block):
    //   [base 8B BE]   v at the block's first row (the replay seed)
    //   [min 8B BE]    true block value minimum (bracket; base row included)
    //   [max 8B BE]    true block value maximum (bracket)
    //   [sum 16B BE]   exact block sum (unsigned __int128 as two BE words)
    //   [w u8]         packed width 0..64: REGULAR (d - dmin) fit 2^w
    //   [nexc u16 BE]  exception count, == popcount of this block's bitmap
    //   [dmin 8B BE]   block's minimum delta (SIGNED order; stored as its
    //                  uint64 bit pattern -- the unsigned-wrapping subtract
    //                  d - dmin then lands every regular delta in
    //                  [0, max-min], the kS4 convention)
    // then the payloads, blocks in order, each byte-aligned:
    //   [bmp ceil((rows-1)/8) B MSB-first]  bit t set = delta t is an
    //     exception (row t+1's first difference)
    //   [packed ((rows-1)*w+7)/8 B]  (d - dmin) at w bits for ALL deltas
    //     (exception deltas carry low bits too -- garbage for replay,
    //     overwritten by the exception list; dense lanes are what lets the
    //     bulk decode reuse kBp128's AVX2 unpack kernels for the deltas)
    //   [exceptions: nexc x 8B BE]  full delta values, row order
    //
    // Replay: v_0 = base, v_t = v_{t-1} + (exception delta t-1, else
    // dmin + lane), all unsigned-wrapping -- a column visiting both int64
    // extremes round-trips bit for bit (the kS4/D2PFOR convention).
    //
    // Compressed-domain surface: the stored value brackets prune whole
    // blocks on any CompareOp (zero lane reads), the stored block sums
    // answer a fully-selected block's SUM without decoding (the kBp128/
    // FastPFOR metadata arm), and only boundary blocks pay a lane decode
    // plus the sequential in-block replay the delta chain requires. Random
    // access is one metadata walk (<= 8 blocks per row group) plus a <= 127
    // step replay -- the probe tier FastPFOR prices at 2.20. int64 only;
    // NULL cells are raw 0 (the region family convention). ValidateWalk
    // case 67 replays every block: bitmap popcount == nexc, regular
    // offsets < 2^w, and min/max/sum metadata matching the replay -- the
    // entitlement behind the bracket kernels. Not handled by the WASM
    // module (native-only, the post-55 convention; TINYLAMB_NO_NEW_SCHEMES=1
    // also stops the encoder from emitting it).
    kDeltaFastPfor = 67,
  };

  struct Footer {
    uint64_t schema_version{0};
    // Chunk format version (independent of schema_version, which the ALTER
    // layer owns): 1 = original layout, 2 = adds CRC32C integrity fields.
    uint32_t format_version{1};
    // Integrity (format_version >= 2): crc_data covers all bytes before the
    // footer; crc_footer covers every footer byte except itself.
    uint32_t crc_data{0};
    uint32_t crc_footer{0};
    uint32_t row_group_count{0};
    uint16_t column_count{0};
    uint16_t key_column_index{0};
    // ValueType of the key column (serialized as 1 byte). Lets a reader
    // (native or WASM) interpret zone-map bounds without external schema.
    // Whether group_min_keys/group_max_keys are present in the footer.
    // LSM generation of the newest input that produced this chunk. Higher
    // generation wins when the same key exists in several artifacts during
    // a read-time merge across memtable + runs + chunks.
    uint64_t generation{0};
    // Per-row-group, per-column offsets (row_group_count * column_count
    // entries). Each offset is the byte position within the chunk data where
    // that column's array begins for that row group. For varchar, the array
    // is length-prefixed per value; for fixed-width, it's dense.
    std::vector<uint32_t> column_offsets;
    // Per-row-group row count (may be < kRowGroupSize for the last group).
    std::vector<uint16_t> row_counts;
    // Zone map: min/max of the key column per row group (int64 keys only).
    // Groups arrive ascending from the merge, so ranges are disjoint and
    // sorted; this enables binary-search row-group pruning for point and
    // range lookups.
    std::vector<int64_t> group_min_keys;
    std::vector<int64_t> group_max_keys;
    // Footer v4 compression catalog: scheme id + one parameter word per
    // (row_group, column) cell. Params: RLE=run count, kForBp/kDeltaFp/kXorFor
    // = bit width, kDict = code count. Region payload starts at the matching
    // column_offsets entry and is self-contained big-endian.
    std::vector<uint8_t> encodings;
    std::vector<uint32_t> params;
    // Footer v5: per-column scalar statistics computed at build time.
    // Enables COUNT/SUM/AVG answers from metadata alone (no region reads).
    // Only int64 columns carry meaningful sums; others store zeros.
    std::vector<uint8_t> col_sum_valid;  // 1 = int64 column (sum meaningful)
    std::vector<uint64_t> col_counts;
    std::vector<int64_t> col_sums;
    // All-column zone maps (v6): min/max for EVERY int64 column per RG.
    std::vector<int64_t> all_zone_mins;
    std::vector<int64_t> all_zone_maxs;
    // Per-(RG, int64 col) bloom filter: one u64 word each.
    std::vector<uint64_t> rg_blooms;
    // Per-(RG, int64 col) HyperLogLog sketch: 64B each.
    std::vector<uint8_t> hll_sketches;  // flat: rg*col*64B
    // Per-RG partial sums for int64 columns: [rg * col_count + col]
    std::vector<int64_t> rg_sums;
    // Hash index for O(1) point lookup on int64 key column.
    uint32_t hash_idx_log2{0};  // table size = 2^log2
    std::vector<int64_t> hash_keys;
    std::vector<uint32_t> hash_rows;
    // Per-(RG,col) null bitmap (v11): one bit per row, 1 = value is null.
    // Stored concatenated for every (RG,col) cell in (RG-major, col-minor)
    // order; each cell uses ceil(rc/8) bytes where rc is that group's row
    // count. Lets the byte-slice/null-aware scan mask nulls without ever
    // materializing them. Absent when null_present is false.
    std::vector<uint8_t> null_bitmaps;
    uint32_t null_uniform_groups{0};
    uint64_t null_uniform_stride{0};
    // Extended column statistics (v12): per-int64-column MIN/MAX / null count
    // / mode (most frequent value) / 16-bin equi-width histogram over
    // [min,max]. Answer COUNT(col), COUNT(col) non-null, MIN/MAX, and (for the
    // mode value) exact equality COUNT/SUM from metadata alone, plus a
    // histogram estimate for range/equality predicates. Non-int64 columns
    // store zero placeholders. Serialized as a new flag-gated footer section
    // AFTER the null bitmaps so earlier readers skip it safely.
    std::vector<int64_t> col_mins;
    std::vector<int64_t> col_maxs;
    std::vector<uint64_t> col_nulls;
    std::vector<int64_t> col_modes;
    std::vector<uint64_t> col_mode_counts;
    // Flat [col * HIST_BINS] histogram bins.
    std::vector<uint64_t> col_hists;
    // Inverted (Roaring-lite) secondary index (v13): per-(row-group, int64
    // column) postings for low-cardinality cells (<= 64 distinct raw values).
    // Each posting lists the sorted row ids holding one value, letting
    // equality / IN / conjunction queries count (and intersect) without
    // decoding the column. Built from raw values, so NULL cells appear under
    // key 0 exactly as the decode path reports them.
    std::vector<InvertedCell> inv_index;
    // Xor8 point pre-filter (v14): per-(row-group, int64 column) fingerprint
    // tables for cells the inverted index skips (non-low-cardinality).
    // Answers "definitely absent" for point predicates so row groups decode
    // nothing; "maybe" (1/256 false positives) falls through to the scan.
    std::vector<XorCell> xor_cells;
    // PGM learned index (v15) over the ascending int64 key column: optimal
    // piecewise-linear segments (ShrinkingCone, error eps) mapping keys to
    // global rows. A compact stand-in for the hash index on NO_HASH_IDX
    // builds, and a probe-saver for range seeks on expensive (delta-coded)
    // key columns. Absent for non-int64 / nullable / unsorted keys.
    uint32_t pgm_eps{0};
    std::vector<PgmSegment> pgm_segments;
    // Total byte size of the footer section (for self-describing read).
    uint32_t footer_size{0};

    // Keep byte-sized flags together; the footer codec uses named fields.
    uint8_t key_column_type{0};
    bool zone_map_present{false};
    bool col_stats_present{false};
    bool all_zm_present{false};
    bool bloom_present{false};
    bool hll_present{false};
    bool rg_sums_present{false};
    bool hash_idx_present{false};
    bool null_present{false};
    bool null_summary_valid{false};
    bool null_any{false};
    bool ext_stats_present{false};
    bool inv_idx_present{false};
    bool xor_present{false};
    bool pgm_present{false};

    struct ColumnStats {
      uint64_t count;
      int64_t sum;
      // Extended statistics
      int64_t min;
      int64_t max;
      uint64_t null_count;
      // Histogram: equi-width, HIST_BINS bins over [min,max]
      static constexpr int HIST_BINS = 16;
      std::array<uint64_t, HIST_BINS> hist_counts{};
      // Mode (most frequent value among int64 values)
      int64_t mode_value;
      uint64_t mode_count;
      ColumnStats() {
        count = 0;
        sum = 0;
        min = INT64_MAX;
        max = INT64_MIN;
        null_count = 0;
        mode_value = 0;
        mode_count = 0;
        for (unsigned long& hist_count : hist_counts) {
          hist_count = 0;
        }
      }
    };
  };

  // Returns true when RG g may contain values in [lo,hi] for column c.
  // Uses all-column zone maps when available; falls back to false-safe true.
  // Zone maps cover non-null values only: a cell holding NULLs overlaps any
  // range that contains 0 (NULL decodes as raw 0).
  [[nodiscard]] bool RGOverlapsCol(int32_t g, slot_t col, int64_t lo,
                                   int64_t hi) const;

  // O(1) point lookup via the embedded hash index (if present). Duplicate
  // keys resolve to the FIRST row (builder keeps the earliest occurrence,
  // matching PointLookup's lower-bound semantics). Returns kNotImplemented
  // without an index, kNotExists for a missing key, and kCorruption when a
  // hand-built footer stores a row number past the chunk end.
  [[nodiscard]] Status HashLookup(int64_t key_value,
                                  size_t* row_index_out) const;

  [[nodiscard]] Status ColumnStats(slot_t col_idx, const Schema& sc,
                                   Footer::ColumnStats* out) const;

  // Metadata-only int64 aggregates (single chunk, no cross-run composition).
  // COUNT(col) non-null; equality COUNT (exact for the mode value, histogram
  // estimate otherwise); range COUNT estimate over [lo,hi]. kNotImplemented
  // when the column is not int64 or the v12 extended stats are absent.
  [[nodiscard]] Status CountNonNull(slot_t col_idx, const Schema& sc,
                                    uint64_t* out) const;
  [[nodiscard]] Status CountEqualFromStats(slot_t col_idx, const Schema& sc,
                                           int64_t lit, uint64_t* count_out,
                                           bool* exact_out) const;
  [[nodiscard]] Status CountRangeFromStats(slot_t col_idx, const Schema& sc,
                                           int64_t lo, int64_t hi,
                                           uint64_t* count_out) const;

  // MIN/MAX of an int64 column from the all-column zone maps (metadata
  // only, O(row groups)). Returns kNotExists when no zone coverage.
  [[nodiscard]] Status ColumnMinMax(slot_t col_idx, int64_t* min_out,
                                    int64_t* max_out) const;

  // Null-aware byte-slice comparison reference (scalar). Counts rows in one
  // column region whose decoded value satisfies `op` against `lit`, skipping
  // positions whose bit is set in `null_bmp` (1 bit = null, MSB-first per
  // byte). The WASM scan module implements the SIMD/byte-slice accelerated
  // equivalent of this routine.
  //
  // PRECONDITION: `region`/`param`/`n` must already be validated against the
  // containing chunk (e.g. via ValidateChunkBytes or a RegionStartAt/
  // RegionEndAt gate). This kernel takes no region_end argument, so its
  // per-scheme extent checks are disabled (unlike Int64RegionAt); handing it
  // a hostile footer's raw offsets would read past the buffer.
  enum class CompareOp { kEq, kLe, kLt, kGe, kGt };
  [[nodiscard]] static uint32_t ByteSliceNullAwareCount(
      const char* region, Scheme s, uint32_t param, uint32_t n,
      const uint8_t* null_bmp, CompareOp op, int64_t lit);

  // BSI slice-parallel predicate mask (Direct Operation on Compressed Data).
  // ORs bit i into out_bits[(n+63)/64 words] when row i of a ByteSlice
  // region satisfies `op` against `lit`, without materializing any value:
  // EQ is a slice AND-chain, GT/LT walk slices MSB-first maintaining an
  // equality frontier (O(w * ceil(n/8)) byte ops). Values are raw int64 bit
  // patterns; with null_bmp == nullptr every row participates (null-as-0
  // semantics of the decode path), otherwise null rows are skipped.
  static void ByteSliceCompareMask(const char* region, uint32_t n, CompareOp op,
                                   int64_t lit, const uint8_t* null_bmp,
                                   uint64_t* out_bits);

  // BSI predicated sum: Σ raw values of rows with sel_bits set (nullptr =
  // all rows), skipping nulls. Uses the popcount identity per slice, so no
  // value is ever materialized. Truncates to 64 bits (two's complement).
  [[nodiscard]] static int64_t ByteSliceFilteredSum(const char* region,
                                                    uint32_t n,
                                                    const uint8_t* null_bmp,
                                                    const uint64_t* sel_bits);

  [[nodiscard]] static uint32_t PBitSNullAwareCount(const char* region,
                                                    uint32_t param, uint32_t n,
                                                    const char* region_end,
                                                    const uint8_t* null_bmp,
                                                    CompareOp op, int64_t lit);
  static void PBitSCompareMask(const char* region, uint32_t param, uint32_t n,
                               const char* region_end, CompareOp op,
                               int64_t lit, const uint8_t* null_bmp,
                               uint64_t* out_bits);
  [[nodiscard]] static int64_t PBitSFilteredSum(const char* region,
                                                uint32_t param, uint32_t n,
                                                const char* region_end,
                                                const uint8_t* null_bmp,
                                                const uint64_t* sel_bits);

  PaxChunk() = default;
  PaxChunk(std::vector<char> data, Footer footer)
      : data_(std::move(data)), footer_(std::move(footer)) {
    BuildRowCountPrefix();
    BuildNullBitmapSummary();
    BuildEfSelectDirs();
    BuildFsstDirs();
    BuildDecodedIntCache();
  }
  // Construct from a plaintext (scannable) buffer plus its compressed wire
  // form. `is_compressed` records that the chunk is persisted in `compressed`
  // so the compression layer's size win can be reported without changing the
  // scan path (which always operates on plaintext `data_`).
  PaxChunk(std::vector<char> data, Footer footer, std::vector<char> compressed,
           bool is_compressed)
      : data_(std::move(data)),
        compressed_(std::move(compressed)),
        is_compressed_(is_compressed),
        footer_(std::move(footer)) {
    BuildRowCountPrefix();
    BuildNullBitmapSummary();
    BuildEfSelectDirs();
    BuildFsstDirs();
    BuildDecodedIntCache();
  }
  PaxChunk(const PaxChunk&) = default;
  PaxChunk(PaxChunk&&) = default;
  PaxChunk& operator=(const PaxChunk&) = default;
  PaxChunk& operator=(PaxChunk&&) = default;
  ~PaxChunk() = default;

  [[nodiscard]] const std::vector<char>& Data() const { return data_; }

  // ---- Block-compression layer (item 6) -------------------------------------
  // Lightweight LZ77-style codec over a raw byte region. Compression ratio is
  // secondary to correctness/speed; the decoder is a tight single-pass loop.
  [[nodiscard]] static std::vector<char> CompressRegion(std::string_view in);
  [[nodiscard]] static std::vector<char> DecompressRegion(
      std::string_view in, size_t uncompressed_size);
  // Strict variant for storage use: the stream must decode to EXACTLY
  // uncompressed_size bytes with no trailing garbage, otherwise kCorruption.
  // Unlike DecompressRegion (which stops early and returns partial output),
  // this is fail-closed against truncated, over-long, or hostile streams.
  [[nodiscard]] static StatusOr<std::vector<char>> DecompressRegionExact(
      std::string_view in, size_t uncompressed_size);
  [[nodiscard]] bool IsCompressed() const { return is_compressed_; }
  [[nodiscard]] size_t CompressedSize() const { return compressed_.size(); }
  [[nodiscard]] size_t UncompressedRegionSize() const {
    // Saturate: a default-constructed / footer-less chunk would wrap
    // `0 - 0 - 4` into a ~2^64 "region size" for any caller that trusts
    // it as an allocation bound.
    if (data_.size() <= footer_.footer_size + 4) {
      return 0;
    }
    return data_.size() - footer_.footer_size - 4;
  }
  [[nodiscard]] const Footer& GetFooter() const { return footer_; }
  [[nodiscard]] bool HasValidatedBp128Metadata() const {
    return bp128_metadata_trusted_;
  }
  [[nodiscard]] bool HasValidatedChimp128Metadata() const {
    return chimp128_metadata_trusted_;
  }
  [[nodiscard]] bool HasValidatedAlpRdTansMetadata() const {
    return alp_rd_tans_metadata_trusted_;
  }
  [[nodiscard]] bool HasValidatedPatasMetadata() const {
    return patas_metadata_trusted_;
  }
  [[nodiscard]] bool HasValidatedFastPforMetadata() const {
    return fastpfor_metadata_trusted_;
  }
  [[nodiscard]] bool HasValidatedDeltaFastPforMetadata() const {
    return deltafastpfor_metadata_trusted_;
  }
  // Decode-side acceleration (see BuildEfSelectDirs): the per-cell EF select
  // directory, or nullptr when this cell has none.
  [[nodiscard]] const uint32_t* EfSelectDirAt(size_t off_idx) const {
    if (off_idx + 1 >= ef_sel_off_.size()) {
      return nullptr;
    }
    if (ef_sel_off_[off_idx] == ef_sel_off_[off_idx + 1]) {
      return nullptr;
    }
    return ef_sel_flat_.data() + ef_sel_off_[off_idx];
  }
  [[nodiscard]] FsstDecodeView FsstViewAt(size_t off_idx) const {
    FsstDecodeView view;
    if (fsst_meta_index_.size() < 2 || off_idx > fsst_meta_index_.size() - 2) {
      return view;
    }
    const uint32_t begin = fsst_meta_index_[off_idx];
    const uint32_t end = fsst_meta_index_[off_idx + 1];
    if (begin == end || begin >= fsst_meta_.size()) {
      return view;
    }
    const FsstDecodeMeta& meta = fsst_meta_[begin];
    if (!meta.valid) {
      return view;
    }
    view.meta = &meta;
    if (meta.symbol_count != 0 &&
        static_cast<size_t>(meta.symbol_index) + meta.symbol_count <=
            fsst_symbol_offsets_.size() &&
        static_cast<size_t>(meta.symbol_index) + meta.symbol_count <=
            fsst_symbol_lengths_.size()) {
      view.symbol_offsets = fsst_symbol_offsets_.data() + meta.symbol_index;
      view.symbol_lengths = fsst_symbol_lengths_.data() + meta.symbol_index;
    }
    return view;
  }
  [[nodiscard]] uint64_t Generation() const { return footer_.generation; }
  [[nodiscard]] size_t TotalRowCount() const {
    if (!rg_prefix_.empty()) {
      return static_cast<size_t>(rg_prefix_.back());
    }
    // Bound by row_group_count like every other read path: a hand-built
    // footer with extra row_counts entries would otherwise inflate the
    // total and weaken HashLookup's row-index bounds check.
    const size_t ngroups = footer_.row_counts.size() < footer_.row_group_count
                               ? footer_.row_counts.size()
                               : footer_.row_group_count;
    size_t total = 0;
    for (size_t g = 0; g < ngroups; ++g) {
      total += footer_.row_counts[g];
    }
    return total;
  }
  [[nodiscard]] StatusOr<Row> ReadRow(size_t global_row_index,
                                      const Schema& sc) const;
  Status ReadRowInto(size_t global_row_index, const Schema& sc,
                     Row* row_out) const;
  // Sequential-scan variant of ReadRowInto: `cache` (owned by the reader,
  // e.g. FullScanIterator) holds one row group of decoded varchar values per
  // column, so restart-chain schemes (kFrontCode) decode each entry once per
  // sweep instead of replaying its restart block for every row. On any
  // decode anomaly the cache stays invalid and rows fall back to the
  // per-row decoder (fail closed).
  struct VarcharGroupCache {
    uint32_t group = static_cast<uint32_t>(-1);
    bool valid = false;      // by_col holds the decoded values of `group`
    uint32_t rows_seen = 0;  // rows served for `group` (fill amortization)
    std::array<uint64_t, 16> column_mask{};
    std::vector<uint64_t> wide_column_mask;
    bool projection_active = false;
    std::vector<std::vector<std::string>> by_col;

    // Sequential-scan column cursors: everything the per-row decode loop
    // needs for one row group, resolved once when the reader enters the
    // group (per-row group resolution, data-area clamp, footer bounds and
    // offset arithmetic are all row-group-invariant). Kind semantics mirror
    // the legacy per-row branches exactly; a resolution anomaly leaves
    // seq_ready=false and rows keep taking the legacy path (fail closed).
    struct SeqCol {
      // 0 = unselected (projection): store null; 1 = decoded-int cache hit;
      // 2 = int/double via region decoder; 3 = varchar via region decoder.
      uint8_t kind = 0;
      bool is_double = false;
      // The null test runs per row only when the column's bitmap slice has
      // at least one set byte: an all-zero bitmap is pre-collapsed here so
      // null-free columns skip the test entirely.
      bool has_null_bits = false;
      const uint8_t* null_bits = nullptr;  // column's bitmap base
      const int64_t* vals = nullptr;       // kind 1: decoded_int_cache_ span
      uint32_t span = 0;                   // kind 1: rows available
      const char* region = nullptr;        // kinds 2/3
      const char* region_end = nullptr;    // kinds 2/3
      Scheme scheme{};                     // kinds 2/3
      uint32_t prm = 0;                    // kinds 2/3
      size_t off_idx = 0;                // kinds 2/3: cell index (Ef/Fsst dirs)
      const uint32_t* ef_dir = nullptr;  // kind 2: EF select directory
      FsstDecodeView fsst;               // kind 3: FSST symbol directory
      // Sequential-walk state for kind 2 columns in scheme kD2Pfor: the
      // region parse is hoisted to resolve time and the running (v, sd)
      // prefix-sum pair is carried across rows, so a loc == prev+1 serve
      // costs two adds instead of a checkpoint replay walk. The struct type
      // lives in the .cpp's anonymous namespace, so the parsed fields are
      // kept flat here and re-aggregated per step.
      bool d2p_ok = false;     // region parsed at resolve; cursor may advance
      bool d2p_armed = false;  // (d2p_val, d2p_sd) == (v[d2p_loc], sd[d2p_loc])
      uint32_t d2p_loc = 0;
      uint64_t d2p_val = 0;
      uint64_t d2p_sd = 0;
      const char* d2p_ckpt = nullptr;
      const char* d2p_inner = nullptr;
      uint32_t d2p_n = 0;
      uint32_t d2p_ncp = 0;
    };
    bool seq_ready = false;  // seq_cols resolved for seq_group
    // Single not-taken branch per row while any deferred fill is still
    // outstanding (varchar group cache and/or the int bulk decode); both
    // share the kVarcharCacheMinRows amortization gate.
    bool seq_fill_pending = false;
    // Iterator-owned bulk decode of kind-2 int/double columns: flipped to
    // kind 1 once per group under the same rows_seen amortization gate as
    // the varchar fill. A column whose bulk decode fails keeps kind 2 and
    // keeps taking Int64AtFast per row (fail closed).
    bool seq_int_filled = false;
    std::vector<std::vector<int64_t>> int_by_col;
    uint32_t seq_group = static_cast<uint32_t>(-1);
    size_t seq_lo = 0;    // global row index of the group's first row
    size_t seq_hi = 0;    // exclusive end (seq_lo + group rows)
    uint16_t seq_rc = 0;  // row_counts of seq_group (Int64AtFast argument)
    std::vector<SeqCol> seq_cols;

    void SelectColumns(std::span<const slot_t> columns) {
      column_mask.fill(0);
      wide_column_mask.clear();
      projection_active = true;
      for (slot_t column : columns) {
        if (column < column_mask.size() * 64U) {
          column_mask[column / 64U] |= uint64_t{1} << (column % 64U);
        } else {
          const size_t word = (column / 64U) - column_mask.size();
          if (word >= wide_column_mask.size()) {
            wide_column_mask.resize(word + 1);
          }
          wide_column_mask[word] |= uint64_t{1} << (column % 64U);
        }
      }
    }

    [[nodiscard]] bool IsColumnSelected(slot_t column) const {
      if (!projection_active) {
        return true;
      }
      const size_t word = column / 64U;
      const uint64_t bit = uint64_t{1} << (column % 64U);
      if (word < column_mask.size()) {
        return (column_mask[word] & bit) != 0;
      }
      const size_t wide_word = word - column_mask.size();
      return wide_word < wide_column_mask.size() &&
             (wide_column_mask[wide_word] & bit) != 0;
    }
  };
  Status ReadRowIntoCached(size_t global_row_index, const Schema& sc,
                           VarcharGroupCache* cache, Row* row_out) const;
  // Three-state point lookup by the key column's int64 value. Returns
  // kSuccess with *row_index_out set to the global row index, or kNotExists.
  // (Merged chunks never contain tombstones, so kDeleted is never returned
  // here.) Duplicate keys resolve to the FIRST row. Hash index first, then
  // zone-map group pruning + binary search; chunks without zone maps fall
  // back to scanning every group. Requires kInt64 key columns; other key
  // types return kNotImplemented. Hand-built footers with corrupt offsets
  // yield kCorruption.
  [[nodiscard]] Status PointLookup(int64_t key_value, const Schema& sc,
                                   size_t* row_index_out,
                                   WorkloadTracker* workload = nullptr) const;
  // Convenience: PointLookup + ReadRow.
  [[nodiscard]] StatusOr<Row> ReadRowByKey(int64_t key_value,
                                           const Schema& sc) const;
  // Lower-bound seek for range scans: smallest global row index whose key
  // column int64 value is >= key_value. kNotExists when every stored key is
  // smaller than key_value (the range is empty). Requires kInt64 keys.
  [[nodiscard]] Status LowerBoundIndex(
      int64_t key_value, const Schema& sc, size_t* index_out,
      WorkloadTracker* workload = nullptr) const;
  [[nodiscard]] StatusOr<Value> ReadColumnValue(size_t global_row_index,
                                                slot_t col_idx,
                                                const Schema& sc) const;

  // --- Vectorized scan engine (compressed-data processing) ---
  // Decode the entire int64 column of row group `g` into `out` (must hold
  // row_counts[g] entries). Uses block (AVX2 where supported) unpacking for the
  // hot encodings and falls back to the verified single-value decoder
  // otherwise. Throws std::out_of_range for an out-of-range group/column
  // (hand-built footers); empty groups (rc == 0) decode to zero rows.
  void DecodeInt64ColumnRg(slot_t col, uint32_t g, int64_t* out) const;

  // The encoder's per-scheme linear decode cost, normalized to PLAIN = 1.0
  // (the unit Interface.md §6.1 defines as "compare one PLAIN int64 against
  // one literal"). The same table drives the adaptive encoder's choice and
  // the fragment IR's estimate, so the two can never disagree about what a
  // scheme costs to expand. The cost of a COMPRESSED-DOMAIN kernel (a count
  // that never expands the column) is not in this table: it is priced by the
  // kernel itself, not by the expansion.
  [[nodiscard]] static double SchemeDecodeCost(Scheme s);

  // True when the cell at (row group g, column c, local row index) holds
  // NULL. The null-bitmap section's layout (including the uniform-group
  // compaction) is internal to the chunk; this accessor keeps the offset
  // walk in one place so callers outside the chunk do not have to
  // re-derive it. Returns false for a chunk without nulls and for a
  // hand-built footer whose bitmap section is too short to answer.
  [[nodiscard]] bool IsCellNull(uint32_t g, slot_t c, uint32_t local) const;

  // True when the cell (row group g, column c) contains at least one NULL.
  // The zone maps cover non-null values only, so a cell with nulls
  // overlaps any range that contains 0 (a NULL decodes as raw 0).
  [[nodiscard]] bool CellHasNull(uint32_t g, slot_t c) const;

  // The null bitmap of the cell (row group g, column c): ceil(rows/8)
  // bytes, MSB-first, bit i set = row i of the group is NULL. Nullptr when
  // the cell provably has no nulls.
  //
  // A per-row caller MUST hoist this pointer out of the row loop: the
  // single-cell accessor (IsCellNull) re-derives the bitmap's offset by
  // walking every preceding row group, so calling it per row is
  // O(row_groups) per row -- quadratic over a chunk.
  [[nodiscard]] const uint8_t* CellNullBits(uint32_t g, slot_t c) const;

  // Build a PAX chunk from a sorted run of rows (ascending by key). The rows
  // are read via the iterator and re-encoded into column-contiguous row
  // groups. Invalid key order or corrupt row payload returns kCorrupt.
  static StatusOr<PaxChunk> BuildFromSortedRun(const SortedRun& run,
                                               const Schema& sc,
                                               bool compress = false);

  // Standalone structural validation of serialized chunk bytes. Performs
  // the full footer walk with overflow-safe arithmetic, sanity gates on
  // every count/index, and (format_version >= 2) CRC32C verification of
  // both the data region and the footer itself. Returns kSuccess or
  // kCorruption; never throws, never reads out of bounds.
  [[nodiscard]] static Status ValidateChunkBytes(
      const std::vector<char>& bytes);

  // Reconstruct a queryable chunk from its self-describing wire image.
  // Structural bounds and both CRCs are verified before any footer field is
  // materialized. This standalone wire decoder is currently exercised by fuzz
  // harnesses; database persistence does not use a separate pack-file path;
  // callers never need to retain an out-of-band Footer alongside the bytes.
  [[nodiscard]] static StatusOr<PaxChunk> Deserialize(std::vector<char> bytes);

  // Fused scan + aggregate over the chunk. Decodes only the columns referenced
  // by `plan` (plus those needed to evaluate its predicates), applies all
  // predicates as a conjoined filter (with per-row-group zone-map pruning), and
  // accumulates the requested aggregate terms directly from decoded int64
  // buffers -- no per-row Row materialization. When the plan has no predicates
  // and a single plain-column term, a metadata fast-path (rg_sums) is used.
  // `parallel` splits row groups across a thread pool; `scan_sweeps` repeats
  // the scan to model steady-state throughput. Returns the sum (as __int128 to
  // avoid overflow on large decimal products) and the matched tuple count.
  // The kernel decodes every referenced column as int64; passing `sc` opts
  // into a schema type gate that rejects plans touching a non-int64 (or
  // out-of-range) column with an empty AggResult, instead of decoding
  // foreign bytes as if they were int64. Null `sc` keeps the old
  // caller-knows-best behavior.
  static AggResult ScanAggregate(const PaxChunk& chunk, const AggPlan& plan,
                                 bool parallel = false, int nthreads = 0,
                                 uint32_t scan_sweeps = 1,
                                 const Schema* sc = nullptr,
                                 WorkloadTracker* workload = nullptr);
  friend std::ostream& operator<<(std::ostream& o, const PaxChunk& c);

 private:
  Status ReadRowIntoImpl(size_t global_row_index, const Schema& sc,
                         VarcharGroupCache* cache, Row* row_out) const;
  // Resolves the sequential-scan column cursors (VarcharGroupCache::SeqCol)
  // for row group `g`. Returns kSuccess with seq_ready=true, or a failure
  // status (hostile footer) with seq_ready left false; the resolve-failure
  // status propagates to the caller so corrupt chunks fail fast, exactly like
  // the legacy per-row bounds check would on first touch.
  Status ResolveSeqCursors(uint32_t g, const Schema& sc,
                           VarcharGroupCache* cache) const;

  std::vector<char> data_;
  // Compressed wire form of the column-data region (empty unless the chunk was
  // built with compression enabled). Kept alongside the plaintext `data_` so
  // the scan path is unchanged while the storage/IO size win is still
  // measurable.
  std::vector<char> compressed_;
  bool is_compressed_{false};
  bool bp128_metadata_trusted_{false};
  bool chimp128_metadata_trusted_{false};
  bool alp_rd_tans_metadata_trusted_{false};
  bool patas_metadata_trusted_{false};
  bool fastpfor_metadata_trusted_{false};
  bool deltafastpfor_metadata_trusted_{false};
  Footer footer_;
  // Cumulative row counts: rg_prefix_[i] = rows before row group i, with
  // rg_prefix_.back() = total row count. Populated once per construction
  // (chunks are immutable afterwards, so no synchronization is needed) and
  // used by the per-row read paths to resolve a global row index in
  // O(log row_group_count) instead of walking every group. Empty when the
  // footer is inconsistent (row_counts shorter than row_group_count): the
  // read paths then report kCorruption exactly as before.
  void BuildRowCountPrefix();
  void BuildNullBitmapSummary();
  // Decode-side select directory for Elias-Fano regions, built once per
  // construction (chunks are immutable afterwards) and never serialized. For
  // every (row group, column) cell encoded kEliasFano it stores the bit
  // position of every 8th set bit of that region's upper bitmap, so the
  // per-row decode jumps straight to the sample instead of re-scanning the
  // bitmap from word 0 on every access (SelectBit was O(bitmap words) per
  // cell and dominated full-scan profiles). Cells without a directory (not
  // Elias-Fano, zero rows, or bounds that cannot be established for a
  // hand-built footer) fall back to the stateless SelectBit.
  void BuildEfSelectDirs();
  void BuildFsstDirs();
  void BuildDecodedIntCache();
  // Resolves a global row index to (row group, local index).
  // Returns 0 on success, 1 when the prefix is unavailable (hostile footer),
  // 2 when the row index is past the end of the chunk.
  int ResolveRowGroup(size_t global_row_index, uint32_t* g_out,
                      uint32_t* local_out) const;
  std::vector<uint64_t> rg_prefix_;
  // Flat select-sample storage: ef_sel_flat_[ef_sel_off_[i] ..
  // ef_sel_off_[i+1]) holds the samples of cell i (empty span = no
  // directory). Sample j of a cell is the position of its (8j)-th set bit.
  std::vector<uint32_t> ef_sel_off_;
  std::vector<uint32_t> ef_sel_flat_;
  std::vector<uint32_t> fsst_meta_index_;
  std::vector<FsstDecodeMeta> fsst_meta_;
  std::vector<uint32_t> fsst_symbol_offsets_;
  std::vector<uint8_t> fsst_symbol_lengths_;
  std::vector<int64_t> decoded_int_cache_;
  std::vector<size_t> decoded_int_offsets_;
  bool decoded_int_cache_ready_{false};
};

// Metadata-only range aggregate across multiple chunks whose key ranges are
// pairwise disjoint and fully inside [lo,hi]. Exact SUM(column) + COUNT(*)
// over [lo,hi] from per-chunk stats; returns false (fall back to scan) on any
// overlap or partial boundary. `max_gen` is an MVCC read version: chunks with
// generation > max_gen are invisible (UINT64_MAX = no filter).
bool MetadataRangeAggregate(const std::vector<const PaxChunk*>& chunks,
                            const Schema& sc, int64_t lo, int64_t hi,
                            slot_t col, int64_t* sum_out, uint64_t* count_out,
                            uint64_t max_gen = UINT64_MAX);

// Vectorized, late-materialization column scan over a PAX chunk. Decodes one
// row group at a time into a native int64 buffer and applies `decoded OP lit`
// over that buffer. Uses per-(row-group,column) zone maps to skip row groups
// that cannot contain a match (no decode) and, when the whole group provably
// satisfies the predicate, folds in precomputed partial sums without decoding.
enum class ScanOp { kEq, kLe, kLt, kGe, kGt };

struct ScanResult {
  uint64_t hit_count = 0;
  int64_t hit_sum = 0;  // sum of matching decoded values
};

// `col` is decoded as int64; pass `sc` to gate the call on that column's
// declared type (empty ScanResult on a non-int64 column) instead of
// silently decoding foreign region bytes.
ScanResult VectorizedScanColumn(const PaxChunk& chunk, slot_t col, ScanOp op,
                                int64_t lit, bool parallel = false,
                                int nthreads = 0, uint32_t scan_sweeps = 1,
                                const Schema* sc = nullptr,
                                WorkloadTracker* workload = nullptr);

// Compressed-domain IN-list scan: `value IN (lits...)` evaluated once over
// the column. Literals are deduplicated (set semantics — a duplicate entry
// does not double-count a row). Dictionary-family schemes translate the
// literal set into a code pass-table once per cell and scan the packed
// codes once; run-structured schemes fold per run; everything else decodes
// the group once and binary-searches the sorted literals per row. Same
// null-as-0, zone-prune and inverted-index semantics as
// VectorizedScanColumn.
ScanResult VectorizedScanColumnInList(const PaxChunk& chunk, slot_t col,
                                      std::span<const int64_t> lits,
                                      bool parallel = false, int nthreads = 0,
                                      const Schema* sc = nullptr,
                                      WorkloadTracker* workload = nullptr);

// Metadata-only aggregate: returns true if the whole-column SUM/COUNT could be
// answered from footer statistics without decoding (int64 columns only).
bool FastColumnSum(const PaxChunk& chunk, slot_t col, int64_t* sum_out,
                   uint64_t* count_out);

// --- Fused compressed-execution aggregate (TPC-H style pushdown) ------------
// A single pass over the chunk that combines multi-column predicates, a
// selection (AND of predicates) and an arithmetic aggregate expression, all
// without materializing rows. Decimal columns are represented as scaled int64
// (see Column::Scale); literals in predicates and the expression output scale
// are supplied by the caller, so all arithmetic stays in the integer domain.
//
// Predicates are ANDed. The aggregate expression is a sum of monomials, each a
// signed coefficient times the product of one or more column values; this is
// enough to express every TPC-H revenue expression, e.g.
//   SUM(l_extendedprice * l_discount)
//       -> one term { coeff=1, cols={ext, disc} }
//   SUM(l_extendedprice * (1 - l_discount))
//       -> two terms { +1, cols={ext} } and { -1, cols={ext, disc} }
struct Predicate {
  slot_t col;
  ScanOp op;
  int64_t lit;  // literal already in the column's scaled integer domain
};

struct AggTerm {
  int64_t coeff = 1;         // signed fixed-point coefficient
  std::vector<slot_t> cols;  // product of these columns' decoded values
};

struct AggPlan {
  std::vector<Predicate> preds;  // ANDed; empty => no filter
  std::vector<AggTerm> terms;    // result = Σ coeff * Π value(cols)
  int out_scale = 0;             // decimal scale of the reported sum
  bool count_only = false;       // COUNT(*) (ignore terms)
};

struct AggResult {
  __extension__ __int128 sum = 0;  // Σ expression, in out_scale units (128-bit:
                                   // TPC-H revenue over SF100 exceeds 64 bits)
  uint64_t count = 0;              // matching rows
};

// Single-column reduction (M3 pushdown): MIN / MAX / COUNT / SUM /
// APPROX_COUNT_DISTINCT over the rows that survive the (optional) predicates.
// Unpredicated COUNT and APPROX_COUNT_DISTINCT use footer metadata. With
// predicates, COUNT evaluates selection masks and APPROX_COUNT_DISTINCT counts
// exact distinct selected values by decoding; MIN/MAX use zone maps when
// unpredicated and fall back
// to a decode over the selected rows otherwise.
enum class AggOp { kSum, kMin, kMax, kCount, kApproxDistinct };

struct ReducePlan {
  AggOp op = AggOp::kCount;
  slot_t col = 0;                // column to reduce
  int64_t coeff = 1;             // multiplier for kSum
  std::vector<Predicate> preds;  // ANDed filter (empty => whole column)
  int out_scale = 0;             // informational (decimal scale of result)
};

struct ReduceResult {
  __extension__ __int128 sum = 0;  // kSum accumulator (128-bit)
  int64_t min_val = 0;             // kMin
  int64_t max_val = 0;             // kMax
  uint64_t count = 0;              // kCount / rows matched
  uint64_t approx_distinct = 0;    // kApproxDistinct (HLL estimate)
  bool valid = false;
};

// Fused filter + single-column reduce with predicate pushdown. Reuses the
// selection-vector machinery: unpredicated COUNT uses row counts and
// unpredicated APPROX_COUNT_DISTINCT reads HLL sketches. Predicates may decode
// for COUNT and require an exact decoded distinct set for
// APPROX_COUNT_DISTINCT. MIN/MAX use zone maps when possible. Every column
// referenced by `plan` (reduce col + predicate cols) is decoded as int64; pass
// `sc` to gate on the declared types and get an invalid ReduceResult on a
// non-int64 column instead of garbage numbers. NOTE: this kernel is SEQUENTIAL
// — the parallel/nthreads parameters are accepted for signature compatibility
// with VectorizedScanColumn / ScanAggregate but currently ignored
// (single-threaded execution).
ReduceResult ReduceColumn(const PaxChunk& chunk, const ReducePlan& plan,
                          bool parallel = false, int nthreads = 0,
                          uint32_t scan_sweeps = 1, const Schema* sc = nullptr,
                          WorkloadTracker* workload = nullptr);

// HyperLogLog approximate distinct count for an int64 column, combining the
// per-(row-group,column) 64-register sketches stored in the footer. Returns 0
// when no sketch is present.
uint64_t ApproxCountDistinct(const PaxChunk& chunk, slot_t col);

// Decode-free SUM of a bit-sliced int64 column region: Σ_b 2^b ·
// popcount(slice_b). Returns true only when the column region uses the
// ByteSlice scheme.
bool ByteSliceColumnSum(const PaxChunk& chunk, slot_t col, uint32_t g,
                        int64_t* sum_out);

bool PBitSColumnSum(const PaxChunk& chunk, slot_t col, uint32_t g,
                    int64_t* sum_out);

bool Chimp128ColumnCountSum(const PaxChunk& chunk, slot_t col,
                            PaxChunk::CompareOp op, double literal,
                            uint64_t* count_out, double* sum_out,
                            const Schema* sc = nullptr);

bool AlpRdTansColumnCountSum(const PaxChunk& chunk, slot_t col,
                             PaxChunk::CompareOp op, double literal,
                             uint64_t* count_out, double* sum_out,
                             const Schema* sc = nullptr);

bool PatasColumnCountSum(const PaxChunk& chunk, slot_t col,
                         PaxChunk::CompareOp op, double literal,
                         uint64_t* count_out, double* sum_out,
                         const Schema* sc = nullptr);

// Dictionary code histogram over one int64 DICT region: freq[d] counts rows
// with code d, freq[ncodes] counts clamped (corrupt high) codes. Nulls arrive
// as raw 0 codes like the decode path. Returns false when the region is not
// a validated int64 DICT cell (wrong scheme, bad widths, truncated dict).
bool DictCodeHistogram(const char* region, uint32_t n, uint32_t param,
                       const char* region_end, std::vector<uint64_t>* freq_out);

// Decode-free SUM over one int64 DICT region: Σ dict[d]·freq[d] in a single
// code pass (LowCardinality-style). Two's-complement truncation to 64 bits.
bool DictColumnSum(const char* region, uint32_t n, uint32_t param,
                   const char* region_end, int64_t* sum_out);

// Order-preserving dictionary (kOpDict) compressed-domain predicate fold.
// Maps `lit OP` to a rank window [lo_code, hi_code) once via binary search
// over the ascending dictionary, then folds the codes inside the window in a
// single unpack pass -- no dictionary evaluation, pass table, or per-row
// decode. `count_out` receives the matching-row count, `sum_out` (nullable)
// the two's-complement sum of the matching raw values. Corrupt high codes
// decode as raw 0 like every reader, so they participate exactly when 0
// satisfies the predicate (decided once per call). Returns false when the
// region is not a validated int64 OP-DICT cell (wrong header, truncated
// dict) so callers can fall back to the generic decode path.
bool OpDictRegionCountSum(const char* region, uint32_t n, uint32_t param,
                          const char* region_end, PaxChunk::CompareOp op,
                          int64_t lit, uint64_t* count_out, int64_t* sum_out);

// Run-length over dictionary codes (kRleDict) compressed-domain predicate
// fold. Evaluates `lit OP` once per dictionary entry into a pass table, then
// folds the RUN stream: each run contributes its length (count) and
// dict[code] * length (sum) without materializing any row. Corrupt high
// codes decode as raw 0 like every reader, so they participate exactly when
// 0 satisfies the predicate (decided once per call); rows past the run walk
// decode as raw 0 too and are folded the same way. Returns false when the
// region is not a validated int64 RLE-DICT cell (wrong header, truncated
// dict/run table) so callers can fall back to the generic decode path.
bool RleDictRegionCountSum(const char* region, uint32_t n, uint32_t param,
                           const char* region_end, PaxChunk::CompareOp op,
                           int64_t lit, uint64_t* count_out, int64_t* sum_out);

bool RleBpRegionCountSum(const char* region, uint32_t n, uint32_t param,
                         const char* region_end, PaxChunk::CompareOp op,
                         int64_t lit, uint64_t* count_out, int64_t* sum_out);

// Simple-8b (kSimple8b) compressed-domain predicate fold. Each word's
// selector fixes the value count and the per-word max offset, so a word
// whose [base, base+mask] value range cannot satisfy `lit OP` is skipped
// without decoding, and a zero-width word (sel 0/1) folds as one range set.
// Surviving words decode value-by-value (shift/mask, no branching on data).
// `count_out` receives the matching-row count; `sum_out` (nullable) the
// two's-complement sum of the matching raw values. Tail rows past the last
// word's coverage decode as raw 0 like Int64RegionAt and participate
// exactly when 0 satisfies the predicate. Returns false when the region is
// not a validated int64 SIMPLE-8B cell (bad param, truncated word table).
bool Simple8bRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* count_out, int64_t* sum_out);

bool Rle8RegionCountSum(const char* region, uint32_t n, uint32_t param,
                        const char* region_end, PaxChunk::CompareOp op,
                        int64_t literal, uint64_t* count_out, int64_t* sum_out);

bool RleDeltaRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t literal, uint64_t* count_out,
                            int64_t* sum_out);

// kRleIdx (scheme 49) compressed-domain predicate fold. The encoder gates on
// NON-DECREASING input, so the run values are sorted and every comparison maps
// to ONE contiguous run window whose endpoints are two bisections over those
// values (EQ is [lower_bound(lit), upper_bound(lit))). No run outside the
// window is touched: `count_out` is the restart-index difference
// row_before(last) - row_before(first), and `sum_out` (nullable) folds
// value*length over the window only. Returns false when the region is not a
// validated int64 RLE-IDX cell (bad run count, hostile restart table).
bool RleIdxRegionCountSum(const char* region, uint32_t n, uint32_t param,
                          const char* region_end, PaxChunk::CompareOp op,
                          int64_t literal, uint64_t* count_out,
                          int64_t* sum_out);

// Binary interpolative coding (kInterp) compressed-domain predicate fold
// over a NON-DECREASING region. The predicate maps to an inclusive value
// window once; each 128-row block whose checkpoint bracket misses the window
// contributes nothing without touching its bitstream, and a fully-inside
// bracket contributes its whole row count from the header (with `sum_out ==
// nullptr`; the sum needs the values, so blocks then walk). Boundary blocks
// answer from a tree walk -- one compare per node, no row materialization.
// Returns false when the region is not a validated int64 INTERP cell
// (header/param disagreement, broken bracket monotonicity) so callers can
// fall back to the generic decode path.
bool InterpRegionCountSum(const char* region, uint32_t n, uint32_t param,
                          const char* region_end, PaxChunk::CompareOp op,
                          int64_t lit, uint64_t* count_out, int64_t* sum_out);

bool DeltaPForRegionCountSum(const char* region, uint32_t n, uint32_t param,
                             const char* region_end, PaxChunk::CompareOp op,
                             int64_t lit, uint64_t* count_out,
                             int64_t* sum_out);

bool WaveletRRegionCountSum(const char* region, uint32_t n, uint32_t param,
                            const char* region_end, PaxChunk::CompareOp op,
                            int64_t lit, uint64_t* count_out, int64_t* sum_out);

bool QppRegionCountSum(const char* region, uint32_t n, uint32_t param,
                       const char* region_end, PaxChunk::CompareOp op,
                       int64_t literal, uint64_t* count_out, int64_t* sum_out);

bool Bp128RegionCountSum(const char* region, uint32_t n, uint32_t param,
                         const char* region_end, PaxChunk::CompareOp op,
                         int64_t literal, uint64_t* count_out,
                         int64_t* sum_out);

// Single-key GROUP BY SUM+COUNT over a whole chunk (both columns decoded as
// int64; pass `sc` to gate on declared int64 types, else an empty vector).
// DICT/RLE/CONST group columns intern to accumulators with zero per-row
// hashing or group-value decoding; the sum column folds via its own
// compressed path (CONST multiply, RLE run walk, DICT code lookup) and only
// falls back to a full decode for other schemes. Null group keys participate
// as raw 0, matching the WASM pax_groupby_sum pushdown and the ReduceColumn
// compressed paths. Entries are returned in first-seen group order.
struct GroupByEntry {
  int64_t key = 0;
  int64_t sum = 0;     // Σ sum-col values (two's-complement truncation)
  uint64_t count = 0;  // rows in the group
};

std::vector<GroupByEntry> GroupBySumColumn(const PaxChunk& chunk,
                                           slot_t group_col, slot_t sum_col,
                                           const Schema* sc = nullptr,
                                           WorkloadTracker* workload = nullptr);

// One entry of an N-key GROUP BY: the group key (one value per group
// column), the sum and the row count.
struct GroupByEntryN {
  std::vector<int64_t> key;
  int64_t sum = 0;
  uint64_t count = 0;
};

// GROUP BY SUM over N key columns (N >= 1). The one-key form above is the
// specialized kernel with the per-scheme folding (DICT interning, RLE run
// folding, CONST in O(1)); this form composes the group columns into a
// composite key and accumulates through a hash table, so it works for any
// number of columns.
//
// The group columns are decoded with the per-scheme decoder (the same one
// the vectorized scan uses), so a DICT or RLE group column costs one code
// expansion per row. The SUM column is decoded the same way: a CONST sum
// column therefore materializes one value per row instead of folding in
// O(1) -- correct, and the price is documented rather than hidden.
//
// The group key values are the raw int64 values (a NULL cell participates
// as raw 0, matching the decode path and the WASM pushdown). Entries are
// returned in first-seen group order. SUM wraps in two's-complement int64,
// like GroupBySumColumn. Invalid arguments or corrupt row groups return Status;
// no partial aggregate is returned on failure.
StatusOr<std::vector<GroupByEntryN>> GroupBySumColumns(
    const PaxChunk& chunk, const std::vector<slot_t>& group_cols,
    slot_t sum_col, const Schema* sc = nullptr,
    WorkloadTracker* workload = nullptr);

// ---- Varchar predicate pushdown ------------------------------------------
// Binary (byte-wise) predicate evaluation over one VARCHAR column without
// materializing rows. NULL cells decode as the EMPTY string (the encoder
// stores raw "" and the footer null bitmap marks them), so they participate
// in comparisons exactly like the ReadColumnValue/ReadRow decode path.
// Comparisons are std::string_view (unsigned-byte lexicographic); nothing
// locale-dependent ever enters the decision.
// The string comparison operators. kPrefix is "the value starts with the
// needle" (the needle must be a byte prefix of the value; the value may be
// longer). It is NOT the same as the range [needle, needle\0): a value
// whose byte after the prefix is 0x00 is a prefix match but falls outside
// that half-open range.
enum class StrScanOp { kEq, kNe, kLt, kLe, kGt, kGe, kPrefix };

// Rows in `chunk` whose decoded varchar value in `col` satisfies
// `value OP needle`. Non-varchar columns (when `sc` gates) and out-of-range
// or hostile footers yield 0 rather than throwing.
uint64_t VarcharFilterCount(const PaxChunk& chunk, slot_t col, StrScanOp op,
                            std::string_view needle, const Schema* sc = nullptr,
                            WorkloadTracker* workload = nullptr);

// Rows whose value lies in [lo, hi) — either side may be nullopt (unbounded).
// Half-open on purpose: prefix queries are [pre, pre\0) ... practically
// [pre, pre + 0xFF...] — half-open composes without adjacent-boundary traps.
uint64_t VarcharRangeCount(const PaxChunk& chunk, slot_t col,
                           std::optional<std::string_view> lo,
                           std::optional<std::string_view> hi,
                           const Schema* sc = nullptr,
                           WorkloadTracker* workload = nullptr);

// Lookup helpers over the in-memory footer (nullptr when absent).
const InvertedCell* FindInvertedCell(const PaxChunk::Footer& f, uint32_t g,
                                     slot_t c);
const InvertedPosting* FindInvertedPosting(const InvertedCell& cell,
                                           int64_t value);

// Decode-free counts from the inverted index. Each returns false (no output)
// when any row group lacks the indexed cell, so callers fall back to a scan;
// a present-but-empty posting match counts 0 (still true). Values are raw
// (NULL cells match key 0), matching the decode path and the WASM pushdown.
// InvertedInCount uses BAG semantics for `values` (mirroring repeated OR
// terms and the WASM kernel): a duplicated list entry counts its rows
// again; dedupe the list when SET semantics are wanted.
bool InvertedEqCount(const PaxChunk& chunk, slot_t col, int64_t value,
                     uint64_t* count_out);
bool InvertedInCount(const PaxChunk& chunk, slot_t col, const int64_t* values,
                     size_t nvalues, uint64_t* count_out);
bool InvertedAndCount(const PaxChunk& chunk, slot_t col_a, int64_t val_a,
                      slot_t col_b, int64_t val_b, uint64_t* count_out);

// Xor8 cell lookup (nullptr when the cell was not built) and membership test.
// Raw values (NULL cells as 0), matching the decode path. Member keys never
// report absent; non-members report present with probability ~1/256.
const XorCell* FindXorCell(const PaxChunk::Footer& f, uint32_t g, slot_t c);
bool XorCellMayContain(const XorCell& cell, int64_t value);

// Row-group pass mask for a point predicate: bit g set when group g may hold
// `value` in column `col` (missing cells pass safe). Lets hosts prune row
// groups before materializing anything.
std::vector<uint64_t> XorRgMask(const PaxChunk& chunk, slot_t col,
                                int64_t value);

// PGM prediction: model-estimated global row for `key` (floor, clamped to
// [0, total)). Within eps+1 rows of the true lower bound on well-formed
// chunks. Returns total (past-the-end) when the model is absent.
uint64_t PgmPredict(const PaxChunk::Footer& f, int64_t key, uint64_t total);

// Top-K entry: a qualifying (value, global row index) pair. ORDER BY col
// LIMIT k without materializing the column: row groups whose zone-map range
// cannot improve the current threshold are skipped unread (block-max
// pruning); the rest decode once into a bounded heap.
struct TopKEntry {
  int64_t value = 0;
  uint64_t row = 0;
};

// Whole-chunk Top-K over one int64 column (no predicates). `descending` picks
// the k largest (ORDER BY col DESC LIMIT k), otherwise the k smallest.
// Ties break by smaller global row index (stable, deterministic across
// runtimes). NULL cells participate as raw 0 like the decode path. Returns at
// most k entries sorted by (value, row) in the requested direction; fewer
// when the chunk holds fewer rows. Empty for out-of-range / non-int64
// (with `sc`) columns.
std::vector<TopKEntry> TopKColumn(const PaxChunk& chunk, slot_t col, size_t k,
                                  bool descending, const Schema* sc = nullptr,
                                  WorkloadTracker* workload = nullptr);
// Bit-packed per-row predicate result over a single row group. Bit i set means
// "row i passes every predicate". The scan path uses it to materialize
// aggregate columns only for the selected rows (late materialization), and the
// WASM pushdown surface can build/consume the same representation.
struct PaxSelectionVector {
  std::vector<uint64_t> bits;  // row_count/64 words, row-major
  uint64_t count = 0;          // number of set bits
};

// The ROW-EXTRACTION surface of the varchar predicates: the same predicate,
// answered as a selection mask over the chunk's rows instead of a count.
//
// The count kernels stay in the compressed domain (a Front-Code sorted
// region answers a range COUNT with two binary searches and never
// materializes a string). The selection path has to NAME the rows, so it
// enumerates them: the Front-Code sorted region still answers in the
// compressed domain (the matching rows are the contiguous index range the
// two binary searches bracket), and the other schemes walk the region's
// values.
//
// The mask is over the CHUNK's rows (row groups in order), the same space
// `ReadColumnValue` and `ReadRow` index. A NULL cell decodes as the EMPTY
// string, exactly like the count path, so it participates in the
// comparisons the same way.
[[nodiscard]] PaxSelectionVector VarcharFilterSelection(
    const PaxChunk& chunk, slot_t col, StrScanOp op, std::string_view needle,
    const Schema* sc = nullptr, WorkloadTracker* workload = nullptr);

[[nodiscard]] PaxSelectionVector VarcharRangeSelection(
    const PaxChunk& chunk, slot_t col, std::optional<std::string_view> lo,
    std::optional<std::string_view> hi, const Schema* sc = nullptr,
    WorkloadTracker* workload = nullptr);

// The row indices a selection marks, in ascending order. The extraction
// step of the "filter then project" pipeline: the mask is the compressed
// form, this is the row list.
[[nodiscard]] std::vector<uint32_t> SelectionRows(const PaxSelectionVector& sv);

// Fuse `preds` into a single selection bitmask over row group `g`. When
// `all_inside` is true (the caller has proven via zone maps that the whole
// group satisfies every predicate) or `preds` is empty, every row is selected
// without decoding any column.
PaxSelectionVector BuildSelectionVector(const PaxChunk& chunk,
                                        const std::vector<Predicate>& preds,
                                        uint32_t g, bool all_inside = false);

}  // namespace tinylamb

#endif  // TINYLAMB_PAX_CHUNK_HPP
