// S0 execution experiment only: no SQL semantics or production ABI.
// The same bounded, non-null kernels are compiled with clang -O2 for both
// native and memory64. Inputs are initialized once, outside timed regions.
using U64 = __UINT64_TYPE__;
using I64 = __INT64_TYPE__;
using U32 = __UINT32_TYPE__;
using U8 = unsigned char;

static U64 Hash(U64 key) { return key * 0x9e3779b97f4a7c15ULL; }

extern "C" I64 filter_sum(U64 column, U64 count, U64 literal, U64) {
  const auto* values = reinterpret_cast<const I64*>(column);
  I64 sum = 0;
  for (U64 i = 0; i < count; ++i) {
    sum += values[i] < static_cast<I64>(literal) ? values[i] : 0;
  }
  return sum;
}

// 17-bit FOR deltas, with eight zero padding bytes after the final value.
// Byte loads deliberately support unaligned source addresses on both targets.
extern "C" I64 packed_sum(U64 column, U64 count, U64 literal, U64) {
  const auto* bytes = reinterpret_cast<const U8*>(column);
  I64 sum = 0;
  for (U64 i = 0; i < count; ++i) {
    const U64 bit = i * 17;
    const U8* p = bytes + bit / 8;
    U64 word = 0;
    __builtin_memcpy(&word, p, sizeof(word));
    const I64 value = static_cast<I64>((word >> (bit % 8)) & 131071) - 65536;
    sum += value < static_cast<I64>(literal) ? value : 0;
  }
  return sum;
}

struct Entry {
  U64 key;
  U64 payload;
};

static const Entry* Find(const Entry* slots, U64 key) {
  U64 slot = Hash(key) >> 48;
  for (U64 steps = 0; steps != 65536; ++steps) {
    if (slots[slot].key == key) return slots + slot;
    if (slots[slot].key == 0) return nullptr;
    slot = (slot + 1) & 65535;
  }
  return nullptr;
}

struct Fact {
  U64 first_key;
  U64 second_key;
  I64 value;
};
static_assert(sizeof(Fact) == 24);

// Independent dimension keys and a stored measure, matching S0's P3 shape.
// No synthetic remainder/division is added to the measured aggregate loop.
extern "C" I64 probe_aggregate(U64 column, U64 count, U64 auxiliary,
                               U64 output) {
  const auto* facts = reinterpret_cast<const Fact*>(column);
  const auto* first = reinterpret_cast<const Entry*>(auxiliary);
  const auto* second = first + 65536;
  const auto* bloom = reinterpret_cast<const U64*>(second + 65536);
  auto* groups = reinterpret_cast<I64*>(output);
  for (U64 i = 0; i < 256; ++i) groups[i] = 0;
  I64 sum = 0;
  for (U64 i = 0; i < count; ++i) {
    const U64 key = facts[i].first_key;
    const U64 h = Hash(key) >> 44;
    if ((bloom[h / 64] & (U64{1} << (h % 64))) == 0) continue;
    if (Find(first, key) == nullptr) continue;
    const Entry* match = Find(second, facts[i].second_key);
    if (match == nullptr) continue;
    const I64 value = facts[i].value;
    groups[match->payload] += value;
    sum += value;
  }
  return sum;
}

#ifndef __wasm__
extern "C" void initialize(U64 column, U64 packed, U64 keys, U64 count,
                           U64 auxiliary) {
  auto* values = reinterpret_cast<I64*>(column);
  auto* bytes = reinterpret_cast<U8*>(packed);
  auto* fact = reinterpret_cast<Fact*>(keys);
  auto* first = reinterpret_cast<Entry*>(auxiliary);
  auto* second = first + 65536;
  auto* bloom = reinterpret_cast<U64*>(second + 65536);
  U64 random = 17;
  for (U64 i = 0; i < count; ++i) {
    random ^= random << 13;
    random ^= random >> 7;
    random ^= random << 17;
    const U64 delta = random & 131071;
    values[i] = static_cast<I64>(delta) - 65536;
    fact[i] = {(random >> 17) % 40000 + 1, (random >> 33) % 40000 + 1,
               values[i]};
    const U64 bit = i * 17;
    U64 word = delta << (bit % 8);
    for (U64 b = 0; b < 3; ++b) bytes[bit / 8 + b] |= (word >> (b * 8)) & 255;
  }
  for (U64 key = 1; key <= 30000; ++key) {
    U64 slot = Hash(key) >> 48;
    while (first[slot].key != 0) slot = (slot + 1) & 65535;
    first[slot] = {key, 0};
    const U64 h = Hash(key) >> 44;
    bloom[h / 64] |= U64{1} << (h % 64);
    if (key % 3 == 0) {
      slot = Hash(key) >> 48;
      while (second[slot].key != 0) slot = (slot + 1) & 65535;
      second[slot] = {key, key % 256};
    }
  }
}
#endif
