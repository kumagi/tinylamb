/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_DIGEST_HPP
#define TINYLAMB_DIGEST_HPP

// Self-contained MD5 / SHA-1 / SHA-256 / SHA-512 implementations backing the
// GoogleSQL hashing functions (MD5(), SHA1(), SHA256(), SHA512()).  Raw
// digests are returned as byte strings; TO_HEX() performs the encoding.

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace tinylamb::digest {

inline std::string ToHex(std::string_view raw) {
  static const char kDigits[] = "0123456789abcdef";
  std::string out;
  out.reserve(raw.size() * 2);
  for (const char c : raw) {
    const unsigned char b = static_cast<unsigned char>(c);
    out.push_back(kDigits[b >> 4]);
    out.push_back(kDigits[b & 0xF]);
  }
  return out;
}

// ---------------------------------------------------------------- MD5 ----
class Md5 {
 public:
  Md5() { Init(); }
  void Init() {
    a_ = 0x67452301;
    b_ = 0xefcdab89;
    c_ = 0x98badcfe;
    d_ = 0x10325476;
    total_ = 0;
    buffer_.clear();
  }
  void Update(std::string_view data) {
    total_ += data.size();
    size_t pos = 0;
    if (!buffer_.empty()) {
      const size_t take = std::min<size_t>(64 - buffer_.size(), data.size());
      buffer_.append(data.substr(0, take));
      pos = take;
      if (buffer_.size() == 64) {
        Block(buffer_.data());
        buffer_.clear();
      }
    }
    while (pos + 64 <= data.size()) {
      Block(data.data() + pos);
      pos += 64;
    }
    buffer_.append(data.substr(pos));
  }
  std::string Finalize() {
    const uint64_t bits = static_cast<uint64_t>(total_) * 8;
    std::string padding = "\x80";
    while ((buffer_.size() + padding.size()) % 64 != 56) {
      padding.push_back('\0');
    }
    Update(padding);
    for (int i = 0; i < 8; ++i) {
      buffer_.push_back(static_cast<char>((bits >> (8 * i)) & 0xFF));
    }
    Block(buffer_.data());
    buffer_.clear();
    std::string out(16, '\0');
    for (int i = 0; i < 4; ++i) {
      const size_t k = static_cast<size_t>(i);
      out[k] = static_cast<char>((a_ >> (8 * i)) & 0xFF);
      out[4 + k] = static_cast<char>((b_ >> (8 * i)) & 0xFF);
      out[8 + k] = static_cast<char>((c_ >> (8 * i)) & 0xFF);
      out[12 + k] = static_cast<char>((d_ >> (8 * i)) & 0xFF);
    }
    return out;
  }

 private:
  static uint32_t Rotl(uint32_t v, int s) { return (v << s) | (v >> (32 - s)); }
  void Block(const char* p) {
    static const uint32_t kK[64] = {
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a,
        0xa8304613, 0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
        0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340,
        0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
        0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8,
        0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
        0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
        0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92,
        0xffeff47d, 0x85845dd1, 0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
        0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391};
    static const int kS[64] = {
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
        5, 9,  14, 20, 5, 9,  14, 20, 5, 9,  14, 20, 5, 9,  14, 20,
        4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
        6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21};
    uint32_t m[16];
    for (size_t i = 0; i < 16; ++i) {
      m[i] = static_cast<uint8_t>(p[i * 4]) |
             (static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4 + 1])) << 8) |
             (static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4 + 2])) << 16) |
             (static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4 + 3])) << 24);
    }
    uint32_t a = a_, b = b_, c = c_, d = d_;
    for (int i = 0; i < 64; ++i) {
      uint32_t f;
      int g;
      if (i < 16) {
        f = (b & c) | (~b & d);
        g = i;
      } else if (i < 32) {
        f = (d & b) | (~d & c);
        g = (5 * i + 1) % 16;
      } else if (i < 48) {
        f = b ^ c ^ d;
        g = (3 * i + 5) % 16;
      } else {
        f = c ^ (b | ~d);
        g = (7 * i) % 16;
      }
      const uint32_t tmp = d;
      d = c;
      c = b;
      b = b + Rotl(a + f + kK[i] + m[g], kS[i]);
      a = tmp;
    }
    a_ += a;
    b_ += b;
    c_ += c;
    d_ += d;
  }
  uint32_t a_{}, b_{}, c_{}, d_{};
  size_t total_{0};
  std::string buffer_;
};

inline std::string Md5Digest(std::string_view data) {
  Md5 h;
  h.Update(data);
  return h.Finalize();
}

// --------------------------------------------------------------- SHA1 ----
class Sha1 {
 public:
  Sha1() { Init(); }
  void Init() {
    h_[0] = 0x67452301;
    h_[1] = 0xEFCDAB89;
    h_[2] = 0x98BADCFE;
    h_[3] = 0x10325476;
    h_[4] = 0xC3D2E1F0;
    total_ = 0;
    buffer_.clear();
  }
  void Update(std::string_view data) {
    total_ += data.size();
    size_t pos = 0;
    if (!buffer_.empty()) {
      const size_t take = std::min<size_t>(64 - buffer_.size(), data.size());
      buffer_.append(data.substr(0, take));
      pos = take;
      if (buffer_.size() == 64) {
        Block(buffer_.data());
        buffer_.clear();
      }
    }
    while (pos + 64 <= data.size()) {
      Block(data.data() + pos);
      pos += 64;
    }
    buffer_.append(data.substr(pos));
  }
  std::string Finalize() {
    const uint64_t bits = static_cast<uint64_t>(total_) * 8;
    std::string padding = "\x80";
    while ((buffer_.size() + padding.size()) % 64 != 56) {
      padding.push_back('\0');
    }
    Update(padding);
    for (int i = 7; i >= 0; --i) {
      buffer_.push_back(static_cast<char>((bits >> (8 * i)) & 0xFF));
    }
    Block(buffer_.data());
    buffer_.clear();
    std::string out(20, '\0');
    for (size_t w = 0; w < 5; ++w) {
      for (size_t i = 0; i < 4; ++i) {
        out[w * 4 + i] = static_cast<char>((h_[w] >> (24 - 8 * i)) & 0xFF);
      }
    }
    return out;
  }

 private:
  static uint32_t Rotl(uint32_t v, int s) { return (v << s) | (v >> (32 - s)); }
  void Block(const char* p) {
    uint32_t w[80];
    for (size_t i = 0; i < 16; ++i) {
      w[i] = (static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4])) << 24) |
             (static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4 + 1])) << 16) |
             (static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4 + 2])) << 8) |
             static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4 + 3]));
    }
    for (size_t i = 16; i < 80; ++i) {
      w[i] = Rotl(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }
    uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3], e = h_[4];
    for (int i = 0; i < 80; ++i) {
      uint32_t f, k;
      if (i < 20) {
        f = (b & c) | (~b & d);
        k = 0x5A827999;
      } else if (i < 40) {
        f = b ^ c ^ d;
        k = 0x6ED9EBA1;
      } else if (i < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8F1BBCDC;
      } else {
        f = b ^ c ^ d;
        k = 0xCA62C1D6;
      }
      const uint32_t tmp = Rotl(a, 5) + f + e + k + w[i];
      e = d;
      d = c;
      c = Rotl(b, 30);
      b = a;
      a = tmp;
    }
    h_[0] += a;
    h_[1] += b;
    h_[2] += c;
    h_[3] += d;
    h_[4] += e;
  }
  uint32_t h_[5]{};
  size_t total_{0};
  std::string buffer_;
};

inline std::string Sha1Digest(std::string_view data) {
  Sha1 h;
  h.Update(data);
  return h.Finalize();
}

// ------------------------------------------------------------- SHA256 ----
class Sha256 {
 public:
  Sha256() { Init(); }
  void Init() {
    h_[0] = 0x6a09e667;
    h_[1] = 0xbb67ae85;
    h_[2] = 0x3c6ef372;
    h_[3] = 0xa54ff53a;
    h_[4] = 0x510e527f;
    h_[5] = 0x9b05688c;
    h_[6] = 0x1f83d9ab;
    h_[7] = 0x5be0cd19;
    total_ = 0;
    buffer_.clear();
  }
  void Update(std::string_view data) {
    total_ += data.size();
    size_t pos = 0;
    if (!buffer_.empty()) {
      const size_t take = std::min<size_t>(64 - buffer_.size(), data.size());
      buffer_.append(data.substr(0, take));
      pos = take;
      if (buffer_.size() == 64) {
        Block(buffer_.data());
        buffer_.clear();
      }
    }
    while (pos + 64 <= data.size()) {
      Block(data.data() + pos);
      pos += 64;
    }
    buffer_.append(data.substr(pos));
  }
  std::string Finalize() {
    const uint64_t bits = static_cast<uint64_t>(total_) * 8;
    std::string padding = "\x80";
    while ((buffer_.size() + padding.size()) % 64 != 56) {
      padding.push_back('\0');
    }
    Update(padding);
    for (int i = 7; i >= 0; --i) {
      buffer_.push_back(static_cast<char>((bits >> (8 * i)) & 0xFF));
    }
    Block(buffer_.data());
    buffer_.clear();
    std::string out(32, '\0');
    for (size_t w = 0; w < 8; ++w) {
      for (size_t i = 0; i < 4; ++i) {
        out[w * 4 + i] = static_cast<char>((h_[w] >> (24 - 8 * i)) & 0xFF);
      }
    }
    return out;
  }

 private:
  static uint32_t Rotr(uint32_t v, int s) { return (v >> s) | (v << (32 - s)); }
  void Block(const char* p) {
    static const uint32_t kK[64] = {
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
        0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
        0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
        0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
        0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
        0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
        0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
        0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};
    uint32_t w[64];
    for (size_t i = 0; i < 16; ++i) {
      w[i] = (static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4])) << 24) |
             (static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4 + 1])) << 16) |
             (static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4 + 2])) << 8) |
             static_cast<uint32_t>(static_cast<uint8_t>(p[i * 4 + 3]));
    }
    for (size_t i = 16; i < 64; ++i) {
      const uint32_t s0 =
          Rotr(w[i - 15], 7) ^ Rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
      const uint32_t s1 =
          Rotr(w[i - 2], 17) ^ Rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
      w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3];
    uint32_t e = h_[4], f = h_[5], g = h_[6], h = h_[7];
    for (int i = 0; i < 64; ++i) {
      const uint32_t s1 = Rotr(e, 6) ^ Rotr(e, 11) ^ Rotr(e, 25);
      const uint32_t ch = (e & f) ^ (~e & g);
      const uint32_t t1 = h + s1 + ch + kK[i] + w[i];
      const uint32_t s0 = Rotr(a, 2) ^ Rotr(a, 13) ^ Rotr(a, 22);
      const uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
      const uint32_t t2 = s0 + maj;
      h = g;
      g = f;
      f = e;
      e = d + t1;
      d = c;
      c = b;
      b = a;
      a = t1 + t2;
    }
    h_[0] += a;
    h_[1] += b;
    h_[2] += c;
    h_[3] += d;
    h_[4] += e;
    h_[5] += f;
    h_[6] += g;
    h_[7] += h;
  }
  uint32_t h_[8]{};
  size_t total_{0};
  std::string buffer_;
};

inline std::string Sha256Digest(std::string_view data) {
  Sha256 h;
  h.Update(data);
  return h.Finalize();
}

// ------------------------------------------------------------- SHA512 ----
class Sha512 {
 public:
  Sha512() { Init(); }
  void Init() {
    h_[0] = 0x6a09e667f3bcc908ULL;
    h_[1] = 0xbb67ae8584caa73bULL;
    h_[2] = 0x3c6ef372fe94f82bULL;
    h_[3] = 0xa54ff53a5f1d36f1ULL;
    h_[4] = 0x510e527fade682d1ULL;
    h_[5] = 0x9b05688c2b3e6c1fULL;
    h_[6] = 0x1f83d9abfb41bd6bULL;
    h_[7] = 0x5be0cd19137e2179ULL;
    total_ = 0;
    buffer_.clear();
  }
  void Update(std::string_view data) {
    total_ += data.size();
    size_t pos = 0;
    if (!buffer_.empty()) {
      const size_t take = std::min<size_t>(128 - buffer_.size(), data.size());
      buffer_.append(data.substr(0, take));
      pos = take;
      if (buffer_.size() == 128) {
        Block(buffer_.data());
        buffer_.clear();
      }
    }
    while (pos + 128 <= data.size()) {
      Block(data.data() + pos);
      pos += 128;
    }
    buffer_.append(data.substr(pos));
  }
  std::string Finalize() {
    const uint64_t bits = static_cast<uint64_t>(total_) * 8;
    std::string padding = "\x80";
    while ((buffer_.size() + padding.size()) % 128 != 112) {
      padding.push_back('\0');
    }
    Update(padding);
    // SHA-512 appends a 128-bit length field: eight zero bytes followed by
    // the 64-bit bit count.
    for (int i = 0; i < 8; ++i) {
      buffer_.push_back('\0');
    }
    for (int i = 7; i >= 0; --i) {
      buffer_.push_back(static_cast<char>((bits >> (8 * i)) & 0xFF));
    }
    Block(buffer_.data());
    buffer_.clear();
    std::string out(64, '\0');
    for (size_t w = 0; w < 8; ++w) {
      for (size_t i = 0; i < 8; ++i) {
        out[w * 8 + i] = static_cast<char>((h_[w] >> (56 - 8 * i)) & 0xFF);
      }
    }
    return out;
  }

 private:
  static uint64_t Rotr(uint64_t v, int s) { return (v >> s) | (v << (64 - s)); }
  void Block(const char* p) {
    static const uint64_t kK[80] = {
        0x428a2f98d728ae22ULL, 0x7137449123ef65cdULL, 0xb5c0fbcfec4d3b2fULL,
        0xe9b5dba58189dbbcULL, 0x3956c25bf348b538ULL, 0x59f111f1b605d019ULL,
        0x923f82a4af194f9bULL, 0xab1c5ed5da6d8118ULL, 0xd807aa98a3030242ULL,
        0x12835b0145706fbeULL, 0x243185be4ee4b28cULL, 0x550c7dc3d5ffb4e2ULL,
        0x72be5d74f27b896fULL, 0x80deb1fe3b1696b1ULL, 0x9bdc06a725c71235ULL,
        0xc19bf174cf692694ULL, 0xe49b69c19ef14ad2ULL, 0xefbe4786384f25e3ULL,
        0x0fc19dc68b8cd5b5ULL, 0x240ca1cc77ac9c65ULL, 0x2de92c6f592b0275ULL,
        0x4a7484aa6ea6e483ULL, 0x5cb0a9dcbd41fbd4ULL, 0x76f988da831153b5ULL,
        0x983e5152ee66dfabULL, 0xa831c66d2db43210ULL, 0xb00327c898fb213fULL,
        0xbf597fc7beef0ee4ULL, 0xc6e00bf33da88fc2ULL, 0xd5a79147930aa725ULL,
        0x06ca6351e003826fULL, 0x142929670a0e6e70ULL, 0x27b70a8546d22ffcULL,
        0x2e1b21385c26c926ULL, 0x4d2c6dfc5ac42aedULL, 0x53380d139d95b3dfULL,
        0x650a73548baf63deULL, 0x766a0abb3c77b2a8ULL, 0x81c2c92e47edaee6ULL,
        0x92722c851482353bULL, 0xa2bfe8a14cf10364ULL, 0xa81a664bbc423001ULL,
        0xc24b8b70d0f89791ULL, 0xc76c51a30654be30ULL, 0xd192e819d6ef5218ULL,
        0xd69906245565a910ULL, 0xf40e35855771202aULL, 0x106aa07032bbd1b8ULL,
        0x19a4c116b8d2d0c8ULL, 0x1e376c085141ab53ULL, 0x2748774cdf8eeb99ULL,
        0x34b0bcb5e19b48a8ULL, 0x391c0cb3c5c95a63ULL, 0x4ed8aa4ae3418acbULL,
        0x5b9cca4f7763e373ULL, 0x682e6ff3d6b2b8a3ULL, 0x748f82ee5defb2fcULL,
        0x78a5636f43172f60ULL, 0x84c87814a1f0ab72ULL, 0x8cc702081a6439ecULL,
        0x90befffa23631e28ULL, 0xa4506cebde82bde9ULL, 0xbef9a3f7b2c67915ULL,
        0xc67178f2e372532bULL, 0xca273eceea26619cULL, 0xd186b8c721c0c207ULL,
        0xeada7dd6cde0eb1eULL, 0xf57d4f7fee6ed178ULL, 0x06f067aa72176fbaULL,
        0x0a637dc5a2c898a6ULL, 0x113f9804bef90daeULL, 0x1b710b35131c471bULL,
        0x28db77f523047d84ULL, 0x32caab7b40c72493ULL, 0x3c9ebe0a15c9bebcULL,
        0x431d67c49c100d4cULL, 0x4cc5d4becb3e42b6ULL, 0x597f299cfc657e2aULL,
        0x5fcb6fab3ad6faecULL, 0x6c44198c4a475817ULL};
    uint64_t w[80];
    for (size_t i = 0; i < 16; ++i) {
      w[i] = 0;
      for (size_t j = 0; j < 8; ++j) {
        w[i] = (w[i] << 8) | static_cast<uint8_t>(p[i * 8 + j]);
      }
    }
    for (size_t i = 16; i < 80; ++i) {
      const uint64_t s0 =
          Rotr(w[i - 15], 1) ^ Rotr(w[i - 15], 8) ^ (w[i - 15] >> 7);
      const uint64_t s1 =
          Rotr(w[i - 2], 19) ^ Rotr(w[i - 2], 61) ^ (w[i - 2] >> 6);
      w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint64_t a = h_[0], b = h_[1], c = h_[2], d = h_[3];
    uint64_t e = h_[4], f = h_[5], g = h_[6], h = h_[7];
    for (int i = 0; i < 80; ++i) {
      const uint64_t s1 = Rotr(e, 14) ^ Rotr(e, 18) ^ Rotr(e, 41);
      const uint64_t ch = (e & f) ^ (~e & g);
      const uint64_t t1 = h + s1 + ch + kK[i] + w[i];
      const uint64_t s0 = Rotr(a, 28) ^ Rotr(a, 34) ^ Rotr(a, 39);
      const uint64_t maj = (a & b) ^ (a & c) ^ (b & c);
      const uint64_t t2 = s0 + maj;
      h = g;
      g = f;
      f = e;
      e = d + t1;
      d = c;
      c = b;
      b = a;
      a = t1 + t2;
    }
    h_[0] += a;
    h_[1] += b;
    h_[2] += c;
    h_[3] += d;
    h_[4] += e;
    h_[5] += f;
    h_[6] += g;
    h_[7] += h;
  }
  uint64_t h_[8]{};
  size_t total_{0};
  std::string buffer_;
};

inline std::string Sha512Digest(std::string_view data) {
  Sha512 h;
  h.Update(data);
  return h.Finalize();
}

}  // namespace tinylamb::digest

#endif  // TINYLAMB_DIGEST_HPP
