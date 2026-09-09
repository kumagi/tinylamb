/**
 * Copyright 2024 KUMAZAKI Hiroki
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

#include "index/lsm_detail/sorted_run.hpp"

#include <cstddef>
#include <filesystem>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include "blob_file.hpp"
#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "gtest/gtest.h"

namespace tinylamb {
class SortedRunEntryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    filepath_ = "sorted_run_entry_test-" + RandomString() + ".db";
  }

  void TearDown() override { std::ignore = std::filesystem::remove(filepath_); }

  std::string filepath_;
};

TEST_F(SortedRunEntryTest, Generate) {
  // Arrange -- create a BlobFile and three entries (short, middle, long key)
  auto l = BlobFile::Create(filepath_).MoveValue();
  SortedRun::Entry short_entry =
      SortedRun::Entry::Create("abc", LSMValue("val"), *l).MoveValue();
  SortedRun::Entry middle_entry =
      SortedRun::Entry::Create("abcdefhijk", LSMValue("foobar"), *l)
          .MoveValue();
  std::string long_key(200, 'a');
  SortedRun::Entry long_entry =
      SortedRun::Entry::Create(long_key, LSMValue("long value"), *l)
          .MoveValue();

  // Act -- wait for 6 writes to land, then destroy the writer
  while (l->Written() < 6) {
    std::this_thread::yield();
  }
  l.reset();

  // Assert -- read back the three entries via a fresh BlobFile and verify
  // key/value
  auto blob_holder = BlobFile::Create(filepath_).MoveValue();
  CHECK(blob_holder != nullptr);
  BlobFile& blob = *blob_holder;
  ASSERT_EQ(short_entry.BuildKey(blob).Value(), "abc");
  ASSERT_EQ(short_entry.BuildValue(blob).Value(), "val");
  ASSERT_EQ(middle_entry.BuildKey(blob).Value(), "abcdefhijk");
  ASSERT_EQ(middle_entry.BuildValue(blob).Value(), "foobar");
  ASSERT_EQ(long_entry.BuildKey(blob).Value(), long_key);
  ASSERT_EQ(long_entry.BuildValue(blob).Value(), "long value");
}

TEST_F(SortedRunEntryTest, Compare) {
  // Arrange -- create a BlobFile and three entries (short, middle, long key)
  auto l = BlobFile::Create(filepath_).MoveValue();
  SortedRun::Entry short_entry =
      SortedRun::Entry::Create("abc", LSMValue("val"), *l).MoveValue();
  SortedRun::Entry middle_entry =
      SortedRun::Entry::Create("abcdefhijk", LSMValue("foobar"), *l)
          .MoveValue();
  std::string long_key("abcdefghijklmnopqrstuvwxyz");
  SortedRun::Entry long_entry =
      SortedRun::Entry::Create(long_key, LSMValue("long value"), *l)
          .MoveValue();

  // Act -- wait for 6 writes to land, then destroy the writer
  while (l->Written() < 6) {
    std::this_thread::yield();
  }
  l.reset();

  // Assert -- comparisons against external keys yield expected ordering
  auto blob_holder = BlobFile::Create(filepath_).MoveValue();
  CHECK(blob_holder != nullptr);
  BlobFile& blob = *blob_holder;
  ASSERT_LT(short_entry.Compare("abb", blob).Value(), 0);
  ASSERT_EQ(short_entry.Compare("abc", blob).Value(), 0);
  ASSERT_GT(short_entry.Compare("abd", blob).Value(), 0);
  ASSERT_LT(middle_entry.Compare("abcdefhijj", blob).Value(), 0);
  ASSERT_EQ(middle_entry.Compare("abcdefhijk", blob).Value(), 0);
  ASSERT_GT(middle_entry.Compare("abcdefhijkl", blob).Value(), 0);

  std::string long_smaller_key(long_key);
  long_smaller_key[long_smaller_key.size() - 1]--;
  ASSERT_EQ(long_entry.Compare(long_smaller_key, blob).Value(), -1);
  ASSERT_EQ(long_entry.Compare(long_key, blob).Value(), 0);

  std::string long_bigger_key(long_key);
  long_bigger_key[long_smaller_key.size() - 1]++;
  ASSERT_EQ(long_entry.Compare(long_bigger_key, blob).Value(), 1);
}

TEST_F(SortedRunEntryTest, MoreCompare) {
  // Arrange -- build a vector of 12 keys with varied lengths and prefixes;
  // build entries for each + extension
  std::vector<std::string> keys = {
      std::string(1, 0), std::string(2, 0),  std::string(3, 0),
      std::string(1, 0), std::string(2, 0),  std::string(3, 0),
      std::string(4, 0), std::string(4, 0),  std::string(8, 0),
      std::string(8, 0), std::string(12, 0), std::string(12, 0)};
  keys[3][0] = keys[4][1] = keys[5][1] = keys[7][3] = keys[10][7] =
      keys[11][11] = 1;
  std::vector<std::string> candidates;
  std::vector<SortedRun::Entry> entries;
  auto blob_holder = BlobFile::Create(filepath_).MoveValue();
  CHECK(blob_holder != nullptr);
  BlobFile& blob = *blob_holder;

  // Act -- for each key, create an entry; also create entries for key+ext pairs
  {
    for (const auto& key : keys) {
      candidates.push_back(key);
      entries.push_back(
          SortedRun::Entry::Create(key, LSMValue(""), blob).MoveValue());
      for (const auto& ext : keys) {
        candidates.push_back(key + ext);
        entries.push_back(
            SortedRun::Entry::Create(key + ext, LSMValue(""), blob)
                .MoveValue());
      }
    }
  }

  // Assert -- pairwise comparison of all candidates/entries yields consistent
  // ordering
  for (size_t i = 0; i < candidates.size(); ++i) {
    for (size_t j = 0; j < candidates.size(); ++j) {
      if (candidates[i] < candidates[j]) {
        ASSERT_GT(entries[i].Compare(candidates[j], blob).Value(), 0);
        ASSERT_GT(entries[i].Compare(entries[j], blob).Value(), 0);
      } else if (candidates[i] > candidates[j]) {
        ASSERT_LT(entries[i].Compare(candidates[j], blob).Value(), 0);
        ASSERT_LT(entries[i].Compare(entries[j], blob).Value(), 0);
      } else {
        ASSERT_EQ(entries[i].Compare(candidates[j], blob).Value(), 0);
        ASSERT_EQ(entries[i].Compare(entries[j], blob).Value(), 0);
      }
    }
  }
}

class SortedRunTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::map<std::string, LSMValue> input;
    for (int i = 0; i < 1000; ++i) {
      input.emplace("common_prefix:" + std::to_string(i),
                    LSMValue(std::to_string(i * i)));
    }
    data_file_ = "sorted_run_build-test-" + RandomString() + ".db";
    index_file_ = "sorted_run_build-test-index-" + RandomString() + ".idx";
    blob_ = BlobFile::Create(data_file_).MoveValue();
    SortedRun::Construct(index_file_, input, *blob_, 1);
    sr_ = std::make_unique<SortedRun>(
        SortedRun::Restore(index_file_).MoveValue());
  }

  void TearDown() override {
    std::ignore = std::filesystem::remove(data_file_);
    std::ignore = std::filesystem::remove(index_file_);
  }

  std::filesystem::path data_file_;
  std::filesystem::path index_file_;
  std::unique_ptr<SortedRun> sr_;
  std::unique_ptr<BlobFile> blob_;
};

TEST_F(SortedRunTest, First) {
  // Arrange -- SortedRun is pre-constructed by SetUp() with 1000 key/value
  // pairs Act -- find the first key "common_prefix:0"
  auto result = sr_->Find("common_prefix:0", *blob_);

  // Assert -- the result is "0" (i^2 = 0^2 = 0)
  ASSERT_SUCCESS_AND_EQ(result, "0");
}

TEST_F(SortedRunTest, Build) {
  // Arrange -- SortedRun is pre-constructed by SetUp() with 1000 key/value
  // pairs Act -- find the key "common_prefix:121"
  auto result = sr_->Find("common_prefix:121", *blob_);

  // Assert -- the result is "14641" (121^2 = 14641)
  ASSERT_SUCCESS_AND_EQ(result, "14641");
}

TEST_F(SortedRunTest, Find) {
  // Arrange -- SortedRun is pre-constructed by SetUp() with 1000 key/value
  // pairs Act -- find all 1000 keys in the run
  for (int i = 0; i < 1000; ++i) {
    auto result = sr_->Find("common_prefix:" + std::to_string(i), *blob_);
    ASSERT_SUCCESS_AND_EQ(result, std::to_string(i * i));
  }

  // Assert -- out-of-range keys return kNotExists
  auto minus = sr_->Find("common_prefix:" + std::to_string(-1), *blob_);
  ASSERT_EQ(minus.GetStatus(), Status::kNotExists);
  auto over = sr_->Find("common_prefix:" + std::to_string(10000), *blob_);
  ASSERT_EQ(over.GetStatus(), Status::kNotExists);
}

TEST_F(SortedRunTest, FindOnEmptyRunIsNotExists) {
  // An empty run (flushed from an empty map) must report kNotExists for every
  // key.  SortedRun::Find currently admits the empty key past the range check
  // (min_key_ == max_key_ == "") and then reads index entry 0 of a
  // zero-length run out of bounds, returning garbage instead of kNotExists.
  // This test documents that bug and should turn green once the empty run is
  // guarded.
  std::filesystem::path data_file =
      "sorted_run_empty-test-" + RandomString() + ".db";
  std::filesystem::path index_file =
      "sorted_run_empty-test-index-" + RandomString() + ".idx";
  auto blob = BlobFile::Create(data_file).MoveValue();
  const std::map<std::string, LSMValue> empty;
  SortedRun::Construct(index_file, empty, *blob, 1);
  const SortedRun run = SortedRun::Restore(index_file).MoveValue();
  ASSERT_EQ(run.Find("", *blob).GetStatus(), Status::kNotExists);
  ASSERT_EQ(run.Find("any key", *blob).GetStatus(), Status::kNotExists);
  std::ignore = std::filesystem::remove(data_file);
  std::ignore = std::filesystem::remove(index_file);
}

TEST_F(SortedRunTest, FindValueEqualToDeletedMarkerIsNotTombstone) {
  // An inline value whose 8 bytes are all 0xff must not be mistaken for the
  // deleted marker.  SortedRun::Entry stores values of up to 8 bytes inline in
  // a union, and IsDeleted() tests value_.offset_ == kDeletedValue -- which
  // reads the same bits as an all-0xff inline value -- so the stored value is
  // misread as a tombstone and Find() reports kDeleted.  This test documents
  // that bug and should turn green once IsDeleted() distinguishes inline
  // values (value_length_ != 0) from tombstones.
  std::filesystem::path data_file =
      "sorted_run_ff-test-" + RandomString() + ".db";
  std::filesystem::path index_file =
      "sorted_run_ff-test-index-" + RandomString() + ".idx";
  auto blob = BlobFile::Create(data_file).MoveValue();
  const std::string value(8, '\xff');
  std::map<std::string, LSMValue> input;
  input.emplace("key", LSMValue(value));
  SortedRun::Construct(index_file, input, *blob, 1);
  const SortedRun run = SortedRun::Restore(index_file).MoveValue();
  ASSERT_SUCCESS_AND_EQ(run.Find("key", *blob), value);
  std::ignore = std::filesystem::remove(data_file);
  std::ignore = std::filesystem::remove(index_file);
}

TEST_F(SortedRunTest, FindDistinguishesKeysSharingTheFirstFourBytes) {
  // Two keys of inline length (5..11) that share the same first four bytes
  // must compare correctly.  SortedRun::Entry stores the key tail inline in
  // an 8-byte field, copying only `length - 4` bytes and leaving the rest
  // uninitialized; Compare() then reads that garbage tail, so the two entries
  // are ordered wrongly and Find() misses a stored key.  This test documents
  // that bug and should turn green once the inline tail is zero-initialized.
  const std::string key_a("\x00\x00\x01\x10\x00\xd4", 6);
  const std::string key_b("\x00\x00\x01\x10\xff\xff", 6);
  std::filesystem::path data_file =
      "sorted_run_inline-test-" + RandomString() + ".db";
  std::filesystem::path index_file =
      "sorted_run_inline-test-index-" + RandomString() + ".idx";
  auto blob = BlobFile::Create(data_file).MoveValue();
  std::map<std::string, LSMValue> input;
  input.emplace(key_a, LSMValue(std::string("\x00\x00", 2)));
  input.emplace(key_b, LSMValue::Delete());
  SortedRun::Construct(index_file, input, *blob, 1);
  const SortedRun run = SortedRun::Restore(index_file).MoveValue();
  ASSERT_SUCCESS_AND_EQ(run.Find(key_a, *blob), std::string("\x00\x00", 2));
  std::ignore = std::filesystem::remove(data_file);
  std::ignore = std::filesystem::remove(index_file);
}

TEST_F(SortedRunTest, InlineValueRoundTripsThroughFind) {
  // Values of up to 8 bytes are stored inline in the Entry; every such length
  // must be found back verbatim.  The all-0xff 8-byte value is a separate
  // known bug covered by FindValueEqualToDeletedMarkerIsNotTombstone; this
  // test pins the healthy inline path for ordinary payloads.
  std::filesystem::path data_file =
      "sorted_run_inlineval-test-" + RandomString() + ".db";
  std::filesystem::path index_file =
      "sorted_run_inlineval-test-index-" + RandomString() + ".idx";
  auto blob = BlobFile::Create(data_file).MoveValue();
  std::map<std::string, LSMValue> input;
  for (int len = 1; len <= 8; ++len) {
    input.emplace("k" + std::to_string(len),
                  LSMValue(std::string(static_cast<size_t>(len),
                                       static_cast<char>('a' + len - 1))));
  }
  SortedRun::Construct(index_file, input, *blob, 1);
  const SortedRun run = SortedRun::Restore(index_file).MoveValue();
  for (int len = 1; len <= 8; ++len) {
    const std::string expected(static_cast<size_t>(len),
                               static_cast<char>('a' + len - 1));
    ASSERT_SUCCESS_AND_EQ(run.Find("k" + std::to_string(len), *blob), expected);
  }
  std::ignore = std::filesystem::remove(data_file);
  std::ignore = std::filesystem::remove(index_file);
}

TEST_F(SortedRunTest, Delete) {
  // Arrange -- build a SortedRun with 1000 keys where i%3==0 are deletes,
  // i%3==1 are values, i%3==2 absent
  std::filesystem::path data_file;
  std::filesystem::path index_file;
  {
    std::map<std::string, LSMValue> input;
    for (int i = 0; i < 1000; ++i) {
      if (i % 3 == 0) {
        input.emplace(std::to_string(i), LSMValue::Delete());
      } else if (i % 3 == 1) {
        input.emplace(std::to_string(i), LSMValue(std::to_string(i * 2)));
      }
    }
    data_file = "sorted_run_build-test-" + RandomString() + ".db";
    index_file = "sorted_run_build-test-index-" + RandomString() + ".idx";
    auto l = BlobFile::Create(data_file).MoveValue();
    blob_ = BlobFile::Create(data_file).MoveValue();
    SortedRun::Construct(index_file, input, *blob_, 1);
    sr_ =
        std::make_unique<SortedRun>(SortedRun::Restore(index_file).MoveValue());
  }

  // Act -- find all 1000 keys in the run
  for (int i = 0; i < 1000; ++i) {
    auto result = sr_->Find(std::to_string(i), *blob_);
    if (i % 3 == 0) {
      // Assert -- deleted keys return kDeleted
      ASSERT_EQ(result.GetStatus(), Status::kDeleted);
    } else if (i % 3 == 1) {
      // Assert -- value keys return the doubled value
      ASSERT_SUCCESS_AND_EQ(result, std::to_string(i * 2));
    } else {
      // Assert -- absent keys return kNotExists
      ASSERT_EQ(result.GetStatus(), Status::kNotExists);
    }
  }
  std::ignore = std::filesystem::remove(data_file);
  std::ignore = std::filesystem::remove(index_file);
}

TEST_F(SortedRunTest, Iterator) {
  // Arrange -- SortedRun is pre-constructed by SetUp() with 1000 key/value
  // pairs Act -- walk the iterator from begin to end
  auto iter = sr_->Begin(*blob_);
  while (iter.IsValid()) {
    ++iter;
  }

  // Assert -- implicit; iterator exhausts after 1000 entries; gtest green on
  // pass
}

TEST_F(SortedRunTest, DeleteScan) {
  // Arrange -- build a SortedRun with 1000 keys where i%3==0 are deletes,
  // i%3==1 are values, i%3==2 absent
  std::filesystem::path data_file;
  std::filesystem::path index_file;
  {
    std::map<std::string, LSMValue> input;
    for (int i = 0; i < 1000; ++i) {
      if (i % 3 == 0) {
        input.emplace(std::to_string(i), LSMValue::Delete());
      } else if (i % 3 == 1) {
        input.emplace(std::to_string(i), LSMValue(std::to_string(i * 2)));
      }
    }
    data_file = "sorted_run_build-test-" + RandomString() + ".db";
    index_file = "sorted_run_build-test-index-" + RandomString() + ".idx";
    auto l = BlobFile::Create(data_file).MoveValue();
    blob_ = BlobFile::Create(data_file).MoveValue();
    SortedRun::Construct(index_file, input, *blob_, 1);
    sr_ =
        std::make_unique<SortedRun>(SortedRun::Restore(index_file).MoveValue());
  }

  // Act -- walk the iterator; for non-deleted entries, verify key%3==1
  SortedRun::Iterator it = sr_->Begin(*blob_);
  while (it.IsValid()) {
    if (!it.IsDeleted().Value()) {
      ASSERT_EQ(std::stoi(it.Key().Value()) % 3, 1);
    }
    ++it;
  }
  std::ignore = std::filesystem::remove(data_file);
  std::ignore = std::filesystem::remove(index_file);
}

TEST_F(SortedRunTest, IteratorEquality) {
  // Arrange -- the fixture SortedRun and a fresh iterator
  SortedRun::Iterator first = sr_->Begin(*blob_);
  ASSERT_TRUE(first.IsValid());
  SortedRun::Iterator copy = first;

  // Act -- compare equal iterators, then advance one of them
  // Assert -- identical offsets compare equal, differing offsets compare
  // unequal
  ASSERT_TRUE(copy == first);
  ASSERT_FALSE(copy != first);
  ++first;
  ASSERT_TRUE(copy != first);
  ASSERT_FALSE(copy == first);
}
TEST_F(SortedRunTest, IteratorStreamOperator) {
  // Arrange -- an iterator pointing at a live (non-deleted) entry
  SortedRun::Iterator it = sr_->Begin(*blob_);
  ASSERT_TRUE(it.IsValid());
  ASSERT_FALSE(it.IsDeleted().Value());

  // Act -- stream the iterator
  std::stringstream ss;
  ss << it;

  // Assert -- the dump contains the key and the value
  std::string dumped = ss.str();
  EXPECT_NE(dumped.find("=>"), std::string::npos);
  EXPECT_NE(dumped.find(it.Key().Value()), std::string::npos);
}
TEST_F(SortedRunTest, SortedRunStreamOperator) {
  // Arrange -- the fixture SortedRun holds 1000 entries
  // Act -- stream the whole run
  std::stringstream ss;
  ss << *sr_;
  std::string dumped = ss.str();

  // Assert -- the dump carries key range, entry count, and generation
  EXPECT_NE(dumped.find("key range:"), std::string::npos);
  EXPECT_NE(dumped.find("Entries: 1000"), std::string::npos);
  EXPECT_NE(dumped.find("Generation:"), std::string::npos);
}
TEST_F(SortedRunTest, EntryStreamOperatorVariants) {
  // Arrange -- entries with short/inline, long/indirect, deleted, and large
  // value payloads
  SortedRun::Entry short_key =
      SortedRun::Entry::Create("abc", LSMValue("val"), *blob_).MoveValue();
  SortedRun::Entry long_key =
      SortedRun::Entry::Create(std::string(20, 'a'), LSMValue("long value"),
                               *blob_)
          .MoveValue();
  SortedRun::Entry deleted =
      SortedRun::Entry::Create("del", LSMValue::Delete(), *blob_).MoveValue();
  SortedRun::Entry inline_value =
      SortedRun::Entry::Create("k2", LSMValue(std::string(3, 'x')), *blob_)
          .MoveValue();
  SortedRun::Entry long_value =
      SortedRun::Entry::Create("k3", LSMValue(std::string(100, 'y')), *blob_)
          .MoveValue();

  // Act -- stream every entry
  std::stringstream ss;
  ss << short_key << "\n"
     << long_key << "\n"
     << deleted << "\n"
     << inline_value << "\n"
     << long_value;
  std::string dumped = ss.str();

  // Assert -- each serialization arm is exercised
  EXPECT_NE(dumped.find("length:"), std::string::npos);
  EXPECT_NE(dumped.find("key: "), std::string::npos);
  EXPECT_NE(dumped.find("stored at offset:"), std::string::npos);
  EXPECT_NE(dumped.find("(deleted)"), std::string::npos);
  EXPECT_NE(dumped.find("value_len:"), std::string::npos);
  EXPECT_NE(dumped.find(" offset: "), std::string::npos);
}
}  // namespace tinylamb
