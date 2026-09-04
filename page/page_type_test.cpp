/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#include "page/page_type.hpp"

#include <cstdint>
#include <sstream>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(PageTypeTest, StreamOperator) {
  std::stringstream ss;
  ss << PageType::kUnknown << " " << PageType::kFreePage << " "
     << PageType::kMetaPage << " " << PageType::kRowPage << " "
     << PageType::kLeafPage << " " << PageType::kBranchPage;
  EXPECT_EQ(ss.str(),
            "UnknownPageType FreePageType MetaPageType RowPageType "
            "LeafPageType BranchPageType");
}

TEST(PageTypeTest, SerializeDeserialize) {
  std::stringstream ss;
  Encoder e(ss);
  e << PageType::kLeafPage;
  Decoder d(ss);
  PageType out = PageType::kUnknown;
  d >> out;
  EXPECT_EQ(out, PageType::kLeafPage);
}

TEST(PageTypeTest, AllTypesRoundTrip) {
  for (const PageType type :
       {PageType::kUnknown, PageType::kFreePage, PageType::kMetaPage,
        PageType::kRowPage, PageType::kLeafPage, PageType::kBranchPage}) {
    std::stringstream ss;
    Encoder e(ss);
    e << type;
    Decoder d(ss);
    PageType out = PageType::kUnknown;
    d >> out;
    EXPECT_EQ(out, type);
  }
}

TEST(PageTypeTest, RoundTripKeepsSerializedSizeStable) {
  // A PageType is stored as a fixed-width uint64_t on the wire.  Verify that
  // every value serializes to exactly 8 bytes so the page header layout never
  // depends on the enum value.
  for (const PageType type :
       {PageType::kUnknown, PageType::kFreePage, PageType::kMetaPage,
        PageType::kRowPage, PageType::kLeafPage, PageType::kBranchPage}) {
    std::stringstream ss;
    Encoder e(ss);
    e << type;
    EXPECT_EQ(ss.str().size(), sizeof(uint64_t));
  }
}

TEST(PageTypeTest, StreamOperatorPrintsEachTypeIndividually) {
  std::stringstream ss;
  ss << PageType::kFreePage;
  EXPECT_EQ(ss.str(), "FreePageType");
  ss.str("");
  ss << PageType::kBranchPage;
  EXPECT_EQ(ss.str(), "BranchPageType");
}

TEST(PageTypeTest, PageTypeStringHelper) {
  EXPECT_EQ(PageTypeString(PageType::kFreePage), "FreePage");
  EXPECT_EQ(PageTypeString(PageType::kMetaPage), "MetaPage");
  EXPECT_EQ(PageTypeString(PageType::kRowPage), "RowPage");
  EXPECT_EQ(PageTypeString(PageType::kLeafPage), "LeafPage");
  EXPECT_EQ(PageTypeString(PageType::kBranchPage), "BranchPage");
  EXPECT_EQ(PageTypeString(PageType::kUnknown), "(unknown)");
  // Deliberately invalid value: exercises the "(unknown)" fallback path.
  EXPECT_EQ(PageTypeString(static_cast<PageType>(
                999)),  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange)
            "(unknown)");
}

}  // namespace tinylamb
