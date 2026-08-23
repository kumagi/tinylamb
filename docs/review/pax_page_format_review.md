# pax_page_format.md レビュー指摘事項

## サマリー

本ドキュメントは「PAX page format v1」の仕様として書かれているが、対応するページ実体は存在しない。`pax_layout.hpp` の構造体定義(`PaxPageHeader`/`PaxColumnDirectory`)とオフセット基準(Page::body 先頭からの uint32_t 相対値)は文書と一致する一方、ページ型・読み書き経路・可視性領域・version 検証はいずれも未実装であり、実装済みなのはメモリ内エンコーダ(`pax_block.cpp`)のみである。さらにその唯一の実装が行うエンコーディング(bit packing + 辞書 + frame-of-reference)は、文書の「INT64/DOUBLE は 8 byte 配列」等の記述と正面から矛盾する。「仕様通りに実装するのか、実装に合わせて文書を直すのか」を決めないまま放置すると、双方の読者が必ず誤解する状態になっている。

## 指摘一覧

### P-1: PAX ページ型・ページ実体・読み書き経路が存在しない
- 区分: 実態との乖離
- 対象: docs/pax_page_format.md:3「既存`RowPage`とは別のページ型として導入し、移行中のDBを読み書きできるようにする。」および :1(タイトル「PAX page format v1」)
- 問題: (a) `PageType` 列挙に PAX 型がない、(b) `Page::PageBody` 共用体に PAX ページがない、(c) `PaxPageHeader`/`PaxColumnDirectory` を読み書きするコードが皆無(使用箇所は `CompressedBytes()` の sizeof 集計とレイアウトテストのみ)、(d) `PaxBlock`/`PaxColumnBlock` は executor・table・storage のどこからも参照されていない(利用者は `pax_block_test.cpp` のみ)。つまり「RowPage とは別のページ型」「移行中の DB の読み書き」は存在せず、文書は実装済み機能のように読めるが実際は構造体定義だけの状態である。
- 根拠:
  - page/page_type.hpp:28-35 — PageType は kUnknown/kFreePage/kMetaPage/kRowPage/kLeafPage/kBranchPage のみ。
  - page/page.hpp:181-190 — PageBody 共用体に PaxPage 相当なし。
  - page/pax_block.cpp:157-164 — `PaxPageHeader`/`PaxColumnDirectory` は `sizeof` 集計でのみ使用。grep では他に page/pax_layout_test.cpp:12-16 のサイズ検証のみ。
  - grep `PaxBlock|PaxColumnBlock`(executor/, table/, database/) — 呼び出し 0 件。CMakeLists.txt:574-575 はテスト登録のみ。
- 提案: 冒頭に現状(「v1 レイアウト定義とメモリ内エンコーダのみ実装。ディスク直結・ページ型追加・MVCC 可視性分離は未実装」)を明記し、設計提案であることを示す。

### P-2: 「列値領域」の記述(INT64/DOUBLE 8byte 配列、VARCHAR offset 配列)が実装済みエンコーダと矛盾
- 区分: 実態との乖離
- 対象: docs/pax_page_format.md:11「5. 列値領域: INT64/DOUBLEは8 byte配列。VARCHARは`uint32_t[row_count + 1]`のoffset配列とbyte payload。」
- 問題: 唯一の PAX 実装(`PaxColumnBlock::Encode`)はこの形式を一切生成しない。INT64/DATE は frame-of-reference bit packing(`kBitPacked`: min 基準の delta を `bit_width_` 幅で pack)、VARCHAR は圧縮成立時に辞書+bit-packed ID(`kDictionary`)、不成立時や DOUBLE は `std::vector<Value>` の plain 保持であり、「8 byte 配列」「uint32 offset 配列 + byte payload」のどちらにもならない。また実装が DATE 型を特殊扱い(`DateFromDays` 復元)するのに対し、文書は DATE に触れていない。
- 根拠:
  - page/pax_block.cpp:30-58 — INT64/DATE → `kBitPacked` + `frame_base_`/`bit_width_`。
  - page/pax_block.cpp:60-93 — VARCHAR → 辞書サイズ比較で `kDictionary` 採否、不成立時は plain へ。
  - page/pax_block.cpp:95-100,115-128 — plain は `std::vector<Value>`、復元も Value 単位(offset 配列ではない)。
  - page/pax_block_test.cpp:22-24,50,65 — BitPacked/Dictionary/Plain の選択を検証するテスト。
- 提案: v1 仕様を更新するか、現行エンコーディング(kPlain/kDictionary/kBitPacked とその選択条件)を文書化する。いずれにせよ :12 の「bit packing metadata...を後方互換に追加できる」との整合を取る(P-3)。

### P-3: 補助領域「後方互換に追加できる」対象が既に v1 エンコードとして実装されており、直列化先フィールドも欠落
- 区分: 実態との乖離
- 対象: docs/pax_page_format.md:12「6. 補助領域: dictionary、bit packing metadata、zone mapを後方互換に追加できる。」
- 問題: dictionary と bit packing は「将来追加」ではなく既に `PaxEncoding::kDictionary`/`kBitPacked` として v1 の enum に定義され、Encode が日常的に選択している(pax_layout.hpp:27-31)。一方で `PaxColumnDirectory` には bit packing metadata(`frame_base_`/`bit_width_`)を格納するフィールドがなく、辞書 payload を置く規約もない。現行のメモリ表現をそのままディスクに書き出すことは v1 構造体では不可能で、「後方互換に追加できる」という記述は実態と逆(すでに必要だが置き場所がない)。zone map については実装・言及とも皆無で将来性の主張にとどまる。
- 根拠:
  - page/pax_layout.hpp:27-31 — `enum class PaxEncoding { kPlain = 0, kDictionary = 1, kBitPacked = 2 }`(v1 に定義済み)。
  - page/pax_layout.hpp:35-45 — `PaxColumnDirectory` のフィールド一覧(value_type/encoding/flags/data_offset/data_length/null_bitmap_*/auxiliary_*)に frame_base/bit_width 相当がない。
  - page/pax_block.hpp:31-35 — メモリ側は `int64_t frame_base_` / `uint8_t bit_width_` を別途保持。
  - page/CODE_REVIEW.md:132 — 「`pax_layout.hpp` ... が示すディスク直結化を見据えると破損メタデータがベクタ OOB 読みに直結する」(ディスク直結化が未着手であることの裏付け)。
- 提案: auxiliary 領域の規約に「辞書本体・frame base/bit width を aux 領域に置くレイアウト」を定義するか、v1 の data 領域内配置として明文化する。zone map は未実装であることを明示する。

### P-4: visibility 領域のセマンティクス(version-chain 参照・固定幅・0=物理行)が未実装かつ幅と指す先が未定義
- 区分: 不明瞭
- 対象: docs/pax_page_format.md:8「2. visibility領域: 行ごとのversion-chain参照を固定幅offsetで保持する。0は物理行を使用することを表す。」
- 問題: (a) 「固定幅offset」の幅が書かれていない(uint16? uint32?)。(b) 「version-chain参照」が指す先が定義されていない——現行の MVCC version chain はトランザクション層のメモリ上(transaction_manager の version_shards_)にあり、ディスク上に chain は存在しないため、ページ内 offset が何を参照するのか構成上ありえない選択肢(同一ページ内? 他ページ? それとも将来的な永続 chain?)を読者が想像するしかない。(c) 「0は物理行」の特別値規約も実装なし。ヘッダには `visibility_offset`/`visibility_length` フィールドだけが存在し意味論を持つコードはない。
- 根拠:
  - page/pax_layout.hpp:19-20 — `uint32_t visibility_offset{0}; uint32_t visibility_length{0};`(意味論を実装するコードなし)。
  - transaction/transaction_manager.hpp:138-162 — VersionChain は `std::unordered_map<RowPosition, VersionChain>` のメモリ上シャードに存在(RowPosition = page_id+slot)。
  - page/row_page.hpp:33-42,84-87 — RowPage の行は serialized record + slot 配列であり、ページ上に visibility/version 構造を持たない。
- 提案: 「固定幅」を uint32_t と明記し、参照先(将来の永続 version chain の位置づけ)、0 センチネルとの排他規約、RowPage 移行期間の二重管理方法を追記する。未確定なら「設計未確定」と明示する。

### P-5: NULL bitmap「未使用capacity bitも1にする」ルールが未実装で、既存メモリ実装は逆(0初期化)
- 区分: 粒度不足
- 対象: docs/pax_page_format.md:10「4. NULL bitmap: 1 bit/row。1をNULLとし、未使用capacity bitも1にする。」
- 問題: 1=NULL と 1 bit/row 自体は実装(ColumnVector/PaxColumnBlock)と整合するが、「未使用 capacity bit を 1 で埋める」規約を満たすコードが存在しない。既存のメモリ側 bitmap は新規ワードを **0 で** リサイズするため、この規約を採用してディスク直結化すると既存経路と矛盾する。さらに全ゼロページが「未使用=非NULL」に解釈されるリスク(zero fill された破損領域が有効行に見える)という設計判断の理由も文書にない。
- 根拠:
  - executor/data_chunk.cpp:127-135 — `EnsureNullBit`: 新規ワードは `resize(word + 1, 0)`(未使用 bit = 0 = 非 NULL)。:138-140 — `IsNull` は bit set(1)を NULL 判定。
  - page/pax_block.cpp:28,116 — `null_bitmap_` は ColumnVector 由来をそのまま保持し、bit==1 を NULL として復元。
  - page/pax_layout.hpp:47-49 — `PaxBitmapBytes(row_capacity) = (row_capacity + 7) / 8`(容量単位のバイト数は定義するが埋め方は未規定)。
- 提案: パディング規約を採用する理由(torn/zero-fill 時の安全側倒など)とともに明記するか、既存実装に合わせ「未使用 bit は 0、読み出しは row_count 未満のみ有効」へ修正する。

### P-6: 「versionが未知なら読み込みを拒否する」等の強制がどこにも存在しない
- 区分: 粒度不足
- 対象: docs/pax_page_format.md:14「ヘッダのversionが未知なら読み込みを拒否する。」
- 問題: `kPaxFormatVersion = 1` は定義されているが、これを検査する読み出し経路自体が存在しないため、「拒否する」主体がなく、将来実装者が「どこで検証するか(GetPage? PageManager? 専用 reader?)」を文書から決められない。「領域は重複せずページ境界内に収まり」(:14)の検証も同様に実装・検証箇所が未定である。
- 根拠:
  - page/pax_layout.hpp:10 — `inline constexpr uint16_t kPaxFormatVersion = 1;`(定義のみ)。grep で比較・拒否処理は存在しない(page_type.hpp:28-35 の PageType dispatch にも PAX なし)。
  - page/pax_layout_test.cpp:11-16 — 検証内容は standard layout と sizeof のみ。
- 提案: 「version 検証は(将来の)PAX reader の入口で format_version != kPaxFormatVersion を reject、境界検証は ReadFrom 直後に行う」のように検証責任位置を特定して記載する。未定なら TODO であることを明示する。

### P-7: 番号付き「レイアウト」列が物理配置順序と読めるが、実態は offset ベースで順序未固定
- 区分: 不明瞭
- 対象: docs/pax_page_format.md:7-12(項目1〜6の列挙)
- 問題: 1〜6 の番号列挙は「header → visibility → directory → NULL bitmap → 列値 → 補助」の物理順に読めるが、実装はすべて offset 参照(`*_offset` フィールド)であり配置順序を固定するものはない。実際 `PaxColumnDirectory` は directory_offset のみで長さフィールドがなく(`PaxDirectoryBytes(column_count)` から算出可能とはいえ文書に記載なし)、visibility/directory/payload の相対位置規約が存在しない。図なしで番号順の記述だけだと、実装者が誤った固定順序を仮定しかねない。
- 根拠:
  - page/pax_layout.hpp:12-13 — コメント「All offsets are relative to the beginning of Page::body.」(順序規約への言及なし)。
  - page/pax_layout.hpp:21-23 — `directory_offset` のみで directory_length は不在。:47-53 — 長さは `PaxBitmapBytes`/`PaxDirectoryBytes` で算出する設計。
- 提案: 「配置順序は非固定、すべて offset+length(または算出式)でアクセスする」と明記し、可能なら ASCII 図に各 region の offset/length の組を併記する。

### P-8: 構造体サイズ・アラインメントの保証が文書になく、コード側も static_assert なし
- 区分: 粒度不足
- 対象: docs/pax_page_format.md:9(項目3 `PaxColumnDirectory[column_count]`)および :14(offset 幅の記述全体)
- 問題: オンディスクフォーマット文書でありながら `sizeof(PaxPageHeader)=32` / `sizeof(PaxColumnDirectory)=28`、パディングなし(1+1+2+4×6)という最低限の安定性情報が書かれておらず、保証もテスト EXPECT のみで static_assert が定義側にない。コンパイラ差でパディングが変わるリスク(standard layout ではあるが明示的保証なし)を文書が拾っていない。
- 根拠:
  - page/pax_layout_test.cpp:12-15 — `EXPECT_EQ(sizeof(PaxPageHeader), 32U)` / `EXPECT_EQ(sizeof(PaxColumnDirectory), 28U)`(テストのみ)。
  - page/CODE_REVIEW.md:133 — 「`static_assert(sizeof(...)==32)` 等をヘッダへ移動し、単独includeでも保証されるように」(既存指摘)。
  - page/pax_layout.hpp:14-45 — フィールド並びから 32/28 は導けるが明示保証なし。
- 提案: 文書に各構造体の正確なサイズ・フィールドオフセット表を記載し、コード側には static_assert(std::is_standard_layout_v<...>) と sizeof の static_assert を追加する。

## 未検証事項

- :14「MVCC判定後に必要列だけを連続走査できる」の性能主張は、走査経路自体が未実装のため検証不能(PaxBlock は executor 未接続)。
- `PaxColumnDirectory.flags`(uint16_t)の用途がコード・ドキュメント双方で未定義であり、予約領域として妥当かは判断材料がない。
- 既知の関連指摘として improvements2.md §3.14(:82、pax_block.cpp:123 の符号付き加算オーバーフロー)があるが、メモリ内経路のため本レビューでは深追いしていない。
