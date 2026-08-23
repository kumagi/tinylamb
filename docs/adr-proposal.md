# ADR 移行提案 (docs/review/ → docs/adr/) — たたき台

improvement3.md §4.1 (S9-1) の実施案。現状 `docs/review/*_review.md` は
10 ファイル・計 ~730 行あり、形式は「設計判断への第三者レビュー + 指摘一覧 +
根拠 (ファイル:行) + 提案」で、事実上の ADR (Architecture Decision Record) と
同じ情報を持っている。ただし番号付けもステータスもないため、「この判断は今も
有効なのか」「後に上書きされたのか」を未来から検索できない
(CRC32C 移行のように、フォーマット決定の「なぜ」が後から必要になる場面が既にある)。

## 提案スキーム

1. **配置**: `docs/adr/NNNN-title.md` (NNNN はゼロ埋め連番、採番は作成順)。
   元ファイルは `docs/review/` に残置し、先頭に「→ docs/adr/NNNN へ移行済み」
   のリンクを追記する (履歴 URL の切断を避ける)
2. **ステータス**: 各 ADR 冒頭に `Status: proposed | accepted | superseded by NNNN`
   を持たせる。レビュー指摘のうち未反映のものがある文書は最初 `proposed` にする
3. **形式** (1 ファイル = 1 判断):

   ```
   # NNNN. タイトル (例: ページチェックサムに CRC32C を採用する)
   Date: YYYY-MM-DD / Status: accepted
   ## Context       — どんな問題があったか (元 review のサマリー節)
   ## Decision      — 何を決めたか (1 段落)
   ## Consequences  — 根拠・トレードオフ・残留課題
                      (元 review の指摘 C-x を「反映済み/未反映」タグ付きで引用)
   ```

4. **移行単位**: review 1 ファイル → ADR 1 ファイル。内容の書き換えは最小限
   (ステータス行と見出し整理のみ)。指摘一覧の根拠行番号は作成時点のスナップショット
   としてそのまま保存し、「現在のコードではずれている」旨を注記するだけにする

## 移行順序案 (依存・重要度順)

| ADR | 元 review | 備考 |
|---|---|---|
| 0001 | page_checksum_review | オンディスク互換に関わる最重要指摘 (C-1 ダイジェスト範囲) を含む |
| 0002 | pax_page_format_review | 同じくフォーマット系 |
| 0003 | commit_durability_review | M2 実装済み部分は accepted 化 |
| 0004 | lock_timeout_review | |
| 0005 | page_pool_sharding_review | |
| 0006 | jit_profile_review | |
| 0007 | cascades_optimizer_review | optimizer 改善着手時の参照元 |
| 0008–0010 | distributed_raft / _membership / _migration review | Raft 着手時の前提知識。着手前に番号のみ先に振る |

## 運用ルール (新規 ADR 用)

- 新しい設計判断は最初から `docs/adr/` に書く (review → adr の二段階を廃止)
- 判断を覆すときは旧 ADR の Status を `superseded by NNNN` に変えるだけで、
  本文は書き換えない (決定の履歴を保持するのが目的)
- README / docs インデックスの参照を `docs/review/` → `docs/adr/` へ張り替える
- 工数目安: 既存 10 本の機械的な見出し整理で各 ~15 分。半日で完了する規模

## 非目標

- 指摘事項の再検証・追加レビュー (別タスク)
- 英語化・文体統一 (元文の日本語混在は許容)
