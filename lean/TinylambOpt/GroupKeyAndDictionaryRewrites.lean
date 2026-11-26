import TinylambOpt.UnionAndJoinRewrites
import TinylambOpt.ScalarOverflowAndCoreAlgebra

/-!
These proofs cover dropping functionally determined group keys and aggregating
dictionary codes. SQL can use narrower keys or postpone decoding when equality, order,
and error behavior are preserved.

Named results include `group_by_overflow_expr_drop_bug`,
`group_by_functional_expr_reduction_sound`,
`unordered_group_by_nonkey_filter_corrupts_count_bug`.
-/

/-
# Step 11: ClickBench（全 43 クエリ・1 億行単一ワイド表）向け証明付き最適化ルールの探索

TPC-DS が「多段 JOIN・CTE・相関副問合せ・OUTER JOIN」中心であるのに対し、
ClickBench（`hits` 105 列 × 1 億行、全 43 クエリ）は **単一テーブル上の高基数 GROUP BY・
複合式キー・文字列関数・部分順序 Top-N・条件付き分岐キー** が支配的です。

`tinylamb` の 1 億行実測レポート（`build/benchmarks/clickbench-full-20261116/report-8t/REPORT.md`）
および 1,000 万行レポートを解析し、DuckDB / ClickHouse にまだ差をつけられているクエリ群
（Q18, Q35, Q36, Q10, Q22, Q23, Q29, Q40, Q43 等）に直結する **6 系統の新規最適化定理** と
**6 つの SQL 意味論トラップ反例定理** を Lean 4 で機械検証します（`sorry` ゼロ）。

## 収録する 6 大 ClickBench 最適化ルール

1. **`GROUP BY` 関数従属・定数・無オーバーフロー算術キー縮約 (`group_by_functional_expr_reduction_sound`)**
   - 対象: **Q35** (`GROUP BY 1, URL`)、**Q36** (`GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3`)
   - トラップ反例: `group_by_overflow_expr_drop_bug`（`INT64_MIN - 3` の遅延評価によるオーバーフロー消失罠）
2. **順序なし `GROUP BY ... LIMIT K` のキー・ファイバー厳密部分集約 (`unordered_group_by_key_fiber_subset_sound`)**
   - 対象: **Q18** (`SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10`: `230.9ms` vs ClickHouse `4.5ms`)
   - トラップ反例: `unordered_group_by_nonkey_filter_corrupts_count_bug`
3. **Top-N 勝者決定後の遅延ペイロード集約実体化 (`two_stage_topn_late_agg_materialization_sound`)**
   - 対象: **Q10** (`COUNT(DISTINCT UserID)`)、**Q22** (`MIN(URL)`)、**Q23** (`MIN(URL), MIN(Title), COUNT(DISTINCT UserID)`)、**Q29** (`MIN(Referer)`)
   - トラップ反例: `two_stage_late_agg_sql_in_null_group_bug`（SQL `IN` による `NULL` グループ消失罠）
4. **辞書コード準同型 `GROUP BY` ＆ 順序保存辞書の整数 `MIN`/`MAX` 還元 (`dict_code_homomorphism_group_agg_sound`, `order_preserving_dict_min_code_sound`)**
   - 対象: **Q29** (`REGEXP_REPLACE(Referer, ...)` + `MIN(Referer)`)、**Q13–Q15, Q22, Q23, Q34–Q39**
   - トラップ反例: `unsorted_dict_min_code_bug`
5. **条件付き `CASE WHEN` キーの排他パーティション分割 (`conditional_key_disjoint_partition_sound`)**
   - 対象: **Q40** (`CASE WHEN SearchEngineID = 0 AND AdvEngineID = 0 THEN Referer ELSE '' END`)
   - トラップ反例: `conditional_key_without_guard_cols_collision_bug`
6. **単調時刻切り捨て (`TIMESTAMP_TRUNC`) の昇順ストリーム早期打ち切り (`contiguous_prefix_preserves_complete_buckets`, `timestampTrunc_monotone`)**
   - 対象: **Q43** (`TIMESTAMP_TRUNC(EventTime, MINUTE) ... ORDER BY 1 LIMIT 10 OFFSET 1000`)
   - トラップ反例: `periodic_extract_breaks_monotone_runs_bug`（Q19 `EXTRACT(MINUTE ...)` の非単調罠）
-/

namespace TinylambOpt

/-!
## 共通補題: 単射写像と `Nodup`
-/

private theorem nodup_map_of_inj {A B : Type} (f : A → B)
    (hf : ∀ x y, f x = f y → x = y) :
    ∀ {xs : List A}, xs.Nodup → (xs.map f).Nodup
  | [], _ => List.nodup_nil
  | x :: rest, h => by
    rw [List.nodup_cons] at h
    rw [List.map_cons, List.nodup_cons]
    refine ⟨?_, nodup_map_of_inj f hf h.2⟩
    rw [List.mem_map]
    rintro ⟨y, hy, hEq⟩
    exact h.1 (hf y x hEq ▸ hy)

private theorem groupBy_nodup_keys {Row K V : Type} [DecidableEq K]
    (key : Row → K) (agg : K → Rel Row → V) (r : Rel Row) :
    ((groupBy key agg r).map Prod.fst).Nodup := by
  unfold groupBy
  rw [List.map_map]
  have hId : (Prod.fst ∘ fun k => (k, agg k (r.filter fun x => key x == k))) = id := by
    funext k; rfl
  rw [hId, List.map_id]
  exact nodup_distinctKeys (r.map key)

/-!
## Rule CB-1: `GROUP BY` 関数従属・定数・無オーバーフロー算術キー縮約 (ClickBench Q35, Q36)

- **Q35**: `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10`
- **Q36**: `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10`

`d(r) = f(k(r))` が主グループキー `k(r)` の **全域（エラーを起こさない純粋）関数** であるとき、
`(k(r), f(k(r)))` で 4 列複合ハッシュ集約する代わりに `k(r)` 1 列だけで集約し、
出力（あるいは `LIMIT 10` 通過後の 10 行だけ！）で `f(k)` を計算しても結果マルチセットは完全一致します。
-/

/-- 🚨 トラップ反例 CB-1 (`group_by_overflow_expr_drop_bug`):
    `ClientIP - 3` のような符号付き `INT64` 減算を含む `GROUP BY` キーを、列最小値チェック
    （`min(ClientIP) >= INT64_MIN + 3`）なしに無条件削除して `LIMIT 1` の後段に遅延すると、
    `LIMIT 1` で脱落した行（例: `ClientIP = INT64_MIN`, `COUNT(*) = 1`）で本来発生すべきだった
    SQL `INT64` オーバーフローエラー（`.err`）が握り潰されてしまいます。 -/
theorem group_by_overflow_expr_drop_bug :
    ∃ (rows : List Int),
      -- 全行で `x - 3` を評価する本来の GROUP BY ではオーバーフロー（`.err`）が発生する
      (rows.any (fun x => checkedSub64 (.val x) (.val 3) == .err) = true) ∧
      -- しかし出現頻度最大の先頭グループ `x = 100` だけを取り出してから `x - 3` を計算するとエラーが消える！
      checkedSub64 (.val 100) (.val 3) = .val 97 := by
  refine ⟨[100, 100, int64Min], ?_, ?_⟩
  · decide
  · decide

private theorem filter_pair_func_key {Row K D : Type} [DecidableEq K] [DecidableEq D]
    (k : Row → K) (f : K → D) (gk : K) (r : Rel Row) :
    r.filter (fun x => ((k x, f (k x)) == (gk, f gk))) =
      r.filter (fun x => k x == gk) := by
  apply List.filter_congr
  intro x _
  by_cases hk : k x = gk
  · have hEq : (k x, f (k x)) = (gk, f gk) := by rw [hk]
    rw [beq_iff_eq.mpr hEq, beq_iff_eq.mpr hk]
  · have hNePair : (k x, f (k x)) ≠ (gk, f gk) := fun h => hk (Prod.mk.inj h).1
    have hF1 : ((k x, f (k x)) == (gk, f gk)) = false :=
      Bool.eq_false_iff.mpr (fun h => hNePair (beq_iff_eq.mp h))
    have hF2 : (k x == gk) = false :=
      Bool.eq_false_iff.mpr (fun h => hk (beq_iff_eq.mp h))
    rw [hF1, hF2]

/-- ✅ 証明付き最適化 CB-1 (`group_by_functional_expr_reduction_sound`):
    全域関数 `f : K → D`（定数 `fun _ => 1` や、ゾーンマップ `min >= INT64_MIN + c` で
    無オーバーフローが証明済みの `ClientIP - c`、`EXTRACT`、`BYTE_LENGTH` 等）について、
    `(k x, f (k x))` による複合 `GROUP BY` は単一キー `k x` による `GROUP BY` と完全同値（`BagEq`）！ -/
theorem group_by_functional_expr_reduction_sound
    {Row K D V Out : Type} [DecidableEq K] [DecidableEq D]
    (k : Row → K) (f : K → D) (agg : Rel Row → V) (proj : K → D → V → Out)
    (r : Rel Row) :
    BagEq
      ((groupBy (fun x => (k x, f (k x))) (fun _ b => agg b) r).map
        (fun kv => proj kv.1.1 kv.1.2 kv.2))
      ((groupBy k (fun _ b => agg b) r).map
        (fun kv => proj kv.1 (f kv.1) kv.2)) := by
  have hPermKeys :
      BagEq (distinctKeys (r.map (fun x => (k x, f (k x)))))
            ((distinctKeys (r.map k)).map (fun gk => (gk, f gk))) := by
    apply perm_of_nodup_iff_mem
    · exact nodup_distinctKeys _
    · exact nodup_map_of_inj (fun gk => (gk, f gk)) (fun _ _ h => (Prod.mk.inj h).1) (nodup_distinctKeys _)
    · intro p
      rw [mem_distinctKeys, List.mem_map, List.mem_map]
      constructor
      · rintro ⟨x, hx, rfl⟩
        exact ⟨k x, (mem_distinctKeys (r.map k) (k x)).mpr (List.mem_map_of_mem hx), rfl⟩
      · rintro ⟨gk, hgk, rfl⟩
        have hMemMap : gk ∈ r.map k := (mem_distinctKeys (r.map k) gk).mp hgk
        rw [List.mem_map] at hMemMap
        rcases hMemMap with ⟨x, hx, rfl⟩
        exact ⟨x, hx, rfl⟩
  unfold groupBy
  rw [List.map_map, List.map_map]
  have hCongr := hPermKeys.map (fun kd : K × D =>
    proj kd.1 kd.2 (agg (r.filter (fun x => (k x, f (k x)) == kd))))
  rw [List.map_map] at hCongr
  have hLhsEq :
      List.map ((fun kv : (K × D) × V => proj kv.1.1 kv.1.2 kv.2) ∘
        fun k_1 => (k_1, agg (r.filter (fun row => (k row, f (k row)) == k_1))))
        (distinctKeys (r.map (fun x => (k x, f (k x))))) =
      List.map (fun kd : K × D =>
        proj kd.1 kd.2 (agg (r.filter (fun x => (k x, f (k x)) == kd))))
        (distinctKeys (r.map (fun x => (k x, f (k x))))) := rfl
  rw [hLhsEq]
  refine hCongr.trans ?_
  have hEqLists :
      List.map ((fun kd : K × D => proj kd.1 kd.2 (agg (r.filter (fun x => (k x, f (k x)) == kd)))) ∘
        fun gk => (gk, f gk)) (distinctKeys (r.map k)) =
      List.map ((fun kv : K × V => proj kv.1 (f kv.1) kv.2) ∘
        fun gk => (gk, agg (r.filter (fun row => k row == gk)))) (distinctKeys (r.map k)) := by
    apply List.map_congr_left
    intro gk _
    simp only [Function.comp_apply]
    rw [filter_pair_func_key k f gk r]
  rw [hEqLists]

/-!
## Rule CB-2: 順序なし `GROUP BY ... LIMIT K` のキー・ファイバー厳密部分集約 (ClickBench Q18)

**ClickBench Q18**:
`SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10`

`ORDER BY` のない `GROUP BY ... LIMIT K` では、全グループ集合の任意のサイズ `K` 部分集合が
正当な SQL 出力です。しかし入力を単純に `LIMIT` すると `COUNT(*)` が壊れます。
一方、**`GROUP BY` キーの一部だけを見る任意の述語 `P : K1 → Bool`**（例: 整数列 `UserID` のみの
ハッシュマスク `(hash(UserID) &&& 4095) == 0`）で入力行を事前フィルタすれば、
条件を満たす `UserID` については全行が漏れなく集約されるため **`COUNT(*)` / `SUM` / `AVG` が 100% 厳密** になり、
しかも非該当行（99.97%）では重い文字列列 `SearchPhrase` のデコードすら不要になります！
-/

/-- 🚨 トラップ反例 CB-2 (`unordered_group_by_nonkey_filter_corrupts_count_bug`):
    `GROUP BY` キーに従属しない列（または先頭行切り詰め）で事前フィルタすると、
    同一グループの行が欠落し `COUNT(*)` が破壊されます。 -/
theorem unordered_group_by_nonkey_filter_corrupts_count_bug :
    ∃ (r : Rel (Int × Int)) (g : Int × Int → Int),
      -- 非キー列 `r.2 == 0` でフィルタして集約した結果 `(1, 1)` は、本来のグループ集合 `[(1, 2)]` にすら存在しない！
      ∃ kv ∈ groupBy g (fun _ b => b.length) (r.filter (fun x => x.2 == 0)),
        kv ∉ groupBy g (fun _ b => b.length) r := by
  refine ⟨[(1, 0), (1, 1)], Prod.fst, ⟨(1, 1), ?_, ?_⟩⟩
  · decide
  · decide

/-- ✅ 証明付き最適化 CB-2 (`unordered_group_by_key_fiber_subset_sound`):
    複合キー `(k1 r, k2 r)` の第 1 成分のみに依存する任意のファイバー述語 `P : K1 → Bool` で
    事前フィルタして `GROUP BY`（＋任意の `HAVING`）を実行した出力リスト `sub` は：
    1. 重複行がなく（`Nodup`）、
    2. すべての出力行が **全表 `GROUP BY`（＋`HAVING`）と完全に同一の集約値を持つ厳密な要素** であり、
    3. したがって `K ≤ sub.length` ならば `sub.take K` は全表結果の厳密なサイズ `K` 部分集合となる！ -/
theorem unordered_group_by_key_fiber_subset_sound
    {Row K1 K2 V : Type} [DecidableEq K1] [DecidableEq K2]
    (k1 : Row → K1) (k2 : Row → K2)
    (P : K1 → Bool)
    (agg : (K1 × K2) → Rel Row → V)
    (having : (K1 × K2) → V → Bool)
    (r : Rel Row) (K : Nat) :
    let full := (groupBy (fun x => (k1 x, k2 x)) agg r).filter (fun kv => having kv.1 kv.2)
    let sub := (groupBy (fun x => (k1 x, k2 x)) agg (r.filter (fun x => P (k1 x)))).filter
                 (fun kv => having kv.1 kv.2)
    (sub.take K).Nodup ∧
    (∀ kv ∈ sub.take K, kv ∈ full) ∧
    (K ≤ sub.length → (sub.take K).length = K) := by
  intro full sub
  have hSubNodup : sub.Nodup :=
    (nodup_groupBy (fun x => (k1 x, k2 x)) agg (r.filter (fun x => P (k1 x)))).filter _
  refine ⟨hSubNodup.take, ?_, ?_⟩
  · intro kv hMemTake
    have hMemSub : kv ∈ sub := List.mem_of_mem_take hMemTake
    simp only [sub, List.mem_filter] at hMemSub
    rcases hMemSub with ⟨hInGrp, hHav⟩
    simp only [full, List.mem_filter]
    refine ⟨?_, hHav⟩
    unfold groupBy at hInGrp ⊢
    rw [List.mem_map] at hInGrp ⊢
    rcases hInGrp with ⟨gk, hGkMem, rfl⟩
    have hGkMap : gk ∈ (r.filter (fun x => P (k1 x))).map (fun x => (k1 x, k2 x)) :=
      (mem_distinctKeys _ gk).mp hGkMem
    rw [List.mem_map] at hGkMap
    rcases hGkMap with ⟨x0, hx0Filt, hKeyX0⟩
    rw [List.mem_filter] at hx0Filt
    have hPk1 : P gk.1 = true := by
      have h1 : k1 x0 = gk.1 := congrArg Prod.fst hKeyX0
      rw [← h1]
      exact hx0Filt.2
    have hGkFull : gk ∈ distinctKeys (r.map (fun x => (k1 x, k2 x))) := by
      rw [mem_distinctKeys, List.mem_map]
      exact ⟨x0, hx0Filt.1, hKeyX0⟩
    refine ⟨gk, hGkFull, ?_⟩
    have hSameBucket :
        (r.filter (fun x => P (k1 x))).filter (fun x => (k1 x, k2 x) == gk) =
        r.filter (fun x => (k1 x, k2 x) == gk) := by
      rw [List.filter_filter]
      apply List.filter_congr
      intro x _
      by_cases hEq : (k1 x, k2 x) = gk
      · have hk1 : k1 x = gk.1 := congrArg Prod.fst hEq
        have hBeqT : ((k1 x, k2 x) == gk) = true := beq_iff_eq.mpr hEq
        rw [hBeqT, hk1, hPk1]
        rfl
      · have hBeqF : ((k1 x, k2 x) == gk) = false :=
          Bool.eq_false_iff.mpr (fun h => hEq (beq_iff_eq.mp h))
        rw [hBeqF, Bool.false_and]
    rw [hSameBucket]
  · intro hLe
    rw [List.length_take, Nat.min_eq_left hLe]

/-!
## Rule CB-3: Top-N 勝者決定後の遅延ペイロード集約実体化 (ClickBench Q10, Q22, Q23, Q29)

- **Q22**: `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c ... GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10`
- **Q23**: `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) ... ORDER BY c DESC LIMIT 10`
- **Q10**: `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(...), COUNT(DISTINCT UserID) ... ORDER BY c DESC LIMIT 10`

`HAVING` と `ORDER BY ... LIMIT K` が軽量な順序決定集約 `agg1`（`COUNT(*)` 等）のみに依存するとき、
全数万〜数百万グループに対して重い `MIN(URL)` や `COUNT(DISTINCT UserID)` ハッシュ集合を構築するのは無駄です。
Pass 1 で `(key, agg1)` のみを計算して上位 `K` キー集合 `W` を確定し、
Pass 2 では **`key(r)` が NULL 安全同値（`==`）で `W` に含まれる行だけ** に対して重い `agg2` を計算すれば、
順序・タイブレークも含めて **リストとして完全一致（`=`）** します！
-/

/-- 🚨 トラップ反例 CB-3 (`two_stage_late_agg_sql_in_null_group_bug`):
    上位 `K` グループに `NULL` キーグループが含まれるとき、Pass 2 の行フィルタを
    SQL の `key IN (W)`（3 値論理の `sqlEq`）で書くと `NULL = NULL` が `UNKNOWN` となって
    `NULL` グループの行が全滅し、`MIN(URL)` 等が `NULL` に化けます！ -/
theorem two_stage_late_agg_sql_in_null_group_bug :
    ∃ (r : Rel (SqlVal Int × Int)) (winners : List (SqlVal Int)),
      winners = [none] ∧
      -- SQL `IN`（`sqlEq`）でフィルタすると `none` グループの行が空になってしまう
      (r.filter (fun row => winners.any (fun w => (sqlEq row.1 w).inWhere))) = [] ∧
      -- 本来の NULL 安全メンバシップ（`IS NOT DISTINCT FROM` / `==`）なら行が保持される
      (r.filter (fun row => winners.contains row.1)) = [(none, 42)] := by
  refine ⟨[(none, 42)], [none], rfl, ?_, ?_⟩
  · decide
  · decide

/-- 決定的な安定挿入ソート（`ORDER BY` の形式モデル） -/
def insertByRank {α : Type} (le : α → α → Bool) (x : α) : List α → List α
  | [] => [x]
  | y :: ys => if le x y then x :: y :: ys else y :: insertByRank le x ys

def sortByRank {α : Type} (le : α → α → Bool) : List α → List α
  | [] => []
  | x :: xs => insertByRank le x (sortByRank le xs)

private theorem insertByRank_map {α β : Type} (f : α → β)
    (leA : α → α → Bool) (leB : β → β → Bool)
    (hCompat : ∀ x y, leB (f x) (f y) = leA x y)
    (x : α) (ys : List α) :
    insertByRank leB (f x) (ys.map f) = (insertByRank leA x ys).map f := by
  induction ys with
  | nil => rfl
  | cons y rest ih =>
    simp only [List.map_cons, insertByRank, hCompat]
    cases leA x y <;> simp [ih]

private theorem sortByRank_map {α β : Type} (f : α → β)
    (leA : α → α → Bool) (leB : β → β → Bool)
    (hCompat : ∀ x y, leB (f x) (f y) = leA x y)
    (xs : List α) :
    sortByRank leB (xs.map f) = (sortByRank leA xs).map f := by
  induction xs with
  | nil => rfl
  | cons x rest ih =>
    simp only [List.map_cons, sortByRank, ih]
    exact insertByRank_map f leA leB hCompat x (sortByRank leA rest)

private theorem filter_sublist_filter_same_key {Row K : Type} [DecidableEq K]
    (key : Row → K) (winners : List K) (gk : K) (hMem : gk ∈ winners) (r : Rel Row) :
    (r.filter (fun x => winners.contains (key x))).filter (fun x => key x == gk) =
      r.filter (fun x => key x == gk) := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro x _
  by_cases hk : key x = gk
  · subst hk
    have hCont : winners.contains (key x) = true := List.elem_eq_true_of_mem hMem
    have hSelf : (key x == key x) = true := beq_self_eq_true (key x)
    rw [hCont, hSelf]
    rfl
  · have hBeqF : (key x == gk) = false :=
      Bool.eq_false_iff.mpr (fun h => hk (beq_iff_eq.mp h))
    rw [hBeqF, Bool.false_and]

private theorem filter_map_comm {α β : Type} (f : α → β) (p : β → Bool) :
    ∀ (xs : List α), (xs.map f).filter p = (xs.filter (fun x => p (f x))).map f
  | [] => rfl
  | x :: rest => by
    simp only [List.map_cons, List.filter]
    cases p (f x) <;> simp [filter_map_comm f p rest]

/-- ✅ 証明付き最適化 CB-3 (`two_stage_topn_late_agg_materialization_sound`):
    任意のランク比較 `leRank`（`ORDER BY agg1 DESC` 等）と `HAVING` 述語 `hav` が
    軽量集約 `agg1` とキー `K` のみに依存するとき：
    1. Pass 1: `(key, agg1)` のみで `HAVING` + `sortByRank` + `take K` を実行して勝者リスト `top1` を得る
    2. Pass 2: `winners = top1.map Prod.fst` に NULL 安全一致（`winners.contains (key x)`）する行だけを走査して
       重いペイロード集約 `agg2`（`MIN(URL)`, `COUNT(DISTINCT UserID)` 等）を計算する
    この 2 段実行は、最初から全グループで `(agg1, agg2)` を同時計算してソート・`LIMIT K` した結果と
    **順序も含めリストとして完全一致（`=`）** します！ -/
theorem two_stage_topn_late_agg_materialization_sound
    {Row K A1 A2 Out : Type} [DecidableEq K]
    (key : Row → K)
    (agg1 : K → Rel Row → A1)
    (agg2 : K → Rel Row → A2)
    (hav : K → A1 → Bool)
    (leRank : (K × A1) → (K × A1) → Bool)
    (proj : K → A1 → A2 → Out)
    (r : Rel Row) (Klimit : Nat) :
    let fullGroups := (groupBy key (fun gk b => (agg1 gk b, agg2 gk b)) r).filter
                        (fun kv => hav kv.1 kv.2.1)
    let sortedFull := sortByRank (fun a b => leRank (a.1, a.2.1) (b.1, b.2.1)) fullGroups
    let eagerPlan := (sortedFull.take Klimit).map (fun kv => proj kv.1 kv.2.1 kv.2.2)
    let lightGroups := (groupBy key agg1 r).filter (fun kv => hav kv.1 kv.2)
    let top1 := (sortByRank leRank lightGroups).take Klimit
    let winners := top1.map Prod.fst
    let rFiltered := r.filter (fun x => winners.contains (key x))
    let latePlan := top1.map (fun kv =>
      proj kv.1 kv.2 (agg2 kv.1 (rFiltered.filter (fun x => key x == kv.1))))
    eagerPlan = latePlan := by
  intro fullGroups sortedFull eagerPlan lightGroups top1 winners rFiltered latePlan
  let enrich : K × A1 → K × (A1 × A2) :=
    fun kv => (kv.1, (kv.2, agg2 kv.1 (r.filter (fun x => key x == kv.1))))
  have hFullEqMap : fullGroups = lightGroups.map enrich := by
    simp only [fullGroups, lightGroups, groupBy]
    rw [filter_map_comm, filter_map_comm, List.map_map]
    rfl
  have hSorted : sortedFull = (sortByRank leRank lightGroups).map enrich := by
    simp only [sortedFull]
    rw [hFullEqMap]
    exact sortByRank_map enrich leRank (fun a b => leRank (a.1, a.2.1) (b.1, b.2.1))
      (fun _ _ => rfl) lightGroups
  simp only [eagerPlan, latePlan, top1]
  rw [hSorted, ← List.map_take, List.map_map]
  apply List.map_congr_left
  intro kv hKvMem
  have hKeyInWinners : kv.1 ∈ winners := List.mem_map_of_mem hKvMem
  have hBucket := filter_sublist_filter_same_key key winners kv.1 hKeyInWinners r
  simp only [Function.comp_apply, enrich, rFiltered]
  rw [hBucket]

/-!
## Rule CB-4: 辞書コード準同型 `GROUP BY` ＆ 順序保存辞書の整数 `MIN` 還元 (ClickBench Q29, Q22, Q23)

- **Q29**: `SELECT REGEXP_REPLACE(Referer, ..., '\1') AS k, AVG(BYTE_LENGTH(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k ...`
  - 1 億行の `Referer` 列に対して毎行正規表現エンジンを回す代わりに、辞書 `D : Code → String` の各エントリに対して **1 回だけ** `REGEXP_REPLACE(D c)` と `BYTE_LENGTH(D c)` を評価し、1 億行本体は整数コードテーブル引き＋整数集約に帰着させます。
  - さらに辞書 `D` が辞書順ソート済み（`c1 ≤ c2 → D c1 ≤ D c2`）ならば、文字列 `MIN(Referer)` は **整数コード最小値 `D (MIN(code))`** に還元され、走査中の文字列比較が完全にゼロになります！
-/

/-- 🚨 トラップ反例 CB-4 (`unsorted_dict_min_code_bug`):
    辞書が未ソート（例: `D 0 = 20`, `D 1 = 5`）のとき、コード最小値 `MIN(code) = 0` を辞書引きすると
    `D 0 = 20` となり、真の最小値 `MIN(D(code)) = 5` と食い違います。 -/
theorem unsorted_dict_min_code_bug :
    ∃ (D : Nat → Int) (codes : List Nat),
      (sqlMin (fun c => some (D c)) codes) ≠
        (match sqlMin (fun c => some (Int.ofNat c)) codes with
         | none => none
         | some minC => some (D minC.toNat)) := by
  refine ⟨(fun c => if c == 0 then 20 else 5), [0, 1], ?_⟩
  decide

/-- ✅ 証明付き最適化 CB-4a (`dict_code_homomorphism_group_agg_sound`):
    列値が `val r = D (code r)` と辞書符号化されているとき、任意の関数 `φ : Val → K`（`REGEXP_REPLACE` 等）と
    値依存集約 `agg : List Val → V`（`AVG(BYTE_LENGTH(Referer))`, `MIN(Referer)`, `COUNT(*)` 等）に対し、
    辞書上で事前合成した `φ_code := fun c => φ (D c)` を使ってコード列を集約する計画は元クエリと完全一致！ -/
theorem dict_code_homomorphism_group_agg_sound
    {Code Val K V : Type} [DecidableEq K]
    (D : Code → Val) (φ : Val → K) (agg : List Val → V)
    (codes : List Code) :
    let vals := codes.map D
    let φ_code : Code → K := fun c => φ (D c)
    groupBy φ (fun _ b => agg b) vals =
      groupBy φ_code (fun _ cb => agg (cb.map D)) codes := by
  intro vals φ_code
  unfold groupBy
  have hMapKeys : vals.map φ = codes.map φ_code := by
    simp only [vals, φ_code, List.map_map]
    rfl
  rw [hMapKeys]
  apply List.map_congr_left
  intro gk _
  have hFiltMap :
      ∀ (cs : List Code),
        (cs.map D).filter (fun v => φ v == gk) =
          (cs.filter (fun c => φ (D c) == gk)).map D := by
    intro cs
    induction cs with
    | nil => rfl
    | cons c rest ih =>
      simp only [List.map_cons, List.filter]
      cases φ (D c) == gk <;> simp [ih]
  simp only [vals, φ_code]
  rw [hFiltMap codes]

/-- 順序付き辞書上での整数コード `MIN` ヘルパー -/
def minNatOpt : List Nat → Option Nat
  | [] => none
  | c :: rest =>
    match minNatOpt rest with
    | none => some c
    | some m => some (min c m)

/-- ✅ 証明付き最適化 CB-4b (`order_preserving_dict_min_code_sound`):
    単調非減少な辞書 `D : Nat → Int`（`i ≤ j → D i ≤ D j`、ソート済み文字列辞書の順序同型モデル）において、
    値列 `codes.map D` 上の `sqlMin` は、**整数コード列 `codes` 上の最小コード `minNatOpt codes` を 1 回だけ `D` で引いた値**
    と完全に一致します！ -/
theorem order_preserving_dict_min_code_sound
    (D : Nat → Int)
    (hMono : ∀ i j, i ≤ j → D i ≤ D j)
    (codes : List Nat) :
    sqlMin (fun c => some (D c)) codes = (minNatOpt codes).map D := by
  induction codes with
  | nil => rfl
  | cons c rest ih =>
    simp only [sqlMin_cons, minNatOpt, ih]
    cases minNatOpt rest with
    | none => rfl
    | some m =>
      simp only [Option.map, mergeMin]
      congr 1
      by_cases hle : c ≤ m
      · have hDle : D c ≤ D m := hMono c m hle
        rw [Nat.min_eq_left hle, Int.min_eq_left hDle]
      · have hmle : m ≤ c := by omega
        have hDmle : D m ≤ D c := hMono m c hmle
        rw [Nat.min_eq_right hmle, Int.min_eq_right hDmle]

/-!
## Rule CB-5: 条件付き `CASE WHEN` キーの排他パーティション分割 (ClickBench Q40)

**ClickBench Q40**:
`SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE ... GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ...`

`CASE` 条件 `p(g)` が参照する列 `g = (SearchEngineID, AdvEngineID)` が **すでに `GROUP BY` キーに含まれている** とき、
- `p(g) = true` の行集合（`(g, Referer, URL)` で集約）と
- `p(g) = false` の行集合（`Referer` 列を一切デコードせず `(g, URL)` のみで集約し `Src = ''` を付与！）
は、`Referer = ''` の行があっても `g` 自体が異なるため **グループキーが 100% 排他** となり、
再マージ不要の単純連結（`++`）で厳密一致（`BagEq`）します！
-/

/-- 🚨 トラップ反例 CB-5 (`conditional_key_without_guard_cols_collision_bug`):
    もし条件列 `g` が `GROUP BY` キーに含まれていない場合（`GROUP BY CASE WHEN g = 0 THEN referer ELSE "" END` のみ）、
    `g = 0` 側でたまたま `referer = ""` だった行と `g ≠ 0` 側の `ELSE ""` が同じキー `""` に衝突するため、
    再マージなしの分割連結は誤って `""` グループを 2 行に分裂させます！ -/
theorem conditional_key_without_guard_cols_collision_bug :
    ∃ (r : Rel (Int × String)),
      let cKey : Int × String → String := fun x => if x.1 == 0 then x.2 else ""
      let orig := groupBy cKey (fun _ b => b.length) r
      let splitNoMerge :=
        groupBy cKey (fun _ b => b.length) (r.filter (fun x => x.1 == 0)) ++
        groupBy cKey (fun _ b => b.length) (r.filter (fun x => !(x.1 == 0)))
      orig.length ≠ splitNoMerge.length := by
  refine ⟨[(0, ""), (1, "ignored")], ?_⟩
  decide

/-- ✅ 証明付き最適化 CB-5 (`conditional_key_disjoint_partition_sound`):
    条件 `p : G → Bool` の入力 `g : Row → G` が `GROUP BY` キーに含まれているとき、
    `(g x, restKey x, if p (g x) then col x else defVal)` による `GROUP BY` は、
    1. `p (g x) = true` の行を `(g x, restKey x, col x)` で集約した結果と、
    2. `p (g x) = false` の行を **`col x` を一切読まずに** `(g x, restKey x)` だけで集約して `defVal` を添えた結果
    の再マージなし連結（`++`）と完全同値（`BagEq`）！ -/
theorem conditional_key_disjoint_partition_sound
    {Row G R C V : Type} [DecidableEq G] [DecidableEq R] [DecidableEq C]
    (g : Row → G) (restKey : Row → R) (p : G → Bool)
    (col : Row → C) (defVal : C)
    (agg : Rel Row → V) (r : Rel Row) :
    let fullKey : Row → G × R × C :=
      fun x => (g x, restKey x, if p (g x) then col x else defVal)
    let rTrue := r.filter (fun x => p (g x))
    let rFalse := r.filter (fun x => !p (g x))
    BagEq
      (groupBy fullKey (fun _ b => agg b) r)
      (groupBy (fun x => (g x, restKey x, col x)) (fun _ b => agg b) rTrue ++
       (groupBy (fun x => (g x, restKey x)) (fun _ b => agg b) rFalse).map
         (fun kv => ((kv.1.1, kv.1.2, defVal), kv.2))) := by
  intro fullKey rTrue rFalse
  have hLhsKeyNodup : ((groupBy fullKey (fun _ b => agg b) r).map Prod.fst).Nodup :=
    groupBy_nodup_keys fullKey _ r
  have hRhs1KeyNodup : ((groupBy (fun x => (g x, restKey x, col x)) (fun _ b => agg b) rTrue).map Prod.fst).Nodup :=
    groupBy_nodup_keys _ _ rTrue
  have hRhs2BaseNodup : ((groupBy (fun x => (g x, restKey x)) (fun _ b => agg b) rFalse).map Prod.fst).Nodup :=
    groupBy_nodup_keys _ _ rFalse
  have hRhsKeyNodup :
      (((groupBy (fun x => (g x, restKey x, col x)) (fun _ b => agg b) rTrue ++
         (groupBy (fun x => (g x, restKey x)) (fun _ b => agg b) rFalse).map
           (fun kv => ((kv.1.1, kv.1.2, defVal), kv.2)))).map Prod.fst).Nodup := by
    rw [List.map_append, List.nodup_append]
    refine ⟨hRhs1KeyNodup, ?_, ?_⟩
    · have hRewrite :
          (((groupBy (fun x => (g x, restKey x)) (fun _ b => agg b) rFalse).map
            (fun kv => ((kv.1.1, kv.1.2, defVal), kv.2))).map Prod.fst) =
          ((groupBy (fun x => (g x, restKey x)) (fun _ b => agg b) rFalse).map Prod.fst).map
            (fun gr => (gr.1, gr.2, defVal)) := by
        rw [List.map_map, List.map_map]
        rfl
      rw [hRewrite]
      exact nodup_map_of_inj (fun gr : G × R => (gr.1, gr.2, defVal))
        (fun a b hEq => by cases a; cases b; cases hEq; rfl) hRhs2BaseNodup
    · intro k1 hk1 k2 hk2 hSameK
      unfold groupBy at hk1
      rw [List.mem_map] at hk1
      rcases hk1 with ⟨kv1, hkv1, rfl⟩
      rw [List.mem_map] at hkv1
      rcases hkv1 with ⟨gk1, hgk1, rfl⟩
      have hIn1 := (mem_distinctKeys _ gk1).mp hgk1
      rw [List.mem_map] at hIn1
      rcases hIn1 with ⟨x1, hx1, hkx1⟩
      rw [List.mem_filter] at hx1
      have hpTrue : p gk1.1 = true := by
        have hg1 : g x1 = gk1.1 := congrArg Prod.fst hkx1
        rw [← hg1]
        exact hx1.2
      rw [List.map_map, List.mem_map] at hk2
      rcases hk2 with ⟨kv2, hkv2, hEqK⟩
      unfold groupBy at hkv2
      rw [List.mem_map] at hkv2
      rcases hkv2 with ⟨gk2, hgk2, rfl⟩
      have hIn2 := (mem_distinctKeys _ gk2).mp hgk2
      rw [List.mem_map] at hIn2
      rcases hIn2 with ⟨x2, hx2, hkx2⟩
      rw [List.mem_filter] at hx2
      have hpFalse : p gk2.1 = false := by
        have hg2 : g x2 = gk2.1 := congrArg Prod.fst hkx2
        have hnp := hx2.2
        rw [hg2] at hnp
        simpa using hnp
      have hComb : (gk2.1, gk2.2, defVal) = gk1 := hEqK.trans hSameK.symm
      have hgEq : gk2.1 = gk1.1 := congrArg Prod.fst hComb
      rw [hgEq, hpTrue] at hpFalse
      exact Bool.false_ne_true hpFalse.symm
  have nodup_of_fst_nodup {A B : Type} {xs : List (A × B)} (h : (xs.map Prod.fst).Nodup) : xs.Nodup := by
    induction xs with
    | nil => exact List.nodup_nil
    | cons x rest ih =>
      rw [List.map_cons, List.nodup_cons] at h
      rw [List.nodup_cons]
      refine ⟨fun hMem => h.1 (List.mem_map_of_mem hMem), ih h.2⟩
  apply perm_of_nodup_iff_mem (nodup_of_fst_nodup hLhsKeyNodup) (nodup_of_fst_nodup hRhsKeyNodup)
  intro item
  rcases item with ⟨⟨gk, rk, ck⟩, v⟩
  rw [List.mem_append]
  by_cases hp : p gk = true
  · have hNotRhs2 :
        ((gk, rk, ck), v) ∉
          (groupBy (fun x => (g x, restKey x)) (fun _ b => agg b) rFalse).map
            (fun kv => ((kv.1.1, kv.1.2, defVal), kv.2)) := by
      intro hMem2
      rw [List.mem_map] at hMem2
      rcases hMem2 with ⟨kv2, hkv2, hEq2⟩
      unfold groupBy at hkv2
      rw [List.mem_map] at hkv2
      rcases hkv2 with ⟨gr2, hgr2, rfl⟩
      have hIn2 := (mem_distinctKeys _ gr2).mp hgr2
      rw [List.mem_map] at hIn2
      rcases hIn2 with ⟨x2, hx2, hkx2⟩
      rw [List.mem_filter] at hx2
      have hg2 : g x2 = gk := by
        have h1 : g x2 = gr2.1 := congrArg Prod.fst hkx2
        have h2 : gr2.1 = gk := congrArg (fun z => z.1.1) hEq2
        exact h1.trans h2
      rw [hg2, hp] at hx2
      exact absurd hx2.2 (by decide)
    have hBucketTrue :
        r.filter (fun x => fullKey x == (gk, rk, ck)) =
        rTrue.filter (fun x => (g x, restKey x, col x) == (gk, rk, ck)) := by
      simp only [rTrue, List.filter_filter]
      apply List.filter_congr
      intro x _
      by_cases hg : g x = gk
      · simp [fullKey, hg, hp]
      · have hF1 : (fullKey x == (gk, rk, ck)) = false :=
          Bool.eq_false_iff.mpr (fun h => hg (congrArg Prod.fst (beq_iff_eq.mp h)))
        have hF2 : ((g x, restKey x, col x) == (gk, rk, ck)) = false :=
          Bool.eq_false_iff.mpr (fun h => hg (congrArg Prod.fst (beq_iff_eq.mp h)))
        rw [hF1, hF2, Bool.false_and]
    have hKeyMemIff :
        (gk, rk, ck) ∈ distinctKeys (r.map fullKey) ↔
        (gk, rk, ck) ∈ distinctKeys (rTrue.map (fun x => (g x, restKey x, col x))) := by
      rw [mem_distinctKeys, mem_distinctKeys, List.mem_map, List.mem_map]
      constructor
      · rintro ⟨x, hx, hfk⟩
        have hg : g x = gk := congrArg Prod.fst hfk
        have hxT : x ∈ rTrue := List.mem_filter.mpr ⟨hx, by rw [hg]; exact hp⟩
        refine ⟨x, hxT, ?_⟩
        simp only [fullKey, hg, hp, ite_true] at hfk
        simpa [hg] using hfk
      · rintro ⟨x, hxT, htk⟩
        rw [List.mem_filter] at hxT
        refine ⟨x, hxT.1, ?_⟩
        simp only [fullKey, hxT.2, ite_true]
        exact htk
    constructor
    · intro hL
      left
      unfold groupBy at hL ⊢
      rw [List.mem_map] at hL ⊢
      rcases hL with ⟨k0, hk0, hEq0⟩
      cases hEq0
      exact ⟨(gk, rk, ck), hKeyMemIff.mp hk0, by rw [hBucketTrue]⟩
    · rintro (hR1 | hR2)
      · unfold groupBy at hR1 ⊢
        rw [List.mem_map] at hR1 ⊢
        rcases hR1 with ⟨k0, hk0, hEq0⟩
        cases hEq0
        exact ⟨(gk, rk, ck), hKeyMemIff.mpr hk0, by rw [hBucketTrue]⟩
      · exact False.elim (hNotRhs2 hR2)
  · have hpF : p gk = false := Bool.eq_false_iff.mpr hp
    have hNotRhs1 :
        ((gk, rk, ck), v) ∉
          groupBy (fun x => (g x, restKey x, col x)) (fun _ b => agg b) rTrue := by
      intro hMem1
      unfold groupBy at hMem1
      rw [List.mem_map] at hMem1
      rcases hMem1 with ⟨k1, hk1, hEq1⟩
      cases hEq1
      have hIn1 := (mem_distinctKeys _ (gk, rk, ck)).mp hk1
      rw [List.mem_map] at hIn1
      rcases hIn1 with ⟨x1, hx1, hkx1⟩
      rw [List.mem_filter] at hx1
      have hg1 : g x1 = gk := congrArg Prod.fst hkx1
      rw [hg1, hpF] at hx1
      exact Bool.false_ne_true hx1.2
    have hBucketFalse :
        r.filter (fun x => fullKey x == (gk, rk, defVal)) =
        rFalse.filter (fun x => (g x, restKey x) == (gk, rk)) := by
      simp only [rFalse, List.filter_filter]
      apply List.filter_congr
      intro x _
      by_cases hg : g x = gk
      · by_cases hrk : restKey x = rk
        · simp [fullKey, hg, hrk, hpF]
        · have hF1 : (fullKey x == (gk, rk, defVal)) = false :=
            Bool.eq_false_iff.mpr (fun h => hrk (congrArg (fun z => z.2.1) (beq_iff_eq.mp h)))
          have hF2 : ((g x, restKey x) == (gk, rk)) = false :=
            Bool.eq_false_iff.mpr (fun h => hrk (congrArg Prod.snd (beq_iff_eq.mp h)))
          rw [hF1, hF2, Bool.false_and]
      · have hF1 : (fullKey x == (gk, rk, defVal)) = false :=
          Bool.eq_false_iff.mpr (fun h => hg (congrArg Prod.fst (beq_iff_eq.mp h)))
        have hF2 : ((g x, restKey x) == (gk, rk)) = false :=
          Bool.eq_false_iff.mpr (fun h => hg (congrArg Prod.fst (beq_iff_eq.mp h)))
        rw [hF1, hF2, Bool.false_and]
    constructor
    · intro hL
      right
      unfold groupBy at hL
      rw [List.mem_map] at hL
      rcases hL with ⟨k0, hk0, hEq0⟩
      cases hEq0
      have hIn0 := (mem_distinctKeys _ (gk, rk, ck)).mp hk0
      rw [List.mem_map] at hIn0
      rcases hIn0 with ⟨x0, hx0, hfk0⟩
      have hg0 : g x0 = gk := congrArg Prod.fst hfk0
      have hckEq : ck = defVal := by
        have hc := congrArg (fun z => z.2.2) hfk0
        simp only [fullKey, hg0, hpF] at hc
        exact hc.symm
      subst hckEq
      have hrk0 : restKey x0 = rk := congrArg (fun z => z.2.1) hfk0
      rw [List.mem_map]
      refine ⟨((gk, rk), agg (rFalse.filter (fun x => (g x, restKey x) == (gk, rk)))), ?_, ?_⟩
      · unfold groupBy
        rw [List.mem_map]
        refine ⟨(gk, rk), ?_, rfl⟩
        rw [mem_distinctKeys, List.mem_map]
        exact ⟨x0, List.mem_filter.mpr ⟨hx0, by simp [hg0, hpF]⟩, Prod.ext hg0 hrk0⟩
      · simp [hBucketFalse]
    · rintro (hR1 | hR2)
      · exact False.elim (hNotRhs1 hR1)
      · rw [List.mem_map] at hR2
        rcases hR2 with ⟨⟨⟨gk2, rk2⟩, v2⟩, hkv2, hEq2⟩
        cases hEq2
        unfold groupBy at hkv2 ⊢
        rw [List.mem_map] at hkv2 ⊢
        rcases hkv2 with ⟨gr0, hgr0, hEqGr⟩
        cases hEqGr
        have hIn2 := (mem_distinctKeys _ (gk2, rk2)).mp hgr0
        rw [List.mem_map] at hIn2
        rcases hIn2 with ⟨x2, hx2, hkx2⟩
        rw [List.mem_filter] at hx2
        have hg2 : g x2 = gk2 := congrArg Prod.fst hkx2
        have hrk2 : restKey x2 = rk2 := congrArg Prod.snd hkx2
        refine ⟨(gk2, rk2, defVal), ?_, ?_⟩
        · rw [mem_distinctKeys, List.mem_map]
          exact ⟨x2, hx2.1, by simp [fullKey, hg2, hrk2, hpF]⟩
        · simp [hBucketFalse]

/-!
## Rule CB-6: 単調時刻切り捨て (`TIMESTAMP_TRUNC`) の昇順ストリーム早期打ち切り (ClickBench Q43 vs Q19)

- **Q43**: `SELECT TIMESTAMP_TRUNC(EventTime, MINUTE) AS M, COUNT(*) AS PageViews FROM hits WHERE ... GROUP BY M ORDER BY M LIMIT 10 OFFSET 1000`
- **Q19**: `SELECT UserID, EXTRACT(MINUTE FROM EventTime) AS m, ...`

整数エポック秒 `t` に対する `TIMESTAMP_TRUNC(t, W)`（`t / W * W`, `W > 0`）は **単調非減少** です。
したがって `EventTime` 昇順に並んだデータ（またはインデックス/ゾーンマップ順スキャン）に対しては
同一 `M` の行が必ず連続（ランを形成）し、**先頭から `M_limit = LIMIT + OFFSET` 個の異なるグループが完了した瞬間に
残りの数千万行を一切読まずに打ち切っても結果が 100% 一致します！**
-/

/-- 整数エポック時刻の単位 `W` 切り捨て（`TIMESTAMP_TRUNC` モデル） -/
def timestampTrunc (w : Int) (t : Int) : Int :=
  (t / w) * w

/-- 周期的な分抽出（`EXTRACT(MINUTE FROM t)` モデル） -/
def timestampExtractMinute (t : Int) : Int :=
  (t / 60) % 60

/-- 🚨 トラップ反例 CB-6 (`periodic_extract_breaks_monotone_runs_bug`):
    `EXTRACT(MINUTE FROM EventTime)`（Q19）は周期関数であり単調ではないため、
    入力が `EventTime` 昇順 `[3540, 3600, 7140]`（59分 → 60分=0分 → 119分=59分）に並んでいても
    同じキー `59` が離れた位置に再出現し、ストリーム先頭打ち切りを行うとカウントが誤ります！ -/
theorem periodic_extract_breaks_monotone_runs_bug :
    let ts := [3540, 3600, 7140]
    ts.Pairwise (· ≤ ·) ∧
    ¬ (ts.map timestampExtractMinute).Pairwise (· ≤ ·) := by
  decide

/-- ✅ 補題 CB-6a (`timestampTrunc_monotone`):
    任意の正の粒度 `w > 0` に対して `timestampTrunc w` は単調非減少（`t1 ≤ t2 → timestampTrunc w t1 ≤ timestampTrunc w t2`）！ -/
theorem timestampTrunc_monotone (w : Int) (hw : 0 < w) {t1 t2 : Int} (h : t1 ≤ t2) :
    timestampTrunc w t1 ≤ timestampTrunc w t2 := by
  unfold timestampTrunc
  have hDiv : t1 / w ≤ t2 / w := Int.ediv_le_ediv hw h
  exact Int.mul_le_mul_of_nonneg_right hDiv (Int.le_of_lt hw)

/-- ソート済みリストの「最初の `k` 個の相異なるキーに属する連続プレフィックス」だけを切り出すストリーム走査モデル -/
def takeFirstKKeyRuns {Row K : Type} [DecidableEq K] (key : Row → K) : Nat → List K → List Row → List Row
  | 0, _, _ => []
  | _, _, [] => []
  | k + 1, seen, x :: xs =>
    let kx := key x
    if seen.contains kx then
      x :: takeFirstKKeyRuns key (k + 1) seen xs
    else if seen.length < k + 1 then
      x :: takeFirstKKeyRuns key (k + 1) (kx :: seen) xs
    else
      []

/-- ✅ 証明付き最適化 CB-6b (`contiguous_prefix_preserves_complete_buckets`):
    非減少なキー列を持つストリーム `r = r1 ++ r2` において、プレフィックス `r1` 内で完結し
    境界後の `r2` に二度と現れない確定済みキー `gk` については、`r1` だけから計算した
    すべてのグループ集約値（`COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX`）が全行 `r1 ++ r2` の結果と完全一致します！ -/
theorem contiguous_prefix_preserves_complete_buckets
    {Row K V : Type} [DecidableEq K]
    (key : Row → K) (agg : K → Rel Row → V)
    (r1 r2 : Rel Row)
    (gk : K) (hDisj : gk ∉ r2.map key) :
    agg gk ((r1 ++ r2).filter (fun x => key x == gk)) =
      agg gk (r1.filter (fun x => key x == gk)) := by
  have hEmpty2 : r2.filter (fun x => key x == gk) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx hBeq
    have hEq : key x = gk := beq_iff_eq.mp hBeq
    exact hDisj (hEq ▸ List.mem_map_of_mem hx)
  rw [List.filter_append, hEmpty2, List.append_nil]

end TinylambOpt

-- C++ references to this model or its proof.
-- executor/detail/columnar_tlcs_aggregate_state.cpp
-- executor/detail/columnar_tlcs_bindings.cpp
-- executor/detail/columnar_tlcs_fast_path.cpp
-- executor/detail/columnar_tlcs_internal.hpp
-- executor/detail/partitioned_aggregate_topk.cpp
-- query/select_rewrites.hpp
-- query/select_set_and_join_rewrites.cpp
