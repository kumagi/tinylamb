import TinylambOpt.ClickBenchDiscovery
import TinylambOpt.ScalarOverflowAndCoreAlgebra

/-
# Step 15: ClickBench 第2弾（1億行 `.tlcs` ＆ 1,000万行 `LSTableStore` 残存敗北クエリ群）向け証明付き最適化

`ClickBenchDiscovery.lean`（CB-1..CB-6）の実装後になお残っていた ClickBench 1億行 / 1,000万行の主要ボトルネック：
- **Q34, Q35, Q17, Q19, Q13, Q15, Q16**: 高基数 `VARCHAR` / 複合キー `GROUP BY ... ORDER BY COUNT(*) DESC LIMIT K`
  における数ギガバイトの文字列スラブ退避・ディスクスピル
- **Q28, Q29, Q33**: `LIMIT` のない `HAVING COUNT(*) > T` クエリ（Q29）や `ORDER BY` が遅延集約列を参照する
  `HAVING COUNT(*) > T` クエリ（Q28）、高基数複合キー数値集約（Q33）における全グループペイロード先行計算
- **Q30**: 90 個のアフィン線形和 `SUM(ResolutionWidth + c_i)` による 90 重アキュムレータ評価
- **Q03, Q30**: 検査付き `SUM(INT64)` の直列フォールバックと定数ブロックデコード
- **Q02, Q03, Q07, Q08**: 辞書厳密頻度 `freq(d)` と無フィルタ列境界による `O(|D|)` / `O(1)` 畳み込み
- **Q26**: `WHERE SearchPhrase <> '' ORDER BY SearchPhrase ASC LIMIT 10` における空文字列最小値 `""` による
  ブロック Zone-Map 打ち切り不能問題

これらを解決する **6 系統の新規健全性定理群（CB-7..CB-12）** と **8 本の SQL 意味論トラップ反例定理** を
Lean 4 で機械検証します（`sorry` ゼロ）。
-/

namespace TinylambOpt

/-!
## Rule CB-7: 商ハッシュバケット上界による Exact Heavy-Hitter 刈り込み (ClickBench Q34, Q35, Q17, Q19, Q13, Q15, Q16)
-/

def groupCount {Row K : Type} [DecidableEq K] (key : Row → K) (R : Rel Row) (k : K) : Nat :=
  (R.filter (fun r => key r == k)).length

def bucketCount {Row K B : Type} [DecidableEq B]
    (b : K → B) (key : Row → K) (R : Rel Row) (u : B) : Nat :=
  (R.filter (fun r => b (key r) == u)).length

private theorem filter_length_le_of_imp {α : Type} (p q : α → Bool)
    (hImp : ∀ x, p x = true → q x = true) :
    ∀ (xs : List α), (xs.filter p).length ≤ (xs.filter q).length
  | [] => Nat.le_refl 0
  | x :: rest => by
    by_cases hp : p x = true
    · have hq : q x = true := hImp x hp
      simp only [List.filter_cons, hp, hq, ↓reduceIte, List.length_cons]
      exact Nat.succ_le_succ (filter_length_le_of_imp p q hImp rest)
    · have hpF : p x = false := Bool.eq_false_iff.mpr hp
      by_cases hq : q x = true
      · simp only [List.filter_cons, hpF, hq, Bool.false_eq_true, ↓reduceIte, List.length_cons]
        exact Nat.le_succ_of_le (filter_length_le_of_imp p q hImp rest)
      · have hqF : q x = false := Bool.eq_false_iff.mpr hq
        simp only [List.filter_cons, hpF, hqF, Bool.false_eq_true, ↓reduceIte]
        exact filter_length_le_of_imp p q hImp rest

/-- ✅ 補題 CB-7a (`quotient_bucket_count_ge_group_count`):
    任意のキー `k` の真のグループ度数は、その商バケット `b k` の合計度数以下である。 -/
theorem quotient_bucket_count_ge_group_count
    {Row K B : Type} [DecidableEq K] [DecidableEq B]
    (b : K → B) (key : Row → K) (R : Rel Row) (k : K) :
    groupCount key R k ≤ bucketCount b key R (b k) := by
  unfold groupCount bucketCount
  apply filter_length_le_of_imp
  intro r hr
  have hk : key r = k := beq_iff_eq.mp hr
  exact beq_iff_eq.mpr (congrArg b hk)

/-- 🚨 トラップ反例 CB-7 (`quotient_bucket_asc_order_pruning_bug`):
    `ORDER BY COUNT(*) ASC LIMIT 1` に対して商バケット度数で「小さいバケットだけ残す」刈り込みを行うと、
    高頻度キー（出現 10 回）と同じバケットに衝突した希少キー（出現 1 回、真の最小グループ！）の
    バケット度数が `11` に膨らみ、単独バケット（出現 2 回、バケット度数 `2`）に負けて誤脱落する。 -/
theorem quotient_bucket_asc_order_pruning_bug :
    ∃ (R : List Int) (b : Int → Int),
      groupCount id R 1 = 1 ∧
      groupCount id R 2 = 2 ∧
      bucketCount b id R (b 1) > bucketCount b id R (b 2) := by
  refine ⟨[1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3], (fun k => if k == 2 then 1 else 0), ?_, ?_, ?_⟩
  · decide
  · decide
  · decide

private theorem filter_comm_same_key {Row K B : Type} [DecidableEq K] [DecidableEq B]
    (b : K → B) (key : Row → K) (S : List B) (k : K)
    (hkS : S.contains (b k) = true) :
    ∀ (R : Rel Row),
      (R.filter (fun r => S.contains (b (key r)))).filter (fun r => key r == k) =
        R.filter (fun r => key r == k)
  | [] => rfl
  | r :: rest => by
    by_cases hkr : key r = k
    · have hbr : S.contains (b (key r)) = true := by rw [hkr]; exact hkS
      simp only [List.filter_cons, hbr, beq_iff_eq.mpr hkr, ↓reduceIte]
      exact congrArg (r :: ·) (filter_comm_same_key b key S k hkS rest)
    · have hkrF : (key r == k) = false := Bool.eq_false_iff.mpr (fun h => hkr (beq_iff_eq.mp h))
      by_cases hbr : S.contains (b (key r)) = true
      · simp only [List.filter_cons, hbr, hkrF, Bool.false_eq_true, ↓reduceIte]
        exact filter_comm_same_key b key S k hkS rest
      · have hbrF : S.contains (b (key r)) = false := Bool.eq_false_iff.mpr hbr
        simp only [List.filter_cons, hbrF, hkrF, Bool.false_eq_true, ↓reduceIte]
        exact filter_comm_same_key b key S k hkS rest

/-- ✅ 証明付き最適化 CB-7 (`quotient_bucket_topk_pruning_sound`):
    候補バケット集合 `S` 外のすべてのバケット `u ∉ S` について `bucketCount b key R u < τ` が成り立つとき：
    1. `groupCount key R k ≥ τ` を満たす任意のグループ `k` は必ず `b k ∈ S` を満たす（偽陰性ゼロ・境界同着保存）。
    2. `S` で事前フィルタした部分関係 `R_S := R.filter (fun r => S.contains (b (key r)))` 上の
       各候補グループの度数 `groupCount key R_S k` は、全表 `R` 上の厳密度数と **100% 一致** する！ -/
theorem quotient_bucket_topk_pruning_sound
    {Row K B : Type} [DecidableEq K] [DecidableEq B]
    (b : K → B) (key : Row → K) (R : Rel Row) (S : List B) (τ : Nat)
    (hBound : ∀ u : B, S.contains u = false → bucketCount b key R u < τ) :
    let R_S := R.filter (fun r => S.contains (b (key r)))
    (∀ k : K, τ ≤ groupCount key R k → S.contains (b k) = true) ∧
    (∀ k : K, S.contains (b k) = true → groupCount key R_S k = groupCount key R k) := by
  refine ⟨?_, ?_⟩
  · intro k hGe
    by_cases hIn : S.contains (b k) = true
    · exact hIn
    · have hFalse : S.contains (b k) = false := Bool.eq_false_iff.mpr hIn
      have hLt : bucketCount b key R (b k) < τ := hBound (b k) hFalse
      have hLe : groupCount key R k ≤ bucketCount b key R (b k) :=
        quotient_bucket_count_ge_group_count b key R k
      omega
  · intro k hkS
    unfold groupCount
    exact congrArg List.length (filter_comm_same_key b key S k hkS R)

/-!
## Rule CB-8: Markov / 鳩の巣原理による `HAVING COUNT(*) >= T` 基数上界と 2段遅延集約 (ClickBench Q28, Q29, Q33)
-/

/-- 🚨 トラップ反例 CB-8 (`having_sum_signed_unbounded_groups_bug`):
    `COUNT(*) >= T` は非負性によりグループ数を `|R| / T` 以下に抑えるが、符号付き列の `HAVING SUM(x) <= -T` 等では
    負値の存在により `T` と無関係に多数のグループが通過しうる。 -/
theorem having_sum_signed_unbounded_groups_bug :
    ∃ (R : Rel (Int × Int)) (T : Nat),
      T = 10 ∧
      ((groupBy Prod.fst (fun _ b => b.foldl (fun acc r => acc + r.2) 0) R).filter
        (fun kv => kv.2 ≤ -(T : Int))).length > R.length / T := by
  refine ⟨[(1, -20), (2, -20), (3, -20), (4, -20)], 10, rfl, ?_⟩
  decide

private theorem nodup_filter_pred {α : Type} (p : α → Bool) :
    ∀ {xs : List α}, xs.Nodup → (xs.filter p).Nodup
  | [], _ => List.nodup_nil
  | x :: rest, h => by
    rw [List.nodup_cons] at h
    by_cases hp : p x = true
    · simp only [List.filter_cons, hp, ↓reduceIte, List.nodup_cons]
      refine ⟨?_, nodup_filter_pred p h.2⟩
      intro hMem
      exact h.1 (List.mem_filter.mp hMem).1
    · have hpF : p x = false := Bool.eq_false_iff.mpr hp
      simp only [List.filter_cons, hpF, Bool.false_eq_true, ↓reduceIte]
      exact nodup_filter_pred p h.2

private theorem map_const_zero_sum {α : Type} :
    ∀ (l : List α), (l.map (fun (_ : α) => (0 : Nat))).sum = 0
  | [] => rfl
  | _ :: rest => by simp [map_const_zero_sum rest]

private theorem disjoint_fibers_step {Row K : Type} [DecidableEq K]
    (key : Row → K) (x : Row) (rest : Rel Row) :
    ∀ (l : List K), l.Nodup →
      (l.map (fun k => ((x :: rest).filter (fun r => key r == k)).length)).sum =
        (l.map (fun k => (rest.filter (fun r => key r == k)).length)).sum +
          (if l.contains (key x) then 1 else 0)
  | [], _ => by simp
  | k :: tail, hl => by
    rw [List.nodup_cons] at hl
    have ih := disjoint_fibers_step key x rest tail hl.2
    rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons, ih]
    by_cases hkx : key x = k
    · subst hkx
      have hNotTail : tail.contains (key x) = false := by
        apply Bool.eq_false_iff.mpr
        intro hMem
        exact hl.1 (List.contains_iff_mem.mp hMem)
      have hHead : ((x :: rest).filter (fun r => key r == key x)).length =
          (rest.filter (fun r => key r == key x)).length + 1 := by
        simp
      have hCont : (key x :: tail).contains (key x) = true := by simp
      rw [hHead, hCont, hNotTail]
      simp only [Bool.false_eq_true, ↓reduceIte]
      omega
    · have hBeqF : (key x == k) = false :=
        Bool.eq_false_iff.mpr (fun h => hkx (beq_iff_eq.mp h))
      have hHead : ((x :: rest).filter (fun r => key r == k)).length =
          (rest.filter (fun r => key r == k)).length := by
        simp [hBeqF]
      have hContEq : ((k :: tail).contains (key x)) = tail.contains (key x) := by
        rw [List.contains_cons]
        simp [hBeqF]
      rw [hHead, hContEq]
      omega

private theorem disjoint_fibers_sum_le_length {Row K : Type} [DecidableEq K]
    (key : Row → K) :
    ∀ (R : Rel Row) (ks : List K), ks.Nodup →
      (ks.map (fun k => (R.filter (fun r => key r == k)).length)).sum ≤ R.length
  | [], ks, _ => by
    simpa using Nat.le_of_eq (map_const_zero_sum ks)
  | x :: rest, ks, hNodup => by
    rw [disjoint_fibers_step key x rest ks hNodup]
    have h2 := disjoint_fibers_sum_le_length key rest ks hNodup
    have h3 : (if ks.contains (key x) then 1 else 0) ≤ 1 := by
      split <;> omega
    simp only [List.length_cons]
    omega

private theorem sum_ge_mul_length {T : Nat} :
    ∀ (xs : List Nat), (∀ n ∈ xs, T ≤ n) → T * xs.length ≤ xs.sum
  | [], _ => by simp
  | x :: rest, hAll => by
    have hx : T ≤ x := hAll x List.mem_cons_self
    have hRest : T * rest.length ≤ rest.sum :=
      sum_ge_mul_length rest (fun n hn => hAll n (List.mem_cons_of_mem x hn))
    simp only [List.length_cons, List.sum_cons, Nat.mul_succ]
    omega

/-- ✅ 証明付き最適化 CB-8a (`having_count_markov_bound_sound`):
    任意のキー `key` と閾値 `T` に対し、`COUNT(*) ≥ T` を満たす相異なるグループ数は
    `T * heavyKeys.length ≤ R.length`（`T ≥ 1` なら `≤ R.length / T`）を満たす！ -/
theorem having_count_markov_bound_sound
    {Row K : Type} [DecidableEq K] (key : Row → K) (R : Rel Row) (T : Nat) :
    let heavyKeys := (distinctKeys (R.map key)).filter (fun k => decide (T ≤ groupCount key R k))
    T * heavyKeys.length ≤ R.length := by
  intro heavyKeys
  have hNodup : heavyKeys.Nodup :=
    nodup_filter_pred _ (nodup_distinctKeys (R.map key))
  have hUpper : (heavyKeys.map (fun k => groupCount key R k)).sum ≤ R.length := by
    unfold groupCount
    exact disjoint_fibers_sum_le_length key R heavyKeys hNodup
  have hLower : T * (heavyKeys.map (fun k => groupCount key R k)).length ≤
      (heavyKeys.map (fun k => groupCount key R k)).sum := by
    apply sum_ge_mul_length
    intro n hn
    rw [List.mem_map] at hn
    rcases hn with ⟨k, hkMem, rfl⟩
    have hkFilt := (List.mem_filter.mp hkMem).2
    exact of_decide_eq_true hkFilt
  rw [List.length_map] at hLower
  exact Nat.le_trans hLower hUpper

private theorem filter_comm_in_winners {Row K : Type} [DecidableEq K]
    (key : Row → K) (W : List K) (k : K) (hk : W.contains k = true) :
    ∀ (R : Rel Row),
      (R.filter (fun r => W.contains (key r))).filter (fun r => key r == k) =
        R.filter (fun r => key r == k)
  | [] => rfl
  | r :: rest => by
    by_cases hkr : key r = k
    · have hw : W.contains (key r) = true := by rw [hkr]; exact hk
      simp only [List.filter_cons, hw, beq_iff_eq.mpr hkr, ↓reduceIte]
      exact congrArg (r :: ·) (filter_comm_in_winners key W k hk rest)
    · have hkrF : (key r == k) = false := Bool.eq_false_iff.mpr (fun h => hkr (beq_iff_eq.mp h))
      by_cases hw : W.contains (key r) = true
      · simp only [List.filter_cons, hw, hkrF, Bool.false_eq_true, ↓reduceIte]
        exact filter_comm_in_winners key W k hk rest
      · have hwF : W.contains (key r) = false := Bool.eq_false_iff.mpr hw
        simp only [List.filter_cons, hwF, hkrF, Bool.false_eq_true, ↓reduceIte]
        exact filter_comm_in_winners key W k hk rest

/-- ✅ 証明付き最適化 CB-8b (`having_two_stage_late_agg_sound`):
    `HAVING` が軽量集約 `agg1` とキーのみに依存するとき、`ORDER BY` が遅延集約 `agg2` を参照していても、
    Stage 1 で `agg1` と `HAVING` だけを評価して生存キー集合 `W` を確定し、Stage 2 で `W` に属する行だけに
    対して `agg2` と `ORDER BY` を評価した結果列は、全グループ一括評価と完全一致する！ -/
theorem having_two_stage_late_agg_sound
    {Row K V1 V2 : Type} [DecidableEq K]
    (key : Row → K) (agg1 : K → Rel Row → V1) (agg2 : K → Rel Row → V2)
    (hav : K → V1 → Bool)
    (leOrd : (K × (V1 × V2)) → (K × (V1 × V2)) → Bool)
    (R : Rel Row) :
    let stage1 := (groupBy key agg1 R).filter (fun kv => hav kv.1 kv.2)
    let W := stage1.map Prod.fst
    let R_W := R.filter (fun r => W.contains (key r))
    let twoStage := sortByRank leOrd
      (stage1.map (fun kv =>
        (kv.1, (kv.2, agg2 kv.1 (R_W.filter (fun r => key r == kv.1))))))
    let eager := sortByRank leOrd
      ((groupBy key (fun k b => (agg1 k b, agg2 k b)) R).filter
        (fun kv => hav kv.1 kv.2.1))
    twoStage = eager := by
  intro stage1 W R_W twoStage eager
  suffices hInner :
      stage1.map (fun kv =>
        (kv.1, (kv.2, agg2 kv.1 (R_W.filter (fun r => key r == kv.1))))) =
      (groupBy key (fun k b => (agg1 k b, agg2 k b)) R).filter
        (fun kv => hav kv.1 kv.2.1) by
    exact congrArg (sortByRank leOrd) hInner
  have hStep1 :
      stage1.map (fun kv =>
        (kv.1, (kv.2, agg2 kv.1 (R_W.filter (fun r => key r == kv.1))))) =
      stage1.map (fun kv =>
        (kv.1, (kv.2, agg2 kv.1 (R.filter (fun r => key r == kv.1))))) := by
    apply List.map_congr_left
    intro kv hMem
    have hkW : W.contains kv.1 = true :=
      List.contains_iff_mem.mpr (List.mem_map_of_mem hMem)
    rw [filter_comm_in_winners key W kv.1 hkW R]
  rw [hStep1]
  unfold stage1 groupBy
  clear stage1 W R_W twoStage eager hStep1
  induction (distinctKeys (R.map key)) with
  | nil => rfl
  | cons k rest ih =>
    by_cases hh : hav k (agg1 k (R.filter (fun x => key x == k))) = true
    · simp [hh, ih]
    · have hhF : hav k (agg1 k (R.filter (fun x => key x == k))) = false :=
        Bool.eq_false_iff.mpr hh
      simp [hhF, ih]

/-!
## Rule CB-9: アフィン線形和 `SUM(col + c)` の準同型分解 (ClickBench Q30)
-/

def sqlAddOpt (a b : SqlVal Int) : SqlVal Int := sqlAdd a b
def sqlMulOpt (a b : SqlVal Int) : SqlVal Int := sqlMul a b

def sqlCountNonNull {α : Type} (xs : List (SqlVal α)) : Int :=
  ((xs.filter Option.isSome).length : Int)

/-- 🚨 トラップ反例 CB-9a (`affine_sum_coalesce_empty_bug`):
    `COALESCE(SUM(x), 0) + c * COUNT(x)` に書き換えると全行 `NULL` 時に `NULL` ではなく `0` を返してしまう。 -/
theorem affine_sum_coalesce_empty_bug :
    let emptyRel : Rel (SqlVal Int) := [none, none]
    let c : Int := 5
    sqlSum (fun r => sqlAddOpt r (some c)) emptyRel = none ∧
    some ((sqlSum id emptyRel).getD 0 + c * sqlCountNonNull emptyRel) = some 0 := by
  decide

/-- 🚨 トラップ反例 CB-9b (`affine_sum_overflow_masked_bug`):
    境界証明なしに `SUM(x + 1)` を `SUM(x) + 1 * COUNT(x)` に書き換えると先頭行のオーバーフローが隠蔽される。 -/
theorem affine_sum_overflow_masked_bug :
    let x0 : Int := int64Max
    let x1 : Int := -5
    checkedAdd64 (checkedAdd64 (.val x0) (.val 1)) (checkedAdd64 (.val x1) (.val 1)) = .err ∧
    checkedAdd64 (checkedAdd64 (.val x0) (.val x1)) (.val 2) = .val (int64Max - 3) := by
  decide

private theorem sqlSum_id_none_iff :
    ∀ (xs : List (SqlVal Int)),
      sqlSum id xs = none → sqlCountNonNull xs = 0
  | [], _ => rfl
  | none :: rest, h => by
    have hTail : sqlSum id rest = none := by
      unfold sqlSum aggFold sumMonoid at h ⊢
      simpa [mergeSum] using h
    have hCnt : sqlCountNonNull (none :: rest) = sqlCountNonNull rest := rfl
    rw [hCnt]
    exact sqlSum_id_none_iff rest hTail
  | some v :: rest, h => by
    have hStep : sqlSum id (some v :: rest) = mergeSum (some v) (sqlSum id rest) := rfl
    rw [hStep] at h
    cases hR : sqlSum id rest <;> simp [hR, mergeSum] at h

/-- ✅ 証明付き最適化 CB-9 (`affine_sum_homomorphism_sound`):
    任意の `SqlVal Int` リスト `xs`（空リスト・全 `NULL`・部分 `NULL` すべてを含む）と定数 `c` に対し、
    `SUM(x + c)` は `SUM(x) + c * COUNT(x)` と **3値 NULL 伝播まで含めて恒等的に一致** する！ -/
theorem affine_sum_homomorphism_sound (c : Int) :
    ∀ (xs : List (SqlVal Int)),
      sqlSum (fun r => sqlAddOpt r (some c)) xs =
        sqlAddOpt (sqlSum id xs) (sqlMulOpt (some c) (some (sqlCountNonNull xs)))
  | [] => rfl
  | none :: rest => by
    have ih := affine_sum_homomorphism_sound c rest
    have hL : sqlSum (fun r => sqlAddOpt r (some c)) (none :: rest) =
        sqlSum (fun r => sqlAddOpt r (some c)) rest := rfl
    have hR1 : sqlSum id (none :: rest) = sqlSum id rest := rfl
    have hR2 : sqlCountNonNull (none :: rest) = sqlCountNonNull rest := rfl
    rw [hL, hR1, hR2, ih]
  | some v :: rest => by
    have ih := affine_sum_homomorphism_sound c rest
    have hCnt : sqlCountNonNull (some v :: rest) = sqlCountNonNull rest + 1 := by
      unfold sqlCountNonNull
      simp only [List.filter_cons, Option.isSome_some, ↓reduceIte, List.length_cons]
      omega
    have hL : sqlSum (fun r => sqlAddOpt r (some c)) (some v :: rest) =
        mergeSum (some (v + c)) (sqlSum (fun r => sqlAddOpt r (some c)) rest) := rfl
    have hR : sqlSum id (some v :: rest) = mergeSum (some v) (sqlSum id rest) := rfl
    rw [hL, hR, hCnt, ih]
    cases hSumR : sqlSum id rest with
    | none =>
      have hZeroCnt : sqlCountNonNull rest = 0 := sqlSum_id_none_iff rest hSumR
      rw [hZeroCnt]
      simp only [sqlAddOpt, sqlAdd, sqlMulOpt, sqlMul, mergeSum]
      congr 1
      omega
    | some s =>
      simp only [sqlAddOpt, sqlAdd, sqlMulOpt, sqlMul, mergeSum]
      congr 1
      rw [Int.mul_add, Int.mul_one]
      omega

/-!
## Rule CB-10: 無オーバーフロー証明付き並列 `SUM(INT64)` ＆ 定数ブロック `O(1)` 畳み込み (ClickBench Q03, Q30)
-/

def checkedSumList64 : List Int → EvalRes Int
  | [] => .val 0
  | x :: rest => checkedAdd64 (.val x) (checkedSumList64 rest)

/-- 🚨 トラップ反例 CB-10 (`unbounded_int64_sum_reordering_overflow_bug`):
    正負の巨大値を含む列では評価順の入替で中間和オーバーフローの有無が変化する。 -/
theorem unbounded_int64_sum_reordering_overflow_bug :
    checkedSumList64 [5, int64Max, -10] = .val (int64Max - 5) ∧
    checkedSumList64 [-10, int64Max, 5] = .err := by
  decide

private theorem checkedSumList64_eq_val_of_bounds :
    ∀ (xs : List Int) (lo hi : Int),
      lo ≤ 0 → 0 ≤ hi →
      (∀ x ∈ xs, lo ≤ x ∧ x ≤ hi) →
      int64Min ≤ (xs.length : Int) * lo →
      (xs.length : Int) * hi ≤ int64Max →
      checkedSumList64 xs = .val xs.sum ∧
      (xs.length : Int) * lo ≤ xs.sum ∧
      xs.sum ≤ (xs.length : Int) * hi
  | [], _, _, _, _, _, _, _ => by simp [checkedSumList64]
  | x :: rest, lo, hi, hLo0, hHi0, hMem, hMinB, hMaxB => by
    have hx := hMem x List.mem_cons_self
    have hExpandLo : ((x :: rest).length : Int) * lo = (rest.length : Int) * lo + lo := by
      simp only [List.length_cons]
      have hCast : ((rest.length + 1 : Nat) : Int) = (rest.length : Int) + 1 := by omega
      rw [hCast, Int.add_mul, Int.one_mul]
    have hExpandHi : ((x :: rest).length : Int) * hi = (rest.length : Int) * hi + hi := by
      simp only [List.length_cons]
      have hCast : ((rest.length + 1 : Nat) : Int) = (rest.length : Int) + 1 := by omega
      rw [hCast, Int.add_mul, Int.one_mul]
    have hRestMin : int64Min ≤ (rest.length : Int) * lo := by omega
    have hRestMax : (rest.length : Int) * hi ≤ int64Max := by omega
    rcases checkedSumList64_eq_val_of_bounds rest lo hi hLo0 hHi0
      (fun y hy => hMem y (List.mem_cons_of_mem x hy)) hRestMin hRestMax with
      ⟨hValR, hLoR, hHiR⟩
    have hSumLo : ((x :: rest).length : Int) * lo ≤ x + rest.sum := by omega
    have hSumHi : x + rest.sum ≤ ((x :: rest).length : Int) * hi := by omega
    have hIn64 : inInt64 (x + rest.sum) := ⟨by omega, by omega⟩
    refine ⟨?_, ?_, ?_⟩
    · simp only [checkedSumList64, hValR, checkedAdd64, check64, hIn64, ↓reduceIte, List.sum_cons]
    · simpa [List.sum_cons] using hSumLo
    · simpa [List.sum_cons] using hSumHi

/-- ✅ 証明付き最適化 CB-10a (`bounded_int64_sum_parallel_sound`):
    `lo ≤ 0 ≤ hi` かつ全要素が `[lo, hi]` に収まり、`N * lo ≥ int64Min`・`N * hi ≤ int64Max` が
    保証されているとき、任意の 2 パーティション `xs`, `ys` の並列部分和マージは直列 `checkedSumList64 (xs ++ ys)` と
    オーバーフロー無しで厳密一致する！ -/
theorem bounded_int64_sum_parallel_sound
    (xs ys : List Int) (lo hi : Int)
    (hLo0 : lo ≤ 0) (hHi0 : 0 ≤ hi)
    (hMem : ∀ z ∈ xs ++ ys, lo ≤ z ∧ z ≤ hi)
    (hMinB : int64Min ≤ ((xs ++ ys).length : Int) * lo)
    (hMaxB : ((xs ++ ys).length : Int) * hi ≤ int64Max) :
    checkedSumList64 (xs ++ ys) =
      checkedAdd64 (checkedSumList64 xs) (checkedSumList64 ys) := by
  have hSplitLo : ((xs ++ ys).length : Int) * lo =
      (xs.length : Int) * lo + (ys.length : Int) * lo := by
    rw [List.length_append]
    have hCast : ((xs.length + ys.length : Nat) : Int) = (xs.length : Int) + (ys.length : Int) := by omega
    rw [hCast, Int.add_mul]
  have hSplitHi : ((xs ++ ys).length : Int) * hi =
      (xs.length : Int) * hi + (ys.length : Int) * hi := by
    rw [List.length_append]
    have hCast : ((xs.length + ys.length : Nat) : Int) = (xs.length : Int) + (ys.length : Int) := by omega
    rw [hCast, Int.add_mul]
  have hYLoNonpos : (ys.length : Int) * lo ≤ 0 :=
    Int.mul_nonpos_of_nonneg_of_nonpos (Int.natCast_nonneg _) hLo0
  have hYHiNonneg : 0 ≤ (ys.length : Int) * hi :=
    Int.mul_nonneg (Int.natCast_nonneg _) hHi0
  have hXLoNonpos : (xs.length : Int) * lo ≤ 0 :=
    Int.mul_nonpos_of_nonneg_of_nonpos (Int.natCast_nonneg _) hLo0
  have hXHiNonneg : 0 ≤ (xs.length : Int) * hi :=
    Int.mul_nonneg (Int.natCast_nonneg _) hHi0
  rcases checkedSumList64_eq_val_of_bounds (xs ++ ys) lo hi hLo0 hHi0 hMem hMinB hMaxB with
    ⟨hXY, hSumLo, hSumHi⟩
  rcases checkedSumList64_eq_val_of_bounds xs lo hi hLo0 hHi0
    (fun z hz => hMem z (List.mem_append_left ys hz)) (by omega) (by omega) with ⟨hX, _, _⟩
  rcases checkedSumList64_eq_val_of_bounds ys lo hi hLo0 hHi0
    (fun z hz => hMem z (List.mem_append_right xs hz)) (by omega) (by omega) with ⟨hY, _, _⟩
  have hIn64 : inInt64 (xs.sum + ys.sum) := by
    have hEqS : (xs ++ ys).sum = xs.sum + ys.sum := List.sum_append
    exact ⟨by omega, by omega⟩
  rw [hXY, hX, hY, List.sum_append]
  simp [checkedAdd64, check64, hIn64]

private theorem replicate_sqlSum_succ (v : Int) :
    ∀ (m : Nat), sqlSum id (List.replicate (m + 1) (some v)) = some (((m + 1 : Nat) : Int) * v)
  | 0 => by
    unfold sqlSum aggFold sumMonoid mergeSum
    simp
  | m + 1 => by
    have ih := replicate_sqlSum_succ v m
    have hStep : sqlSum id (List.replicate (m + 2) (some v)) =
        mergeSum (some v) (sqlSum id (List.replicate (m + 1) (some v))) := rfl
    rw [hStep, ih]
    simp only [mergeSum]
    congr 1
    have hCast : ((m + 2 : Nat) : Int) = 1 + ((m + 1 : Nat) : Int) := by omega
    rw [hCast, Int.add_mul, Int.one_mul]

/-- ✅ 証明付き最適化 CB-10b (`const_block_sum_fold_sound`):
    `min_int == max_int == v` かつ非 NULL のブロック（`List.replicate n (some v)`、`n ≥ 1`）に対する
    `sqlSum` は、ブロックを一切デコードせずに `some ((n : Int) * v)` で `O(1)` 計算できる！ -/
theorem const_block_sum_fold_sound (n : Nat) (v : Int) (hn : 0 < n) :
    sqlSum id (List.replicate n (some v)) = some ((n : Int) * v) := by
  cases n with
  | zero => omega
  | succ m => exact replicate_sqlSum_succ v m

/-!
## Rule CB-11: 辞書厳密頻度 `freq(d)` による `COUNT` 畳み込み ＆ 無フィルタ `MIN`/`MAX` 列境界即答
(ClickBench Q02, Q03, Q07, Q08)
-/

/-- 🚨 トラップ反例 CB-11 (`filtered_extremum_bound_unsound_bug`):
    フィルタ `WHERE P(x)` がある場合に Zone-Map 通過ブロックの `blk_min` をそのまま返すと誤答する。 -/
theorem filtered_extremum_bound_unsound_bug :
    ∃ (blk : List Int) (P : Int → Bool),
      (blk.any P = true) ∧
      sqlMin (fun x => some x) (blk.filter P) ≠ sqlMin (fun x => some x) blk := by
  refine ⟨[1, 10], (fun x => decide (x ≥ 5)), ?_, ?_⟩
  · decide
  · decide

private theorem dict_freq_step (c0 : Nat) (rest : List Nat) :
    ∀ (l : List Nat), l.Nodup →
      (l.map (fun d => ((c0 :: rest).filter (· == d)).length)).sum =
        (l.map (fun d => (rest.filter (· == d)).length)).sum +
          (if c0 ∈ l then 1 else 0)
  | [], _ => by simp
  | d :: tail, hl => by
    rw [List.nodup_cons] at hl
    have ih := dict_freq_step c0 rest tail hl.2
    rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons, ih]
    by_cases heq : c0 = d
    · subst heq
      have hNotMem : c0 ∉ tail := hl.1
      have hHead : ((c0 :: rest).filter (· == c0)).length =
          (rest.filter (· == c0)).length + 1 := by
        simp
      rw [hHead]
      simp only [List.mem_cons, true_or, ↓reduceIte, hNotMem]
      omega
    · have hNeqB : (c0 == d) = false :=
        Bool.eq_false_iff.mpr (fun h => heq (beq_iff_eq.mp h))
      have hHead : ((c0 :: rest).filter (· == d)).length =
          (rest.filter (· == d)).length := by
        simp [hNeqB]
      rw [hHead]
      simp only [List.mem_cons, heq, false_or]
      omega

/-- ✅ 証明付き最適化 CB-11 (`dict_exact_freq_filtered_count_sound`):
    辞書配列 `dict : Nat → Val` とコード列 `codes : List Nat` で表現された列について、
    任意の単一列述語 `P : Val → Bool` によるフィルタ件数は、辞書頻度表 `freq(d)` の `O(|D|)` 和に完全一致する！ -/
theorem dict_exact_freq_filtered_count_sound
    {Val : Type} (dict : Nat → Val) (P : Val → Bool) (codes : List Nat)
    (D : List Nat) (hNodup : D.Nodup) (hCover : ∀ c ∈ codes, c ∈ D) :
    (codes.filter (fun c => P (dict c))).length =
      ((D.filter (fun d => P (dict d))).map (fun d => (codes.filter (· == d)).length)).sum := by
  induction codes with
  | nil =>
    simpa using (map_const_zero_sum (D.filter (fun d => P (dict d)))).symm
  | cons c0 rest ih =>
    have hc0 : c0 ∈ D := hCover c0 List.mem_cons_self
    have hRest := ih (fun c hc => hCover c (List.mem_cons_of_mem c0 hc))
    have hNodupFilt : (D.filter (fun d => P (dict d))).Nodup :=
      nodup_filter_pred _ hNodup
    rw [dict_freq_step c0 rest (D.filter (fun d => P (dict d))) hNodupFilt, ← hRest]
    by_cases hp : P (dict c0) = true
    · have hIn : c0 ∈ D.filter (fun d => P (dict d)) :=
        List.mem_filter.mpr ⟨hc0, hp⟩
      simp [hp, hIn]
    · have hpF : P (dict c0) = false := Bool.eq_false_iff.mpr hp
      have hNotIn : c0 ∉ D.filter (fun d => P (dict d)) := by
        intro hm
        exact hp (List.mem_filter.mp hm).2
      simp [hpF, hNotIn]

/-!
## Rule CB-12: 最小元除外 (`col <> ''`) による Zone-Map 下界引き締めと `Top-K` 早期打ち切り (ClickBench Q26)
-/

/-- 🚨 トラップ反例 CB-12 (`interior_exclusion_min_bound_unsound_bug`):
    ボトム元ではない内部値 `c = "middle"` に対する不等号フィルタ `WHERE s <> "middle"` では、
    `"aaa" < "middle"` のような行も生存するため、`s > "middle"` の最小値を下界にすると誤刈り込みが起きる。 -/
theorem interior_exclusion_min_bound_unsound_bug :
    let blk : List String := ["aaa", "middle", "zzz"]
    let cutoff : String := "bbb"
    ("zzz" > cutoff) ∧
    ∃ s ∈ blk, s ≠ "middle" ∧ s < cutoff := by
  refine ⟨by decide, "aaa", by decide, by decide, by decide⟩

/-- ✅ 証明付き最適化 CB-12 (`bottom_exclusion_tightens_min_bound_sound`):
    ボトム元 `bot`（例: `String` 上の `""`）について、ブロック `blk` 内のすべての非ボトム要素 `v ≠ bot` が
    非ボトム下界 `lb_nonbot` 以上（`le lb_nonbot v = true`）であり、かつ現在の `Top-K` 境界 `cutoff` よりも
    `lb_nonbot` が真に大きい（`le lb_nonbot cutoff = false`）ならば、`WHERE x <> bot` を通過するブロック内の
    いかなる要素 `x` も `le x cutoff = true` になり得ず、ブロック全体を 100% 安全にスキップできる！ -/
theorem bottom_exclusion_tightens_min_bound_sound
    {α : Type} [DecidableEq α] (le : α → α → Bool) (bot lb_nonbot cutoff : α)
    (hTrans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (blk : List α)
    (hNonBotBound : ∀ v ∈ blk, v ≠ bot → le lb_nonbot v = true)
    (hStrictlyWorse : le lb_nonbot cutoff = false) :
    ∀ x ∈ blk.filter (fun v => v != bot), le x cutoff = false := by
  intro x hx
  rcases List.mem_filter.mp hx with ⟨hxMem, hxNeB⟩
  have hxNe : x ≠ bot := by
    intro hEq
    subst hEq
    simp at hxNeB
  have hLbLeX : le lb_nonbot x = true := hNonBotBound x hxMem hxNe
  by_cases hXLeCut : le x cutoff = true
  · have hContr : le lb_nonbot cutoff = true := hTrans lb_nonbot x cutoff hLbLeX hXLeCut
    rw [hStrictlyWorse] at hContr
    contradiction
  · exact Bool.eq_false_iff.mpr hXLeCut

/-!
## Rule CB-13: 複数アフィン整数和 `SUM(col + k_i)` の単一パス `(SUM(col), COUNT(col))` 融合と無オーバーフロー証明 (ClickBench Q30)
-/

def i64Min : Int := -(2 ^ 63)
def i64Max : Int := (2 ^ 63) - 1

def inI64 (x : Int) : Prop := i64Min ≤ x ∧ x ≤ i64Max

def rawNonNullSum : List (Option Int) → Int
  | [] => 0
  | none :: xs => rawNonNullSum xs
  | some v :: xs => v + rawNonNullSum xs

def nonNullCnt : List (Option Int) → Int
  | [] => 0
  | none :: xs => nonNullCnt xs
  | some _ :: xs => 1 + nonNullCnt xs

/-- 🚨 トラップ反例 CB-13 (`affine_sum_row_overflow_unsound_bug`):
    最終結果 `(SUM(x) + cnt * k)` が `Int64` に収まっていても、途中の個々の行 `x + k` が `i64Max` を超える場合
    （例: `xs = [some i64Max, some (-2)]`, `k = 1`）、AST 意味論は 1 行目でオーバーフロー例外を送出するため、
    ブロック min/max による行単位無オーバーフロー証明なしに `(SUM(x) + cnt * k)` へ畳み込んではならない！ -/
theorem affine_sum_row_overflow_unsound_bug :
    inI64 (rawNonNullSum [some i64Max, some (-2)] +
            nonNullCnt [some i64Max, some (-2)] * 1) ∧
    ∃ v ∈ ([some i64Max, some (-2)] : List (Option Int)),
      v = some i64Max ∧ ¬ inI64 (i64Max + 1) := by
  refine ⟨?_, some i64Max, by simp, rfl, ?_⟩
  · dsimp [inI64, rawNonNullSum, nonNullCnt, i64Min, i64Max]; omega
  · dsimp [inI64, i64Min, i64Max]; omega

/-- ✅ 証明付き最適化 CB-13 (`affine_sum_no_overflow_fusion_sound`):
    列ブロックの min/max 境界 `[lo, hi]` とシフト定数 `k` について `inI64 (lo + k)` かつ `inI64 (hi + k)` が成り立てば、
    任意の生存行 `some v` (`lo ≤ v ≤ hi`) において行単位の `v + k` は決して `Int64` オーバーフローを起こさず、
    かつ非 NULL 行全体の和 `rawNonNullSum (xs.map (Option.map (· + k)))` は
    単一パスで計算した `rawNonNullSum xs + nonNullCnt xs * k` と厳密に一致する！ -/
theorem affine_sum_no_overflow_fusion_sound
    (xs : List (Option Int)) (lo hi k : Int)
    (hLoOk : inI64 (lo + k))
    (hHiOk : inI64 (hi + k))
    (hBounds : ∀ v : Int, some v ∈ xs → lo ≤ v ∧ v ≤ hi) :
    (∀ v : Int, some v ∈ xs → inI64 (v + k)) ∧
    rawNonNullSum (xs.map (fun o => o.map (fun v => v + k))) =
      rawNonNullSum xs + nonNullCnt xs * k := by
  refine ⟨?_, ?_⟩
  · intro v hv
    rcases hBounds v hv with ⟨hLo, hHi⟩
    rcases hLoOk with ⟨hMin, _⟩
    rcases hHiOk with ⟨_, hMax⟩
    exact ⟨by omega, by omega⟩
  · induction xs with
    | nil => simp [rawNonNullSum, nonNullCnt]
    | cons o rest ih =>
      have ih' := ih (fun v hv => hBounds v (List.mem_cons_of_mem o hv))
      cases o with
      | none =>
        simp [rawNonNullSum, nonNullCnt, ih']
      | some v =>
        simp only [List.map_cons, Option.map_some, rawNonNullSum, nonNullCnt, ih',
                   Int.add_mul, Int.one_mul]
        omega

-- ============================================================================
-- Rule CB-14: ZoneMap Extremum Fold & Narrow-Dict Code Histogram Aggregation
-- Targets: ClickBench Q02, Q03, Q07, Q08
-- ============================================================================

def optMinInt : Option Int → Option Int → Option Int
  | none, b => b
  | some a, none => some a
  | some a, some b => some (min a b)

def listMinInt : List (Option Int) → Option Int
  | [] => none
  | x :: xs => optMinInt x (listMinInt xs)

theorem optMinInt_assoc (a b c : Option Int) :
    optMinInt (optMinInt a b) c = optMinInt a (optMinInt b c) := by
  cases a <;> cases b <;> cases c <;> simp [optMinInt, Int.min_assoc]

/-- ✅ 証明付き最適化 CB-14a (`block_zonemap_min_fold_sound`):
    全テーブルの非 NULL `MIN(col)`（同様に双対の `MAX(col)`）は、各ブロックの ZoneMap 最小値 `listMinInt blk` を
    `optMinInt` で畳み込んだ値と空ブロック・全 NULL ブロックを含めて厳密に一致する。 -/
theorem block_zonemap_min_fold_sound (xs ys : List (Option Int)) :
    listMinInt (xs ++ ys) = optMinInt (listMinInt xs) (listMinInt ys) := by
  induction xs with
  | nil => simp [listMinInt, optMinInt]
  | cons x rest ih =>
    simp only [List.cons_append, listMinInt, ih, optMinInt_assoc]

/-- 1 バイト辞書コード列 `codes` 上で特定コード `c` の出現回数を数えるヒストグラム。 -/
def codeFreq (c : Nat) : List (Option Nat) → Int
  | [] => 0
  | none :: rest => codeFreq c rest
  | some x :: rest => (if x = c then 1 else 0) + codeFreq c rest

/-- `0 .. K-1` の辞書重み付き和 `Σ_{c < K} freq(c) * dict(c)`。 -/
def weightedHistSum (dict : Nat → Int) (codes : List (Option Nat)) : Nat → Int
  | 0 => 0
  | k + 1 => weightedHistSum dict codes k + codeFreq k codes * dict k

theorem weightedHistSum_cons_none (dict : Nat → Int) (codes : List (Option Nat)) (K : Nat) :
    weightedHistSum dict (none :: codes) K = weightedHistSum dict codes K := by
  induction K with
  | zero => rfl
  | succ k ih => simp [weightedHistSum, codeFreq, ih]

theorem weightedHistSum_cons_ge (dict : Nat → Int) (codes : List (Option Nat))
    (x K : Nat) (hge : K ≤ x) :
    weightedHistSum dict (some x :: codes) K = weightedHistSum dict codes K := by
  induction K with
  | zero => rfl
  | succ k ih =>
    have hne : x ≠ k := by omega
    have ih' := ih (by omega)
    simp [weightedHistSum, codeFreq, hne, ih']

theorem weightedHistSum_cons_some (dict : Nat → Int) (codes : List (Option Nat))
    (x K : Nat) (hx : x < K) :
    weightedHistSum dict (some x :: codes) K = dict x + weightedHistSum dict codes K := by
  induction K with
  | zero => omega
  | succ k ih =>
    by_cases hxk : x = k
    · subst hxk
      have hSub := weightedHistSum_cons_ge dict codes x x (Nat.le_refl x)
      simp only [weightedHistSum, codeFreq, ite_true, hSub, Int.add_mul, Int.one_mul]
      omega
    · have hlt : x < k := by omega
      have ih' := ih hlt
      simp only [weightedHistSum, codeFreq, hxk, ite_false, Int.zero_add, ih']
      omega

/-- ✅ 証明付き最適化 CB-14b (`narrow_dict_code_histogram_sum_sound`):
    辞書サイズ `K` 未満の 1 バイト辞書コード列 `codes` に対し、各行で `dict[code]` を展開して加算する `rawNonNullSum` は、
    コード頻度ヒストグラム `codeFreq` と辞書値の積和 `weightedHistSum dict codes K` と厳密に一致する。 -/
theorem narrow_dict_code_histogram_sum_sound
    (dict : Nat → Int) (K : Nat) (codes : List (Option Nat))
    (hValid : ∀ c : Nat, some c ∈ codes → c < K) :
    rawNonNullSum (codes.map (fun o => o.map dict)) = weightedHistSum dict codes K := by
  induction codes with
  | nil =>
    have hZero : ∀ k : Nat, weightedHistSum dict [] k = 0 := by
      intro k
      induction k with
      | zero => rfl
      | succ m ihm => simp [weightedHistSum, codeFreq, ihm]
    simp [rawNonNullSum, hZero K]
  | cons o rest ih =>
    have ih' := ih (fun c hc => hValid c (List.mem_cons_of_mem o hc))
    cases o with
    | none =>
      simp [rawNonNullSum, ih', weightedHistSum_cons_none]
    | some c =>
      have hc : c < K := hValid c (List.mem_cons_self)
      simp [rawNonNullSum, ih', weightedHistSum_cons_some dict rest c K hc]

-- ============================================================================
-- Rule CB-15: Consecutive Duplicate Run Elision for `COUNT(DISTINCT)`
-- Targets: ClickBench Q09, Q10, Q11, Q12, Q14
-- ============================================================================

/-- 隣接する同一要素を O(1) レジスタ比較で間引くストリームフィルタ。 -/
def dedupConsecutiveAux {α : Type} [DecidableEq α] (prev : α) : List α → List α
  | [] => []
  | x :: xs => if x = prev then dedupConsecutiveAux prev xs else x :: dedupConsecutiveAux x xs

def dedupConsecutive {α : Type} [DecidableEq α] : List α → List α
  | [] => []
  | x :: xs => x :: dedupConsecutiveAux x xs

theorem mem_dedupConsecutiveAux_iff {α : Type} [DecidableEq α] (prev v : α) (xs : List α) :
    (v = prev ∨ v ∈ dedupConsecutiveAux prev xs) ↔ (v = prev ∨ v ∈ xs) := by
  induction xs generalizing prev with
  | nil => simp [dedupConsecutiveAux]
  | cons x rest ih =>
    unfold dedupConsecutiveAux
    by_cases hxp : x = prev
    · subst hxp
      simp only [ite_true, ih]
      constructor
      · rintro (rfl | h)
        · exact Or.inl rfl
        · exact Or.inr (List.mem_cons_of_mem _ h)
      · rintro (rfl | h)
        · exact Or.inl rfl
        · rcases List.mem_cons.mp h with rfl | hr
          · exact Or.inl rfl
          · exact Or.inr hr
    · simp only [hxp, ite_false, List.mem_cons]
      constructor
      · rintro (rfl | rfl | hr)
        · exact Or.inl rfl
        · exact Or.inr (Or.inl rfl)
        · rcases (ih x).mp (Or.inr hr) with rfl | hrest
          · exact Or.inr (Or.inl rfl)
          · exact Or.inr (Or.inr hrest)
      · rintro (rfl | rfl | hrest)
        · exact Or.inl rfl
        · exact Or.inr (Or.inl rfl)
        · rcases (ih x).mpr (Or.inr hrest) with rfl | haux
          · exact Or.inr (Or.inl rfl)
          · exact Or.inr (Or.inr haux)

/-- ✅ 証明付き最適化 CB-15 (`consecutive_dup_distinct_elision_sound`):
    `(CounterID, EventDate, UserID)` 順クラスタ化ブロック等で頻出する隣接重複要素を
    ハッシュ集合挿入前にレジスタ比較でスキップしても、出現する異なる値の集合（したがって `COUNT(DISTINCT)`）は
    100% 不変である。 -/
theorem consecutive_dup_distinct_elision_sound {α : Type} [DecidableEq α] (xs : List α) (v : α) :
    v ∈ dedupConsecutive xs ↔ v ∈ xs := by
  cases xs with
  | nil => rfl
  | cons x rest =>
    dsimp [dedupConsecutive]
    simp only [List.mem_cons]
    exact mem_dedupConsecutiveAux_iff x v rest

-- ============================================================================
-- Rule CB-16: Whole-Block Concatenated Buffer Substring Superset Prefilter
-- Targets: ClickBench Q21, Q22, Q23, Q24 (`LIKE '%needle%'`)
-- ============================================================================

/-- `needle` が `haystack` の連続部分列であること（`LIKE '%needle%'` のバイト列モデル）。 -/
def hasContigSublist {α : Type} (needle haystack : List α) : Prop :=
  ∃ pre suf : List α, haystack = pre ++ needle ++ suf

/-- ✅ 証明付き最適化 CB-16 (`concat_buffer_substring_superset_sound`):
    ブロック内の文字列配列 `rows` のある行 `s ∈ rows` が `needle` を連続部分列として含むならば、
    連結バッファ `rows.flatten` も必ず `needle` を連続部分列として含む（偽陰性ゼロ！）。
    したがって連結バッファ全体を 1 回の SIMD `memmem` で走査し、ヒット位置の行だけを再検証するフィルタは 100% 健全である。 -/
theorem concat_buffer_substring_superset_sound
    {α : Type} (needle : List α) (rows : List (List α)) (s : List α)
    (hMem : s ∈ rows) (hSub : hasContigSublist needle s) :
    hasContigSublist needle rows.flatten := by
  induction rows with
  | nil => contradiction
  | cons r rs ih =>
    simp only [List.flatten_cons]
    rcases List.mem_cons.mp hMem with rfl | hrs
    · rcases hSub with ⟨pre, suf, rfl⟩
      refine ⟨pre, suf ++ rs.flatten, ?_⟩
      simp [List.append_assoc]
    · rcases ih hrs with ⟨pre, suf, hFlat⟩
      refine ⟨r ++ pre, suf, ?_⟩
      rw [hFlat]
      simp [List.append_assoc]

end TinylambOpt
