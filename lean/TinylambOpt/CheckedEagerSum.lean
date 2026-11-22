import Std
import Std.Tactic

/-
# 検査付き SUM の事前集約を結合前に移せる値域

C++ 対応 (`plan/cascades.cpp` の `eager_aggregation_over_join`、および
`AggregateAccumulator::Merge` による部分状態の統合):
  * 片側の INT64 値を結合キーごとに先に足してからファンアウト分だけマージする。
  * 適用条件は「部分集約前の行数・結合後のファンアウトに**証明済み**の上限がある」
    「値域は同じスナップショットの全行を覆う」。**推定行数を上限に使ってはならない**
    （issue `checked-eager-sum.md`）。ここでは
      - `totalPos l ≤ P` / `N ≤ totalNeg l`: スナップショット全体の正寄与・負寄与の上限/
        下限（全行を覆うので、どのキーの部分集合にもそのまま適用できる）。
      - `∀ k, fan k ≤ F`: ファンアウトの証明済み上限。
      - `P ≤ i64Max`, `i64Min ≤ N`, `(F:Int) * P ≤ i64Max`, `i64Min ≤ (F:Int) * N`:
        部分和と最終和が INT64 に収まることを保証する値域条件。
  * 正寄与と負寄与を別々に上から押さえることで、**未結合キー（fan = 0）を含むすべての
    部分和**が INT64 に収まり、逐次検査付き SUM がオーバーフローしない。
  * 最後に可換モノイド的性質（和の結合・分配）へ接続し、
    「結合後 SUM」と「事前集約 → ファンアウト分マージ」が同じ整数を返すことを示す。
境界例（下の定理で確認）:
  * 未結合キーに `[INT64_MAX, 1]` があると、結合後は成功 (0 行) するが事前集約は失敗する。
  * `[MAX, 1, -1]` は最終和だけ見れば範囲内だが、逐次検査は途中で失敗する。 -/

namespace TinylambOpt.CheckedEagerSum

abbrev Val := Option Int

def i64Min : Int := -9223372036854775808
def i64Max : Int :=  9223372036854775807

def InRange (z : Int) : Prop := i64Min ≤ z ∧ z ≤ i64Max

instance (z : Int) : Decidable (InRange z) := by
  unfold InRange
  infer_instance

/-- 正寄与 / 負寄与。 -/
def posPart (v : Int) : Int := if 0 ≤ v then v else 0
def negPart (v : Int) : Int := if v < 0 then v else 0

theorem posPart_nonneg (v : Int) : 0 ≤ posPart v := by
  unfold posPart
  split <;> omega
theorem negPart_nonpos (v : Int) : negPart v ≤ 0 := by
  unfold negPart
  split <;> omega
theorem pos_neg (v : Int) : posPart v + negPart v = v := by
  unfold posPart negPart
  by_cases h : 0 ≤ v <;> simp [h] <;> omega

def vPos : Val → Int
  | none => 0
  | some v => posPart v
def vNeg : Val → Int
  | none => 0
  | some v => negPart v
def vVal : Val → Int
  | none => 0
  | some v => v

theorem vPos_nonneg (o : Val) : 0 ≤ vPos o := by cases o <;> simp [vPos, posPart_nonneg]
theorem vNeg_nonpos (o : Val) : vNeg o ≤ 0 := by cases o <;> simp [vNeg, negPart_nonpos]
theorem vPos_vNeg (o : Val) : vPos o + vNeg o = vVal o := by
  cases o <;> simp [vPos, vNeg, vVal, pos_neg]

/-- 正寄与の合計 / 負寄与の合計 / 通常の合計（NULL は 0 としてスキップ）。 -/
def sumPos : List Val → Int
  | [] => 0
  | o :: xs => vPos o + sumPos xs
def sumNeg : List Val → Int
  | [] => 0
  | o :: xs => vNeg o + sumNeg xs
def plainSum : List Val → Int
  | [] => 0
  | o :: xs => vVal o + plainSum xs

theorem sumPos_nonneg : ∀ xs : List Val, 0 ≤ sumPos xs
  | [] => by simp [sumPos]
  | o :: xs => by
      have a := vPos_nonneg o
      have b := sumPos_nonneg xs
      simp [sumPos]
      omega
theorem sumNeg_nonpos : ∀ xs : List Val, sumNeg xs ≤ 0
  | [] => by simp [sumNeg]
  | o :: xs => by
      have a := vNeg_nonpos o
      have b := sumNeg_nonpos xs
      simp [sumNeg]
      omega

theorem plainSum_eq_pos_neg (xs : List Val) : plainSum xs = sumPos xs + sumNeg xs := by
  induction xs with
  | nil => simp [plainSum, sumPos, sumNeg]
  | cons o xs ih =>
      simp only [plainSum, sumPos, sumNeg, ih]
      rw [← vPos_vNeg o]
      omega

theorem sumPos_append (xs ys : List Val) : sumPos (xs ++ ys) = sumPos xs + sumPos ys := by
  induction xs with
  | nil => simp [sumPos]
  | cons o xs ih => simp [sumPos, ih, Int.add_assoc]

theorem sumNeg_append (xs ys : List Val) : sumNeg (xs ++ ys) = sumNeg xs + sumNeg ys := by
  induction xs with
  | nil => simp [sumNeg]
  | cons o xs ih => simp [sumNeg, ih, Int.add_assoc]

theorem plainSum_append (xs ys : List Val) : plainSum (xs ++ ys) = plainSum xs + plainSum ys := by
  induction xs with
  | nil => simp [plainSum]
  | cons o xs ih => simp [plainSum, ih, Int.add_assoc]

/-- `a ≤ b`, `c ≤ 0` なら `b*c ≤ a*c`。 -/
theorem mul_le_mul_nonpos_right {a b c : Int} (hab : a ≤ b) (hc : c ≤ 0) : b * c ≤ a * c := by
  have h1 : 0 ≤ -c := by omega
  have h2 : a * (-c) ≤ b * (-c) := Int.mul_le_mul_of_nonneg_right hab h1
  rw [Int.mul_neg, Int.mul_neg] at h2
  omega

/-- 参照: 各行を加算するたびに InRange を検査する逐次 SUM。 -/
def checkedSum (acc : Int) : List Val → Option Int
  | [] => some acc
  | none :: xs => checkedSum acc xs
  | some v :: xs =>
      let next := acc + v
      if InRange next then checkedSum next xs else none

/-- 逐次 SUM が通る累積値の列（先頭は常に開始値 acc）。 -/
def stepVal (acc : Int) (o : Val) : Int :=
  match o with | none => acc | some v => acc + v

def runningSums (acc : Int) : List Val → List Int
  | [] => [acc]
  | o :: xs => acc :: runningSums (stepVal acc o) xs

def allInRange : List Int → Prop
  | [] => True
  | z :: zs => InRange z ∧ allInRange zs

theorem inRange_of_running (a : Int) (xs : List Val) (h : allInRange (runningSums a xs)) : InRange a := by
  cases xs <;> exact h.1

/-- すべての途中累積値が範囲内なら、検査付き SUM は成功し通常の和に等しい。 -/
theorem checkedSum_eq_plain_of_running (acc : Int) (xs : List Val)
    (h : allInRange (runningSums acc xs)) :
    checkedSum acc xs = some (acc + plainSum xs) := by
  induction xs generalizing acc with
  | nil => simp [checkedSum, plainSum]
  | cons o xs ih =>
      simp only [runningSums] at h
      cases o with
      | none =>
          simp only [checkedSum, plainSum, vVal]
          simpa using ih acc h.2
      | some v =>
          have hnext : InRange (acc + v) := inRange_of_running (acc + v) xs h.2
          have hih := ih (acc + v) h.2
          simp only [checkedSum, plainSum, vVal]
          rw [ite_eq_left hnext]
          have hEq : acc + (v + plainSum xs) = (acc + v) + plainSum xs := by omega
          rw [hEq]
          exact hih

/-- 正負を別々に追跡する不変条件つきの検査付き SUM 定理。
    `p`/`n` は「消費済みプレフィックスの正寄与/負寄与」、`P`/`N` はそのリスト全体の
    正寄与/負寄与の合計。不変条件から現在の累積値は必ず `[N, P]` に収まる。 -/
theorem checkedSum_eq_plain_of_invariant :
    ∀ (xs : List Val) (acc p n P N : Int),
      P ≤ i64Max → i64Min ≤ N → 0 ≤ p → n ≤ 0 →
      acc = p + n → p + sumPos xs = P → n + sumNeg xs = N →
      checkedSum acc xs = some (acc + plainSum xs)
  | [], acc, p, n, P, N, hP, hN, hp, hn, hacc, hpP, hnN => by
      simp [checkedSum, plainSum]
  | none :: xs, acc, p, n, P, N, hP, hN, hp, hn, hacc, hpP, hnN => by
      have hpP' : p + sumPos xs = P := by simpa [sumPos, vPos] using hpP
      have hnN' : n + sumNeg xs = N := by simpa [sumNeg, vNeg] using hnN
      simpa [checkedSum, plainSum, vPos, vVal] using
        checkedSum_eq_plain_of_invariant xs acc p n P N hP hN hp hn hacc hpP' hnN'
  | some v :: xs, acc, p, n, P, N, hP, hN, hp, hn, hacc, hpP, hnN => by
      let p' := p + posPart v
      let n' := n + negPart v
      let acc' := acc + v
      have hp' : 0 ≤ p' := by dsimp [p']; have := posPart_nonneg v; omega
      have hn' : n' ≤ 0 := by dsimp [n']; have := negPart_nonpos v; omega
      have hacc' : acc' = p' + n' := by
        dsimp [acc', p', n']
        rw [hacc]
        have hv := pos_neg v
        omega
      have hpP' : p' + sumPos xs = P := by
        dsimp [p']
        simp only [sumPos, vPos] at hpP ⊢
        omega
      have hnN' : n' + sumNeg xs = N := by
        dsimp [n']
        simp only [sumNeg, vNeg] at hnN ⊢
        omega
      have hAccRange : InRange acc' := by
        have hsp : 0 ≤ sumPos xs := sumPos_nonneg xs
        have hsn : sumNeg xs ≤ 0 := sumNeg_nonpos xs
        have hp_le : p' ≤ P := by omega
        have hn_ge : N ≤ n' := by omega
        unfold InRange
        dsimp [acc']
        omega
      simp only [checkedSum, plainSum, vVal]
      rw [ite_eq_left hAccRange]
      have hEq : acc + (v + plainSum xs) = (acc + v) + plainSum xs := by omega
      rw [hEq]
      have hih := checkedSum_eq_plain_of_invariant xs acc' p' n' P N hP hN hp' hn' hacc' hpP' hnN'
      dsimp [acc'] at hih
      exact hih

/-- 補助: replicate / flatMap の和。 -/
theorem sumPos_replicate (n : Nat) (o : Val) : sumPos (List.replicate n o) = (n : Int) * vPos o := by
  induction n with
  | zero => simp [sumPos]
  | succ n ih =>
      rw [List.replicate_succ, sumPos, ih, Int.natCast_succ, Int.add_mul, Int.one_mul]
      rw [Int.add_comm]

theorem sumNeg_replicate (n : Nat) (o : Val) : sumNeg (List.replicate n o) = (n : Int) * vNeg o := by
  induction n with
  | zero => simp [sumNeg]
  | succ n ih =>
      rw [List.replicate_succ, sumNeg, ih, Int.natCast_succ, Int.add_mul, Int.one_mul]
      rw [Int.add_comm]

theorem plainSum_replicate (n : Nat) (o : Val) : plainSum (List.replicate n o) = (n : Int) * vVal o := by
  induction n with
  | zero => simp [plainSum]
  | succ n ih =>
      rw [List.replicate_succ, plainSum, ih, Int.natCast_succ, Int.add_mul, Int.one_mul]
      rw [Int.add_comm]

theorem sumPos_flatMap_replicate (n : Nat) (xs : List Val) :
    sumPos (xs.flatMap (fun o => List.replicate n o)) = (n : Int) * sumPos xs := by
  induction xs with
  | nil => simp [sumPos]
  | cons o xs ih =>
      rw [List.flatMap_cons, sumPos_append, sumPos_replicate, sumPos, ih, Int.mul_add]

theorem sumNeg_flatMap_replicate (n : Nat) (xs : List Val) :
    sumNeg (xs.flatMap (fun o => List.replicate n o)) = (n : Int) * sumNeg xs := by
  induction xs with
  | nil => simp [sumNeg]
  | cons o xs ih =>
      rw [List.flatMap_cons, sumNeg_append, sumNeg_replicate, sumNeg, ih, Int.mul_add]

theorem plainSum_flatMap_replicate (n : Nat) (xs : List Val) :
    plainSum (xs.flatMap (fun o => List.replicate n o)) = (n : Int) * plainSum xs := by
  induction xs with
  | nil => simp [plainSum]
  | cons o xs ih =>
      rw [List.flatMap_cons, plainSum_append, plainSum_replicate, plainSum, ih, Int.mul_add]

/-- 補助: フィルタ（一部のキー）の正寄与は全体以下、負寄与は全体以上。 -/
theorem sumPos_filter_le (p : Val → Bool) (xs : List Val) :
    sumPos (xs.filter p) ≤ sumPos xs := by
  induction xs with
  | nil => simp [sumPos]
  | cons o xs ih =>
      by_cases ho : p o = true
      · have h1 : sumPos (List.filter p (o :: xs)) = vPos o + sumPos (List.filter p xs) := by
          simp [ho, sumPos]
        rw [h1]
        simp only [sumPos]
        omega
      · have h1 : sumPos (List.filter p (o :: xs)) = sumPos (List.filter p xs) := by
          simp [ho, List.filter]
        rw [h1]
        simp only [sumPos]
        have hvo := vPos_nonneg o
        omega

theorem sumNeg_filter_ge (p : Val → Bool) (xs : List Val) :
    sumNeg xs ≤ sumNeg (xs.filter p) := by
  induction xs with
  | nil => simp [sumNeg]
  | cons o xs ih =>
      by_cases ho : p o = true
      · have h1 : sumNeg (List.filter p (o :: xs)) = vNeg o + sumNeg (List.filter p xs) := by
          simp [ho, sumNeg]
        rw [h1]
        simp only [sumNeg]
        omega
      · have h1 : sumNeg (List.filter p (o :: xs)) = sumNeg (List.filter p xs) := by
          simp [ho, List.filter]
        rw [h1]
        simp only [sumNeg]
        have hvo := vNeg_nonpos o
        omega

/-- 左側の行: (キー, NULL 許容 INT64 値)。 -/
abbrev Row (K : Type) := K × Val

def valsForKey [DecidableEq K] (k : K) (l : List (Row K)) : List Val :=
  (l.filter (fun r => r.1 == k)).map Prod.snd

def totalPos [DecidableEq K] (l : List (Row K)) : Int := sumPos (l.map Prod.snd)
def totalNeg [DecidableEq K] (l : List (Row K)) : Int := sumNeg (l.map Prod.snd)

theorem sumPos_valsForKey_le [DecidableEq K] (k : K) (l : List (Row K)) :
    sumPos (valsForKey k l) ≤ totalPos l := by
  unfold valsForKey totalPos
  induction l with
  | nil => simp [sumPos]
  | cons r rs ih =>
      by_cases hr : r.1 == k
      · simp [hr, sumPos]
        omega
      · simp [hr, sumPos]
        have hv := vPos_nonneg r.2
        omega

theorem totalNeg_le_sumNeg_valsForKey [DecidableEq K] (k : K) (l : List (Row K)) :
    totalNeg l ≤ sumNeg (valsForKey k l) := by
  unfold valsForKey totalNeg
  induction l with
  | nil => simp [sumNeg]
  | cons r rs ih =>
      by_cases hr : r.1 == k
      · simp [hr, sumNeg]
        omega
      · simp [hr, sumNeg]
        have hv := vNeg_nonpos r.2
        omega

/-- 結合後（キーごとにファンアウト回数だけ値が複製される）の値リスト。 -/
def fanVals [DecidableEq K] (fan : K → Nat) (k : K) (l : List (Row K)) : List Val :=
  (valsForKey k l).flatMap (fun o => List.replicate (fan k) o)

/-- 参照評価: 結合後にキーごとの SUM を計算する。 -/
def refKey [DecidableEq K] (fan : K → Nat) (k : K) (l : List (Row K)) : Option Int :=
  checkedSum 0 (fanVals fan k l)

/-- 最適化評価: 先にキーごとの部分和を計算し、ファンアウト回数分だけマージする。 -/
def eagerKey [DecidableEq K] (fan : K → Nat) (k : K) (l : List (Row K)) : Option Int :=
  match checkedSum 0 (valsForKey k l) with
  | none => none
  | some s => checkedSum 0 (List.replicate (fan k) (some s))

/-- **定理 (`checked_eager_sum_sound`)**:
    スナップショット全体の正/負寄与の上限 `P`/`N`、ファンアウト上限 `F`、
    および `P, N, F*P, F*N` が INT64 に収まるという十分条件のもとで、
    (a) すべてのキー（未結合キーを含む）について参照と事前集約が同じ整数値を返し、
    (b) グループのリストに対する出力 bag（エラー伝播を含む）も完全一致する。 -/
private theorem safe_key_totals {K : Type} [DecidableEq K]
    (l : List (Row K)) (fan : K → Nat) (F : Nat) (P N : Int)
    (hFan : ∀ k, fan k ≤ F)
    (hP_le : P ≤ i64Max) (hN_ge : i64Min ≤ N)
    (hFanP : (F : Int) * P ≤ i64Max) (hFanN : i64Min ≤ (F : Int) * N)
    (hPosBound : totalPos l ≤ P) (hNegBound : N ≤ totalNeg l)
    (k : K) :
    refKey fan k l = some ((fan k : Int) * plainSum (valsForKey k l)) ∧
      eagerKey fan k l = some ((fan k : Int) * plainSum (valsForKey k l)) := by
  have hP0 : 0 ≤ P := Int.le_trans (sumPos_nonneg (l.map Prod.snd)) hPosBound
  have hN0 : N ≤ 0 := Int.le_trans hNegBound (sumNeg_nonpos (l.map Prod.snd))
  have hF0 : (0 : Int) ≤ (F : Int) := by exact_mod_cast (Nat.zero_le F)
  let vals := valsForKey k l
  let f := fan k
  let s := plainSum vals
  have hsp_le_P : sumPos vals ≤ P := Int.le_trans (sumPos_valsForKey_le k l) hPosBound
  have hN_le_sn : N ≤ sumNeg vals := Int.le_trans hNegBound (totalNeg_le_sumNeg_valsForKey k l)
  have hsp0 : 0 ≤ sumPos vals := sumPos_nonneg vals
  have hsn0 : sumNeg vals ≤ 0 := sumNeg_nonpos vals
  have hf_le_F : (f : Int) ≤ (F : Int) := by exact_mod_cast hFan k
  have hf0 : (0 : Int) ≤ (f : Int) := by exact_mod_cast (Nat.zero_le f)
  have hposPart_le_P : posPart s ≤ P := by
    unfold posPart
    by_cases hs : 0 ≤ s
    · simp [hs]
      dsimp [s]
      rw [plainSum_eq_pos_neg]
      omega
    · simp [hs]
      exact hP0
  have hnegPart_ge_N : N ≤ negPart s := by
    unfold negPart
    by_cases hs : s < 0
    · simp [hs]
      dsimp [s]
      rw [plainSum_eq_pos_neg]
      omega
    · simp [hs]
      exact hN0
  have hs_le_P : s ≤ P := by
    dsimp [s]
    rw [plainSum_eq_pos_neg]
    omega
  have hN_le_s : N ≤ s := by
    dsimp [s]
    rw [plainSum_eq_pos_neg]
    omega
  -- 1. 事前部分和の成功
  have hpartial : checkedSum 0 vals = some s := by
    dsimp [s]
    have hP' : sumPos vals ≤ i64Max := Int.le_trans hsp_le_P hP_le
    have hN' : i64Min ≤ sumNeg vals := Int.le_trans hN_ge hN_le_sn
    simpa using
      (checkedSum_eq_plain_of_invariant vals 0 0 0 (sumPos vals) (sumNeg vals)
        hP' hN' (by omega) (by omega) (by omega)
        (by simp) (by simp))
  -- 2. ファンアウト分のマージ（eager 側の最終和）の成功
  have hposRep_le : sumPos (List.replicate f (some s)) ≤ i64Max := by
    rw [sumPos_replicate]
    dsimp [vPos]
    have ha : (f : Int) * posPart s ≤ (F : Int) * posPart s :=
      Int.mul_le_mul_of_nonneg_right hf_le_F (posPart_nonneg s)
    have hb : (F : Int) * posPart s ≤ (F : Int) * P :=
      Int.mul_le_mul_of_nonneg_left hposPart_le_P hF0
    exact Int.le_trans (Int.le_trans ha hb) hFanP
  have hnegRep_ge : i64Min ≤ sumNeg (List.replicate f (some s)) := by
    rw [sumNeg_replicate]
    dsimp [vNeg]
    have h1 : (F : Int) * N ≤ (f : Int) * N := mul_le_mul_nonpos_right hf_le_F hN0
    have h2 : (f : Int) * N ≤ (f : Int) * negPart s :=
      Int.mul_le_mul_of_nonneg_left hnegPart_ge_N hf0
    exact Int.le_trans hFanN (Int.le_trans h1 h2)
  have hEagerFinal : checkedSum 0 (List.replicate f (some s)) = some ((f : Int) * s) := by
    have h := checkedSum_eq_plain_of_invariant (List.replicate f (some s)) 0 0 0
      (sumPos (List.replicate f (some s))) (sumNeg (List.replicate f (some s)))
      hposRep_le hnegRep_ge (by omega) (by omega) (by omega)
      (by simp) (by simp)
    rw [plainSum_replicate] at h
    dsimp [vVal] at h
    simpa using h
  -- 3. 結合後 SUM（参照側）の成功
  have hposFan_le : sumPos (fanVals fan k l) ≤ i64Max := by
    rw [fanVals, sumPos_flatMap_replicate]
    dsimp [vals] at hsp_le_P
    have ha : (f : Int) * sumPos vals ≤ (F : Int) * sumPos vals :=
      Int.mul_le_mul_of_nonneg_right hf_le_F hsp0
    have hb : (F : Int) * sumPos vals ≤ (F : Int) * P :=
      Int.mul_le_mul_of_nonneg_left hsp_le_P hF0
    exact Int.le_trans (Int.le_trans ha hb) hFanP
  have hnegFan_ge : i64Min ≤ sumNeg (fanVals fan k l) := by
    rw [fanVals, sumNeg_flatMap_replicate]
    dsimp [vals] at hN_le_sn
    have h1 : (F : Int) * N ≤ (f : Int) * N := mul_le_mul_nonpos_right hf_le_F hN0
    have h2 : (f : Int) * N ≤ (f : Int) * sumNeg vals :=
      Int.mul_le_mul_of_nonneg_left hN_le_sn hf0
    exact Int.le_trans hFanN (Int.le_trans h1 h2)
  have hRef : refKey fan k l = some ((f : Int) * s) := by
    unfold refKey
    have h := checkedSum_eq_plain_of_invariant (fanVals fan k l) 0 0 0
      (sumPos (fanVals fan k l)) (sumNeg (fanVals fan k l))
      hposFan_le hnegFan_ge (by omega) (by omega) (by omega)
      (by simp) (by simp)
    rw [fanVals, plainSum_flatMap_replicate] at h
    simpa [fanVals, vals, s, f] using h
  have hEager : eagerKey fan k l = some ((f : Int) * s) := by
    unfold eagerKey
    rw [hpartial]
    simpa [s] using hEagerFinal
  exact ⟨hRef,hEager⟩

theorem checked_eager_sum_sound {K : Type} [DecidableEq K]
    (l : List (Row K)) (fan : K → Nat) (F : Nat) (P N : Int)
    (hFan : ∀ k, fan k ≤ F)
    (hP_le : P ≤ i64Max) (hN_ge : i64Min ≤ N)
    (hFanP : (F : Int) * P ≤ i64Max) (hFanN : i64Min ≤ (F : Int) * N)
    (hPosBound : totalPos l ≤ P) (hNegBound : N ≤ totalNeg l)
    (keys : List K) :
    (∀ k : K, refKey fan k l = eagerKey fan k l) ∧
    keys.mapM (fun k => (refKey fan k l).map (fun s => (k, s))) =
      keys.mapM (fun k => (eagerKey fan k l).map (fun s => (k, s))) := by
  have hper (k : K) : refKey fan k l = eagerKey fan k l := by
    obtain ⟨hr,he⟩ := safe_key_totals l fan F P N hFan hP_le hN_ge hFanP hFanN hPosBound hNegBound k
    exact hr.trans he.symm
  refine ⟨hper, ?_⟩
  induction keys with
  | nil => rfl
  | cons k ks ih =>
      simp only [List.mapM_cons]
      rw [hper k, ih]

/-- 境界例1: 未結合キーに `[MAX, 1]` があると、結合後は成功するが事前集約は失敗する
    （したがって「全キーの部分和が範囲内」という条件が必須）。 -/
theorem eager_sum_unmatched_key_overflow :
    let l : List (Row Nat) := [(1, some i64Max), (1, some 1)]
    let fan : Nat → Nat := fun _ => 0
    refKey fan 1 l = some 0 ∧ eagerKey fan 1 l = none := by
  decide

/-- 境界例2: `[MAX, 1, -1]` は最終和だけ見れば INT64 に収まるが、
    逐次検査付き SUM は途中の `MAX + 1` で失敗する。 -/
theorem intermediate_overflow_even_if_total_in_range :
    checkedSum 0 [some i64Max, some 1, some (-1)] = none ∧
    plainSum [some i64Max, some 1, some (-1)] = i64Max := by
  decide

/-- SQL equality cannot match NULL, including NULL on both sides. The right
    input is a bag: each matching row produces another copy of the left row. -/
def sqlFan [DecidableEq K] (right : List (Option K)) : Option K → Nat
  | none => 0
  | some k => right.count (some k)

def joinedRows (fan : K → Nat) (left : List (Row K)) : List (Row K) :=
  left.flatMap (fun row => List.replicate (fan row.1) row)

private theorem joinedValues [DecidableEq K] (fan : K → Nat) (k : K) (l : List (Row K)) :
    valsForKey k (joinedRows fan l) = fanVals fan k l := by
  induction l with
  | nil => rfl
  | cons r rs ih =>
    obtain ⟨j,v⟩ := r
    by_cases h : j = k
    · subst j
      simpa [valsForKey, joinedRows, fanVals, List.filter_replicate] using
        congrArg (List.replicate (fan k) v ++ ·) ih
    · simpa [valsForKey, joinedRows, fanVals, List.filter_replicate, h] using ih

/-- Outer Option = arithmetic success, inner Option = presence of a GROUP BY
    row; the value inside that row is itself nullable. -/
abbrev GroupResult (K : Type) := Option (Option (K × Val))

def referenceGroup [DecidableEq K] (k : K) (joined : List (Row K)) : GroupResult K :=
  let vs := valsForKey k joined
  if vs.isEmpty then some none else
    (checkedSum 0 vs).map fun s => some (k, if vs.any Option.isSome then some s else none)

def eagerGroup [DecidableEq K] (fan : K → Nat) (k : K) (left : List (Row K)) : GroupResult K :=
  -- This computation can fail even when fan k = 0.
  (eagerKey fan k left).map fun s =>
    let vs := valsForKey k left
    if fan k = 0 || vs.isEmpty then none
    else some (k, if vs.any Option.isSome then some s else none)

private theorem groupedKey_eq [DecidableEq K] (fan : K → Nat) (k : K) (l : List (Row K))
    (heq : refKey fan k l = eagerKey fan k l)
    (hsafe : ∃ s, refKey fan k l = some s) :
    referenceGroup k (joinedRows fan l) = eagerGroup fan k l := by
  obtain ⟨s,hs⟩ := hsafe
  have he := heq.symm.trans hs
  unfold refKey at hs
  unfold referenceGroup eagerGroup
  rw [joinedValues, he]
  simp only []
  rw [hs]
  unfold fanVals
  cases hf : fan k with
  | zero => simp
  | succ n =>
    cases hv : valsForKey k l with
    | nil => simp
    | cons x xs => simp [List.replicate_succ, List.any_flatMap, List.any_replicate] <;> split <;> split <;> grind

/-- Both plans enumerate potential left keys once and independently determine
    whether a group exists. The reference inspects actual joined rows. -/
def collectGroups [DecidableEq K] (left : List (Row K)) (f : K → GroupResult K) :
    Option (List (K × Val)) :=
  (((left.map Prod.fst).eraseDups).mapM (m := Option) f).map (List.filterMap id)

theorem sql_checked_eager_sum_sound [DecidableEq K]
    (left : List (Row (Option K))) (right : List (Option K)) (F : Nat) (P N : Int)
    (hFan : ∀ k, sqlFan right k ≤ F)
    (hp : P ≤ i64Max) (hn : i64Min ≤ N)
    (hfp : (F : Int) * P ≤ i64Max) (hfn : i64Min ≤ (F : Int) * N)
    (hpos : totalPos left ≤ P) (hneg : N ≤ totalNeg left) :
    collectGroups left (fun k => referenceGroup k (joinedRows (sqlFan right) left)) =
      collectGroups left (fun k => eagerGroup (sqlFan right) k left) := by
  apply congrArg (collectGroups left)
  funext k
  obtain ⟨hr,he⟩ := safe_key_totals left (sqlFan right) F P N hFan hp hn hfp hfn hpos hneg k
  exact groupedKey_eq _ _ _ (hr.trans he.symm) ⟨_,hr⟩

theorem sql_sum_null_unmatched_regression :
    let l : List (Row (Option Nat)) := [(none,some 4),(some 1,none),(some 2,some 9)]
    let fan := sqlFan [none,some 1,some 1]
    collectGroups l (fun k => referenceGroup k (joinedRows fan l)) = some [(some 1,none)] ∧
      collectGroups l (fun k => eagerGroup fan k l) = some [(some 1,none)] := by decide

theorem sql_sum_unmatched_overflow_regression :
    let l : List (Row Nat) := [(1,some i64Max),(1,some 1),(2,some 7)]
    let fan := fun (k : Nat) => if k = 2 then 1 else 0
    collectGroups l (fun k => referenceGroup k (joinedRows fan l)) = some [(2,some 7)] ∧
      collectGroups l (fun k => eagerGroup fan k l) = none := by decide

end TinylambOpt.CheckedEagerSum
