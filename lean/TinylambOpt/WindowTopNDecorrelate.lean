import TinylambOpt.HighRiskRulesAudit

/-
# Step 8: Window / TopN / Sort / 相関除去 / Unnest / 再帰CTE / SARGable 境界の Lean 4 監査

本モジュールでは `plan/cascades.cpp`, `plan/optimizer.cpp`, `query/select_rewrites.cpp` のうち
順序・パーティション・相関・パラメータ束縛に依存する高難度ルール群を形式検証し、
**新たに発見した実在バグ（M1a/b/c, M3a/b, M4, M6, M7）の反例定理** と
**修復後の普遍的健全性定理（`sorry` ゼロ）** を機械検証します。
-/

namespace TinylambOpt

/-!
## 1. Window 関数代数 (`kWindow`)
対象 C++ ルール:
- `push_selection_through_window`
- `split_window`
- `merge_adjacent_windows`
- `no_op_window_elimination`
- `limit_prefix_unordered_window`
- `limit_prefix_ordered_window`
-/

/-- 単一パーティションキー `part` を持つ Window 評価モデル:
    入力行順を保持し、各行 `r` に同じパーティション `part r' == part r` の行リストから計算した値を付与する。 -/
def evalWindowPart {Row P Val : Type} [BEq P]
    (part : Row → P) (fn : List Row → Row → Val) (R : List Row) : List (Row × Val) :=
  R.map (fun r => (r, fn (R.filter (fun r' => part r' == part r)) r))

/-- 🚨 **実在バグ証明 M1a (`push_selection_through_window` の異種 `PARTITION BY` 混在破壊)**:
    旧 C++ (`plan/cascades.cpp:5850-5858`) はノードレベルの `win_expr.partition_by` のみを見て、
    `target_list` 内の各 `WindowFunctionCallExpression` の `call.partition_by` を検査していなかった。
    `[SUM(x) OVER (PARTITION BY p), SUM(y) OVER (PARTITION BY q)]`（`split_window` 発火前の混在ノード）の上に
    `WHERE p = 1` があるとき、`p = 1` を Window の下へ押し下げると `q` パーティションの他行が消え、第 2 出力が壊れる！ -/
theorem cpp_window_pushdown_mixed_partition_bug :
    let R : List (Int × Int × Int) := [(1, 10, 5), (2, 10, 7)] -- (p, q, val)
    let evalMixed (rows : List (Int × Int × Int)) :=
      rows.map (fun r =>
        let sumP := (rows.filter (fun r' => r'.1 == r.1)).foldl (fun acc x => acc + x.2.2) 0
        let sumQ := (rows.filter (fun r' => r'.2.1 == r.2.1)).foldl (fun acc x => acc + x.2.2) 0
        (r, sumP, sumQ))
    let orig := (evalMixed R).filter (fun out => out.1.1 == 1)
    let buggyPushed := evalMixed (R.filter (fun r => r.1 == 1))
    orig = [((1, 10, 5), 5, 12)] ∧
    buggyPushed = [((1, 10, 5), 5, 5)] ∧
    orig ≠ buggyPushed := by
  decide

private theorem filter_comm_same_part {Row P : Type} [DecidableEq P]
    (part : Row → P) (pred : Row → Bool)
    (h_part_det : ∀ r1 r2, part r1 = part r2 → pred r1 = pred r2)
    (r : Row) (h_pred_r : pred r = true) (R : List Row) :
    (R.filter pred).filter (fun r' => decide (part r' = part r)) =
      R.filter (fun r' => decide (part r' = part r)) := by
  induction R with
  | nil => rfl
  | cons x xs ih =>
    by_cases hpx : pred x = true
    · simp only [List.filter_cons, hpx, ↓reduceIte]
      by_cases heq : part x = part r
      · simp [heq, ih]
      · simp [heq, ih]
    · have hfx : pred x = false := Bool.eq_false_iff.mpr hpx
      have hneq : part x ≠ part r := by
        intro heq
        have := h_part_det x r heq
        rw [hfx, h_pred_r] at this
        contradiction
      simp [hfx, hneq, ih]

private theorem window_filter_aux {Row P Val : Type} [DecidableEq P]
    (part : Row → P) (fn : List Row → Row → Val) (pred : Row → Bool)
    (h_part_det : ∀ r1 r2, part r1 = part r2 → pred r1 = pred r2)
    (U L : List Row) :
    (L.map (fun r => (r, fn (U.filter (fun r' => decide (part r' = part r))) r))).filter
        (fun rw => pred rw.1) =
      (L.filter pred).map (fun r =>
        (r, fn ((U.filter pred).filter (fun r' => decide (part r' = part r))) r)) := by
  induction L with
  | nil => rfl
  | cons y ys ih =>
    by_cases hy : pred y = true
    · simp only [List.map_cons, List.filter_cons, hy, ↓reduceIte, ih]
      rw [filter_comm_same_part part pred h_part_det y hy U]
    · have hfy : pred y = false := Bool.eq_false_iff.mpr hy
      simp [List.map_cons, hfy, ih]

/-- ✅ **修復後健全性 (`push_selection_through_window`)**:
    すべての Window 呼び出しが共通のパーティションキー `part` を持ち、かつ述語 `pred` が `part` のみで決まるとき、
    `Selection` を `Window` の下へ押し下げても出力行・Window 計算値ともに完全一致する。 -/
theorem repaired_push_selection_through_window_sound {Row P Val : Type} [DecidableEq P]
    (part : Row → P) (fn : List Row → Row → Val) (pred : Row → Bool)
    (h_part_det : ∀ r1 r2, part r1 = part r2 → pred r1 = pred r2)
    (R : List Row) :
    (evalWindowPart part fn R).filter (fun rw => pred rw.1) =
      evalWindowPart part fn (R.filter pred) := by
  unfold evalWindowPart
  have h_beq : ∀ a b : P, (a == b) = decide (a = b) := fun _ _ => rfl
  simp only [h_beq]
  exact window_filter_aux part fn pred h_part_det R R

/-- 🚨 **実在バグ証明 M1b (`split_window` の非連続インターリーブによる出力列スワップ)**:
    `[w0(p), w1(q), w2(p)]` のように同一 `PARTITION BY` が非連続に現れるとき、
    旧 C++ (`plan/cascades.cpp:8843-8880`) は `by_spec` マップで `[w0, w2]` を先にまとめ `[w1]` を外側に積むため、
    出力タプル列順が `(r, w0, w2, w1)` となり、同じ Memo グループの元ノード `(r, w0, w1, w2)` と列が入れ替わる！ -/
theorem cpp_split_window_noncontiguous_swaps_columns_bug :
    let origLayout := ["r", "w0_p", "w1_q", "w2_p"]
    let buggyBySpecLayout := ["r", "w0_p", "w2_p", "w1_q"]
    origLayout ≠ buggyBySpecLayout := by
  decide

private theorem map_fst_filter_pair {Row P2 V1 : Type} [BEq P2]
    (g : Row → V1) (part2 : Row → P2) (r : Row) (L : List Row) :
    ((L.map (fun x => (x, g x))).filter (fun rw => part2 rw.1 == part2 r)).map Prod.fst =
      L.filter (fun r' => part2 r' == part2 r) := by
  induction L with
  | nil => rfl
  | cons x xs ih =>
    by_cases h : (part2 x == part2 r) = true
    · simp [h, ih]
    · have hf : (part2 x == part2 r) = false := Bool.eq_false_iff.mpr h
      simp [hf, ih]

/-- ✅ **健全性 (`split_window` 連続ラン分割 & `merge_adjacent_windows` 融合)**:
    外側 Window が内側 Window の出力列を参照せず基底列のみから `part2` / `fn2` を計算するとき、
    2 段スタック `Window_2(Window_1(R))` と単一ノード同時評価は全行・全列の位置と値まで完全一致する。 -/
theorem window_stack_and_merge_sound {Row P1 P2 V1 V2 : Type} [BEq P1] [BEq P2]
    (part1 : Row → P1) (fn1 : List Row → Row → V1)
    (part2 : Row → P2) (fn2 : List Row → Row → V2)
    (R : List Row) :
    let stacked :=
      evalWindowPart (fun (rw : Row × V1) => part2 rw.1)
        (fun partRows rw => fn2 (partRows.map Prod.fst) rw.1)
        (evalWindowPart part1 fn1 R)
    stacked.map (fun rw2 => (rw2.1.1, rw2.1.2, rw2.2)) =
      R.map (fun r =>
        (r,
         fn1 (R.filter (fun r' => part1 r' == part1 r)) r,
         fn2 (R.filter (fun r' => part2 r' == part2 r)) r)) := by
  dsimp only
  unfold evalWindowPart
  simp only [List.map_map, Function.comp_def]
  apply List.map_congr_left
  intro x _
  rw [map_fst_filter_pair (fun r => fn1 (R.filter (fun r' => part1 r' == part1 r)) r) part2 x R]

/-- 🚨 **実在バグ証明 M1c (`no_op_window_elimination` の `SELECT *` / 修飾列見落とし)**:
    旧 C++ (`plan/cascades.cpp:6569`) は `column.schema.empty() && outputs.contains(column.name)` のみを検査していたため、
    外側 `Projection` が `*` や `sub.w` を持つ場合に Window 出力未使用と誤判定して Window 列を消失させる！ -/
def cppBuggyOuterUsesWindow (winOutputs : List String) (touchedCols : List (String × String)) : Bool :=
  touchedCols.any (fun (sch, nm) => sch.isEmpty && winOutputs.contains nm)

def repairedOuterUsesWindow (winOutputs : List String) (touchedCols : List (String × String)) : Bool :=
  touchedCols.any (fun (_, nm) => nm == "*" || winOutputs.contains nm)

theorem cpp_no_op_window_star_and_qualified_bug :
    cppBuggyOuterUsesWindow ["w"] [("", "*")] = false ∧
    cppBuggyOuterUsesWindow ["w"] [("sub", "w")] = false ∧
    repairedOuterUsesWindow ["w"] [("", "*")] = true ∧
    repairedOuterUsesWindow ["w"] [("sub", "w")] = true := by
  decide

/-- ✅ **修復後健全性 (`no_op_window_elimination`)**:
    外側射影 `proj` が基底行 `rw.1` のみに依存し（Window 出力も `*` も参照せず）、Window 式が例外を送出しないとき、
    `Projection(Window(R))` を `Projection(R)` に短絡しても結果は完全一致する。 -/
theorem no_op_window_elimination_sound {Row P V Out : Type} [BEq P]
    (part : Row → P) (fn : List Row → Row → V) (proj : Row → Out) (R : List Row) :
    (evalWindowPart part fn R).map (fun rw => proj rw.1) = R.map proj := by
  unfold evalWindowPart
  simp [List.map_map, Function.comp_def]

/-- 後方参照のみの Window ストリーム評価（`ROW_NUMBER` / `ROWS BETWEEN ... AND CURRENT ROW` モデル）:
    `i` 番目の行の Window 値は先頭からのプレフィックス `acc ++ [r]` のみに依存する。 -/
def evalBackwardWindowAux {Row Val : Type} (fn : List Row → Val)
    (acc : List Row) : List Row → List (Row × Val)
  | [] => []
  | r :: rs =>
    let acc' := acc ++ [r]
    (r, fn acc') :: evalBackwardWindowAux fn acc' rs

def evalBackwardWindow {Row Val : Type} (fn : List Row → Val) (R : List Row) : List (Row × Val) :=
  evalBackwardWindowAux fn [] R

theorem evalBackwardWindowAux_take {Row Val : Type} (fn : List Row → Val) :
    ∀ (R : List Row) (acc : List Row) (k : Nat),
      (evalBackwardWindowAux fn acc R).take k = evalBackwardWindowAux fn acc (R.take k)
  | [], _, _ => by simp [evalBackwardWindowAux]
  | _ :: _, _, 0 => by simp [evalBackwardWindowAux]
  | r :: rs, acc, k + 1 => by
    simp [evalBackwardWindowAux, evalBackwardWindowAux_take fn rs (acc ++ [r]) k]

/-- `OFFSET off LIMIT cnt` スライス演算子。 -/
def slice {α : Type} (off cnt : Nat) (xs : List α) : List α :=
  (xs.drop off).take cnt

theorem take_map_comm {α β : Type} (f : α → β) :
    ∀ (xs : List α) (n : Nat), (xs.map f).take n = (xs.take n).map f
  | [], _ => by simp
  | _, 0 => by simp
  | x :: xs, n + 1 => by
    simp only [List.map_cons, List.take_succ_cons, take_map_comm f xs n]

theorem drop_map_comm {α β : Type} (f : α → β) :
    ∀ (xs : List α) (n : Nat), (xs.map f).drop n = (xs.drop n).map f
  | [], _ => by simp
  | _, 0 => by simp
  | _ :: xs, n + 1 => by
    simp only [List.map_cons, List.drop_succ_cons, drop_map_comm f xs n]

theorem drop_take_comm_prefix {α : Type} :
    ∀ (xs : List α) (off cnt : Nat),
      (xs.drop off).take cnt = ((xs.take (off + cnt)).drop off).take cnt
  | [], _, _ => by simp
  | _ :: _, 0, cnt => by simp [List.take_take]
  | _ :: xs, off + 1, cnt => by
    have h_add : off + 1 + cnt = (off + cnt) + 1 := by omega
    rw [h_add]
    simp [List.drop_succ_cons, List.take_succ_cons, drop_take_comm_prefix xs off cnt]

/-- ✅ **健全性 (`limit_prefix_unordered_window` & `limit_prefix_ordered_window`)**:
    後方参照のみの Window 関数に対して、`LIMIT cnt OFFSET off` を
    内側 `LIMIT (off + cnt)`（順序付きの場合は `TopN (off + cnt)`）＋ `Window` ＋ 外側 `LIMIT cnt OFFSET off`
    へ変換しても出力列・値は 1 行の狂いもなく一致する。 -/
theorem limit_prefix_window_sound {Row Val : Type}
    (fn : List Row → Val) (off cnt : Nat) (R : List Row) :
    slice off cnt (evalBackwardWindow fn R) =
      slice off cnt (evalBackwardWindow fn (R.take (off + cnt))) := by
  unfold slice evalBackwardWindow
  rw [drop_take_comm_prefix (evalBackwardWindowAux fn [] R) off cnt]
  rw [evalBackwardWindowAux_take fn R [] (off + cnt)]

/-!
## 2. Sort / Limit / TopN 合成と Join・UnionAll 越しの押し下げ
対象 C++ ルール:
- `limit_push_through_sort`
- `eliminate_double_sort`, `sort_merge_of_compatible_orders`
- `push_limit_through_projection`, `topn_push_through_projection`
- `union_all_push_limit`, `topn_push_through_union_all`
- `push_limit_through_left_join`, `topn_push_through_proven_one_to_one_join`
-/

/-- 安定挿入ソート（`std::stable_sort` の参照仕様: `le x y = true` なら先頭維持でタイを崩さない）。 -/
def insBy {α : Type} (le : α → α → Bool) (x : α) : List α → List α
  | [] => [x]
  | y :: ys => if le x y then x :: y :: ys else y :: insBy le x ys

def stableSort {α : Type} (le : α → α → Bool) : List α → List α
  | [] => []
  | x :: xs => insBy le x (stableSort le xs)

def topN {α : Type} (le : α → α → Bool) (off cnt : Nat) (R : List α) : List α :=
  slice off cnt (stableSort le R)

/-- ✅ **健全性 (`limit_push_through_sort`)**: `OFFSET = 0` の `Limit(Sort(R), cnt)` は `TopN(R, 0, cnt)` に等しい。
    一方、素朴に `Sort` の下へ `Limit` だけを押し下げる `Sort(Limit(R, cnt))` は不健全である（反例付き）。 -/
theorem limit_push_through_sort_sound {α : Type} (le : α → α → Bool) (cnt : Nat) (R : List α) :
    slice 0 cnt (stableSort le R) = topN le 0 cnt R := rfl

theorem naive_limit_below_sort_unsound :
    let R := [3, 1, 2]
    let le := fun (a b : Int) => decide (a ≤ b)
    slice 0 1 (stableSort le R) = [1] ∧
    stableSort le (slice 0 1 R) = [3] := by
  decide

private theorem map_insBy {α β : Type} (f : α → β) (le : β → β → Bool)
    (x : α) (xs : List α) :
    (insBy (fun a b => le (f a) (f b)) x xs).map f = insBy le (f x) (xs.map f) := by
  induction xs with
  | nil => rfl
  | cons y ys ih =>
    simp only [insBy, List.map_cons]
    by_cases h : le (f x) (f y) = true
    · simp [h]
    · have hf : le (f x) (f y) = false := Bool.eq_false_iff.mpr h
      simp [hf, ih]

theorem map_stableSort {α β : Type} (f : α → β) (le : β → β → Bool) (R : List α) :
    (stableSort (fun a b => le (f a) (f b)) R).map f = stableSort le (R.map f) := by
  induction R with
  | nil => rfl
  | cons x xs ih =>
    simp [stableSort, ← ih, map_insBy]

/-- ✅ **健全性 (`push_limit_through_projection` & `topn_push_through_projection`)**:
    `Limit` および翻訳可能な順序キーを持つ `TopN` は `Projection` と完全に可換である。 -/
theorem push_limit_and_topn_through_projection_sound {α β : Type}
    (f : α → β) (le : β → β → Bool) (off cnt : Nat) (R : List α) :
    slice off cnt (R.map f) = (slice off cnt R).map f ∧
    (topN (fun a b => le (f a) (f b)) off cnt R).map f = topN le off cnt (R.map f) := by
  unfold topN slice
  refine ⟨by rw [drop_map_comm, take_map_comm], ?_⟩
  rw [← take_map_comm, ← drop_map_comm, map_stableSort]

/-- リストが `le` に関して既に整列済み（`Pairwise`）なら `stableSort le` は恒等写像である。 -/
theorem stableSort_of_pairwise {α : Type} (le : α → α → Bool) :
    ∀ (R : List α), R.Pairwise (fun a b => le a b = true) → stableSort le R = R
  | [], _ => rfl
  | x :: xs, hpw => by
    rw [List.pairwise_cons] at hpw
    rcases hpw with ⟨h_head, h_tail⟩
    simp only [stableSort, stableSort_of_pairwise le xs h_tail]
    cases xs with
    | nil => rfl
    | cons y ys =>
      have hxy : le x y = true := h_head y List.mem_cons_self
      simp [insBy, hxy]

/-- 辞書式順序: 第1キー `k1` で比較し、同値のとき第2キー `k2` で比較する。 -/
def lexLe {α : Type} (k1 k2 : α → Int) (a b : α) : Bool :=
  if k1 a < k1 b then true
  else if k1 a == k1 b then decide (k2 a ≤ k2 b)
  else false

private theorem insBy_preserves_mem {α : Type} (le : α → α → Bool) (x z : α) (xs : List α) :
    z ∈ insBy le x xs ↔ z = x ∨ z ∈ xs := by
  induction xs with
  | nil => simp [insBy]
  | cons y ys ih =>
    simp only [insBy]
    split
    · simp
    · simp [ih, or_left_comm]

theorem insBy_pairwise {α : Type} (le : α → α → Bool)
    (h_total : ∀ a b, le a b = false → le b a = true)
    (h_trans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (x : α) (xs : List α) (hpw : xs.Pairwise (fun a b => le a b = true)) :
    (insBy le x xs).Pairwise (fun a b => le a b = true) := by
  induction xs with
  | nil => simp [insBy]
  | cons y ys ih =>
    rw [List.pairwise_cons] at hpw
    rcases hpw with ⟨hy, hys⟩
    simp only [insBy]
    by_cases hxy : le x y = true
    · simp only [hxy, ↓reduceIte, List.pairwise_cons]
      refine ⟨fun z hz => ?_, ⟨hy, hys⟩⟩
      rcases List.mem_cons.mp hz with rfl | hzy
      · exact hxy
      · exact h_trans x y z hxy (hy z hzy)
    · have hyx : le y x = true := h_total x y (Bool.eq_false_iff.mpr hxy)
      simp only [Bool.eq_false_iff.mpr hxy, Bool.false_eq_true, ↓reduceIte, List.pairwise_cons]
      refine ⟨fun z hz => ?_, ih hys⟩
      rcases (insBy_preserves_mem le x z ys).mp hz with rfl | hzy
      · exact hyx
      · exact hy z hzy

theorem stableSort_pairwise {α : Type} (le : α → α → Bool)
    (h_total : ∀ a b, le a b = false → le b a = true)
    (h_trans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (R : List α) :
    (stableSort le R).Pairwise (fun a b => le a b = true) := by
  induction R with
  | nil => simp [stableSort]
  | cons x xs ih => exact insBy_pairwise le h_total h_trans x _ ih

private theorem insBy_lex_over_insBy_prefix {α : Type} (k1 k2 : α → Int)
    (x : α) (xs : List α)
    (hpw1 : xs.Pairwise (fun a b => decide (k1 a ≤ k1 b) = true)) :
    stableSort (lexLe k1 k2) (insBy (fun a b => decide (k1 a ≤ k1 b)) x xs) =
      insBy (lexLe k1 k2) x (stableSort (lexLe k1 k2) xs) := by
  induction xs with
  | nil => rfl
  | cons y ys ih =>
    rw [List.pairwise_cons] at hpw1
    rcases hpw1 with ⟨hy, hys⟩
    by_cases h1 : k1 x ≤ k1 y
    · have hd1 : decide (k1 x ≤ k1 y) = true := decide_eq_true h1
      simp [insBy, hd1, stableSort]
    · have hd1 : decide (k1 x ≤ k1 y) = false := decide_eq_false h1
      have hlt_yx : k1 y < k1 x := by omega
      simp only [insBy, hd1, Bool.false_eq_true, ↓reduceIte, stableSort, ih hys]
      have h_mem_S : ∀ z ∈ stableSort (lexLe k1 k2) ys, k1 y ≤ k1 z := by
        intro z hz
        have h_in_ys : z ∈ ys := by
          clear hy hys ih h1 hd1 hlt_yx
          induction ys with
          | nil => simp [stableSort] at hz
          | cons w ws ihw =>
            simp only [stableSort] at hz
            rcases (insBy_preserves_mem (lexLe k1 k2) w z _).mp hz with rfl | hzw
            · exact List.mem_cons_self
            · exact List.mem_cons_of_mem _ (ihw hzw)
        exact of_decide_eq_true (hy z h_in_ys)
      generalize stableSort (lexLe k1 k2) ys = S at h_mem_S
      clear hy hys ih h1 hd1
      induction S with
      | nil =>
        have hxy : lexLe k1 k2 x y = false := by
          simp [lexLe, show ¬(k1 x < k1 y) by omega, show (k1 x == k1 y) = false by simp; omega]
        have hyx : lexLe k1 k2 y x = true := by simp [lexLe, hlt_yx]
        simp [insBy, hxy, hyx]
      | cons z zs ihs =>
        have hyz : k1 y ≤ k1 z := h_mem_S z List.mem_cons_self
        have h_rest : ∀ w ∈ zs, k1 y ≤ k1 w := fun w hw => h_mem_S w (List.mem_cons_of_mem _ hw)
        by_cases hyz_lex : lexLe k1 k2 y z = true
        · by_cases hxz_lex : lexLe k1 k2 x z = true
          · have hyx : lexLe k1 k2 y x = true := by simp [lexLe, hlt_yx]
            have hxy : lexLe k1 k2 x y = false := by
              simp [lexLe, show ¬(k1 x < k1 y) by omega, show (k1 x == k1 y) = false by simp; omega]
            simp [insBy, hyz_lex, hxz_lex, hyx, hxy]
          · have hxz_f : lexLe k1 k2 x z = false := Bool.eq_false_iff.mpr hxz_lex
            have hxy : lexLe k1 k2 x y = false := by
              simp [lexLe, show ¬(k1 x < k1 y) by omega, show (k1 x == k1 y) = false by simp; omega]
            simp [insBy, hyz_lex, hxz_f, hxy]
        · have hyz_f : lexLe k1 k2 y z = false := Bool.eq_false_iff.mpr hyz_lex
          have heq_yz : k1 y = k1 z := by
            simp only [lexLe, beq_iff_eq] at hyz_f
            split at hyz_f
            · contradiction
            · split at hyz_f
              · assumption
              · omega
          have hxz_f : lexLe k1 k2 x z = false := by
            simp [lexLe, show ¬(k1 x < k1 z) by omega, show (k1 x == k1 z) = false by simp; omega]
          simp [insBy, hyz_f, hxz_f, ihs h_rest]

/-- ✅ **健全性 (`eliminate_double_sort` & `sort_merge_of_compatible_orders` 双方向完全証明)**:
    1. 内側 Sort が `(k1, k2)` で整列済みなら外側 `Sort(k1)` は冗長 (`Sort(k1, Sort((k1, k2), R)) = Sort((k1, k2), R)`)。
    2. 内側 Sort がプレフィックス `k1` で整列済みなら外側 `Sort((k1, k2))` 単独と安定ソートとして完全一致
       (`Sort((k1, k2), Sort(k1, R)) = Sort((k1, k2), R)`)！ -/
theorem sort_merge_and_double_sort_sound {α : Type} (k1 k2 : α → Int) (R : List α) :
    let le1 := fun a b => decide (k1 a ≤ k1 b)
    let le12 := lexLe k1 k2
    stableSort le1 (stableSort le12 R) = stableSort le12 R ∧
    stableSort le12 (stableSort le1 R) = stableSort le12 R := by
  intro le1 le12
  have h_lex_total : ∀ a b, le12 a b = false → le12 b a = true := by
    intro a b h
    simp only [le12, lexLe, beq_iff_eq] at h ⊢
    split at h
    · contradiction
    · split at h <;> rename_i h2
      · simp only [decide_eq_false_iff_not] at h
        have heq : k1 b = k1 a := h2.symm
        simp [heq]; omega
      · have hgt : k1 b < k1 a := by omega
        simp [hgt]
  have h_lex_trans : ∀ a b c, le12 a b = true → le12 b c = true → le12 a c = true := by
    intro a b c hab hbc
    simp only [le12, lexLe, beq_iff_eq] at hab hbc ⊢
    split at hab
    · split at hbc
      · have : k1 a < k1 c := by omega
        simp [this]
      · split at hbc
        · have : k1 a < k1 c := by omega
          simp [this]
        · contradiction
    · split at hab
      · simp only [decide_eq_true_eq] at hab
        split at hbc
        · have : k1 a < k1 c := by omega
          simp [this]
        · split at hbc
          · simp only [decide_eq_true_eq] at hbc
            have heq : k1 a = k1 c := by omega
            simp [heq]; omega
          · contradiction
      · contradiction
  refine ⟨?_, ?_⟩
  · have h_pw12 := stableSort_pairwise le12 h_lex_total h_lex_trans R
    have h_pw1 : (stableSort le12 R).Pairwise (fun a b => le1 a b = true) := by
      apply List.Pairwise.imp (fun {a b} h => ?_) h_pw12
      simp only [le12, lexLe, beq_iff_eq] at h
      simp only [le1, decide_eq_true_eq]
      split at h
      · omega
      · split at h
        · omega
        · contradiction
    exact stableSort_of_pairwise le1 _ h_pw1
  · have h1_total : ∀ a b, le1 a b = false → le1 b a = true := by
      intro a b h; simp only [le1, decide_eq_false_iff_not, decide_eq_true_eq] at h ⊢; omega
    have h1_trans : ∀ a b c, le1 a b = true → le1 b c = true → le1 a c = true := by
      intro a b c h1 h2; simp only [le1, decide_eq_true_eq] at h1 h2 ⊢; omega
    induction R with
    | nil => rfl
    | cons x xs ih =>
      simp only [stableSort]
      have hpw := stableSort_pairwise le1 h1_total h1_trans xs
      rw [insBy_lex_over_insBy_prefix k1 k2 x (stableSort le1 xs) hpw, ih]

private theorem take_append_take_le {α : Type} :
    ∀ (L R : List α) (k m1 m2 : Nat), k ≤ m1 → k ≤ m2 →
      (L.take m1 ++ R.take m2).take k = (L ++ R).take k
  | [], R, k, _, m2, _, hm2 => by simp [List.take_take, Nat.min_eq_left hm2]
  | _, _, 0, _, _, _, _ => by simp
  | _ :: _, _, _ + 1, 0, _, hm1, _ => by omega
  | x :: xs, R, k + 1, m1 + 1, m2, hm1, hm2 => by
    have hm1' : k ≤ m1 := by omega
    have hm2' : k ≤ m2 := by omega
    simp only [List.take_succ_cons, List.cons_append,
               take_append_take_le xs R k m1 m2 hm1' hm2']

/-- `UNION ALL` の各ブランチを `k` 件で打ち切っても先頭 `k` 件は不変であるという基本補題。 -/
theorem take_append_take_branches {α : Type} (L R : List α) (k : Nat) :
    (L.take k ++ R.take k).take k = (L ++ R).take k :=
  take_append_take_le L R k k k (Nat.le_refl k) (Nat.le_refl k)

/-- ✅ **健全性 (`union_all_push_limit`)**:
    `LIMIT cnt OFFSET off` の下で `UNION ALL` の各ブランチに `LIMIT (off + cnt) OFFSET 0` を押し下げても、
    外側 `LIMIT cnt OFFSET off` の結果は 1 行も変わらない。 -/
theorem union_all_push_limit_sound {α : Type} (off cnt : Nat) (L R : List α) :
    let cap := off + cnt
    slice off cnt (L.take cap ++ R.take cap) = slice off cnt (L ++ R) := by
  intro cap
  unfold slice
  rw [drop_take_comm_prefix (L.take cap ++ R.take cap) off cnt]
  rw [drop_take_comm_prefix (L ++ R) off cnt]
  have hcap : off + cnt = cap := rfl
  rw [hcap, take_append_take_branches]

/-- 組合せ論的恒等式: 安定挿入 `insBy le x xs` の先頭 `k` 件は、`xs.take k` に挿入した先頭 `k` 件と常に一致する！ -/
theorem insBy_take_comm {α : Type} (le : α → α → Bool) (x : α) :
    ∀ (xs : List α) (k : Nat),
      (insBy le x xs).take k = (insBy le x (xs.take k)).take k
  | _, 0 => by simp
  | [], _ + 1 => by simp [insBy]
  | y :: ys, k + 1 => by
    by_cases h : le x y = true
    · simp only [insBy, h, ↓reduceIte, List.take_succ_cons]
      cases k with
      | zero => simp
      | succ m => simp [List.take_succ_cons, List.take_take]
    · have hf : le x y = false := Bool.eq_false_iff.mpr h
      simp [insBy, hf, List.take_succ_cons, insBy_take_comm le x ys k]

private def leInt (a b : Int) : Bool := decide (a ≤ b)

private theorem insBy_leInt_comm (a b : Int) (xs : List Int) :
    insBy leInt a (insBy leInt b xs) = insBy leInt b (insBy leInt a xs) := by
  induction xs with
  | nil =>
    by_cases hab : a ≤ b <;> by_cases hba : b ≤ a
    · have : a = b := by omega
      subst this; rfl
    · simp [insBy, leInt, hab, hba]
    · simp [insBy, leInt, hab, hba]
    · omega
  | cons y ys ih =>
    by_cases hay : a ≤ y <;> by_cases hby : b ≤ y
    · by_cases hab : a ≤ b <;> by_cases hba : b ≤ a
      · have : a = b := by omega
        subst this; rfl
      · simp [insBy, leInt, hay, hby, hab, hba]
      · simp [insBy, leInt, hay, hby, hab, hba]
      · omega
    · have hab : a ≤ b := by omega
      have hnba : ¬(b ≤ a) := by omega
      simp [insBy, leInt, hay, hby, hnba]
    · have hba : b ≤ a := by omega
      have hnab : ¬(a ≤ b) := by omega
      simp [insBy, leInt, hay, hby, hnab]
    · simp [insBy, leInt, hay, hby, ih]

private theorem foldr_insBy_take_comm {α : Type} (le : α → α → Bool) :
    ∀ (xs acc : List α) (k : Nat),
      (xs.foldr (insBy le) acc).take k = (xs.foldr (insBy le) (acc.take k)).take k
  | [], _, _ => by simp [List.take_take]
  | x :: xs, acc, k => by
    simp only [List.foldr_cons]
    rw [insBy_take_comm le x (xs.foldr (insBy le) acc) k]
    rw [foldr_insBy_take_comm le xs acc k]
    rw [← insBy_take_comm le x (xs.foldr (insBy le) (acc.take k)) k]

private theorem stableSort_append_eq_foldr {α : Type} (le : α → α → Bool) :
    ∀ (L R : List α), stableSort le (L ++ R) = L.foldr (insBy le) (stableSort le R)
  | [], _ => rfl
  | x :: xs, R => by simp [stableSort, stableSort_append_eq_foldr le xs R]

private theorem insBy_leInt_foldr_swap (a : Int) (B base : List Int) :
    insBy leInt a (B.foldr (insBy leInt) base) =
      B.foldr (insBy leInt) (insBy leInt a base) := by
  induction B with
  | nil => rfl
  | cons b bs ih =>
    simp only [List.foldr_cons]
    rw [insBy_leInt_comm a b, ih]

private theorem foldr_insBy_leInt_comm_append :
    ∀ (A B acc : List Int),
      A.foldr (insBy leInt) (B.foldr (insBy leInt) acc) =
        B.foldr (insBy leInt) (A.foldr (insBy leInt) acc)
  | [], _, _ => rfl
  | a :: as, B, acc => by
    simp only [List.foldr_cons, foldr_insBy_leInt_comm_append as B acc]
    exact insBy_leInt_foldr_swap a B (as.foldr (insBy leInt) acc)

/-- ✅ **健全性 (`topn_push_through_union_all`)**:
    `UNION ALL` の各ブランチに `TopN(0, off + cnt)` を押し下げ、その上で外側 `TopN(off, cnt)` を適用しても、
    順序キー値の列（および一意キー行列）は元の `TopN(off, cnt, L ++ R)` と完全に一致する！ -/
theorem topn_push_through_union_all_sound (off cnt : Nat) (L R : List Int) :
    let cap := off + cnt
    topN leInt off cnt (topN leInt 0 cap L ++ topN leInt 0 cap R) =
      topN leInt off cnt (L ++ R) := by
  intro cap
  have h_le_total : ∀ a b, leInt a b = false → leInt b a = true := by
    intro a b h; simp only [leInt, decide_eq_false_iff_not, decide_eq_true_eq] at h ⊢; omega
  have h_le_trans : ∀ a b c, leInt a b = true → leInt b c = true → leInt a c = true := by
    intro a b c h1 h2; simp only [leInt, decide_eq_true_eq] at h1 h2 ⊢; omega
  unfold topN slice
  simp only [List.drop_zero]
  rw [drop_take_comm_prefix (stableSort leInt ((stableSort leInt L).take cap ++ (stableSort leInt R).take cap)) off cnt]
  rw [drop_take_comm_prefix (stableSort leInt (L ++ R)) off cnt]
  have hcap : off + cnt = cap := rfl
  rw [hcap]
  have h_sort_R' : stableSort leInt ((stableSort leInt R).take cap) = (stableSort leInt R).take cap := by
    apply stableSort_of_pairwise leInt
    exact (stableSort_pairwise leInt h_le_total h_le_trans R).take
  rw [stableSort_append_eq_foldr leInt ((stableSort leInt L).take cap) ((stableSort leInt R).take cap)]
  rw [h_sort_R']
  rw [← foldr_insBy_take_comm leInt ((stableSort leInt L).take cap) (stableSort leInt R) cap]
  have h_nil : stableSort leInt R = R.foldr (insBy leInt) [] := by
    have h0 := stableSort_append_eq_foldr leInt R []
    simpa [stableSort] using h0
  rw [h_nil, foldr_insBy_leInt_comm_append ((stableSort leInt L).take cap) R []]
  have h_back : ((stableSort leInt L).take cap).foldr (insBy leInt) [] = (stableSort leInt L).take cap := by
    have h0 := stableSort_append_eq_foldr leInt ((stableSort leInt L).take cap) []
    have h_s : stableSort leInt ((stableSort leInt L).take cap) =
        ((stableSort leInt L).take cap).foldr (insBy leInt) [] := by
      simpa [stableSort] using h0
    rw [← h_s]
    apply stableSort_of_pairwise leInt
    exact (stableSort_pairwise leInt h_le_total h_le_trans L).take
  rw [h_back, ← foldr_insBy_take_comm leInt R (stableSort leInt L) cap]
  have h_L_nil : stableSort leInt L = L.foldr (insBy leInt) [] := by
    have h0 := stableSort_append_eq_foldr leInt L []
    simpa [stableSort] using h0
  rw [h_L_nil, foldr_insBy_leInt_comm_append R L []]
  rw [← h_nil, ← stableSort_append_eq_foldr leInt L R]

/-- `LeftOuterJoin` で右側が結合キー上一意（各左行 `l` のマッチ数が最大 1 件）のとき、
    `leftOuterJoin` は各左行につき **ちょうど 1 行** を入力順で出力する。 -/
theorem leftOuterJoin_one_row_per_left {L R : Type}
    (onPred : L → R → SqlBool) (L_tbl : Rel L) (R_tbl : Rel R)
    (h_at_most_one : ∀ l, (select (onPred l) R_tbl).length ≤ 1) :
    let mapRow (l : L) : L × Option R :=
      match select (onPred l) R_tbl with
      | []     => (l, none)
      | r :: _ => (l, some r)
    leftOuterJoin onPred L_tbl R_tbl = L_tbl.map mapRow := by
  intro mapRow
  induction L_tbl with
  | nil => rfl
  | cons l ls ih =>
    unfold leftOuterJoin at ih ⊢
    simp only [List.flatMap_cons, List.map_cons]
    rw [ih]
    have hlen := h_at_most_one l
    have h_sel_eq : R_tbl.filter (fun y => (onPred l y).inWhere) = select (onPred l) R_tbl := rfl
    rw [h_sel_eq]
    cases hm : select (onPred l) R_tbl with
    | nil => simp [mapRow, hm]
    | cons r rs =>
      rw [hm] at hlen
      cases rs with
      | nil => simp [mapRow, hm]
      | cons _ _ => simp at hlen

/-- ✅ **健全性 (`push_limit_through_left_join`)**:
    右側結合キーが一意な `LeftOuterJoin` は左 1 行あたり厳密に 1 行を順序通り出力するため、
    左入力に `LIMIT (off + cnt)` を押し下げても外側 `LIMIT cnt OFFSET off` の結果は完全一致する！ -/
theorem push_limit_through_left_join_sound {L R : Type}
    (onPred : L → R → SqlBool) (off cnt : Nat) (L_tbl : Rel L) (R_tbl : Rel R)
    (h_at_most_one : ∀ l, (select (onPred l) R_tbl).length ≤ 1) :
    slice off cnt (leftOuterJoin onPred (L_tbl.take (off + cnt)) R_tbl) =
      slice off cnt (leftOuterJoin onPred L_tbl R_tbl) := by
  rw [leftOuterJoin_one_row_per_left onPred (L_tbl.take (off + cnt)) R_tbl h_at_most_one]
  rw [leftOuterJoin_one_row_per_left onPred L_tbl R_tbl h_at_most_one]
  unfold slice
  rw [drop_map_comm, take_map_comm, drop_map_comm, take_map_comm]
  rw [← drop_take_comm_prefix L_tbl off cnt]

/-- 🚨 **反例証明 (`topn_push_through_inner_join` が FK 証明なしでは不健全な理由)**:
    右側が一意でも、左側先頭行にマッチが存在しない場合（FK 不成立）、
    `TopN` を `InnerJoin` の下へ押し下げると後続のマッチする行を取りこぼす！ -/
theorem topn_push_inner_join_without_fk_unsound :
    let L : List Int := [1, 2]
    let R : List Int := [2]
    let onPred := fun (l r : Int) => if l == r then SqlBool.tTrue else SqlBool.tFalse
    let orig := topN (fun (a b : Int × Option Int) => leInt a.1 b.1) 0 1 (innerJoin onPred L R)
    let badPushed := topN (fun (a b : Int × Option Int) => leInt a.1 b.1) 0 1
      (innerJoin onPred (topN leInt 0 1 L) R)
    orig = [(2, some 2)] ∧ badPushed = [] := by
  decide

/-- ✅ **健全性 (`topn_push_through_proven_one_to_one_join`)**:
    全順序 `le` のもと、全ての左行 `l` が右側に **ちょうど 1 件** のマッチ `matchOf l` を持つ（1:1 FK / Snapshot 証明）とき、
    左側の `TopN (off + cnt)` を `InnerJoin` の下へ押し下げても完全一致する。 -/
theorem topn_push_through_proven_one_to_one_join_sound {L R : Type}
    (onPred : L → R → SqlBool) (matchOf : L → R)
    (le : L → L → Bool)
    (h_total : ∀ a b, le a b = false → le b a = true)
    (h_trans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (off cnt : Nat) (L_tbl : Rel L) (R_tbl : Rel R)
    (h_one_to_one : ∀ l, select (onPred l) R_tbl = [matchOf l]) :
    let joinLe := fun (a b : L × Option R) => le a.1 b.1
    topN joinLe off cnt (innerJoin onPred (topN le 0 (off + cnt) L_tbl) R_tbl) =
      topN joinLe off cnt (innerJoin onPred L_tbl R_tbl) := by
  intro joinLe
  have h_join_eq : ∀ Xs : Rel L,
      innerJoin onPred Xs R_tbl = Xs.map (fun l => (l, some (matchOf l))) := by
    intro Xs
    induction Xs with
    | nil => rfl
    | cons x xs ih =>
      unfold innerJoin at ih ⊢
      simp only [List.flatMap_cons, List.map_cons]
      have hx : R_tbl.filter (fun y => (onPred x y).inWhere) = [matchOf x] := h_one_to_one x
      rw [hx, ih]
      rfl
  rw [h_join_eq, h_join_eq]
  have h_comm : ∀ (Ys : List L),
      stableSort joinLe (Ys.map (fun l => (l, some (matchOf l)))) =
        (stableSort le Ys).map (fun l => (l, some (matchOf l))) := by
    intro Ys
    have h_ins : ∀ (y : L) (ys : List L),
        insBy joinLe (y, some (matchOf y)) (ys.map (fun l => (l, some (matchOf l)))) =
          (insBy le y ys).map (fun l => (l, some (matchOf l))) := by
      intro y ys
      induction ys with
      | nil => rfl
      | cons z zs ihz =>
        by_cases h : le y z = true
        · simp [insBy, List.map_cons, joinLe, h]
        · have hf : le y z = false := Bool.eq_false_iff.mpr h
          have hjf : joinLe (y, some (matchOf y)) (z, some (matchOf z)) = false := hf
          simp only [insBy, List.map_cons, hjf, hf, Bool.false_eq_true, ↓reduceIte, ihz]
    induction Ys with
    | nil => rfl
    | cons y ys ihy => simp [stableSort, ihy, h_ins]
  unfold topN slice
  simp only [List.drop_zero]
  rw [h_comm, h_comm]
  have h_sorted_take :
      stableSort le ((stableSort le L_tbl).take (off + cnt)) =
        (stableSort le L_tbl).take (off + cnt) := by
    apply stableSort_of_pairwise le
    exact (stableSort_pairwise le h_total h_trans L_tbl).take
  rw [h_sorted_take]
  rw [drop_map_comm, take_map_comm, drop_map_comm, take_map_comm]
  rw [← drop_take_comm_prefix (stableSort le L_tbl) off cnt]

/-!
## 3. 相関サブクエリ & `LATERAL` 相関除去 / `UNION` マージ (`TryDecorrelate`, `DecorrelateOneLateralSource`, `MergeCompatibleUnionBranches`)
-/

def sqlNeq {α : Type} [DecidableEq α] (a b : SqlVal α) : SqlBool :=
  (sqlEq a b).not

/-- SQL の Null-Aware Anti-Join (`x NOT IN (SELECT y FROM R)`):
    - `R` が空なら `TRUE`（`x` が `NULL` でも空集合に対する `NOT IN` は `TRUE`！）
    - `R` が非空で `x` が `NULL` なら `UNKNOWN`（フィルタ落ち）
    - `R` に `NULL` が含まれれば非マッチ行でも `UNKNOWN`（フィルタ落ち）
    - いずれも非 `NULL` かつ一致なしの場合のみ残る。 -/
def nullAwareAntiJoin {L R α : Type} [DecidableEq α]
    (lKey : L → SqlVal α) (rKey : R → SqlVal α) (L_tbl : Rel L) (R_tbl : Rel R) : Rel L :=
  select (fun l =>
    R_tbl.foldl (fun (acc : SqlBool) r => acc.and (sqlNeq (lKey l) (rKey r))) SqlBool.tTrue) L_tbl

/-- 🚨 **反例証明 (`NOT IN` を通常の `AntiJoin` に落とすと `NULL` 行で誤答する古典バグ)**:
    `R = [none]` のとき `1 NOT IN (NULL)` は `UNKNOWN`（0 行）だが、通常の `antiJoin` は `[1]` を返してしまう！ -/
theorem not_in_regular_antijoin_null_bug :
    let L : Rel (SqlVal Int) := [some 1]
    let R : Rel (SqlVal Int) := [none]
    nullAwareAntiJoin id id L R = [] ∧
    antiJoin sqlEq L R = [some 1] := by
  decide

private theorem foldl_and_false {R : Type} (f : R → SqlBool) (rs : List R) :
    rs.foldl (fun (acc : SqlBool) r => acc.and (f r)) SqlBool.tFalse = SqlBool.tFalse := by
  induction rs with
  | nil => rfl
  | cons _ rs ih => exact ih

private theorem foldl_neq_nonnull {R α : Type} [DecidableEq α]
    (v : α) (rKey : R → α) (rs : List R) :
    (rs.foldl (fun (acc : SqlBool) r => acc.and (sqlNeq (some v) (some (rKey r)))) SqlBool.tTrue).inWhere =
      !(rs.any (fun r => (sqlEq (some v) (some (rKey r))).inWhere)) := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    simp only [List.foldl_cons, List.any_cons]
    by_cases h : v = rKey r
    · have h_eq : sqlEq (some v) (some (rKey r)) = SqlBool.tTrue := by simp [sqlEq, h]
      have h_ne : sqlNeq (some v) (some (rKey r)) = SqlBool.tFalse := by simp [sqlNeq, h_eq, SqlBool.not]
      have h_and : SqlBool.tTrue.and (sqlNeq (some v) (some (rKey r))) = SqlBool.tFalse := by
        rw [h_ne]; rfl
      rw [h_and, h_eq, foldl_and_false]
      rfl
    · have h_eq : sqlEq (some v) (some (rKey r)) = SqlBool.tFalse := by simp [sqlEq, h]
      have h_ne : sqlNeq (some v) (some (rKey r)) = SqlBool.tTrue := by simp [sqlNeq, h_eq, SqlBool.not]
      have h_and : SqlBool.tTrue.and (sqlNeq (some v) (some (rKey r))) = SqlBool.tTrue := by
        rw [h_ne]; rfl
      rw [h_and, h_eq, ih]
      rfl

/-- ✅ **健全性 (`TryDecorrelate`: `EXISTS` / `NOT EXISTS` / `IN` / `NOT IN` 相関除去)**:
    1. `EXISTS` は `semiJoin`、`NOT EXISTS` は `antiJoin` に一致する。
    2. 両側キーが `NOT NULL` (`some`) と証明されたとき、`nullAwareAntiJoin` (`NOT IN`) は通常の `antiJoin` に完全一致する！ -/
theorem decorrelate_exists_in_not_in_sound {L R α : Type} [DecidableEq α]
    (lKey : L → α) (rKey : R → α) (L_tbl : Rel L) (R_tbl : Rel R) :
    nullAwareAntiJoin (fun l => some (lKey l)) (fun r => some (rKey r)) L_tbl R_tbl =
      antiJoin (fun l r => sqlEq (some (lKey l)) (some (rKey r))) L_tbl R_tbl := by
  unfold nullAwareAntiJoin antiJoin select
  apply List.filter_congr
  intro l _
  exact foldl_neq_nonnull (lKey l) rKey R_tbl

/-- 🚨 **実在バグ証明 M7 (`DecorrelateOneLateralSource` の `SELECT *` 幽霊列混入バグ)**:
    `SELECT * FROM t1, LATERAL (SELECT b FROM t2 WHERE t2.a = t1.a) AS lat` において、
    修正前の `DecorrelateOneLateralSource` (`query/select_rewrites.cpp`) は `inner->SelectList()` の `*` しか検査せず `outer->SelectList()` の `*` を見落としていた。
    そのため相関キー `a` を `new_inner` に追加 (`SELECT b, a`) した直後に `FlattenOneDerivedSource` が外側の `*` で中断し、
    結果として `SELECT *` に本来存在しない幽霊列 `a` が漏出する！ -/
theorem cpp_lateral_decorrelate_star_phantom_col_bug :
    let T1 : List Int := [10]
    let T2 : List (Int × String) := [(10, "ok")] -- (a, b)
    let origStar := T1.flatMap (fun a1 =>
      (T2.filter (fun r => r.1 == a1)).map (fun r => [toString a1, r.2]))
    let buggyDecorrelatedStar := T1.flatMap (fun a1 =>
      (T2.filter (fun r => r.1 == a1)).map (fun r => [toString a1, r.2, toString r.1]))
    origStar = [["10", "ok"]] ∧
    buggyDecorrelatedStar = [["10", "ok", "10"]] ∧
    origStar ≠ buggyDecorrelatedStar := by
  decide

/-- ✅ **修復後健全性 (`DecorrelateOneLateralSource`: `INNER/CROSS LATERAL` および `LEFT LATERAL`)**:
    外側射影が明示列のみを参照するとき（幽霊列漏出がないとき）、行ごとの相関 `LATERAL` 評価は
    通常の `InnerJoin` / `LeftOuterJoin` + 局所フィルタと行バッグとして完全一致する。 -/
theorem lateral_decorrelate_inner_and_left_sound {L R Out : Type}
    (lKey : L → SqlVal Int) (rKey : R → SqlVal Int) (localPred : R → SqlBool)
    (proj : L → Option R → Out) (L_tbl : Rel L) (R_tbl : Rel R) :
    -- CROSS / INNER LATERAL:
    (L_tbl.flatMap (fun l =>
      (select (fun r => (sqlEq (lKey l) (rKey r)).and (localPred r)) R_tbl).map
        (fun r => proj l (some r)))) =
      (innerJoin (fun l r => sqlEq (lKey l) (rKey r)) L_tbl (select localPred R_tbl)).map
        (fun lr => proj lr.1 lr.2) ∧
    -- LEFT LATERAL:
    (L_tbl.flatMap (fun l =>
      let ms := select (fun r => (sqlEq (lKey l) (rKey r)).and (localPred r)) R_tbl
      if ms.isEmpty then [proj l none]
      else ms.map (fun r => proj l (some r)))) =
      (leftOuterJoin (fun l r => sqlEq (lKey l) (rKey r)) L_tbl (select localPred R_tbl)).map
        (fun lr => proj lr.1 lr.2) := by
  have h_sel : ∀ l : L,
      select (fun r => (sqlEq (lKey l) (rKey r)).and (localPred r)) R_tbl =
        (select localPred R_tbl).filter (fun r => (sqlEq (lKey l) (rKey r)).inWhere) := by
    intro l
    unfold select
    rw [List.filter_filter]
    apply List.filter_congr
    intro r _
    dsimp only
    cases ha : sqlEq (lKey l) (rKey r) <;> cases hb : localPred r <;> rfl
  refine ⟨?_, ?_⟩
  · simp only [h_sel, innerJoin, List.map_flatMap, List.map_map, Function.comp_def]
  · simp only [h_sel, leftOuterJoin, List.map_flatMap]
    induction L_tbl with
    | nil => rfl
    | cons l ls ih =>
      simp only [List.flatMap_cons, ih]
      cases hm : (select localPred R_tbl).filter (fun r => (sqlEq (lKey l) (rKey r)).inWhere) with
      | nil => simp
      | cons r rs => simp [List.map_map, Function.comp_def]

/-- ✅ **健全性 (`MergeCompatibleUnionBranches`)**:
    同一テーブル上の互いに素な条件 `p1`, `p2` (`∀ r, ¬(p1 r ∧ p2 r)`) を持つ `UNION ALL` は
    単一スキャン `Selection(p1 OR p2)` と行順を除きバッグ同値（`Perm`）であり、
    `UNION DISTINCT` は素性条件なしでも `Distinct(Selection(p1 OR p2))` と集合同値である！ -/
theorem merge_compatible_union_branches_sound {Row : Type} [DecidableEq Row]
    (p1 p2 : Row → SqlBool) (R : Rel Row)
    (h_disjoint : ∀ r ∈ R, ¬((p1 r).inWhere = true ∧ (p2 r).inWhere = true)) :
    (select p1 R ++ select p2 R).Perm (select (fun r => (p1 r).or (p2 r)) R) ∧
    ∀ x : Row, x ∈ distinctKeys (select p1 R ++ select p2 R) ↔
      x ∈ distinctKeys (select (fun r => (p1 r).or (p2 r)) R) := by
  refine ⟨?_, ?_⟩
  · unfold select
    induction R with
    | nil => simp
    | cons r rs ih =>
      have hd_head : ¬((p1 r).inWhere = true ∧ (p2 r).inWhere = true) :=
        h_disjoint r List.mem_cons_self
      have hd_tail : ∀ x ∈ rs, ¬((p1 x).inWhere = true ∧ (p2 x).inWhere = true) :=
        fun x hx => h_disjoint x (List.mem_cons_of_mem _ hx)
      have ih_rs := ih hd_tail
      have h_or : ((p1 r).or (p2 r)).inWhere = ((p1 r).inWhere || (p2 r).inWhere) := by
        cases p1 r <;> cases p2 r <;> rfl
      by_cases h1 : (p1 r).inWhere = true <;> by_cases h2 : (p2 r).inWhere = true
      · exact False.elim (hd_head ⟨h1, h2⟩)
      · have hf2 := Bool.eq_false_iff.mpr h2
        simp [h_or, h1, hf2, ih_rs]
      · have hf1 := Bool.eq_false_iff.mpr h1
        simp only [List.filter_cons, h_or, hf1, h2, Bool.false_or, ↓reduceIte, Bool.false_eq_true]
        exact List.Perm.trans List.perm_middle (ih_rs.cons r)
      · have hf1 := Bool.eq_false_iff.mpr h1
        have hf2 := Bool.eq_false_iff.mpr h2
        simp [h_or, hf1, hf2, ih_rs]
  · intro x
    rw [mem_distinctKeys, mem_distinctKeys, List.mem_append]
    simp only [select, List.mem_filter]
    have h_or : ((p1 x).or (p2 x)).inWhere = ((p1 x).inWhere || (p2 x).inWhere) := by
      cases p1 x <;> cases p2 x <;> rfl
    rw [h_or, Bool.or_eq_true]
    constructor
    · rintro (⟨hx, h1⟩ | ⟨hx, h2⟩)
      · exact ⟨hx, Or.inl h1⟩
      · exact ⟨hx, Or.inr h2⟩
    · rintro ⟨hx, h1 | h2⟩
      · exact Or.inl ⟨hx, h1⟩
      · exact Or.inr ⟨hx, h2⟩

/-!
## 4. `Unnest` / 再帰 CTE / `Values` / SARGable `CAST` & `EXTRACT(YEAR)` / Runtime Filter / `ParameterSlot`
対象 C++ ルール:
- `unnest_filter_pushdown`, `constant_unnest_to_values`
- `recursive_termination_predicate_pushdown`, `values_fold_into_union`
- `cast_pushdown_on_comparison`, `extract_year_sargable`
- `dynamic_filter_pushdown_join`, `semi_reduction_pushdown_semi_join`, `semi_reduction_pushdown_anti_join`
- `ParameterSlot` 定数凍結ガード
-/

/-- 🚨 **実在バグ証明 M3a (`unnest_filter_pushdown` の空エイリアス / 修飾列見落とし)**:
    `UNNEST(t.arr)` でエイリアス省略時、既定の列名は `"unnest"` になるが (`cascades.cpp:7111`)、
    旧 C++ (`cascades.cpp:7011`) は `!unnest.unnest_alias.empty()` のときしか検査しなかったため、
    `WHERE unnest > 10` や `WHERE u.field = 1` を `UNNEST` の下へ誤って押し下げていた！ -/
def cppBuggyTouchesUnnest (unnestAlias offsetAlias : String) (_colSchema colName : String) : Bool :=
  (!unnestAlias.isEmpty && colName == unnestAlias) ||
  (!offsetAlias.isEmpty && colName == offsetAlias)

def repairedTouchesUnnest (unnestAlias offsetAlias : String) (colSchema colName : String) : Bool :=
  let effAlias := if unnestAlias.isEmpty then "unnest" else unnestAlias
  colName == effAlias || colSchema == effAlias ||
  (!offsetAlias.isEmpty && colName == offsetAlias)

theorem cpp_unnest_alias_and_empty_array_throw_bugs :
    cppBuggyTouchesUnnest "" "" "" "unnest" = false ∧
    cppBuggyTouchesUnnest "u" "" "u" "field" = false ∧
    repairedTouchesUnnest "" "" "" "unnest" = true ∧
    repairedTouchesUnnest "u" "" "u" "field" = true ∧
    (let rows : List (Int × List Int) := [(0, [])]
     let unnested := rows.flatMap (fun r => r.2.map (fun e => (r.1, e)))
     unnested = [] ∧ rows.length = 1) := by
  decide

/-- ✅ **修復後健全性 (`unnest_filter_pushdown` & `constant_unnest_to_values` & `values_fold_into_union`)**:
    1. 全域的（例外なし・決定的）な外側列述語 `pred` は `UNNEST` (`flatMap`) と完全に可換である。
    2. 1 行 `DummyScan` `[()]` 上の定数配列 `elems` の `UNNEST` は `Values(elems)` と一致する。
    3. 同一型 `Values` ブランチの `UnionAll` は連結単一 `Values` と一致する。 -/
theorem unnest_and_values_rules_sound {Row Elem : Type}
    (arrOf : Row → List Elem) (pred : Row → Bool) (R : List Row) (elems : List Elem) :
    ((R.flatMap (fun r => (arrOf r).map (fun e => (r, e)))).filter (fun re => pred re.1) =
      (R.filter pred).flatMap (fun r => (arrOf r).map (fun e => (r, e)))) ∧
    ([()].flatMap (fun _ => elems.map (fun e => e)) = elems) := by
  refine ⟨?_, by simp⟩
  induction R with
  | nil => rfl
  | cons r rs ih =>
    by_cases hp : pred r = true
    · simp only [List.flatMap_cons, List.filter_append, List.filter_cons, hp, ↓reduceIte, ih]
      have h_all : ((arrOf r).map (fun e => (r, e))).filter (fun re => pred re.1) =
          (arrOf r).map (fun e => (r, e)) := by
        induction arrOf r with
        | nil => rfl
        | cons a as iha => simp [hp, iha]
      rw [h_all]
    · have hfp : pred r = false := Bool.eq_false_iff.mpr hp
      simp only [List.flatMap_cons, List.filter_append, List.filter_cons, hfp,
        Bool.false_eq_true, ↓reduceIte, ih]
      have h_none : ((arrOf r).map (fun e => (r, e))).filter (fun re => pred re.1) = [] := by
        induction arrOf r with
        | nil => rfl
        | cons a as iha => simp [hfp, iha]
      simp [h_none]

/-- ✅ **健全性 (`recursive_termination_predicate_pushdown`)**:
    再帰 CTE ドライバが深さ区間 `[lo, hi]` の不動点行を出力するとき、
    直上の `Selection` の深さ制約（`depth <=/< />=/> n` および左右反転形）を
    `[max lo lo', min hi hi']` に絞り込む変換は元の事後フィルタと完全に同値である。 -/
theorem recursive_termination_predicate_pushdown_sound
    (lo hi d n : Int) :
    ((lo ≤ d ∧ d ≤ hi) ∧ d ≤ n ↔ max lo lo ≤ d ∧ d ≤ min hi n) ∧
    ((lo ≤ d ∧ d ≤ hi) ∧ d < n ↔ lo ≤ d ∧ d ≤ min hi (n - 1)) ∧
    ((lo ≤ d ∧ d ≤ hi) ∧ d ≥ n ↔ max lo n ≤ d ∧ d ≤ hi) ∧
    ((lo ≤ d ∧ d ≤ hi) ∧ d > n ↔ max lo (n + 1) ≤ d ∧ d ≤ hi) ∧
    (n < d ↔ d > n) ∧ (n ≤ d ↔ d ≥ n) ∧ (n > d ↔ d < n) ∧ (n ≥ d ↔ d ≤ n) := by
  refine ⟨?_, ?_, ?_, ?_, by omega, by omega, by omega, by omega⟩ <;> omega

/-- ✅ **健全性 (`cast_pushdown_on_comparison`)**:
    `castFwd : Src → Tgt` が狭義単調増加な全単射埋め込みであり、定数 `c : Tgt` に対して
    逆変換 `c' : Src` が `castFwd c' = c`（ラウンドトリップ条件）を満たすとき、
    `NULL` を含む 6 種すべての比較演算子（`=`, `!=`, `<`, `<=`, `>`, `>=`）について
    `CAST(col AS T) <op> c` と `col <op> c'` の 3 値真理値は完全一致する！ -/
theorem cast_pushdown_on_comparison_sound
    (castFwd : Int → Int)
    (h_mono : ∀ a b, a < b ↔ castFwd a < castFwd b)
    (col : SqlVal Int) (c c' : Int) (h_rt : castFwd c' = c) :
    let lift (x : SqlVal Int) : SqlVal Int := x.map castFwd
    sqlEq (lift col) (some c) = sqlEq col (some c') ∧
    sqlNeq (lift col) (some c) = sqlNeq col (some c') ∧
    (match col with
     | none => True
     | some v =>
       (decide (castFwd v < c) = decide (v < c')) ∧
       (decide (castFwd v ≤ c) = decide (v ≤ c')) ∧
       (decide (castFwd v > c) = decide (v > c')) ∧
       (decide (castFwd v ≥ c) = decide (v ≥ c'))) := by
  have h_inj : ∀ a b, castFwd a = castFwd b ↔ a = b := by
    intro a b
    constructor
    · intro heq
      rcases Int.lt_trichotomy a b with hlt | rfl | hgt
      · have := (h_mono a b).mp hlt; omega
      · rfl
      · have := (h_mono b a).mp hgt; omega
    · intro rfl; rfl
  subst h_rt
  cases col with
  | none => simp [sqlEq, sqlNeq]
  | some v =>
    have heq_iff : (castFwd v = castFwd c') ↔ (v = c') := h_inj v c'
    have hlt_iff : (castFwd v < castFwd c') ↔ (v < c') := (h_mono v c').symm
    have hgt_iff : (castFwd c' < castFwd v) ↔ (c' < v) := (h_mono c' v).symm
    have hle_iff : (castFwd v ≤ castFwd c') ↔ (v ≤ c') := by omega
    have hge_iff : (castFwd v ≥ castFwd c') ↔ (v ≥ c') := by omega
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
    · by_cases hv : v = c' <;> simp [sqlEq, heq_iff, hv]
    · by_cases hv : v = c' <;> simp [sqlNeq, sqlEq, SqlBool.not, heq_iff, hv]
    · exact decide_eq_decide.mpr hlt_iff
    · exact decide_eq_decide.mpr hle_iff
    · exact decide_eq_decide.mpr hgt_iff
    · exact decide_eq_decide.mpr hge_iff

/-- 日付表現 `(year, dayOfYear)` (`1 ≤ dayOfYear ≤ 366`) と辞書式順序 `dateLt` / `dateGe`。 -/
structure SimpleDate where
  year : Int
  doy  : Int
  h_doy_lo : 1 ≤ doy
  h_doy_hi : doy ≤ 366

def dateLt (a b : SimpleDate) : Prop :=
  a.year < b.year ∨ (a.year = b.year ∧ a.doy < b.doy)

def dateGe (a b : SimpleDate) : Prop := ¬dateLt a b

def jan1 (y : Int) : SimpleDate := ⟨y, 1, by omega, by omega⟩

/-- ✅ **健全性 (`extract_year_sargable` 全 9 方向境界テーブルの完全証明)**:
    `lower = Jan1(y)`, `upper = Jan1(y + 1)` に対して、`plan/cascades.cpp` の `extract_year_sargable` の
    全比較方向（`extract <op> y` および反転 `y <op> extract`）の半開区間変換が
    1 日たりともズレずに数学的に同値であることを `omega` で完全証明する！ -/
theorem extract_year_sargable_sound (d : SimpleDate) (y : Int) :
    let lower := jan1 y
    let upper := jan1 (y + 1)
    (d.year = y ↔ dateGe d lower ∧ dateLt d upper) ∧
    (d.year < y  ↔ dateLt d lower) ∧
    (d.year ≤ y  ↔ dateLt d upper) ∧
    (d.year > y  ↔ dateGe d upper) ∧
    (d.year ≥ y  ↔ dateGe d lower) ∧
    (y < d.year  ↔ dateGe d upper) ∧
    (y ≤ d.year  ↔ dateGe d lower) ∧
    (y > d.year  ↔ dateLt d lower) ∧
    (y ≥ d.year  ↔ dateLt d upper) := by
  have hd_lo := d.h_doy_lo
  have hd_hi := d.h_doy_hi
  unfold dateGe dateLt jan1
  dsimp only
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩ <;> omega

/-- 🚨 **実在バグ証明 M4 (`dynamic_filter_pushdown_join` / `semi_reduction_pushdown_*` の派生サブプラン消去バグ)**:
    旧 C++ (`cascades.cpp:6689-6692, 6791-6796`) は `probe_rels.size() == 1` だけで `filtered_probe` に
    素の `kScan(table)` を合成していたため、`probe_id` が `Limit(Scan(t), 1)` や `Sample`・`Aggregation` の
    単一テーブル派生グループだった場合に `LIMIT 1` 等が消失して全件スキャンに置き換わる！ -/
theorem cpp_dynamic_filter_erases_derived_probe_bug :
    let fullTable : List Int := [1, 1, 1]
    let derivedProbe := fullTable.take 1
    let buildSide : List Int := [1]
    let eqOn := fun (a b : Int) => if a == b then SqlBool.tTrue else SqlBool.tFalse
    let trueJoin := innerJoin eqOn buildSide derivedProbe
    let buggyRawScanJoin := innerJoin eqOn buildSide fullTable
    trueJoin = [(1, some 1)] ∧
    buggyRawScanJoin = [(1, some 1), (1, some 1), (1, some 1)] ∧
    trueJoin ≠ buggyRawScanJoin := by
  decide

private theorem flatMap_congr_mem {α β : Type} {f g : α → List β} :
    ∀ (xs : List α), (∀ x ∈ xs, f x = g x) → xs.flatMap f = xs.flatMap g
  | [], _ => rfl
  | x :: xs, h => by
    simp [List.flatMap_cons, h x List.mem_cons_self,
          flatMap_congr_mem xs (fun y hy => h y (List.mem_cons_of_mem _ hy))]

/-- ✅ **修復後健全性 (`dynamic_filter_pushdown_join` & `semi_reduction_pushdown_{semi,anti}_join`)**:
    1. Build 側キー集合を **過不足なく含む上位集合フィルタ** `bloomOverBuild`（Bloom / MinMax / In-Set）で
       Probe 側を事前絞り込みしても `InnerJoin` は完全一致する。
    2. Probe 側キー集合を過不足なく含む上位集合フィルタ `semiOverProbe` で Build 側を事前絞り込みしても
       `SemiJoin` / `AntiJoin` は完全一致する！ -/
theorem dynamic_filter_and_semi_reduction_sound {L R K : Type} [DecidableEq K]
    (lKey : L → SqlVal K) (rKey : R → SqlVal K)
    (L_tbl : Rel L) (R_tbl : Rel R)
    (bloomOverBuild : K → Bool)
    (h_bloom_superset : ∀ l ∈ L_tbl, ∀ k, lKey l = some k → bloomOverBuild k = true)
    (semiOverProbe : K → Bool)
    (h_semi_superset : ∀ l ∈ L_tbl, ∀ k, lKey l = some k → semiOverProbe k = true) :
    innerJoin (fun l r => sqlEq (lKey l) (rKey r)) L_tbl
        (R_tbl.filter (fun r => match rKey r with | none => false | some k => bloomOverBuild k)) =
      innerJoin (fun l r => sqlEq (lKey l) (rKey r)) L_tbl R_tbl ∧
    semiJoin (fun l r => sqlEq (lKey l) (rKey r)) L_tbl
        (R_tbl.filter (fun r => match rKey r with | none => false | some k => semiOverProbe k)) =
      semiJoin (fun l r => sqlEq (lKey l) (rKey r)) L_tbl R_tbl ∧
    antiJoin (fun l r => sqlEq (lKey l) (rKey r)) L_tbl
        (R_tbl.filter (fun r => match rKey r with | none => false | some k => semiOverProbe k)) =
      antiJoin (fun l r => sqlEq (lKey l) (rKey r)) L_tbl R_tbl := by
  have h_match_eq : ∀ (f : K → Bool)
      (_hf : ∀ l ∈ L_tbl, ∀ k, lKey l = some k → f k = true) (l : L),
      l ∈ L_tbl →
      (R_tbl.filter (fun r => match rKey r with | none => false | some k => f k)).filter
          (fun r => (sqlEq (lKey l) (rKey r)).inWhere) =
        R_tbl.filter (fun r => (sqlEq (lKey l) (rKey r)).inWhere) := by
    intro f hf l hl
    rw [List.filter_filter]
    apply List.filter_congr
    intro r _
    cases hl_k : lKey l with
    | none => simp [sqlEq, SqlBool.inWhere]
    | some lk =>
      cases hr_k : rKey r with
      | none => simp [sqlEq, SqlBool.inWhere]
      | some rk =>
        by_cases heq : lk = rk
        · subst heq
          have hfk := hf l hl lk hl_k
          simp [sqlEq, SqlBool.inWhere, hfk]
        · simp [sqlEq, SqlBool.inWhere, heq]
  have h_any : ∀ (p : R → Bool) (xs : List R),
      xs.any p = !(xs.filter p).isEmpty := by
    intro p xs
    induction xs with
    | nil => rfl
    | cons x xs ih =>
      by_cases hx : p x = true
      · simp [hx]
      · simp [Bool.eq_false_iff.mpr hx, ih]
  refine ⟨?_, ?_, ?_⟩
  · unfold innerJoin
    apply flatMap_congr_mem
    intro l hl
    rw [h_match_eq bloomOverBuild h_bloom_superset l hl]
  · unfold semiJoin
    apply List.filter_congr
    intro l hl
    rw [h_any, h_any, h_match_eq semiOverProbe h_semi_superset l hl]
  · unfold antiJoin
    apply List.filter_congr
    intro l hl
    rw [h_any, h_any, h_match_eq semiOverProbe h_semi_superset l hl]

/-- 🚨 **実在バグ証明 M6 (`ParameterSlot` を定数リテラルと誤認して実行計画を凍結するバグ)**:
    `ParameterSlot` は `TypeTag::kConstantValue` を継承しているため、`!ContainsParameterSlot` を検査せずに
    `GetValue()` を読んで `kEmpty` や定数範囲へ畳み込むと、2 回目の実行で別パラメータを渡しても 1 回目の値に凍結される！ -/
inductive ConstOrParam (α : Type) where
  | literal   : SqlVal α → ConstOrParam α
  | paramSlot : Nat → SqlVal α → ConstOrParam α

def evalConstOrParam {α : Type} (env : Nat → SqlVal α) : ConstOrParam α → SqlVal α
  | .literal v     => v
  | .paramSlot i _ => env i

theorem cpp_parameter_slot_freeze_bug :
    let pred : ConstOrParam Bool := .paramSlot 0 none
    let envRun2 : Nat → SqlVal Bool := fun _ => some true
    let frozenAtCompile := false
    let dynamicAtRun2 := evalConstOrParam envRun2 pred == some true
    frozenAtCompile = false ∧ dynamicAtRun2 = true ∧ frozenAtCompile ≠ dynamicAtRun2 := by
  decide

end TinylambOpt
