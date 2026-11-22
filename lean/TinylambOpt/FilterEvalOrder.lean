import Std.Tactic
import TinylambOpt.ThreeValued

/-
# Cheap-filter reordering that preserves both passing rows and observed errors

Model of the optimizer rule that swaps `p AND q` into `q AND p` in a `WHERE`
clause (`expression/rewrite.cpp`, boolean/filter rules such as `boolean_identity`
and the commutative rewrites around `fold_binary`).

C++侧壁で確認すべき前提（issue `filter-eval-order.md`）:
  * 同じ行に対する述語評価は決定的（参照評価 `p row` / `q row` は関数として一定）。
  * 左短絡: `expression/rewrite.cpp` の短絡 AND は左が `.tFalse` なら右を評価しない。
    `.tFalse` と `.tNull` の扱いが非対称（`.tFalse` で打ち切り、`.tNull` は右を評価）なことを
    モデル化するため、参照評価を `Except Err SqlBool` の左から順に評価する `evalAndShort` で表す。
  * エラー種別（`Err`）まで観測対象に含める。
十分条件（全域性より弱い）:
  (H1) `p` がエラーなら `q` は `.ok .tFalse` ではない。
  (H2) `q` がエラーなら `p` は `.ok .tFalse` ではない。
  (H3) 両方エラーならエラー種別が一致する。
境界例（下の定理で確認）: `p` エラー & `q = FALSE` では順序を変えると結果が変わり、
異なる 2 種類のエラーでも順序で観測されるエラーが変わる。
-/

namespace TinylambOpt.FilterEvalOrder

/-- 観測されるエラー種別（オーバーフロー / 0 除算）。 -/
inductive Err where
  | overflow : Err
  | divZero : Err
  deriving DecidableEq, Repr

/-- 左から右へ短絡評価する SQL AND。
    左が `.error` なら即エラー、`.tFalse` なら右を評価せず `.ok .tFalse`、
    `.tTrue` なら右の結果、`.tNull` なら右を評価して FALSE なら FALSE・それ以外は UNKNOWN。 -/
def evalAndShort {E : Type} (l r : Except E SqlBool) : Except E SqlBool := do
  let vl ← l
  match vl with
  | .tFalse => .ok .tFalse
  | .tTrue  => r
  | .tNull  => do
      let vr ← r
      match vr with
      | .tFalse => .ok .tFalse
      | _       => .ok .tNull

/-- `WHERE` を通過するか（エラーは通过后ない）。 -/
def passes {E : Type} (res : Except E SqlBool) : Bool :=
  match res with
  | .ok v => v.inWhere
  | .error _ => false

/-- 1 行に対する入れ替えの核定理。 -/
theorem evalAndShort_swap {E : Type} (P Q : Except E SqlBool)
    (h1 : ∀ e, P = .error e → Q ≠ .ok .tFalse)
    (h2 : ∀ e, Q = .error e → P ≠ .ok .tFalse)
    (h3 : ∀ e1 e2, P = .error e1 → Q = .error e2 → e1 = e2) :
    evalAndShort P Q = evalAndShort Q P := by
  cases P with
  | error e1 =>
      cases Q with
      | error e2 =>
          have heq := h3 e1 e2 rfl rfl
          cases heq
          rfl
      | ok vq =>
          cases vq with
          | tFalse => exact (h1 e1 rfl rfl).elim
          | tTrue  => rfl
          | tNull  => rfl
  | ok vp =>
      cases Q with
      | error e2 =>
          cases vp with
          | tFalse => exact (h2 e2 rfl rfl).elim
          | tTrue  => rfl
          | tNull  => rfl
      | ok vq =>
          cases vp <;> cases vq <;> rfl

/-- **定理 (`filter_eval_order_sound`)**:
    (H1)-(H3) のもとで `p AND q` と `q AND p` は
    (a) 1 行ごとの評価結果（エラー種別含む）が完全一致し、
    (b) したがって `WHERE` の通過行リストも完全一致する。 -/
theorem filter_eval_order_sound {Row E : Type}
    (p q : Row → Except E SqlBool)
    (h1 : ∀ row e, p row = .error e → q row ≠ .ok .tFalse)
    (h2 : ∀ row e, q row = .error e → p row ≠ .ok .tFalse)
    (h3 : ∀ row e1 e2, p row = .error e1 → q row = .error e2 → e1 = e2)
    (r : List Row) :
    (∀ row, evalAndShort (p row) (q row) = evalAndShort (q row) (p row)) ∧
    r.filter (fun row => passes (evalAndShort (p row) (q row))) =
      r.filter (fun row => passes (evalAndShort (q row) (p row))) := by
  have hswap : ∀ row, evalAndShort (p row) (q row) = evalAndShort (q row) (p row) := by
    intro row
    exact evalAndShort_swap (p row) (q row)
      (fun e he => h1 row e he) (fun e he => h2 row e he)
      (fun e1 e2 he1 he2 => h3 row e1 e2 he1 he2)
  refine ⟨hswap, ?_⟩
  induction r with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons]
      rw [hswap x, ih]

/-- 境界例1: `p` がエラー、`q` が `.tFalse` だと順序で結果が変わる（H1 が必須）。 -/
theorem error_and_false_break_swap (e : Err) :
    evalAndShort (.error e : Except Err SqlBool) (.ok .tFalse) ≠
    evalAndShort (.ok .tFalse : Except Err SqlBool) (.error e) := by
  intro h
  change (.error e : Except Err SqlBool) = .ok .tFalse at h
  cases h

/-- 境界例2: 異なるエラー種別だと観測されるエラーが変わる（H3 が必須）。 -/
theorem distinct_errors_break_swap (e₁ e₂ : Err) (hne : e₁ ≠ e₂) :
    evalAndShort (.error e₁ : Except Err SqlBool) (.error e₂) ≠
    evalAndShort (.error e₂ : Except Err SqlBool) (.error e₁) := by
  intro h
  change (.error e₁ : Except Err SqlBool) = .error e₂ at h
  cases h
  exact hne rfl

/-- 静的に条件が導ける例: `p = 10/x > 0`（x = 0 で 0 除算エラー）、
    `q = (x = 0)`（決してエラーせず x = 0 のときだけ TRUE）。
    p がエラーになる行では q は TRUE なので H1 が成り立ち、入れ替えは安全。 -/
structure DivRow where
  x : Int

def divPred (r : DivRow) : Except Err SqlBool :=
  if r.x = 0 then .error .divZero else .ok (sqlEq (some (10 / r.x)) (some 1))

def zeroPred (r : DivRow) : Except Err SqlBool :=
  .ok (sqlEq (some r.x) (some 0))

theorem div_zero_reorder_static_conditions :
    (∀ row e, divPred row = .error e → zeroPred row ≠ .ok .tFalse) ∧
    (∀ row e, zeroPred row = .error e → divPred row ≠ .ok .tFalse) ∧
    (∀ row e1 e2, divPred row = .error e1 → zeroPred row = .error e2 → e1 = e2) := by
  constructor
  · intro row e he
    by_cases hx : row.x = 0
    · simp [divPred, hx] at he
      cases he
      simp [zeroPred, sqlEq, hx]
    · simp [divPred, hx] at he
  · constructor
    · intro row e he
      simp [zeroPred] at he
    · intro row e1 e2 he1 he2
      simp [zeroPred] at he2

end TinylambOpt.FilterEvalOrder
