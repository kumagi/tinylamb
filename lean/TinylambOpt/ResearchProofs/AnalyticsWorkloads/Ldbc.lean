import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ldbc

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.Ldbc

open TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ldbc
open TinylambOpt.ResearchGoals.AnalyticsWorkloads

/-! ## Shared `Totals` algebra -/

private theorem plus_comm (a b : Totals) : plus a b = plus b a := by
  cases a; cases b; simp [plus]; omega

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  cases a; cases b; cases c; simp [plus]; omega

private theorem plus_zero (a : Totals) : plus a zero = a := by
  cases a; simp [plus, zero]

private theorem zero_plus (a : Totals) : plus zero a = a := by
  cases a; simp [plus, zero]

private theorem weighted_zero (v : Option Int) : weighted 0 v = zero := by
  simp [weighted, zero]

/-! ## `summarize` fold lemmas -/

/-- Folding the per-element `weighted 1` step from an arbitrary `Totals`
    accumulator yields `plus` of that accumulator and the `summarize` fold. -/
private theorem foldl_summarize_step (st : Totals) (xs : List (Option Int)) :
    xs.foldl (fun a v => plus a (weighted 1 v)) st = plus st (summarize xs) := by
  induction xs generalizing st with
  | nil => exact (plus_zero st).symm
  | cons v xs ih =>
      show xs.foldl (fun a v => plus a (weighted 1 v)) (plus st (weighted 1 v)) =
        plus st (summarize (v :: xs))
      have hcons : summarize (v :: xs) =
          xs.foldl (fun a v => plus a (weighted 1 v)) (weighted 1 v) := by
        show (v :: xs).foldl (fun a v => plus a (weighted 1 v)) zero = _
        rw [List.foldl_cons]
        show xs.foldl (fun a v => plus a (weighted 1 v)) (plus zero (weighted 1 v)) = _
        rw [zero_plus]
      rw [hcons, ih (plus st (weighted 1 v)), ih (weighted 1 v)]
      exact plus_assoc _ _ _

private theorem summarize_cons (v : Option Int) (xs : List (Option Int)) :
    summarize (v :: xs) = plus (weighted 1 v) (summarize xs) := by
  show (v :: xs).foldl (fun a v => plus a (weighted 1 v)) zero = _
  rw [List.foldl_cons]
  show xs.foldl (fun a v => plus a (weighted 1 v)) (plus zero (weighted 1 v)) = _
  rw [zero_plus]
  exact foldl_summarize_step (weighted 1 v) xs

/-! ## `summarize` component characterization -/

private def nnCount : List (Option Int) → Nat
  | [] => 0
  | none :: xs => nnCount xs
  | some _ :: xs => nnCount xs + 1

private def nnSum : List (Option Int) → Int
  | [] => 0
  | none :: xs => nnSum xs
  | some v :: xs => v + nnSum xs

private theorem nnCount_append (l₁ l₂ : List (Option Int)) :
    nnCount (l₁ ++ l₂) = nnCount l₁ + nnCount l₂ := by
  induction l₁ with
  | nil => simp [nnCount]
  | cons v xs ih =>
      cases v <;> simp [nnCount, ih] <;> omega

private theorem nnSum_append (l₁ l₂ : List (Option Int)) :
    nnSum (l₁ ++ l₂) = nnSum l₁ + nnSum l₂ := by
  induction l₁ with
  | nil => simp [nnSum]
  | cons v xs ih =>
      cases v <;> simp [nnSum, ih] <;> omega

private theorem nnCount_replicate (v : Option Int) (n : Nat) :
    nnCount (List.replicate n v) = if v.isSome = true then n else 0 := by
  induction n with
  | zero => simp [nnCount]
  | succ n ih =>
      rw [List.replicate_succ]
      cases v <;> simp [nnCount, ih] <;> omega

private theorem nnSum_replicate (v : Option Int) (n : Nat) :
    nnSum (List.replicate n v) = (n : Int) * v.getD 0 := by
  induction n with
  | zero => simp [nnSum]
  | succ n ih =>
      rw [List.replicate_succ]
      cases v with
      | none => simp [nnSum, ih]
      | some x =>
          simp only [nnSum, Option.getD_some]
          rw [ih]
          simp only [Option.getD_some]
          have hcast : ((n + 1 : Nat) : Int) = (n : Int) + 1 := by simp
          rw [hcast, Int.add_mul, Int.one_mul]
          omega

private theorem summarize_eq (xs : List (Option Int)) :
    summarize xs = ⟨xs.length, nnCount xs, nnSum xs⟩ := by
  induction xs with
  | nil => rfl
  | cons v xs ih =>
      rw [summarize_cons, ih]
      cases v <;> simp [weighted, plus, nnCount, nnSum] <;> omega

private theorem summarize_append (l₁ l₂ : List (Option Int)) :
    summarize (l₁ ++ l₂) = plus (summarize l₁) (summarize l₂) := by
  show (l₁ ++ l₂).foldl (fun a v => plus a (weighted 1 v)) zero = _
  rw [List.foldl_append]
  exact foldl_summarize_step (summarize l₁) l₂

/-- `summarize` of `n` concatenated copies of `S` scales each component by `n`. -/
private theorem summarize_flatten_replicate (S : List (Option Int)) (n : Nat) :
    summarize ((List.replicate n S).flatten) =
      ⟨(summarize S).cnt * n, (summarize S).nn * n, (summarize S).sum * (n : Int)⟩ := by
  induction n with
  | zero => simp [summarize, zero]
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons, summarize_append, ih]
      simp only [plus, TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown.PreAgg.mk.injEq]
      refine ⟨?_, ?_, ?_⟩
      · have h : (summarize S).cnt * (n + 1) = (summarize S).cnt * n + (summarize S).cnt := by
          rw [Nat.mul_add, Nat.mul_one]
        omega
      · have h : (summarize S).nn * (n + 1) = (summarize S).nn * n + (summarize S).nn := by
          rw [Nat.mul_add, Nat.mul_one]
        omega
      · have hcast : ((n + 1 : Nat) : Int) = (n : Int) + 1 := by simp
        rw [hcast, Int.mul_add, Int.mul_one]
        omega

/-- The reference side of L4: `flatMap` over forum ids emits the filtered score
    list once per occurrence of `k` in `fs`. -/
private theorem lateForum_flatMap (fs : List Nat) (ms : List ForumMessage) (k : Nat) :
    fs.flatMap (fun f => if f != k then [] else
        (ms.filter fun m => m.forum == some f && m.year == 2012 && m.parent.isSome).map
          ForumMessage.score)
      = (List.replicate (fs.count k)
          ((ms.filter fun m => m.forum == some k && m.year == 2012 &&
            m.parent.isSome).map ForumMessage.score)).flatten := by
  induction fs with
  | nil => simp
  | cons f fs ih =>
      rw [List.flatMap_cons, ih, List.count_cons]
      cases h : f == k
      · -- `f == k` is `false`: `f != k` holds, so the head contributes `[]`.
        rw [bne_eq, h]
        simp
      · -- `f == k` is `true`: `S_f` collapses to `S_k`.
        have hfk : f = k := beq_iff_eq.mp h
        rw [bne_eq, h, hfk]
        simp [List.replicate_succ, List.flatten_cons]

theorem sound_LateForumWeight : LateForumWeight.Goal := by
  intro fs ms k
  rw [lateForum_flatMap fs ms k, summarize_flatten_replicate]
  rfl

/-! ## L5: directory offsets and degree -/

private theorem nnCount_map_const {α : Type} (l : List α) (v : Option Int) :
    nnCount (l.map fun _ => v) = if v.isSome = true then l.length else 0 := by
  induction l with
  | nil => simp [nnCount]
  | cons a l ih =>
      cases v <;> simp [nnCount, ih, List.length_cons] <;> omega

private theorem nnSum_map_const {α : Type} (l : List α) (v : Option Int) :
    nnSum (l.map fun _ => v) = (l.length : Int) * v.getD 0 := by
  induction l with
  | nil => simp [nnSum]
  | cons a l ih =>
      cases v with
      | none => simp [nnSum, ih]
      | some x =>
          simp only [List.map_cons, nnSum, Option.getD_some]
          rw [ih]
          simp only [Option.getD_some, List.length_cons]
          have hcast : ((l.length + 1 : Nat) : Int) = (l.length : Int) + 1 := by simp
          rw [hcast, Int.add_mul, Int.one_mul]
          omega

/-- `summarize` of a constant-mapped list is `weighted` by the source length. -/
private theorem summarize_map_const {α : Type} (l : List α) (v : Option Int) :
    summarize (l.map fun _ => v) = weighted l.length v := by
  rw [summarize_eq, List.length_map, nnCount_map_const, nnSum_map_const]
  cases v <;> simp [weighted]

/-- Generalized `histogram` fold: inserting `+1` per element accumulates the
    `count` of each key on top of the initial map's `getD`. -/
private theorem histogram_foldl_getD {α : Type} [BEq α] [LawfulBEq α] [Hashable α]
    [EquivBEq α] [LawfulHashable α] (m : Std.HashMap α Nat) (xs : List α) (a : α) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD a 0 =
      m.getD a 0 + xs.count a := by
  induction xs generalizing m with
  | nil => simp
  | cons y xs ih =>
      rw [List.foldl_cons]
      rw [List.count_cons, ih]
      rw [Std.HashMap.getD_insert]
      cases h : y == a
      · simp
        omega
      · have hya : y = a := beq_iff_eq.mp h
        rw [hya]
        simp
        omega

private theorem histogram_getD {α : Type} [BEq α] [LawfulBEq α] [Hashable α]
    [EquivBEq α] [LawfulHashable α] (xs : List α) (a : α) :
    (histogram xs).getD a 0 = xs.count a := by
  show ((xs.foldl (fun m x => m.insert x (m.getD x 0 + 1))
      ({} : Std.HashMap α Nat)).getD a 0) = xs.count a
  rw [histogram_foldl_getD]
  simp

/-- The number of edges whose source is `id` equals the filter length. -/
private theorem map_fst_count_eq_filter_length (es : List Edge) (id : Nat) :
    (es.map Prod.fst).count id = (es.filter fun e => e.1 == id).length := by
  rw [List.count_eq_countP, List.countP_map, List.countP_eq_length_filter]
  congr 1

end TinylambOpt.ResearchProofs.AnalyticsWorkloads.Ldbc
