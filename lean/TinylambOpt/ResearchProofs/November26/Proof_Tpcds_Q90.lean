import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q90
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem split_sum_gen {α : Type} [BEq α] [LawfulBEq α] (g : α → Nat) (a : α) : ∀ t : List α,
    (t.map g).sum = t.count a * g a + ((t.filter (fun b => !(b == a))).map g).sum := by
  intro t
  induction t with
  | nil => simp
  | cons b t ih =>
    by_cases h : b = a
    · subst h
      simp only [List.map_cons, List.sum_cons, List.count_cons, List.filter_cons, beq_self_eq_true,
        Bool.not_true, Bool.false_eq_true, ↓reduceIte, Nat.add_mul, ih]
      omega
    · have hb : (b == a) = false := by simpa using h
      simp only [List.map_cons, List.sum_cons, List.count_cons, List.filter_cons, hb,
        Bool.not_false, ↓reduceIte, ih]
      simp only [Bool.false_eq_true, ↓reduceIte, Nat.add_zero]
      omega

theorem freq_cons_gen {α : Type} [BEq α] [LawfulBEq α] (a : α) (t : List α) : frequency (a :: t) =
    (a, (a :: t).count a) :: ((t.filter (fun b => !(b == a))).eraseDups.map (fun x => (x, (a :: t).count x))) := by
  simp [frequency, List.eraseDups_cons]

theorem count_cons_ne_gen {α : Type} [BEq α] [LawfulBEq α] (a : α) (t : List α) (x : α) (hx : x ∈ (t.filter (fun b => !(b == a))).eraseDups) :
    (a :: t).count x = (t.filter (fun b => !(b == a))).count x := by
  rw [List.mem_eraseDups, List.mem_filter] at hx
  have hne : x ≠ a := by simpa using hx.2
  have hax : (a == x) = false := by simpa using (Ne.symm hne)
  rw [List.count_cons, hax]
  simp only [Bool.false_eq_true, ↓reduceIte, Nat.add_zero]
  rw [List.count_filter]
  simp [hx.2]

theorem group_sum_gen {α : Type} [BEq α] [LawfulBEq α] (g : α → Nat) : ∀ (n : Nat) (l : List α), l.length ≤ n →
    (l.map g).sum = ((frequency l).map (fun (k,c) => c * g k)).sum := by
  intro n
  induction n with
  | zero => intro l h; cases l with
    | nil => simp [frequency]
    | cons _ _ => simp at h
  | succ n ih =>
    intro l hl
    cases l with
    | nil => simp [frequency]
    | cons a t =>
      have hs := split_sum_gen g a t
      have h1 := ih (t.filter (fun b => !(b == a))) (by
        have := List.length_filter_le (fun b => !(b == a)) t
        simp at hl
        omega)
      rw [freq_cons_gen]
      simp only [List.map_cons, List.sum_cons, List.map_map]
      have h3 : (List.map ((fun (p : α × Nat) => p.2 * g p.1) ∘ fun x => (x, (a :: t).count x))
            (t.filter (fun b => !(b == a))).eraseDups)
          = (List.map ((fun (p : α × Nat) => p.2 * g p.1) ∘ fun x => (x, (t.filter (fun b => !(b == a))).count x))
            (t.filter (fun b => !(b == a))).eraseDups) := by
        apply List.map_congr_left
        intro x hx
        simp only [Function.comp, count_cons_ne_gen a t x hx]
      rw [h3]
      have h4 : (frequency (t.filter (fun b => !(b == a)))) = (t.filter (fun b => !(b == a))).eraseDups.map (fun x => (x, (t.filter (fun b => !(b == a))).count x)) := rfl
      rw [h4, List.map_map] at h1
      have h1' : (List.map g (t.filter (fun b => !(b == a)))).sum =
          (List.map ((fun (p : α × Nat) => p.2 * g p.1) ∘ fun x => (x, (t.filter (fun b => !(b == a))).count x))
            (t.filter (fun b => !(b == a))).eraseDups).sum := h1
      rw [List.count_cons_self, Nat.add_mul, Nat.one_mul]
      omega

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q90.Goal := by
  intro keys passes hour
  dsimp only
  refine congrArg (fun F : Nat → Nat => [8, 19].map F) (funext fun low => ?_)
  exact group_sum_gen (fun k => if low ≤ hour k.1 ∧ hour k.1 ≤ low + 1 then passes k else 0)
    keys.length keys (Nat.le_refl _)
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q90
