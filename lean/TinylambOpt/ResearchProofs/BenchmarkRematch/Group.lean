import TinylambOpt.ResearchGoals.BenchmarkRematch.Common

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Group
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem split_sum_nat (g : Nat → Nat) (a : Nat) : ∀ t : List Nat,
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
      simp only [Bool.false_eq_true, ↓reduceIte, Nat.add_zero, List.map_cons, List.sum_cons]
      omega

theorem split_sum_int (g : Nat → Int) (a : Nat) : ∀ t : List Nat,
    (t.map g).sum = (t.count a : Int) * g a + ((t.filter (fun b => !(b == a))).map g).sum := by
  intro t
  induction t with
  | nil => simp
  | cons b t ih =>
    by_cases h : b = a
    · subst h
      simp only [List.map_cons, List.sum_cons, List.count_cons, List.filter_cons, beq_self_eq_true,
        Bool.not_true, Bool.false_eq_true, ↓reduceIte, ih]
      push_cast
      simp only [Int.add_mul, Int.one_mul]
      omega
    · have hb : (b == a) = false := by simpa using h
      simp only [List.map_cons, List.sum_cons, List.count_cons, List.filter_cons, hb,
        Bool.not_false, ↓reduceIte, ih]
      simp only [Bool.false_eq_true, ↓reduceIte, Nat.add_zero, List.map_cons, List.sum_cons]
      omega

theorem freq_cons (a : Nat) (t : List Nat) : frequency (a :: t) =
    (a, (a :: t).count a) :: ((t.filter (fun b => !(b == a))).eraseDups.map (fun x => (x, (a :: t).count x))) := by
  simp [frequency, List.eraseDups_cons]

theorem count_cons_ne (a : Nat) (t : List Nat) (x : Nat) (hx : x ∈ (t.filter (fun b => !(b == a))).eraseDups) :
    (a :: t).count x = (t.filter (fun b => !(b == a))).count x := by
  rw [List.mem_eraseDups, List.mem_filter] at hx
  have hne : x ≠ a := by simpa using hx.2
  have hax : (a == x) = false := by simpa using (Ne.symm hne)
  rw [List.count_cons, hax]
  simp only [Bool.false_eq_true, ↓reduceIte, Nat.add_zero]
  rw [List.count_filter]
  simp [hx.2]

theorem group_sum_nat (g : Nat → Nat) : ∀ (n : Nat) (l : List Nat), l.length ≤ n →
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
      have hs := split_sum_nat g a t
      have h1 := ih (t.filter (fun b => !(b == a))) (by
        have := List.length_filter_le (fun b => !(b == a)) t
        simp at hl; omega)
      rw [freq_cons]
      simp only [List.map_cons, List.sum_cons, List.map_map]
      have h3 : (List.map ((fun (p : Nat × Nat) => p.2 * g p.1) ∘ fun x => (x, (a :: t).count x))
            (t.filter (fun b => !(b == a))).eraseDups)
          = (List.map ((fun (p : Nat × Nat) => p.2 * g p.1) ∘ fun x => (x, (t.filter (fun b => !(b == a))).count x))
            (t.filter (fun b => !(b == a))).eraseDups) := by
        apply List.map_congr_left
        intro x hx
        simp only [Function.comp, count_cons_ne a t x hx]
      rw [h3]
      have h4 : (frequency (t.filter (fun b => !(b == a)))) = (t.filter (fun b => !(b == a))).eraseDups.map (fun x => (x, (t.filter (fun b => !(b == a))).count x)) := rfl
      rw [h4, List.map_map] at h1
      have h1' : (List.map g (t.filter (fun b => !(b == a)))).sum =
          (List.map ((fun (p : Nat × Nat) => p.2 * g p.1) ∘ fun x => (x, (t.filter (fun b => !(b == a))).count x))
            (t.filter (fun b => !(b == a))).eraseDups).sum := h1
      rw [List.count_cons_self, Nat.add_mul, Nat.one_mul]
      omega

theorem group_sum_int (g : Nat → Int) : ∀ (n : Nat) (l : List Nat), l.length ≤ n →
    (l.map g).sum = ((frequency l).map (fun (k,c) => (c:Int) * g k)).sum := by
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
      have hs := split_sum_int g a t
      have h1 := ih (t.filter (fun b => !(b == a))) (by
        have := List.length_filter_le (fun b => !(b == a)) t
        simp at hl; omega)
      rw [freq_cons]
      simp only [List.map_cons, List.sum_cons, List.map_map]
      have h3 : (List.map ((fun (p : Nat × Nat) => (p.2:Int) * g p.1) ∘ fun x => (x, (a :: t).count x))
            (t.filter (fun b => !(b == a))).eraseDups)
          = (List.map ((fun (p : Nat × Nat) => (p.2:Int) * g p.1) ∘ fun x => (x, (t.filter (fun b => !(b == a))).count x))
            (t.filter (fun b => !(b == a))).eraseDups) := by
        apply List.map_congr_left
        intro x hx
        simp only [Function.comp, count_cons_ne a t x hx]
      rw [h3]
      have h4 : (frequency (t.filter (fun b => !(b == a)))) = (t.filter (fun b => !(b == a))).eraseDups.map (fun x => (x, (t.filter (fun b => !(b == a))).count x)) := rfl
      rw [h4, List.map_map] at h1
      have h1' : (List.map g (t.filter (fun b => !(b == a)))).sum =
          (List.map ((fun (p : Nat × Nat) => (p.2:Int) * g p.1) ∘ fun x => (x, (t.filter (fun b => !(b == a))).count x))
            (t.filter (fun b => !(b == a))).eraseDups).sum := h1
      rw [List.count_cons_self]
      push_cast
      simp only [Int.add_mul, Int.one_mul]
      omega

theorem inj_pair (w x y a b : Nat) (hy : y < w) (hb : b < w) (h : x * w + y = a * w + b) :
    x = a ∧ y = b := by
  rcases Nat.lt_trichotomy x a with h1 | h1 | h1
  · exfalso
    have := Nat.mul_le_mul_right w (show x + 1 ≤ a by omega)
    rw [Nat.add_one_mul] at this
    omega
  · subst h1
    exact ⟨rfl, by omega⟩
  · exfalso
    have := Nat.mul_le_mul_right w (show a + 1 ≤ x by omega)
    rw [Nat.add_one_mul] at this
    omega

end TinylambOpt.ResearchProofs.BenchmarkRematch.Group
