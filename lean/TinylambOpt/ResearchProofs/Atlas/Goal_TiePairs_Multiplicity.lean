import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `TiePairs.Multiplicity` — `∀ x, measure x = ((x.eraseDups.map (fun a => choose2
  (x.count a))).sum:Int)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_Multiplicity

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Helpers for counting equal-valued row pairs -/

private def tieCount (x : Series) : Nat :=
  (pairs x).countP (fun p => p.1 == p.2)

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  simpa only [beq_iff_eq] using eq_comm

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_ite_filter_nat {α : Type} (xs : List α) (p : α → Bool) (f : α → Nat) :
    (xs.map (fun x => if p x then f x else 0)).sum = ((xs.filter p).map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h, ih]

private theorem sum_eq_single_of_mem_nat (xs : Series) (hnd : xs.Nodup) (a : Int) (f : Int → Nat) :
    (xs.map (fun v => if v == a then f a else 0)).sum =
      if xs.contains a then f a else 0 := by
  induction xs with
  | nil => simp
  | cons v vs ih =>
      have hvnot : v ∉ vs := (List.nodup_cons.mp hnd).1
      have hndvs : vs.Nodup := (List.nodup_cons.mp hnd).2
      rw [List.map_cons, List.sum_cons, List.contains_cons, ih hndvs]
      by_cases hva : (v == a) = true
      · have hveq : v = a := of_decide_eq_true hva
        subst v
        have hnot : vs.contains a = false := by
          apply Bool.eq_false_of_ne_true
          intro hc
          exact hvnot (List.contains_iff_mem.mp hc)
        rw [hnot]
        simp
      · have hvf : (v == a) = false := Bool.eq_false_of_ne_true hva
        have hva' : (a == v) = false := by rw [beq_comm_int]; exact hvf
        simp [hvf, hva']

private theorem sum_filter_ne_nat (xs : Series) (hnd : xs.Nodup) (a : Int) (f : Int → Nat) :
    (xs.map f).sum =
      (if xs.contains a then f a else 0) +
        ((xs.filter (fun b => b != a)).map f).sum := by
  have hpoint : ∀ b : Int, f b =
      (if b == a then f a else 0) + (if b != a then f b else 0) := by
    intro b
    by_cases hba : b = a
    · subst b
      simp
    · simp [hba]
  have hmap : (xs.map f).sum =
      (xs.map (fun b => (if b == a then f a else 0) +
        (if b != a then f b else 0))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro b hb
    exact hpoint b
  rw [hmap, sum_map_add_nat, sum_eq_single_of_mem_nat xs hnd a f, sum_ite_filter_nat]

private theorem countP_eq_count_comm (x : Series) (a : Int) :
    x.countP (fun b => a == b) = x.count a := by
  rw [List.count_eq_countP]
  congr 1
  funext b
  exact beq_comm_int a b

private theorem tieCount_cons (a : Int) (x : Series) :
    tieCount (a :: x) = x.count a + tieCount x := by
  unfold tieCount
  rw [pairs_cons, List.countP_append, countP_map_comp]
  change x.countP (fun b => a == b) + (pairs x).countP (fun p => p.1 == p.2) = _
  rw [countP_eq_count_comm]

private theorem choose2_twice (n : Nat) : 2 * choose2 n = n * (n - 1) := by
  unfold choose2
  rcases Nat.mod_two_eq_zero_or_one n with h0 | h1
  · have he : n = 2 * (n / 2) := by omega
    rw [he]
    have hd : 2 ∣ (2 * (n / 2)) * (2 * (n / 2) - 1) := by
      refine ⟨(n / 2) * (2 * (n / 2) - 1), ?_⟩
      simp [Nat.mul_assoc]
    have hc := Nat.mul_div_cancel' hd
    omega
  · have he : n = 2 * (n / 2) + 1 := by omega
    rw [he]
    have hsub : 2 * (n / 2) + 1 - 1 = 2 * (n / 2) := by omega
    rw [hsub]
    have hd : 2 ∣ (2 * (n / 2) + 1) * (2 * (n / 2)) := by
      refine ⟨(2 * (n / 2) + 1) * (n / 2), ?_⟩
      simp [Nat.mul_assoc, Nat.mul_comm]
    have hc := Nat.mul_div_cancel' hd
    omega

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  have h1 := choose2_twice n
  have h2 := choose2_twice (n + 1)
  have hsub : n + 1 - 1 = n := by omega
  simp only [hsub] at h2
  have hp : n * (n - 1) + 2 * n = (n + 1) * n := by
    cases n with
    | zero => rfl
    | succ m =>
        simp only [Nat.add_sub_cancel, Nat.succ_mul, Nat.mul_succ]
        omega
  omega

private theorem eraseDups_filter : ∀ {α : Type} [DecidableEq α],
    ∀ (xs : List α) (p : α → Bool), (xs.filter p).eraseDups = xs.eraseDups.filter p
  | _, _, [], _ => rfl
  | _, _, a :: as, p => by
      by_cases hpa : p a = true
      · have ih1 := @eraseDups_filter _ _ as (fun x => p x && !(x == a))
        have ih2 := @eraseDups_filter _ _ as (fun x => !(x == a))
        simp [List.eraseDups_cons, List.filter_filter, hpa, ih1, ih2, Bool.and_comm]
      · have hpaf : p a = false := Bool.eq_false_of_ne_true hpa
        have ih1 := @eraseDups_filter _ _ as p
        have ih2 := @eraseDups_filter _ _ as (fun x => !(x == a))
        have hcongr : as.eraseDups.filter p = as.eraseDups.filter (fun x => p x && !(x == a)) := by
          apply List.filter_congr
          intro x hx
          by_cases hxa : x == a
          · have hxeq : x = a := of_decide_eq_true hxa
            subst x
            simp [hpaf]
          · simp [hxa]
        simp [List.eraseDups_cons, List.filter_filter, hpaf, ih1, ih2, hcongr]

private theorem eraseDups_nodup : ∀ x : Series, x.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hmem
        have hm := List.mem_eraseDups.mp hmem
        simp at hm
      · exact eraseDups_nodup (as.filter fun b => !b == a)
termination_by x => x.length
decreasing_by
  have hlen := List.length_filter_le (fun b : Int => !b == a) as
  change (List.filter (fun b : Int => !b == a) as).length < as.length + 1
  omega

private theorem tieCount_multiplicity (x : Series) :
    tieCount x = (x.eraseDups.map (fun a => choose2 (x.count a))).sum := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [tieCount_cons, ih, List.eraseDups_cons, List.map_cons, List.sum_cons]
      have hmap : ((xs.filter (fun b => !b == a)).eraseDups.map
          (fun b => choose2 ((a :: xs).count b))).sum =
          ((xs.filter (fun b => !b == a)).eraseDups.map
            (fun b => choose2 (xs.count b))).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro b hb
        have hbmem : b ∈ xs.filter (fun b => !b == a) := List.mem_eraseDups.mp hb
        have hbne : (b == a) = false := by
          simpa using (List.mem_filter.mp hbmem).2
        have hne : a ≠ b := by
          intro h; subst h; simp at hbne
        simp [hne]
      rw [hmap]
      have hcount : (a :: xs).count a = xs.count a + 1 := by simp
      rw [hcount]
      have hf := sum_filter_ne_nat xs.eraseDups (eraseDups_nodup xs) a
        (fun b => choose2 (xs.count b))
      have hfilter : xs.eraseDups.filter (fun b => b != a) =
          (xs.filter (fun b => !b == a)).eraseDups := by
        change xs.eraseDups.filter (fun b => !b == a) = _
        exact (@eraseDups_filter Int _ xs (fun b => !b == a)).symm
      rw [hfilter] at hf
      by_cases hca : xs.contains a = true
      · have hmem : a ∈ xs := List.contains_iff_mem.mp hca
        have hfa := hf
        simp [hmem] at hfa
        have hchoose := choose2_succ (xs.count a)
        omega
      · have hmem : a ∉ xs := by
          intro hm
          exact hca (List.contains_iff_mem.mpr hm)
        have hfa := hf
        simp [hmem] at hfa
        have hcnt0 : xs.count a = 0 := (List.count_eq_zero).2 hmem
        have hchoose : choose2 (xs.count a + 1) = 0 := by
          rw [hcnt0]
          decide
        omega

theorem sound_TiePairs_Multiplicity : TiePairs.Multiplicity.Goal := by
  intro x
  change (tieCount x : Int) = ((x.eraseDups.map (fun a => choose2 (x.count a))).sum : Int)
  exact_mod_cast tieCount_multiplicity x

end TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_Multiplicity
