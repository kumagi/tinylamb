import TinylambOpt.ResearchProofs.StorageExecution.SE_BagKernels_Common

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved (10 claims, including the following)

- `CoGroup.AntiRevenue` — Compute revenue of all left rows whose key is absent on the
  right.
- `CoGroup.BuildSwap` — `∀ a b k, bag.mass (a,b) k = bag.mass (b,a) k`
- `CoGroup.DistinctOutput` — Output live grouping keys once even if physical codes
  alias.
-/

open TinylambOpt.ResearchProofs.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace CoGroupProofs

open CoGroup

theorem sum_zero_map : ∀ (l : List Nat), (l.map (fun _ => 0)).sum = 0 := by
  intro l; induction l with
  | nil => rfl
  | cons x r ih => simp [ih]

theorem count_filter_pos (a : Nat) : ∀ (ys : List Nat),
    ((ys.filter (fun z => z == a)).count a) = ys.count a := by
  intro ys
  induction ys with
  | nil => rfl
  | cons y r ih =>
      by_cases h : (y == a) = true
      · rw [List.filter_cons, if_pos h, List.count_cons, List.count_cons, h, ih]
      · rw [List.filter_cons, if_neg h, List.count_cons, if_neg h, Nat.add_zero]
        exact ih

theorem count_filter_neg (a k : Nat) (h : ¬(a = k)) : ∀ (ys : List Nat),
    ((ys.filter (fun z => z == a)).count k) = 0 := by
  intro ys
  induction ys with
  | nil => rfl
  | cons y r ih =>
      by_cases hy : (y == a) = true
      · have hay : a = y := by simpa [beq_comm] using hy
        subst hay
        rw [List.filter_cons, if_pos (by simp : (a == a) = true), List.count_cons,
          if_neg (by simpa using h), ih]
      · rw [List.filter_cons, if_neg hy]
        exact ih

theorem filter_map_sum (f : Nat → Nat) (x : Nat) : ∀ (m : List Nat),
    ((m.filter (fun z => z == x)).map f).sum = m.count x * f x := by
  intro m
  induction m with
  | nil => simp
  | cons y t ih =>
      by_cases h : (y == x) = true
      · have hxy : x = y := by simpa [beq_comm] using h
        subst hxy
        rw [List.filter_cons, if_pos h, List.map_cons, List.sum_cons, List.count_cons,
          if_pos h, ih]
        rw [Nat.add_mul, Nat.one_mul]
        omega
      · rw [List.filter_cons, if_neg h, List.count_cons, if_neg h, ih, Nat.add_zero]

theorem ite_pick (b : List Nat) (w : Nat) : ∀ (s : List Nat),
    (s.map (fun y => b.count y * (if w == y then (1:Nat) else 0))).sum = s.count w * b.count w := by
  intro s
  induction s with
  | nil => simp
  | cons z r ih =>
      rw [List.map_cons, List.sum_cons]
      by_cases h : (w == z) = true
      · have hwz : w = z := by simpa using h
        subst hwz
        rw [if_pos (by simp)]
        rw [List.count_cons, if_pos (by simp), ih, Nat.mul_one, Nat.add_mul, Nat.one_mul]
        omega
      · rw [if_neg h, Nat.mul_zero, Nat.zero_add, ih, List.count_cons, beq_comm z w, if_neg h,
          Nat.add_zero]

theorem expand_map_sum (b c : List Nat) : ∀ (a : List Nat),
    ((a.flatMap (fun x => (b.filter (fun z => z == x)))).map (c.count ·)).sum =
      (a.map (fun x => b.count x * c.count x)).sum := by
  intro a
  induction a with
  | nil => rfl
  | cons z r ih =>
      simp only [List.flatMap_cons, List.map_append, List.sum_append, ih, List.map_cons,
        List.sum_cons]
      rw [filter_map_sum]

theorem swap_map (a b : List Nat) : ∀ (c : List Nat),
    (a.map (fun x => b.count x * c.count x)).sum = (c.map (fun y => b.count y * a.count y)).sum := by
  induction a with
  | nil =>
      intro c
      induction c with
      | nil => rfl
      | cons w s ih => simp [ih, sum_zero_map]
  | cons z r ih =>
      intro c
      cases c with
      | nil =>
          rw [List.map_nil, List.sum_nil,
            show (fun x => List.count x b * List.count x []) = fun _ => 0 from funext fun x => by
              rw [List.count_nil, Nat.mul_zero], sum_zero_map]
      | cons w s =>
          rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
          simp only [List.count_cons, Nat.mul_add]
          rw [sum_add_map, sum_add_map]
          rw [ite_pick b w r, ite_pick b z s, ih s]
          by_cases hw : (w == z) = true
          · have hwz : w = z := by simpa using hw
            subst hwz
            rw [if_pos hw, Nat.mul_one,
              Nat.mul_comm (List.count w b) (List.count w s),
              Nat.mul_comm (List.count w r) (List.count w b)]
            omega
          · rw [if_neg hw, if_neg (by simpa [beq_comm] using hw),
              Nat.mul_zero, Nat.add_zero, Nat.mul_zero, Nat.add_zero,
              Nat.mul_comm (List.count w b) (List.count w r),
              Nat.mul_comm (List.count z b) (List.count z s)]
            omega

theorem count_flatMap_filter : ∀ (xs ys : List Nat) (k : Nat),
    ((xs.flatMap (fun x => List.filter (fun z => z == x) ys)).count k) =
      List.count k xs * List.count k ys := by
  intro xs
  induction xs with
  | nil => intro ys k; simp
  | cons a r ih =>
      intro ys k
      rw [List.flatMap_cons, List.count_append, ih, List.count_cons]
      by_cases h : (a == k) = true
      · have hak : a = k := by simpa using h
        subst hak
        rw [count_filter_pos a ys, if_pos (by simp : (a == a) = true),
          Nat.add_mul, Nat.one_mul]
        omega
      · rw [count_filter_neg a k (by simpa using h) ys, Nat.zero_add, if_neg h, Nat.add_zero]

theorem swap_cyc (a b c : List Nat) :
    (a.map (fun x => b.count x * c.count x)).sum =
      (b.map (fun x => c.count x * a.count x)).sum := by
  rw [swap_map b c a]
  rw [show (fun x => List.count x c * List.count x b) =
      (fun x => List.count x b * List.count x c) from funext fun x => Nat.mul_comm _ _]

theorem wf : WellFormed bag := by
  constructor
  · intro ⟨xs, ys⟩ k
    exact count_flatMap_filter xs ys k
  · intro ⟨xs, ys⟩ k hk
    obtain ⟨x, hxm, hf⟩ := List.mem_flatMap.mp hk
    have hfx := List.mem_filter.mp hf
    have hkx : k = x := by simpa using hfx.2
    subst hkx
    exact hxm

theorem sound_SharedProbe : SharedProbe.Goal := by
  intro a b c
  rw [weighted_eq bag wf, weighted_eq bag wf]
  show ((a.flatMap (fun x => (b.filter (fun z => z == x)))).map (c.count ·)).sum =
    ((b.flatMap (fun x => (c.filter (fun z => z == x)))).map (a.count ·)).sum
  rw [expand_map_sum b c a, expand_map_sum c a b]
  exact swap_cyc a b c

theorem sound_DistinctOutput : DistinctOutput.Goal := wf_distinctOutput bag wf
theorem sound_FilteredMoment : FilteredMoment.Goal := wf_filteredMoment bag wf
theorem sound_OrderStatistic : OrderStatistic.Goal := wf_orderStatistic bag wf
theorem sound_JoinFanout : JoinFanout.Goal := wf_joinFanout bag wf
theorem sound_AntiRevenue : AntiRevenue.Goal := wf_antiRevenue bag wf
theorem sound_SelfPairs : SelfPairs.Goal := wf_selfPairs bag wf
theorem sound_IntersectAll : IntersectAll.Goal := wf_intersectAll bag wf
theorem sound_OuterWeight : OuterWeight.Goal := wf_outerWeight bag wf

theorem sound_BuildSwap : BuildSwap.Goal := fun _ _ _ => Nat.mul_comm _ _

end CoGroupProofs
