import TinylambOpt.ResearchProofs.StorageExecution.SE_BagKernels_Common

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved (10 claims, including the following)

- `MultiplicityRuns.AntiRevenue` — Compute revenue of all left rows whose key is absent
  on the right.
- `MultiplicityRuns.DistinctOutput` — Output live grouping keys once even if physical
  codes alias.
- `MultiplicityRuns.FilteredMoment` — Use a histogram for a filtered sum of squared
  values.
-/

open TinylambOpt.ResearchProofs.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace MultiplicityRunsProofs

open MultiplicityRuns

/-- Unfolded form of `bag.mass`. -/
def msum (rs : List (Nat × Nat)) (k : Nat) : Nat :=
  ((rs.filter (fun r => r.1 == k)).map Prod.snd).sum

theorem mass_eq_msum : ∀ rs k, bag.mass rs k = msum rs k := fun _ _ => rfl

theorem msum_cons (x n : Nat) (rs : List (Nat × Nat)) (k : Nat) :
    msum ((x, n) :: rs) k = (if x == k then n else 0) + msum rs k := by
  simp only [msum, List.filter_cons]
  by_cases h : (x == k) = true
  · rw [if_pos h]
    try rw [if_pos h]
    rw [List.map_cons, List.sum_cons]
  · rw [if_neg h]
    try rw [if_neg h]
    rw [Nat.zero_add]

theorem count_replicate_flat : ∀ (rs : List (Nat × Nat)) (k : Nat),
    (rs.flatMap (fun (x, n) => List.replicate n x)).count k = msum rs k := by
  intro rs
  induction rs with
  | nil => intro k; rfl
  | cons h t ih =>
      intro k
      obtain ⟨x, n⟩ := h
      rw [List.flatMap_cons, List.count_append, ih, List.count_replicate, msum_cons]

theorem wf : WellFormed bag := by
  constructor
  · intro rs k
    exact count_replicate_flat rs k
  · intro rs k hk
    obtain ⟨u, hu, hk'⟩ := List.mem_flatMap.mp hk
    have hr := List.mem_replicate.mp hk'
    have hke : bag.keys rs = rs.map Prod.fst := rfl
    rw [hke, List.mem_map]
    exact ⟨u, hu, hr.2.symm⟩

theorem sound_DistinctOutput : DistinctOutput.Goal := wf_distinctOutput bag wf
theorem sound_FilteredMoment : FilteredMoment.Goal := wf_filteredMoment bag wf
theorem sound_OrderStatistic : OrderStatistic.Goal := wf_orderStatistic bag wf
theorem sound_JoinFanout : JoinFanout.Goal := wf_joinFanout bag wf
theorem sound_AntiRevenue : AntiRevenue.Goal := wf_antiRevenue bag wf
theorem sound_SelfPairs : SelfPairs.Goal := wf_selfPairs bag wf
theorem sound_IntersectAll : IntersectAll.Goal := wf_intersectAll bag wf
theorem sound_OuterWeight : OuterWeight.Goal := wf_outerWeight bag wf

theorem msum_fp : ∀ rs k, msum (rs.filter (fun r => 0 < r.2)) k = msum rs k := by
  intro rs
  induction rs with
  | nil => intro k; rfl
  | cons h t ih =>
      intro k
      obtain ⟨x, n⟩ := h
      rw [List.filter_cons]
      by_cases hn : 0 < n
      · rw [if_pos (decide_eq_true hn), msum_cons, ih, msum_cons]
      · rw [if_neg (fun hc => hn (of_decide_eq_true hc)), msum_cons]
        have hn0 : n = 0 := by omega
        rw [hn0]
        by_cases hx : (x == k) = true
        · rw [if_pos hx, Nat.zero_add]
          exact ih k
        · rw [if_neg hx, Nat.zero_add]
          exact ih k

theorem exists_of_msum_pos : ∀ (rs : List (Nat × Nat)) (k : Nat),
    0 < msum rs k → ∃ r ∈ rs, r.1 = k ∧ 0 < r.2 := by
  intro rs
  induction rs with
  | nil => intro k h; simp only [msum, List.filter_nil, List.map_nil, List.sum_nil] at h; omega
  | cons h t ih =>
      intro k hpos
      obtain ⟨x, n⟩ := h
      rw [msum_cons] at hpos
      by_cases hx : (x == k) = true
      · rw [if_pos hx] at hpos
        by_cases hn : 0 < n
        · exact ⟨(x, n), (by simp : (x, n) ∈ (x, n) :: t), (Nat.beq_eq_true_eq x k).mp hx, hn⟩
        · have hn0 : n = 0 := by omega
          rw [hn0] at hpos
          rw [Nat.zero_add] at hpos
          obtain ⟨r, hr, h1, h2⟩ := ih k hpos
          exact ⟨r, List.mem_cons_of_mem _ hr, h1, h2⟩
      · rw [if_neg hx] at hpos
        rw [Nat.zero_add] at hpos
        obtain ⟨r, hr, h1, h2⟩ := ih k hpos
        exact ⟨r, List.mem_cons_of_mem _ hr, h1, h2⟩

theorem sound_ZeroRuns : ZeroRuns.Goal := by
  intro rs
  have hm : ∀ k, bag.mass (rs.filter (fun r => 0 < r.2)) k = bag.mass rs k := by
    intro k
    rw [mass_eq_msum, msum_fp, ← mass_eq_msum]
  refine (List.perm_ext_iff_of_nodup (nodup_filter _ (nodup_eraseDups _))
    (nodup_filter _ (nodup_eraseDups _))).mpr (fun k => ?_)
  have hke : bag.keys (rs.filter (fun r => 0 < r.2)) = (rs.filter (fun r => 0 < r.2)).map Prod.fst :=
    rfl
  have hkr : bag.keys rs = rs.map Prod.fst := rfl
  rw [hke, hkr]
  constructor
  · intro h
    obtain ⟨h1, h2⟩ := List.mem_filter.mp h
    rw [List.mem_eraseDups, List.mem_map] at h1
    obtain ⟨u, hu, hfu⟩ := h1
    obtain ⟨hu1, _hu2⟩ := List.mem_filter.mp hu
    rw [List.mem_filter]
    refine ⟨List.mem_eraseDups.mpr (List.mem_map.mpr ⟨u, hu1, hfu⟩), ?_⟩
    rw [← hm]
    exact of_decide_eq_true h2
  · intro h
    obtain ⟨h1, h2⟩ := List.mem_filter.mp h
    rw [List.mem_eraseDups, List.mem_map] at h1
    obtain ⟨r, hr, hr1⟩ := h1
    have hpos : 0 < msum rs k := by
      rw [← mass_eq_msum]
      exact of_decide_eq_true h2
    obtain ⟨w, hw, hw1, hw2⟩ := exists_of_msum_pos rs k hpos
    rw [List.mem_filter]
    refine ⟨List.mem_eraseDups.mpr
      (List.mem_map.mpr ⟨w, List.mem_filter.mpr ⟨hw, decide_eq_true hw2⟩, hw1⟩), ?_⟩
    rw [hm]
    exact of_decide_eq_true h2

theorem mass_pair (x y a b k : Nat) :
    bag.mass [(x, a), (y, b)] k = (if x == k then a else 0) + (if y == k then b else 0) := by
  by_cases hx : (x == k) = true <;> by_cases hy : (y == k) = true <;>
    simp [mass_eq_msum, msum, hx, hy]

theorem sound_WeightedOrderStatistic : WeightedOrderStatistic.Goal := by
  intro x y a b i hlt hltab
  have hxy : (x == y) = false := beq_ff (fun he => by omega)
  have hyx : (y == x) = false := beq_ff (fun he => by omega)
  have hmx : bag.mass [(x, a), (y, b)] x = a := by
    rw [mass_pair, if_pos (by simpa : (x == x) = true), if_neg (by simpa using hyx), Nat.add_zero]
  have hmy : bag.mass [(x, a), (y, b)] y = b := by
    rw [mass_pair, if_neg (by simpa using hxy), if_pos (by simpa : (y == y) = true), Nat.zero_add]
  have hkED : ([x, y] : List Nat).eraseDups = [x, y] := by
    rw [List.eraseDups_cons]
    have hf : (List.filter (fun b => !b == x) [y] : List Nat) = [y] := by
      rw [List.filter_cons_of_pos (by simp [hyx]), List.filter_nil]
    rw [hf, List.eraseDups_cons, List.filter_nil, List.eraseDups_nil]
  have hbelow : ∀ v : Nat, below bag [(x, a), (y, b)] v =
      (if x ≤ v then a else 0) + (if y ≤ v then b else 0) := by
    intro v
    show ((([x, y] : List Nat).eraseDups
      ).map (fun k => bag.mass [(x, a), (y, b)] k * (decide (k ≤ v)).toNat)).sum = _
    rw [hkED, List.map_cons, List.map_cons, List.map_nil, List.sum_cons, List.sum_cons,
      List.sum_nil, hmx, hmy]
    by_cases hx : x ≤ v <;> by_cases hy : y ≤ v <;> simp [hx, hy]
  have hbx : below bag [(x, a), (y, b)] x = a := by
    rw [hbelow, if_pos (by omega : x ≤ x), if_neg (by omega : ¬ y ≤ x)]
    omega
  have hby : below bag [(x, a), (y, b)] y = a + b := by
    rw [hbelow, if_pos (by omega : x ≤ y), if_pos (by omega : y ≤ y)]
  have hl_xy : 0 < a → 0 < b → live bag [(x, a), (y, b)] = [x, y] := by
    intro ha hb
    show ([x, y] : List Nat).eraseDups.filter (fun k => 0 < bag.mass [(x, a), (y, b)] k) = [x, y]
    rw [hkED, List.filter_cons, List.filter_cons, List.filter_nil]
    rw [if_pos (show decide (0 < bag.mass [(x, a), (y, b)] x) = true from
      decide_eq_true (by rw [hmx]; exact ha))]
    rw [if_pos (show decide (0 < bag.mass [(x, a), (y, b)] y) = true from
      decide_eq_true (by rw [hmy]; exact hb))]
  have hl_x : 0 < a → b = 0 → live bag [(x, a), (y, b)] = [x] := by
    intro ha hb
    show ([x, y] : List Nat).eraseDups.filter (fun k => 0 < bag.mass [(x, a), (y, b)] k) = [x]
    rw [hkED, List.filter_cons, List.filter_cons, List.filter_nil]
    rw [if_pos (show decide (0 < bag.mass [(x, a), (y, b)] x) = true from
      decide_eq_true (by rw [hmx]; exact ha))]
    rw [if_neg (show ¬ (decide (0 < bag.mass [(x, a), (y, b)] y) = true) from by rw [hmy, hb]; simp)]
  have hl_y : a = 0 → 0 < b → live bag [(x, a), (y, b)] = [y] := by
    intro ha hb
    show ([x, y] : List Nat).eraseDups.filter (fun k => 0 < bag.mass [(x, a), (y, b)] k) = [y]
    rw [hkED, List.filter_cons, List.filter_cons, List.filter_nil]
    rw [if_neg (show ¬ (decide (0 < bag.mass [(x, a), (y, b)] x) = true) from by rw [hmx, ha]; simp)]
    rw [if_pos (show decide (0 < bag.mass [(x, a), (y, b)] y) = true from
      decide_eq_true (by rw [hmy]; exact hb))]
  have hms_xy : ([x, y] : List Nat).mergeSort = [x, y] := by
    refine List.mergeSort_of_pairwise ?h
    rw [List.pairwise_cons]
    constructor
    · intro b hb
      have hb' : b = y := by simpa using hb
      subst hb'
      exact decide_eq_true (Nat.le_of_lt hlt)
    · rw [List.pairwise_cons]
      exact ⟨fun b hb => absurd hb List.not_mem_nil, List.Pairwise.nil⟩
  show ((live bag [(x, a), (y, b)]).mergeSort).find? (fun z => i < below bag [(x, a), (y, b)] z) =
    some (if i < a then x else y)
  by_cases ha : 0 < a
  · by_cases hb : 0 < b
    · rw [hl_xy ha hb, hms_xy, List.find?_cons]
      by_cases hi : i < a
      · rw [decide_eq_true (show i < below bag [(x, a), (y, b)] x from by rw [hbx]; exact hi)]
        rw [if_pos hi]
      · rw [decide_eq_false (show ¬ i < below bag [(x, a), (y, b)] x from by rw [hbx]; omega)]
        rw [List.find?_cons]
        rw [decide_eq_true (show i < below bag [(x, a), (y, b)] y from by rw [hby]; omega)]
        rw [if_neg hi]
    · -- a > 0, b = 0
      have hb0 : b = 0 := by omega
      rw [hl_x ha hb0, List.mergeSort_singleton, List.find?_cons]
      have hi : i < a := by omega
      rw [decide_eq_true (show i < below bag [(x, a), (y, b)] x from by rw [hbx]; exact hi)]
      rw [if_pos hi]
  · -- a = 0
    have ha0 : a = 0 := by omega
    have hb : 0 < b := by omega
    rw [hl_y ha0 hb, List.mergeSort_singleton, List.find?_cons]
    rw [decide_eq_true (show i < below bag [(x, a), (y, b)] y from by rw [hby, ha0]; omega)]
    rw [if_neg (by omega : ¬ i < a)]

end MultiplicityRunsProofs
