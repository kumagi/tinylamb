import TinylambOpt.ResearchProofs.StorageExecution.SE_BagKernels_Common

open TinylambOpt.ResearchProofs.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.Compression.Wire

namespace IntervalCoverProofs

open IntervalCover

theorem hexp : ∀ (rs : List (Nat × Nat)),
    bag.expand rs = rs.flatMap (fun (s, n) => (List.range n).map (s + ·)) := fun _ => rfl

theorem hkeys : ∀ (rs : List (Nat × Nat)),
    bag.keys rs = List.range (peak (rs.map (fun (s, n) => s + n))) := fun _ => rfl

theorem block_eq (s n : Nat) : (List.range n).map (fun t => s + t) = List.range' s n :=
  List.range'_eq_map_range.symm

theorem and_left {a b : Bool} (h : (a && b) = true) : a = true := by
  cases a <;> cases b <;> simpa using h

theorem and_right {a b : Bool} (h : (a && b) = true) : b = true := by
  cases a <;> cases b <;> simpa using h

theorem count_range' (s n k : Nat) :
    (List.range' s n).count k = if s ≤ k && k < s + n then 1 else 0 := by
  induction n generalizing k with
  | zero =>
      show ([] : List Nat).count k = _
      rw [List.count_nil]
      by_cases h1 : s ≤ k <;> by_cases h2 : k < s + 0 <;> simp [h1, h2, Nat.add_zero] <;> omega
  | succ n ih =>
      rw [List.range'_1_concat, List.count_append, ih]
      have h1 : List.count k [s + n] = if s + n == k then 1 else 0 := by
        rw [List.count_cons, List.count_nil, Nat.zero_add]
      rw [h1]
      rcases Nat.lt_trichotomy k (s + n) with hlt | heq | hgt
      · by_cases hle : s ≤ k
        · have e1 : (s ≤ k && k < s + n) = true := by
            rw [decide_eq_true hle, decide_eq_true hlt, Bool.true_and]
          have e3 : (s ≤ k && k < s + (n + 1)) = true := by
            rw [decide_eq_true hle, decide_eq_true (by omega : k < s + (n + 1)), Bool.true_and]
          have e2 : ¬((s + n == k) = true) := by
            intro hh
            exact absurd (Nat.eq_of_beq_eq_true hh) (by omega)
          rw [if_pos e1, if_pos e3, if_neg e2]
        · have e1 : ¬((s ≤ k && k < s + n) = true) := by
            intro hh
            exact hle (of_decide_eq_true (and_left hh))
          have e3 : ¬((s ≤ k && k < s + (n + 1)) = true) := by
            intro hh
            exact hle (of_decide_eq_true (and_left hh))
          have e2 : ¬((s + n == k) = true) := by
            intro hh
            exact absurd (Nat.eq_of_beq_eq_true hh) (by omega)
          rw [if_neg e1, if_neg e3, if_neg e2]
      · subst heq
        have e1 : ¬((s ≤ s + n && s + n < s + n) = true) := by
          intro hh
          exact Nat.lt_irrefl _ (of_decide_eq_true (and_right hh))
        have e3 : (s ≤ s + n && s + n < s + (n + 1)) = true := by
          rw [decide_eq_true (Nat.le_add_right s n),
            decide_eq_true (by omega : s + n < s + (n + 1)), Bool.true_and]
        have e2 : (s + n == s + n) = true := by simp
        rw [if_neg e1, if_pos e3, if_pos e2]
      · have hnlt : ¬(k < s + n) := by omega
        have hnlt' : ¬(k < s + (n + 1)) := by omega
        have e1 : ¬((s ≤ k && k < s + n) = true) := by
          intro hh
          exact hnlt (of_decide_eq_true (and_right hh))
        have e3 : ¬((s ≤ k && k < s + (n + 1)) = true) := by
          intro hh
          exact hnlt' (of_decide_eq_true (and_right hh))
        have e2 : ¬((s + n == k) = true) := by
          intro hh
          exact absurd (Nat.eq_of_beq_eq_true hh) (by omega)
        rw [if_neg e1, if_neg e3, if_neg e2]

theorem foldl_max_ge : ∀ (l : List Nat) (x : Nat), x ≤ l.foldl max x := by
  intro l
  induction l with
  | nil => intro x; exact Nat.le_refl x
  | cons y r ih =>
      intro x
      simp only [List.foldl_cons]
      have h1 := ih (max x y)
      omega

theorem mass_eq_ic : ∀ (rs : List (Nat × Nat)) (k : Nat),
    (rs.flatMap (fun (s, n) => (List.range n).map (s + ·))).count k =
      rs.countP (fun (s, n) => s ≤ k && k < s + n) := by
  intro rs
  induction rs with
  | nil => intro k; rfl
  | cons h t ih =>
      intro k
      obtain ⟨s, n⟩ := h
      rw [List.flatMap_cons, List.count_append, ih, List.countP_cons]
      dsimp only
      rw [block_eq, count_range']
      by_cases hc : (s ≤ k && k < s + n) = true
      · rw [if_pos hc]
        try rw [if_pos hc]
        omega
      · rw [if_neg hc]
        try rw [if_neg hc]
        omega

theorem hpeak : ∀ (l : List Nat) (x : Nat), x ≤ peak (x :: l) := by
  intro l x
  show x ≤ ((x :: l).max?).getD 0
  rw [List.max?_cons', Option.getD_some]
  exact foldl_max_ge l x

theorem peak_ge_head (g : Nat × Nat → Nat) (x : Nat × Nat) (l : List (Nat × Nat)) :
    g x ≤ peak ((x :: l).map g) := by
  rw [List.map_cons]
  exact hpeak _ _

theorem foldl_max_mono :
    ∀ (l : List Nat) (a b : Nat), a ≤ b → l.foldl max a ≤ l.foldl max b
  | [], _, _, h => h
  | y :: r, a, b, h => by
      simp only [List.foldl_cons]
      exact foldl_max_mono r (max a y) (max b y) (by omega)

theorem peak_le_cons (v : Nat) (w : List Nat) : peak w ≤ peak (v :: w) := by
  cases w with
  | nil =>
      show (([] : List Nat).max?).getD 0 ≤ ((v :: []).max?).getD 0
      simp
  | cons u r =>
      show ((u :: r).max?).getD 0 ≤ ((v :: u :: r).max?).getD 0
      rw [List.max?_cons', List.max?_cons', Option.getD_some, Option.getD_some]
      rw [List.foldl_cons]
      exact foldl_max_mono r u (max v u) (by omega)

theorem wf : WellFormed bag := by
  constructor
  · intro rs k
    exact mass_eq_ic rs k
  · intro rs
    induction rs with
    | nil =>
        intro k hk
        rw [hexp] at hk
        simp at hk
    | cons x xs ih =>
        intro k hk
        obtain ⟨s, n⟩ := x
        rw [hexp, List.flatMap_cons, List.mem_append] at hk
        cases hk with
        | inl hk1 =>
            obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hk1
            have hin : i < n := List.mem_range.mp hi
            rw [hkeys, List.mem_range, List.map_cons]
            dsimp only
            exact Nat.lt_of_lt_of_le (by omega : s + i < s + n) (hpeak _ _)
        | inr hk2 =>
            have hres := ih k (by rw [hexp]; exact hk2)
            rw [hkeys, List.mem_range] at hres
            rw [hkeys, List.mem_range, List.map_cons]
            dsimp only
            exact Nat.lt_of_lt_of_le hres (peak_le_cons (s + n) _)

theorem sound_DistinctOutput : DistinctOutput.Goal := wf_distinctOutput bag wf
theorem sound_FilteredMoment : FilteredMoment.Goal := wf_filteredMoment bag wf
theorem sound_OrderStatistic : OrderStatistic.Goal := wf_orderStatistic bag wf
theorem sound_JoinFanout : JoinFanout.Goal := wf_joinFanout bag wf
theorem sound_AntiRevenue : AntiRevenue.Goal := wf_antiRevenue bag wf
theorem sound_SelfPairs : SelfPairs.Goal := wf_selfPairs bag wf
theorem sound_IntersectAll : IntersectAll.Goal := wf_intersectAll bag wf
theorem sound_OuterWeight : OuterWeight.Goal := wf_outerWeight bag wf

theorem sum_one_block (s n : Nat) : ((List.range' s n).map (fun _ => 1)).sum = n := by
  induction n generalizing s with
  | zero =>
      show (([] : List Nat).map (fun _ => 1)).sum = 0
      rw [List.map_nil, List.sum_nil]
  | succ n ih =>
      rw [List.range'_1_concat, List.map_append, List.sum_append, ih, List.map_singleton,
        List.sum_singleton]

theorem cov_sum : ∀ (rs : List (Nat × Nat)),
    ((bag.expand rs).map (fun _ => 1)).sum = (rs.map Prod.snd).sum := by
  intro rs
  induction rs with
  | nil => rfl
  | cons h t ih =>
      obtain ⟨s, n⟩ := h
      rw [hexp, List.flatMap_cons]
      dsimp only
      rw [← hexp, List.map_append, List.sum_append, ih, List.map_cons, List.sum_cons]
      rw [show ((List.range n).map (fun t => s + t)).map (fun _ => 1) =
          (List.range' s n).map (fun _ => 1) from congrArg (List.map fun _ => 1) (block_eq s n)]
      rw [sum_one_block]

theorem sum_block_lim (s n lim : Nat) :
    (((List.range' s n).map (fun x => (decide (x < lim)).toNat))).sum = min n (lim - s) := by
  induction n generalizing s lim with
  | zero =>
      show (([] : List Nat).map (fun x => (decide (x < lim)).toNat)).sum = min 0 (lim - s)
      rw [List.map_nil, List.sum_nil]
      omega
  | succ n ih =>
      rw [List.range'_1_concat, List.map_append, List.sum_append, ih s lim]
      have hc : ((([s + n].map (fun x => (decide (x < lim)).toNat))).sum) =
          (decide (s + n < lim)).toNat := rfl
      rw [hc]
      by_cases hlt : s + n < lim
      · have hL : min n (lim - s) = n := Nat.min_eq_left (by omega : n ≤ lim - s)
        have hR : min (n + 1) (lim - s) = n + 1 := Nat.min_eq_left (by omega : n + 1 ≤ lim - s)
        rw [hL, hR]
        have hd : (decide (s + n < lim)).toNat = 1 := by simp [decide_eq_true hlt]
        rw [hd]
      · have hL : min n (lim - s) = lim - s := Nat.min_eq_right (by omega : lim - s ≤ n)
        have hR : min (n + 1) (lim - s) = lim - s := Nat.min_eq_right (by omega : lim - s ≤ n + 1)
        rw [hL, hR]
        have hd : (decide (s + n < lim)).toNat = 0 := by simp [decide_eq_false hlt]
        rw [hd, Nat.add_zero]

theorem clip_sum : ∀ (rs : List (Nat × Nat)) (lim : Nat),
    ((bag.expand rs).map (fun x => (decide (x < lim)).toNat)).sum =
      (rs.map (fun (s, n) => min n (lim - s))).sum := by
  intro rs
  induction rs with
  | nil => intro lim; rfl
  | cons h t ih =>
      intro lim
      obtain ⟨s, n⟩ := h
      rw [hexp, List.flatMap_cons]
      dsimp only
      rw [← hexp, List.map_append, List.sum_append, ih, List.map_cons, List.sum_cons]
      rw [show ((List.range n).map (fun t => s + t)).map (fun x => (decide (x < lim)).toNat) =
          (List.range' s n).map (fun x => (decide (x < lim)).toNat) from
        congrArg (List.map fun x => (decide (x < lim)).toNat) (block_eq s n)]
      rw [sum_block_lim]

theorem sound_CoverageIntegral : CoverageIntegral.Goal := by
  intro rs
  rw [weighted_eq bag wf]
  exact cov_sum rs

theorem sound_ClipRanges : ClipRanges.Goal := by
  intro rs lim
  rw [weighted_eq bag wf]
  exact clip_sum rs lim

end IntervalCoverProofs
