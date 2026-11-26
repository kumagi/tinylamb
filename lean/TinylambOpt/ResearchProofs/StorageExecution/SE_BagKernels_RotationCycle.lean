import TinylambOpt.ResearchProofs.StorageExecution.SE_BagKernels_Common

set_option maxHeartbeats 0
set_option linter.unusedVariables false

open TinylambOpt.ResearchProofs.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace RotationCycleProofs

open RotationCycle

theorem hrange_add (a b : Nat) : List.range (a + b)
    = List.range a ++ List.map (fun x => a + x) (List.range b) := by
  induction b with
  | zero => rw [Nat.add_zero, List.range_zero, List.map_nil, List.append_nil]
  | succ b ih =>
    rw [Nat.add_succ, List.range_succ, ih, ← Nat.succ_eq_add_one, List.range_succ,
      List.map_append, List.map_singleton, ← List.append_assoc]

theorem rcyc_shift (s q M j : Nat) : (s + q*M + j) % M = (s + j) % M := by
  have h1 : (s + q*M) % M = s % M := by
    rw [Nat.add_mod, Nat.mul_mod, Nat.mod_self, Nat.mul_zero, Nat.zero_mod, Nat.add_zero,
      Nat.mod_mod]
  rw [Nat.add_mod, h1, ← Nat.add_mod]

theorem filter_all_true (l : List Nat) : l.filter (fun _ => true) = l := by
  induction l with
  | nil => rfl
  | cons x t ih =>
    rw [List.filter_cons, ih]
    simp

theorem filter_congr_lists {p q : Nat → Bool} : ∀ (l : List Nat), (∀ x ∈ l, p x = q x) →
    l.filter p = l.filter q := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons x t ih =>
    intro h
    rw [List.filter_cons, List.filter_cons]
    by_cases hq : q x = true
    · have hp : p x = true := by rw [h x List.mem_cons_self]; exact hq
      rw [if_pos hq, if_pos hp, ih (fun y hy => h y (List.mem_cons_of_mem x hy))]
    · have hp : ¬ p x = true := fun hh => hq (by rw [← h x List.mem_cons_self]; exact hh)
      rw [if_neg hq, if_neg hp, ih (fun y hy => h y (List.mem_cons_of_mem x hy))]

theorem eraseDups_of_nodup : ∀ (l : List Nat), l.Nodup → l.eraseDups = l := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons x t ih =>
    intro h
    cases h with
    | cons hxy ht =>
      rw [List.eraseDups_cons]
      have hpq : ∀ b ∈ t, (! (b == x)) = (true : Bool) := by
        intro b hb
        cases hbe : b == x with
        | true => exact absurd (beq_iff_eq.mp hbe).symm (hxy b hb)
        | false => simp [hbe]
      rw [filter_congr_lists t hpq, filter_all_true t, ih ht]

theorem eraseDups_range (n : Nat) : (List.range n).eraseDups = List.range n :=
  eraseDups_of_nodup _ List.nodup_range

/-- Window membership: for `x < M` the residue `x` is the unique preimage of `a`. -/
theorem mod_sub_eq (t M : Nat) (h1 : M ≤ t) (h2 : t - M < M) : t % M = t - M := by
  rw [Nat.mod_eq_sub, show t / M = 1 from
    Nat.div_eq_of_lt_le (by omega) (show t < (1 + 1) * M from by omega), Nat.mul_one]

theorem rcyc_scalar (s M a x : Nat) (hM : 0 < M) (ha : a < M) (hx : x < M) :
    (s + x) % M = a ↔ (a + M - s % M) % M = x := by
  have h1 : (s + x) % M = (s % M + x) % M := by
    rw [Nat.add_mod, Nat.mod_eq_of_lt hx]
  have hA : s % M < M := Nat.mod_lt _ hM
  rw [h1]
  constructor
  · intro h
    by_cases h2 : s % M + x < M
    · rw [Nat.mod_eq_of_lt h2] at h
      rw [show a + M - s % M = x + M from by omega, Nat.add_mod, Nat.mod_eq_of_lt hx,
        Nat.mod_self, Nat.add_zero, Nat.mod_eq_of_lt hx]
    · have h3 : M ≤ s % M + x := Nat.le_of_not_lt h2
      have hsub : s % M + x - M < M := by omega
      have hmod := mod_sub_eq (s % M + x) M h3 hsub
      rw [hmod] at h
      rw [show a + M - s % M = x from by omega, Nat.mod_eq_of_lt hx]
  · intro h
    rw [← h, Nat.add_comm, Nat.mod_add_mod,
      show a + M - s % M + s % M = a + M from by omega,
      Nat.add_mod, Nat.mod_eq_of_lt ha, Nat.mod_self, Nat.add_zero, Nat.mod_eq_of_lt ha]

theorem rcyc_win (M : Nat) (hM : 0 < M) (s a r : Nat) (hr : r ≤ M) (ha : a < M) :
    ((List.range r).filter (fun j => (s + j) % M == a)).length =
      if (a + M - s % M) % M < r then 1 else 0 := by
  induction r with
  | zero =>
    rw [List.range_zero, List.filter_nil, List.length_nil]
    rw [if_neg (Nat.not_lt_zero _)]
  | succ r ih =>
    have hrM : r < M := Nat.lt_of_lt_of_le (Nat.lt_succ_self r) hr
    rw [List.range_succ, List.filter_append, List.length_append, ih (by omega),
      List.filter_cons, List.filter_nil]
    cases hbe : (s + r) % M == a with
    | true =>
      simp
      have hw : (a + M - s % M) % M = r :=
        (rcyc_scalar s M a r hM ha hrM).mp (beq_iff_eq.mp hbe)
      rw [hw, if_neg (Nat.lt_irrefl r), if_pos (Nat.lt_succ_self r)]
      try omega
    | false =>
      simp
      have hw : (a + M - s % M) % M ≠ r := by
        intro hrw
        have hh : ((s + r) % M == a) = true :=
          beq_iff_eq.mpr ((rcyc_scalar s M a r hM ha hrM).mpr hrw)
        rw [hbe] at hh
        exact absurd hh (by decide)
      by_cases hwr : (a + M - s % M) % M < r
      · rw [if_pos hwr, if_pos (by omega : (a + M - s % M) % M < r + 1)]
        try omega
      · rw [if_neg hwr, if_neg (fun hh => hw (by omega))]
        try omega

theorem rcyc_countP_map (s M k : Nat) : ∀ (l : List Nat),
    (l.map (fun j => (s + j) % M)).countP (fun v => v == k) =
      l.countP (fun j => (s + j) % M == k) := by
  intro l
  rw [List.countP_map]
  exact List.countP_congr (fun _ _ => Iff.rfl)

theorem rcyc_full (M : Nat) (hM : 0 < M) (s k : Nat) : ∀ q : Nat,
    ((List.range (q * M)).map (fun i => (s + i) % M)).countP (fun v => v == k) =
      if k < M then q else 0 := by
  intro q
  induction q with
  | zero =>
    rw [Nat.zero_mul, List.range_zero, List.map_nil, List.countP_nil]
    split <;> rfl
  | succ q ih =>
    rw [Nat.succ_mul]
    rw [hrange_add (q * M) M, List.map_append, List.countP_append, ih]
    have hb2 : (List.map (fun i => (s + i) % M) (List.map (fun j => q * M + j) (List.range M))).countP
        (fun v => v == k) = (List.range M).countP (fun j => (s + j) % M == k) := by
      rw [rcyc_countP_map s M k, List.countP_map]
      refine List.countP_congr (fun j hj => ?_)
      simp only [Function.comp_apply]
      rw [← Nat.add_assoc, rcyc_shift s q M j]
    by_cases hkk : k < M
    · rw [hb2, List.countP_eq_length_filter, rcyc_win M hM s k M (Nat.le_refl M) hkk]
      simp only [if_pos (Nat.mod_lt (k + M - s % M) hM), if_pos hkk]
      try omega
    · rw [hb2]
      simp only [if_neg hkk]
      rw [Nat.zero_add]
      refine (List.countP_eq_zero).mpr (fun j hj h => ?_)
      have hne : (s + j) % M ≠ k :=
        Nat.ne_of_lt (Nat.lt_of_lt_of_le (Nat.mod_lt _ hM) (Nat.le_of_not_lt hkk))
      exact hne (beq_iff_eq.mp h)

theorem rcyc_count (M : Nat) (hM : 0 < M) (q r : Nat) (hr : r < M) (s k : Nat) (hk : k < M) :
    ((List.range (q * M + r)).map (fun i => (s + i) % M)).countP (fun v => v == k)
      = q + if (k + M - s % M) % M < r then 1 else 0 := by
  rw [hrange_add (q * M) r, List.map_append, List.countP_append, rcyc_full M hM s k q,
    if_pos hk]
  have hb2 : (List.map (fun i => (s + i) % M) (List.map (fun j => q * M + j) (List.range r))).countP
      (fun v => v == k) = (List.range r).countP (fun j => (s + j) % M == k) := by
    rw [rcyc_countP_map s M k, List.countP_map]
    refine List.countP_congr (fun j hj => ?_)
    simp only [Function.comp_apply]
    rw [← Nat.add_assoc, rcyc_shift s q M j]
  rw [hb2, List.countP_eq_length_filter, rcyc_win M hM s k r (Nat.le_of_lt hr) hk]

theorem mass_eq_RC : ∀ (p : Nat × Nat × Nat) (k : Nat), (bag.expand p).count k = bag.mass p k := by
  intro p k
  obtain ⟨n, m, s⟩ := p
  have hM : 0 < m + 1 := Nat.succ_pos m
  have hr : n % (m + 1) < m + 1 := Nat.mod_lt n hM
  show ((List.range n).map (fun i => (s + i) % (m + 1))).countP (fun v => v == k)
    = if k ≤ m then n / (m + 1) + (decide ((k + (m + 1) - s % (m + 1)) % (m + 1) < n % (m + 1))).toNat
      else 0
  by_cases hk : k ≤ m
  · rw [if_pos hk]
    have hkM : k < m + 1 := Nat.lt_of_le_of_lt hk (Nat.lt_succ_self m)
    have hn : n = n / (m + 1) * (m + 1) + n % (m + 1) := (Nat.div_add_mod' n (m + 1)).symm
    have h1 : (n / (m + 1) * (m + 1) + n % (m + 1)) / (m + 1) = n / (m + 1) := by
      refine Nat.div_eq_of_lt_le (by omega) ?_
      rw [Nat.add_mul, Nat.one_mul]
      omega
    have h2 : (n / (m + 1) * (m + 1) + n % (m + 1)) % (m + 1) = n % (m + 1) := by
      rw [Nat.add_comm, Nat.mul_comm (n / (m + 1)) (m + 1), Nat.add_mul_mod_self_left,
        Nat.mod_eq_of_lt hr]
    have hto : (decide ((k + (m + 1) - s % (m + 1)) % (m + 1) < n % (m + 1))).toNat
        = if (k + (m + 1) - s % (m + 1)) % (m + 1) < n % (m + 1) then (1 : Nat) else 0 := by
      by_cases h : (k + (m + 1) - s % (m + 1)) % (m + 1) < n % (m + 1) <;> simp [h]
    rw [hn, h1, h2, hto, rcyc_count (m + 1) hM (n / (m + 1)) (n % (m + 1)) hr s k hkM]
  · rw [if_neg hk]
    refine (List.countP_eq_zero).mpr (fun v hv h => ?_)
    obtain ⟨i, _, rfl⟩ := List.mem_map.mp hv
    have hlt : (s + i) % (m + 1) < m + 1 := Nat.mod_lt _ hM
    have heq : (s + i) % (m + 1) = k := beq_iff_eq.mp h
    rw [heq] at hlt
    exact absurd (Nat.le_of_lt_succ hlt) hk

theorem cover_RC :
    ∀ (p : Nat × Nat × Nat) (k : Nat), k ∈ bag.expand p → k ∈ bag.keys p := by
  intro p k hk
  obtain ⟨n, m, s⟩ := p
  rw [show bag.keys (n, m, s) = List.range (m + 1) from rfl]
  rw [show bag.expand (n, m, s) = (List.range n).map (fun i => (s + i) % (m + 1)) from rfl] at hk
  obtain ⟨i, _, hkeq⟩ := List.mem_map.mp hk
  rw [← hkeq]
  exact List.mem_range.mpr (Nat.mod_lt _ (Nat.succ_pos m))

theorem wf : WellFormed bag :=
  ⟨fun p k => mass_eq_RC p k, cover_RC⟩

theorem sound_DistinctOutput : DistinctOutput.Goal := wf_distinctOutput bag wf
theorem sound_FilteredMoment : FilteredMoment.Goal := wf_filteredMoment bag wf
theorem sound_OrderStatistic : OrderStatistic.Goal := wf_orderStatistic bag wf
theorem sound_JoinFanout : JoinFanout.Goal := wf_joinFanout bag wf
theorem sound_AntiRevenue : AntiRevenue.Goal := wf_antiRevenue bag wf
theorem sound_SelfPairs : SelfPairs.Goal := wf_selfPairs bag wf
theorem sound_IntersectAll : IntersectAll.Goal := wf_intersectAll bag wf
theorem sound_OuterWeight : OuterWeight.Goal := wf_outerWeight bag wf

theorem rcyc_smul (c : Nat) (f : Nat → Nat) : ∀ (l : List Nat),
    (l.map (fun v => c * f v)).sum = c * (l.map f).sum := by
  intro l
  induction l with
  | nil => rfl
  | cons x t ih =>
    rw [List.map_cons, List.sum_cons, ih, List.map_cons, List.sum_cons, Nat.mul_add]

theorem wsum_len (lo hi M : Nat) :
    ((List.range M).filter (fun v => (lo ≤ v) && (v < hi))).length = min M hi - lo := by
  induction M with
  | zero =>
    rw [List.range_zero, List.filter_nil, List.length_nil, Nat.min_eq_left (Nat.zero_le hi)]
    simp
  | succ M ih =>
    have hr1 : List.range (M + 1) = List.range M ++ [M] := by
      rw [hrange_add M 1, List.range_one, List.map_singleton]
      simp
    rw [hr1, List.filter_append, List.length_append, ih, List.filter_cons, List.filter_nil]
    by_cases hmid : M < hi
    · rw [Nat.min_eq_left (by omega : M ≤ hi), Nat.min_eq_left (by omega : M + 1 ≤ hi)]
      by_cases hlo : lo ≤ M
      · simp [hmid, hlo]
        try omega
      · simp [hmid, hlo]
        try omega
    · rw [Nat.min_eq_right (by omega : hi ≤ M), Nat.min_eq_right (by omega : hi ≤ M + 1)]
      simp [hmid]
      try omega

theorem rcyc_wsum (lo hi M : Nat) (h1 : lo ≤ hi) (h2 : hi ≤ M) :
    ((List.range M).map (fun v => (decide (lo ≤ v ∧ v < hi)).toNat)).sum = hi - lo := by
  have hsz : ∀ (l : List Nat),
      (l.map (fun v => (decide (lo ≤ v ∧ v < hi)).toNat)).sum
        = (l.filter (fun v => (lo ≤ v) && (v < hi))).length := by
    intro l
    induction l with
    | nil => rfl
    | cons x t ih =>
      rw [List.map_cons, List.sum_cons, ih, List.filter_cons]
      have hb : decide (lo ≤ x ∧ x < hi) = ((lo ≤ x) && (x < hi)) := by
        by_cases p1 : lo ≤ x <;> by_cases p2 : x < hi <;> simp [p1, p2]
      rw [hb]
      cases hcb : ((lo ≤ x) && (x < hi)) with
      | true => simp; omega
      | false => simp
  rw [hsz, wsum_len, Nat.min_eq_right h2]

theorem sound_RangePerCycle : RangePerCycle.Goal := by
  intro m k s lo hi hle hhi
  have hM : 0 < m + 1 := Nat.succ_pos m
  have hdiv : k * (m + 1) / (m + 1) = k := by
    refine Nat.div_eq_of_lt_le (Nat.le_refl _) ?_
    rw [Nat.add_mul, Nat.one_mul]
    omega
  have hmod0 : k * (m + 1) % (m + 1) = 0 :=
    Nat.mod_eq_zero_of_dvd ⟨k, Nat.mul_comm k (m + 1)⟩
  have hmass : ∀ v ∈ List.range (m + 1), bag.mass (k * (m + 1), m, s) v = k := by
    intro v hv
    show (if v ≤ m then (k * (m + 1)) / (m + 1)
        + (decide ((v + (m + 1) - s % (m + 1)) % (m + 1) < (k * (m + 1)) % (m + 1))).toNat else 0) = k
    rw [if_pos (Nat.le_of_lt_succ (List.mem_range.mp hv)), hdiv, hmod0]
    rw [show (decide ((v + (m + 1) - s % (m + 1)) % (m + 1) < 0)).toNat = 0 from by simp]
    omega
  show ((List.range (m + 1)).eraseDups.map
      (fun v => bag.mass (k * (m + 1), m, s) v * (decide (lo ≤ v ∧ v < hi)).toNat)).sum
    = k * (hi - lo)
  rw [eraseDups_range (m + 1)]
  rw [sum_map_congr_of_mem (List.range (m + 1)) (fun v => bag.mass (k * (m + 1), m, s) v
        * (decide (lo ≤ v ∧ v < hi)).toNat) (fun v => k * (decide (lo ≤ v ∧ v < hi)).toNat)
      (fun v hv => by rw [hmass v hv])]
  rw [rcyc_smul, rcyc_wsum lo hi (m + 1) hle hhi]

theorem sound_CompleteTurns : CompleteTurns.Goal := by
  intro n m s k
  have hM : 0 < m + 1 := Nat.succ_pos m
  have hdiv : (n + 1) * (m + 1) / (m + 1) = n + 1 := by
    refine Nat.div_eq_of_lt_le (Nat.le_refl _) ?_
    rw [show (n + 1 + 1) * (m + 1) = (n + 1) * (m + 1) + (m + 1) from by
      rw [Nat.add_mul, Nat.one_mul]]
    omega
  have hmod0 : (n + 1) * (m + 1) % (m + 1) = 0 :=
    Nat.mod_eq_zero_of_dvd ⟨n + 1, Nat.mul_comm (n + 1) (m + 1)⟩
  show (if k ≤ m then ((n + 1) * (m + 1)) / (m + 1)
      + (decide ((k + (m + 1) - s % (m + 1)) % (m + 1) < ((n + 1) * (m + 1)) % (m + 1))).toNat else 0)
    = (if k ≤ m then n + 1 else 0)
  by_cases hk : k ≤ m
  · rw [if_pos hk, if_pos hk, hdiv, hmod0]
    rw [show (decide ((k + (m + 1) - s % (m + 1)) % (m + 1) < 0)).toNat = 0 from by simp]
    try omega
  · rw [if_neg hk, if_neg hk]

end RotationCycleProofs
