import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_OrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

private abbrev leNat (a b : Nat) : Bool := decide (a ≤ b)

private theorem leNat_trans : ∀ a b c : Nat, leNat a b = true → leNat b c = true → leNat a c = true := by
  intro a b c hab hbc
  simp [leNat] at hab hbc ⊢
  omega

private theorem leNat_total : ∀ a b : Nat, (leNat a b || leNat b a) = true := by
  intro a b
  simp [leNat]
  omega

private theorem sort_pairwise (l : List Nat) : (l.mergeSort).Pairwise (fun a b => a ≤ b) := by
  exact (List.pairwise_mergeSort leNat_trans leNat_total l).imp (by
    intro a b h
    simpa [leNat] using h)

private theorem sort_perm_eq {l₁ l₂ : List Nat} (h : l₁.Perm l₂) : l₁.mergeSort = l₂.mergeSort := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
  · intro a b _ _ hab hba
    omega
  · exact sort_pairwise l₁
  · exact sort_pairwise l₂
  · exact (List.mergeSort_perm l₁ leNat).trans (h.trans (List.mergeSort_perm l₂ leNat).symm)

private theorem countP_le_of_imp {p q : Nat → Bool} (h : ∀ x, p x = true → q x = true) :
    ∀ l : List Nat, l.countP p ≤ l.countP q
  | [] => by simp
  | a :: l => by
      simp only [List.countP_cons]
      have hp : (if p a = true then 1 else 0) ≤ (if q a = true then 1 else 0) := by
        by_cases hpa : p a = true
        · have hqa : q a = true := h a hpa
          simp [hpa, hqa]
        · simp [hpa]
      have hi := countP_le_of_imp h l
      omega

private theorem rank_bounds {zs : List Nat} (hs : zs.Pairwise (fun a b => a ≤ b)) {i : Nat}
    (hi : i < zs.length) :
    zs.countP (fun y => y < zs[i]) ≤ i ∧ i < zs.countP (fun y => y ≤ zs[i]) := by
  induction zs generalizing i with
  | nil => simp at hi
  | cons a tl ih =>
      cases hs with
      | cons hforall htail =>
          by_cases hi0 : i = 0
          · subst i
            have hzero : tl.countP (fun y => y < a) = 0 := by
              apply List.countP_eq_zero.mpr
              intro y hy
              have hay : a ≤ y := hforall y hy
              simp [Nat.not_lt.mpr hay]
            constructor
            · simp [hzero]
            · simp
          · have hilen : i < tl.length + 1 := by simpa [List.length_cons] using hi
            have hitl : i - 1 < tl.length := by omega
            have ihres := ih htail hitl
            have hsome : some ((a :: tl)[i]) = some (tl[i - 1]) := by
              rw [← List.getElem?_eq_getElem hi]
              simp [List.getElem?_cons, hi0]
            have hget : (a :: tl)[i] = tl[i - 1] := Option.some.inj hsome
            have hmem : tl[i - 1] ∈ tl := List.getElem_mem hitl
            have hav : a ≤ tl[i - 1] := hforall _ hmem
            constructor
            · rw [hget, List.countP_cons]
              by_cases hv : a < tl[i - 1] <;> simp [hv] <;> omega
            · rw [hget, List.countP_cons]
              simp [hav]
              omega

private theorem find?_sorted {d : List Nat} (hs : d.Pairwise (fun a b => a ≤ b)) (pred : Nat → Bool)
    {v : Nat} (hv : v ∈ d) (hp : pred v = true)
    (hb : ∀ x, x ∈ d → x < v → pred x = false) :
    d.find? pred = some v := by
  induction d with
  | nil => simp at hv
  | cons a tl ih =>
      cases hs with
      | cons hforall htail =>
          by_cases hva : a = v
          · subst a
            simp [hp]
          · have hv' : v ∈ tl := by
              cases List.mem_cons.mp hv with
              | inl h => exact False.elim (hva h.symm)
              | inr h => exact h
            have hav : a < v := by
              have hle : a ≤ v := hforall v hv'
              omega
            have hpa : pred a = false := hb a (by simp) hav
            simp [hpa]
            exact ih htail hv' (by
              intro x hx hxv
              exact hb x (by simp [hx]) hxv)

private theorem quantile_aux (xs d : List Nat) (i : Nat)
    (hds : d.Pairwise (fun a b => a ≤ b))
    (hsub : ∀ x, x ∈ xs → x ∈ d) :
    d.find? (fun x => decide (i < (xs.filter (fun y => y ≤ x)).length)) = xs.mergeSort[i]? := by
  let zs := xs.mergeSort
  have hlen : zs.length = xs.length := by simp [zs]
  have hperm : zs.Perm xs := by simpa [zs] using List.mergeSort_perm xs leNat
  have hsp : zs.Pairwise (fun a b => a ≤ b) := by simpa [zs] using sort_pairwise xs
  by_cases hi : i < xs.length
  · have hizs : i < zs.length := by omega
    let v := zs[i]
    have hv_zs : v ∈ zs := by
      dsimp [v]
      exact List.getElem_mem hizs
    have hv_xs : v ∈ xs := (hperm.mem_iff).mp hv_zs
    have hv_d : v ∈ d := hsub v hv_xs
    have hrank := rank_bounds hsp hizs
    have hrank' : zs.countP (fun y => y < v) ≤ i ∧ i < zs.countP (fun y => y ≤ v) := by
      simpa [v] using hrank
    have hp : decide (i < (xs.filter (fun y => y ≤ v)).length) = true := by
      have hcp : xs.countP (fun y => y ≤ v) = zs.countP (fun y => y ≤ v) :=
        (List.Perm.countP_eq (fun y => y ≤ v) hperm).symm
      have hcnt : i < (xs.filter (fun y => y ≤ v)).length := by
        rw [← List.countP_eq_length_filter]
        rw [hcp]
        exact hrank'.2
      simp [hcnt]
    have hbefore : ∀ x, x ∈ d → x < v →
        decide (i < (xs.filter (fun y => y ≤ x)).length) = false := by
      intro x hx hxv
      have hcp : xs.countP (fun y => y ≤ x) = zs.countP (fun y => y ≤ x) :=
        (List.Perm.countP_eq (fun y => y ≤ x) hperm).symm
      have hmono : zs.countP (fun y => y ≤ x) ≤ zs.countP (fun y => y < v) := by
        apply countP_le_of_imp
        intro y hy
        simp at hy ⊢
        omega
      have hcnt : (xs.filter (fun y => y ≤ x)).length ≤ i := by
        rw [← List.countP_eq_length_filter]
        rw [hcp]
        omega
      have hnot : ¬ i < (xs.filter (fun y => y ≤ x)).length := by omega
      simp [hnot]
    have hf : d.find? (fun x => decide (i < (xs.filter (fun y => y ≤ x)).length)) = some v :=
      find?_sorted hds _ hv_d hp hbefore
    have hg' : xs.mergeSort[i]? = some v := by
      have hg : zs[i]? = some v := by
        dsimp [v]
        exact List.getElem?_eq_getElem hizs
      simpa [zs] using hg
    rw [hg']
    exact hf
  · have hgez : zs.length ≤ i := by omega
    have hnone : zs[i]? = none := List.getElem?_eq_none hgez
    have hnone' : xs.mergeSort[i]? = none := by simpa [zs] using hnone
    rw [hnone']
    apply List.find?_eq_none.mpr
    intro x hx
    have hbnd : (xs.filter (fun y => y ≤ x)).length ≤ xs.length := List.length_filter_le _ _
    have hnot : ¬ i < (xs.filter (fun y => y ≤ x)).length := by omega
    simp [hnot]

private theorem eraseDups_range : ∀ n : Nat, (List.range n).eraseDups = List.range n
  | 0 => by simp
  | n + 1 => by
      rw [List.range_succ, List.eraseDups_append, eraseDups_range n]
      have hrm : ([n] : List Nat).removeAll (List.range n) = [n] := by
        simp [List.removeAll, List.mem_range]
      rw [hrm]
      simp [List.eraseDups_cons]

private theorem sum_map_zero (n : Nat) : ((List.range n).map (fun _ : Nat => 0)).sum = 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp

private theorem sum_range_eq_single (n y : Nat) (f : Nat → Nat) :
    ((List.range n).map (fun k => (if k == y then 1 else 0) * f k)).sum =
      if y < n then f y else 0 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp only [List.map_singleton, List.sum_cons, List.sum_nil, Nat.add_zero]
      by_cases h : y = n
      · subst y
        simp
      · have hne : n ≠ y := by omega
        by_cases hlt : y < n
        · have hsucc : y < n.succ := Nat.lt_succ_of_lt hlt
          simp [hlt, hsucc, hne]
        · have hnlt : ¬ y < n.succ := by omega
          simp [hlt, hnlt, hne]

private theorem sum_map_add_nat (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a tl ih =>
      simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_range_count (n : Nat) (xs : List Nat) (x : Nat)
    (hpeak : ∀ y ∈ xs, y < n) :
    ((List.range n).map (fun k => xs.count k * (decide (k ≤ x)).toNat)).sum =
      ((xs.filter (fun y => y ≤ x)).length) := by
  induction xs with
  | nil =>
      simpa using sum_map_zero n
  | cons y tl ih =>
      have hpeak_tl : ∀ z ∈ tl, z < n := by
        intro z hz
        exact hpeak z (by simp [hz])
      have ih' := ih hpeak_tl
      have hpoint : (List.range n).map (fun k => (y :: tl).count k * (decide (k ≤ x)).toNat) =
          (List.range n).map (fun k =>
            tl.count k * (decide (k ≤ x)).toNat +
              (if k == y then 1 else 0) * (decide (k ≤ x)).toNat) := by
        apply List.map_congr_left
        intro k hk
        rw [List.count_cons]
        by_cases hyk : y = k
        · subst k
          simp [Nat.add_mul]
        · have hky : k ≠ y := by intro h; exact hyk h.symm
          simp [hyk, hky]
      have hmain : ((List.range n).map (fun k => (y :: tl).count k * (decide (k ≤ x)).toNat)).sum =
          ((List.range n).map (fun k => tl.count k * (decide (k ≤ x)).toNat)).sum +
          ((List.range n).map (fun k => (if k == y then 1 else 0) * (decide (k ≤ x)).toNat)).sum := by
        rw [hpoint, sum_map_add_nat]
      have hsingle : ((List.range n).map (fun k => (if k == y then 1 else 0) * (decide (k ≤ x)).toNat)).sum =
          (if y ≤ x then 1 else 0) := by
        have hylt : y < n := hpeak y (by simp)
        have hs := sum_range_eq_single n y (fun k => (decide (k ≤ x)).toNat)
        by_cases hyx : y ≤ x
        · simpa [hylt, hyx] using hs
        · simpa [hylt, hyx] using hs
      rw [hmain, ih', hsingle]
      by_cases hyx : y ≤ x <;> simp [hyx] <;> omega

/-! Modular counting lemmas for the repeated residue cycle. -/

private theorem residue_shift (q r k t : Nat) (_hq : 0 < q) (hr : r < q) (hk : k < q) (ht : t < q) :
    ((r + t) % q == k) = (((k + q - r) % q) == t) := by
  by_cases hrk : r ≤ k
  · have hd : (k + q - r) % q = k - r := by
      rw [Nat.mod_eq_sub_mod (by omega : q ≤ k + q - r)]
      have : k + q - r - q = k - r := by omega
      rw [this]
      exact Nat.mod_eq_of_lt (by omega)
    rw [hd]
    by_cases htkr : t = k - r
    · subst t
      have h1 : (r + (k - r)) % q = k := by
        rw [Nat.mod_eq_of_lt]
        · omega
        · omega
      simp [h1]
    · have hne : k - r ≠ t := by omega
      have h2 : (r + t) % q ≠ k := by
        intro h
        have hlt2 : r + t < 2 * q := by omega
        by_cases hqt : r + t < q
        · have heq : r + t = k := by
            rw [Nat.mod_eq_of_lt hqt] at h
            exact h
          omega
        · have heq : r + t - q = k := by
            rw [Nat.mod_eq_sub_mod (by omega : q ≤ r + t)] at h
            rw [Nat.mod_eq_of_lt (by omega : r + t - q < q)] at h
            exact h
          omega
      have hb1 : ((r + t) % q == k) = false := by simp [h2]
      have hb2 : ((k - r) == t) = false := by simp [hne]
      rw [hb1, hb2]
  · have hrk' : k < r := by omega
    have hd : (k + q - r) % q = k + q - r := by
      exact Nat.mod_eq_of_lt (by omega)
    rw [hd]
    by_cases htd : t = k + q - r
    · subst t
      have hsum : r + (k + q - r) = q + k := by omega
      have h1 : (r + (k + q - r)) % q = k := by
        rw [hsum, Nat.mod_eq_sub_mod (by omega : q ≤ q + k)]
        simp [Nat.mod_eq_of_lt hk]
      simp [h1]
    · have h2 : (r + t) % q ≠ k := by
        intro h
        have hlt2 : r + t < 2 * q := by omega
        by_cases hqt : r + t < q
        · have heq : r + t = k := by
            rw [Nat.mod_eq_of_lt hqt] at h
            exact h
          omega
        · have heq : r + t - q = k := by
            rw [Nat.mod_eq_sub_mod (by omega : q ≤ r + t)] at h
            rw [Nat.mod_eq_of_lt (by omega : r + t - q < q)] at h
            exact h
          omega
      have hb1 : ((r + t) % q == k) = false := by simp [h2]
      have hne2 : k + q - r ≠ t := by omega
      have hb2 : ((k + q - r) == t) = false := by simp [hne2]
      rw [hb1, hb2]

private theorem beq_comm (a b : Nat) : (a == b) = (b == a) := by
  cases hab : (a == b) <;> cases hba : (b == a) <;> simp_all

private theorem mod_shift_eq (q s k i : Nat) (hq : 0 < q) (hk : k < q) :
    ((s + i) % q == k) = (i % q == (k + q - s % q) % q) := by
  have hr : s % q < q := Nat.mod_lt _ hq
  calc
    ((s + i) % q == k) = ((s % q + i % q) % q == k) := by rw [Nat.add_mod]
    _ = ((k + q - s % q) % q == i % q) :=
      residue_shift q (s % q) k (i % q) hq hr hk (Nat.mod_lt i hq)
    _ = (i % q == (k + q - s % q) % q) := beq_comm ((k + q - s % q) % q) (i % q)

private theorem count_map_mod_shift (l : List Nat) (q s k : Nat) (hq : 0 < q) (hk : k < q) :
    (l.map (fun i => (s + i) % q)).count k =
      l.countP (fun i => i % q == (k + q - s % q) % q) := by
  induction l with
  | nil => simp
  | cons a tl ih =>
      simp only [List.map_cons, List.count_cons, List.countP_cons]
      rw [ih]
      by_cases h : (s + a) % q == k
      · have h' := mod_shift_eq q s k a hq hk
        rw [h] at h'
        simp [h, h']
      · have h' := mod_shift_eq q s k a hq hk
        simp [h] at h'
        simp [h, h']

private theorem decomp (n q : Nat) : (n / q) * q + n % q = n := by
  simpa [Nat.mul_comm] using (Nat.div_add_mod n q)

private theorem mod_succ_of_lt (n q : Nat) (_hq : 0 < q) (h : n % q + 1 < q) : (n+1)%q = n%q+1 := by
  have hn : n + 1 = (n % q + 1) + (n / q) * q := by
    have hd := decomp n q
    omega
  rw [hn, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt h]

private theorem div_succ_of_lt (n q : Nat) (hq : 0 < q) (h : n % q + 1 < q) : (n+1)/q = n/q := by
  have hn : n + 1 = (n % q + 1) + (n / q) * q := by
    have hd := decomp n q
    omega
  rw [hn, Nat.add_mul_div_right _ _ hq]
  rw [Nat.div_eq_of_lt h]
  simp

private theorem mod_succ_of_wrap (n q : Nat) (hq : 0 < q) (h : ¬ n % q + 1 < q) : (n+1)%q = 0 := by
  have hn : n + 1 = q + (n / q) * q := by
    have hd := decomp n q
    have hlt : n % q < q := Nat.mod_lt _ hq
    omega
  rw [hn, Nat.add_mul_mod_self_right, Nat.mod_self]

private theorem div_succ_of_wrap (n q : Nat) (hq : 0 < q) (h : ¬ n % q + 1 < q) : (n+1)/q = n/q+1 := by
  have hn : n + 1 = q + (n / q) * q := by
    have hd := decomp n q
    have hlt : n % q < q := Nat.mod_lt _ hq
    omega
  rw [hn, Nat.add_mul_div_right _ _ hq]
  rw [Nat.div_self hq]
  omega

private theorem count_mod_eq (n q d : Nat) (hq : 0 < q) (hd : d < q) :
    (List.range n).countP (fun i => i % q == d) = n / q + (decide (d < n % q)).toNat := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, ih]
      simp only [List.countP_singleton]
      have hltmod : n % q < q := Nat.mod_lt n hq
      by_cases h : n % q + 1 < q
      · rw [mod_succ_of_lt n q hq h, div_succ_of_lt n q hq h]
        by_cases hdn : n % q == d
        · have hdeq : n % q = d := by simpa using hdn
          have hold : ¬ d < n % q := by omega
          have hnew : d < n % q + 1 := by omega
          simp [hdn, hold, hnew]
        · have hdene : n % q ≠ d := by simpa using hdn
          by_cases hold : d < n % q
          · have hnew : d < n % q + 1 := by omega
            simp [hdn, hold, hnew]
          · have hnew : ¬ d < n % q + 1 := by omega
            simp [hdn, hold, hnew]
      · rw [mod_succ_of_wrap n q hq h, div_succ_of_wrap n q hq h]
        have hnq : n % q + 1 = q := by omega
        by_cases hdn : n % q == d
        · have hdeq : n % q = d := by simpa using hdn
          have hold : ¬ d < n % q := by omega
          simp [hdn, hold]
        · have hdene : n % q ≠ d := by simpa using hdn
          have hold : d < n % q := by omega
          simp [hdn, hold]

private theorem mass_count (n m s k : Nat) :
    bag.mass (n, m, s) k = (bag.expand (n, m, s)).count k := by
  unfold bag
  by_cases hkm : k ≤ m
  · simp only [hkm, ite_true]
    have hq : 0 < m + 1 := by omega
    have hkq : k < m + 1 := by omega
    have hdlt : (k + (m + 1) - s % (m + 1)) % (m + 1) < m + 1 := Nat.mod_lt _ hq
    have hcnt := count_mod_eq n (m + 1) ((k + (m + 1) - s % (m + 1)) % (m + 1)) hq hdlt
    rw [count_map_mod_shift (List.range n) (m + 1) s k hq hkq]
    exact hcnt.symm
  · have hz : ((List.range n).map (fun i => (s + i) % (m + 1))).count k = 0 := by
      apply List.count_eq_zero.mpr
      intro hx
      rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
      have hlt : (s + i) % (m + 1) < m + 1 := Nat.mod_lt _ (by omega)
      omega
    simp [hkm, hz]

private theorem expand_lt (n m s : Nat) {x : Nat} (hx : x ∈ bag.expand (n, m, s)) : x < m + 1 := by
  rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
  exact Nat.mod_lt _ (by omega)

private theorem expand_subset_live (n m s : Nat) {x : Nat} (hx : x ∈ bag.expand (n, m, s)) :
    x ∈ live bag (n, m, s) := by
  unfold live
  have hkeys : x ∈ bag.keys (n, m, s) := by
    change x ∈ List.range (m + 1)
    exact List.mem_range.mpr (expand_lt n m s hx)
  have hkeys2 : x ∈ (bag.keys (n, m, s)).eraseDups := by
    simpa [bag, eraseDups_range] using hkeys
  simp only [List.mem_filter]
  constructor
  · exact hkeys2
  · rw [mass_count]
    have hpos : 0 < (bag.expand (n, m, s)).count x := List.count_pos_iff.mpr hx
    simp [hpos]

private theorem below_eq_countP (n m s x : Nat) :
    below bag (n, m, s) x = ((bag.expand (n, m, s)).filter (fun y => y ≤ x)).length := by
  unfold below weighted
  change (((List.range (m + 1)).eraseDups.map
    (fun k => bag.mass (n, m, s) k * (decide (k ≤ x)).toNat)).sum) = _
  rw [eraseDups_range]
  simp only [mass_count]
  apply sum_range_count
  intro y hy
  exact expand_lt n m s hy

theorem sound_OrderStatistic : OrderStatistic.Goal := by
  intro p i
  rcases p with ⟨n, m, s⟩
  unfold quantile
  simpa [below_eq_countP] using
    (quantile_aux (bag.expand (n, m, s)) ((live bag (n, m, s)).mergeSort) i
      (sort_pairwise (live bag (n, m, s))) (by
        intro x hx
        exact List.mem_mergeSort.mpr (expand_subset_live n m s hx)))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_OrderStatistic
