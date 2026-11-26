import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `RotationCycle.IntersectAll` — Intersect two bags by the minimum multiplicity at each
  key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_IntersectAll

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

private theorem div_eq_one_of_bounds (t p : Nat) (hp : 0 < p)
    (hge : p ≤ t) (hlt : t < 2 * p) : t / p = 1 := by
  have hlt2 : t / p < 2 := (Nat.div_lt_iff_lt_mul hp).mpr hlt
  have hnle : ¬ t / p ≤ 0 := by
    intro h
    have hh := (Nat.div_le_iff_le_mul hp).mp h
    omega
  omega

private theorem mod_step_count (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (n - r + p - 1) / p = n / p + if r < n % p then 1 else 0 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases hnr : n < r
  · have hdiv : n / p = 0 := Nat.div_eq_of_lt (by omega)
    have hm : n - r + p - 1 = p - 1 := by omega
    have hnot : ¬ r < n % p := by omega
    have hpdiv : (p - 1) / p = 0 := Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))
    simp [hm, hdiv, hnot, hpdiv]
  · have hrn : r ≤ n := by omega
    have hsub : n - r + p - 1 = n / p * p + (n % p + p - 1 - r) := by
      grind
    rw [hsub, Nat.mul_comm (n / p) p]
    rw [Nat.mul_add_div hp (n / p) (n % p + p - 1 - r)]
    by_cases hlt : r < n % p
    · have hge : p ≤ n % p + p - 1 - r := by omega
      have hlt2 : n % p + p - 1 - r < 2 * p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 1 := div_eq_one_of_bounds _ _ hp hge hlt2
      simp [hlt, hdiv2]
    · have hle : n % p ≤ r := by omega
      have hlt2 : n % p + p - 1 - r < p := by omega
      have hdiv2 : (n % p + p - 1 - r) / p = 0 := Nat.div_eq_of_lt hlt2
      simp [hlt, hdiv2]

private theorem succ_mod_step (n p : Nat) (hp : 0 < p) :
    (n + 1) % p = if n % p + 1 < p then n % p + 1 else 0 := by
  by_cases hp1 : p = 1
  · subst p
    have hm : n % 1 = 0 := by
      have := Nat.mod_lt n (by omega : 0 < 1)
      omega
    simp [hm]
  · have hp' : 1 < p := by omega
    have h1 : (n + 1) % p = (n % p + 1 % p) % p := Nat.add_mod n 1 p
    have h1p : 1 % p = 1 := Nat.mod_eq_of_lt hp'
    rw [h1, h1p]
    by_cases h : n % p + 1 < p
    · rw [Nat.mod_eq_of_lt h]
      simp [h]
    · have heq : n % p + 1 = p := by
        have hs := Nat.mod_lt n hp
        omega
      rw [heq, Nat.mod_self]
      simp

private theorem succ_div_step (n p : Nat) (hp : 0 < p) :
    (n + 1) / p = n / p + if n % p + 1 < p then 0 else 1 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases h : n % p + 1 < p
  · have hEq : n + 1 = p * (n / p) + (n % p + 1) := by omega
    rw [hEq, Nat.mul_add_div hp (n / p) (n % p + 1), Nat.div_eq_of_lt h]
    simp [h]
  · have heq : n % p + 1 = p := by omega
    have hEq : n + 1 = p * (n / p + 1) := by grind
    rw [hEq, Nat.mul_comm p (n / p + 1), Nat.mul_div_left _ hp]
    simp [h]

private theorem range_filter_mod_eq (n p r : Nat) (hp : 0 < p) (hr : r < p) :
    (List.range n).filter (fun i => i % p == r) =
      (List.range ((n - r + p - 1) / p)).map (fun q => r + q * p) := by
  induction n with
  | zero =>
      have hm : (0 - r + p - 1) / p = 0 := by
        have : 0 - r + p - 1 = p - 1 := by omega
        rw [this, Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))]
      have hpdiv : (p - 1) / p = 0 := Nat.div_eq_of_lt (Nat.sub_lt hp (by omega : 0 < 1))
      simp [hpdiv]
  | succ n ih =>
      rw [List.range_succ, List.filter_append, ih]
      rw [mod_step_count n p r hp hr, mod_step_count (n + 1) p r hp hr]
      rw [succ_mod_step n p hp, succ_div_step n p hp]
      by_cases hn : n % p == r
      · have hnp : n % p = r := Nat.eq_of_beq_eq_true hn
        have hltold : ¬ r < n % p := by omega
        have htail : List.map (fun q => r + q * p) (List.range (n / p)) ++ [n] =
            List.map (fun q => r + q * p) (List.range (n / p + 1)) := by
          rw [show n / p + 1 = Nat.succ (n / p) by omega, List.range_succ, List.map_append]
          simp
          have hd := Nat.div_add_mod n p
          rw [Nat.mul_comm p (n / p)] at hd
          have hval : r + n / p * p = n := by omega
          rw [hval]
        by_cases hs : r + 1 < p
        · have hltnew : r < r + 1 := by omega
          simp [hnp, hs, hltnew]
          exact htail
        · have hltnew : ¬ r < 0 := by omega
          simp [hnp, hs, hltnew]
          exact htail
      · have hne0 : n % p ≠ r := by intro he; exact hn (by rw [he]; simp)
        have hltold : r < n % p ∨ n % p < r := by
          by_cases hrlt : r < n % p
          · exact Or.inl hrlt
          · right; omega
        by_cases h : n % p + 1 < p
        · rcases hltold with hlt | hgt
          · have hltnew : r < n % p + 1 := by omega
            simp [hn, h, hlt, hltnew]
          · have hltnew : ¬ r < n % p + 1 := by omega
            have hnlt : ¬ r < n % p := by omega
            simp [hn, h, hnlt, hltnew]
        · have hne : n % p ≠ r := by intro he; exact hn (by rw [he]; simp)
          have heq : n % p + 1 = p := by
            have hs' := Nat.mod_lt n hp
            omega
          have hlt : r < n % p := by omega
          have hltnew : ¬ r < 0 := by omega
          simp [hn, h, hlt, hltnew]

private theorem mod_add_eq_iff (d u k r : Nat) (hd : 0 < d) (hu : u < d)
    (hk : k < d) (hr : r < d) : ((u + r) % d = k) ↔ (r = (k + d - u) % d) := by
  constructor
  · intro h
    by_cases ht : u + r < d
    · have hmod : (u + r) % d = u + r := Nat.mod_eq_of_lt ht
      rw [hmod] at h
      have hsum : k + d - u = d + r := by omega
      rw [hsum, Nat.add_mod_left, Nat.mod_eq_of_lt hr]
    · have hle : d ≤ u + r := by omega
      have ht2 : u + r - d < d := by omega
      have hmod : (u + r) % d = (u + r - d) % d := Nat.mod_eq_sub_mod hle
      rw [hmod, Nat.mod_eq_of_lt ht2] at h
      have hsum : k + d - u = r := by omega
      rw [hsum, Nat.mod_eq_of_lt hr]
  · intro h
    by_cases huk : u ≤ k
    · have hf : (k + d - u) % d = k - u := by
        have hsum : k + d - u = d + (k - u) := by omega
        rw [hsum, Nat.add_mod_left, Nat.mod_eq_of_lt (by omega)]
      rw [hf] at h
      have ht : u + r < d := by omega
      rw [Nat.mod_eq_of_lt ht]
      omega
    · have hf : (k + d - u) % d = k + d - u := by
        apply Nat.mod_eq_of_lt
        omega
      rw [hf] at h
      have ht : d ≤ u + r := by omega
      have ht2 : u + r - d < d := by omega
      rw [Nat.mod_eq_sub_mod ht, Nat.mod_eq_of_lt ht2]
      omega

private theorem count_map_eq_filter_length (l : List Nat) (g : Nat → Nat) (k : Nat) :
    (l.map g).count k = (l.filter (fun i => g i == k)).length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : g x == k
      · have h' : g x = k := beq_iff_eq.mp h
        simp [ih, h']
      · simp [List.count_cons, ih, h]

private theorem count_expand_eq_mass (n m s k : Nat) :
    (bag.expand (n, m, s)).count k = bag.mass (n, m, s) k := by
  dsimp only [bag]
  by_cases hk : k ≤ m
  · have hd : 0 < m + 1 := by omega
    have hklt : k < m + 1 := by omega
    let d := m + 1
    let f := (k + d - s % d) % d
    have hfd : f < d := Nat.mod_lt _ hd
    have hfilter : (List.range n).filter (fun i => (s + i) % d == k) =
        (List.range n).filter (fun i => i % d == f) := by
      apply List.filter_congr
      intro i hi
      have hu : s % d < d := Nat.mod_lt s hd
      have hi' : i % d < d := Nat.mod_lt i hd
      have hmod : (s + i) % d = (s % d + i % d) % d := Nat.add_mod s i d
      have hiff : ((s + i) % d = k) ↔ (i % d = f) := by
        rw [hmod]
        change ((s % d + i % d) % d = k) ↔ (i % d = (k + d - s % d) % d)
        exact mod_add_eq_iff d (s % d) k (i % d) hd hu hklt hi'
      by_cases h1 : (s + i) % d == k
      · have htrue : (s + i) % d = k := beq_iff_eq.mp h1
        have hrtrue : i % d = f := hiff.mp htrue
        have h2 : (i % d == f) = true := beq_iff_eq.mpr hrtrue
        simp [h1, h2]
      · have hfalse : (s + i) % d ≠ k := by
          intro heq
          exact h1 (beq_iff_eq.mpr heq)
        have hrfalse : i % d ≠ f := by
          intro heq
          exact hfalse (hiff.mpr heq)
        have h2 : (i % d == f) = false := Bool.eq_false_of_ne_true (by
          intro hb
          exact hrfalse (beq_iff_eq.mp hb))
        simp [h1, h2]
    rw [count_map_eq_filter_length]
    rw [hfilter]
    rw [range_filter_mod_eq n d f hd hfd]
    simp only [List.length_map, List.length_range]
    rw [mod_step_count n d f hd hfd]
    dsimp [d, f]
    by_cases hlt : (k + (m + 1) - s % (m + 1)) % (m + 1) < n % (m + 1) <;>
      simp [hk, hlt]
  · have hnotmem : k ∉ (List.range n).map (fun i => (s + i) % (m + 1)) := by
      intro hm
      rw [List.mem_map] at hm
      rcases hm with ⟨i, hi, hval⟩
      have hlt : (s + i) % (m + 1) < m + 1 := Nat.mod_lt _ (by omega)
      have hkd : m + 1 ≤ k := by omega
      omega
    rw [List.count_eq_zero_of_not_mem hnotmem]
    simp [hk]

private theorem count_flatMap_replicate (xs : List Nat) (f : Nat → Nat) (k : Nat) :
    (xs.flatMap (fun x => List.replicate (f x) x)).count k =
      (xs.map (fun x => if x = k then f x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, List.count_replicate, beq_iff_eq]

private theorem nodup_filter {α : Type} [DecidableEq α] (l : List α) (hn : l.Nodup) (p : α → Bool) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hn
      simp only [List.filter_cons]
      by_cases hp : p a = true
      · simp [hp]
        constructor
        · exact hn.1
        · exact ih hn.2
      · simp [hp]
        exact ih hn.2

private theorem nodup_eraseDups_self (l : List Nat) : l.eraseDups.Nodup := by
  suffices H : ∀ n (l : List Nat), l.length ≤ n → l.eraseDups.Nodup from H l.length l (Nat.le_refl _)
  intro n
  induction n with
  | zero =>
      intro l h
      cases l with
      | nil => simp
      | cons a as => simp at h
  | succ n ih =>
      intro l h
      cases l with
      | nil => simp [List.eraseDups]
      | cons a as =>
          have hlen : as.length ≤ n := by simpa using h
          rw [List.eraseDups_cons]
          have hf : (as.filter (fun b => !b == a)).length ≤ n :=
            Nat.le_trans (List.length_filter_le _ _) hlen
          have ht : ((as.filter (fun b => !b == a)).eraseDups).Nodup := ih _ hf
          rw [List.nodup_cons]
          constructor
          · intro hm
            rw [List.mem_eraseDups, List.mem_filter] at hm
            simp at hm
          · exact ht

private theorem nodup_live (n m s : Nat) : (live bag (n, m, s)).Nodup := by
  unfold live
  exact nodup_filter _ (nodup_eraseDups_self (bag.keys (n, m, s))) _

private theorem mem_live_of_mass_pos (n m s k : Nat) (h : 0 < bag.mass (n, m, s) k) :
    k ∈ live bag (n, m, s) := by
  have hk : k ≤ m := by
    by_cases hle : k ≤ m
    · exact hle
    · simp [bag, hle] at h
  unfold live
  rw [List.mem_filter]
  constructor
  · rw [List.mem_eraseDups]
    change k ∈ List.range (m + 1)
    rw [List.mem_range]
    omega
  · simpa using h

private theorem sum_eq_single_of_nodup (xs : List Nat) (hn : xs.Nodup) (k : Nat) (f : Nat → Nat) :
    (xs.map (fun x => if x = k then f x else 0)).sum = if k ∈ xs then f k else 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.map_cons, List.sum_cons]
      by_cases hxk : x = k
      · subst x
        have hnot : k ∉ xs := hn.1
        simpa [hnot] using (ih hn.2)
      · have hi := ih hn.2
        have hkx : k ≠ x := fun h => hxk h.symm
        by_cases hk : k ∈ xs
        · simp [hxk, hkx, hk, hi]
        · simp [hxk, hkx, hk, hi]

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  obtain ⟨n₁, m₁, s₁⟩ := a
  obtain ⟨n₂, m₂, s₂⟩ := c
  rw [count_flatMap_replicate]
  rw [sum_eq_single_of_nodup (live bag (n₁, m₁, s₁)) (nodup_live n₁ m₁ s₁)]
  rw [count_expand_eq_mass, count_expand_eq_mass]
  by_cases hk : k ∈ live bag (n₁, m₁, s₁)
  · simp [hk]
  · have hm0 : bag.mass (n₁, m₁, s₁) k = 0 := by
      by_cases hz : bag.mass (n₁, m₁, s₁) k = 0
      · exact hz
      · have hpos : 0 < bag.mass (n₁, m₁, s₁) k := Nat.pos_of_ne_zero hz
        exact False.elim (hk (mem_live_of_mass_pos n₁ m₁ s₁ k hpos))
    simp [hk, hm0]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_IntersectAll
