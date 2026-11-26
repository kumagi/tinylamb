import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `RotationCycle.OuterWeight` — LEFT JOIN contributes one unmatched right placeholder
  or the complete right multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_OuterWeight

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

private def ck (a c : Nat × Nat × Nat) : Bool :=
  weighted bag a (fun x => max 1 (bag.mass c x)) ==
    (pairs (bag.expand a) (bag.expand c) (· == ·)).length +
    ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).length

#eval (List.range 6).all (fun n => (List.range 5).all (fun m => (List.range 6).all (fun s =>
  (List.range 6).all (fun n2 => (List.range 5).all (fun m2 => (List.range 6).all (fun s2 =>
    ck (n, m, s) (n2, m2, s2)))))))

/-! ## Generic list lemmas -/

private theorem split_count (l : List Nat) (x : Nat) (f : Nat → Nat) :
    (l.map f).sum = l.count x * f x + ((l.filter fun y => !(y == x)).map f).sum := by
  induction l with
  | nil => simp
  | cons y l ih =>
      by_cases hy : y = x
      · subst y
        simp [ih, Nat.add_mul, Nat.add_assoc, Nat.add_comm]
      · have hbeq : (y == x) = false := by simp [hy]
        simp [hbeq, hy, ih, Nat.add_left_comm]

private theorem sum_count_of_sub_aux : ∀ n (ks : List Nat), ks.length = n →
    ∀ (xs : List Nat) (f : Nat → Nat), (∀ x ∈ xs, x ∈ ks) →
    (ks.eraseDups.map (fun k => xs.count k * f k)).sum = (xs.map f).sum := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro ks hlen xs f hsub
      cases ks with
      | nil =>
          have hxnil : xs = [] := List.eq_nil_iff_forall_not_mem.mpr (by
            intro x hx
            simpa using hsub x hx)
          subst xs
          simp
      | cons k ks =>
          rw [List.eraseDups_cons]
          simp only [List.map_cons, List.sum_cons]
          by_cases hk : k ∈ xs
          · let xs' := xs.filter (fun y => !(y == k))
            have hnotk : ∀ y ∈ xs', y ≠ k := by
              intro y hy
              have hp : (!(y == k)) = true := (List.mem_filter.mp hy).2
              intro hyk
              subst y
              simp at hp
            have hsub' : ∀ y ∈ xs', y ∈ ks.filter (fun b => !(b == k)) := by
              intro y hy
              have hyks : y ∈ ks := by
                have hyall := hsub y (List.mem_filter.mp hy).1
                rcases List.mem_cons.mp hyall with hEq | hTail
                · exact False.elim (hnotk y hy hEq)
                · exact hTail
              exact List.mem_filter.mpr ⟨hyks, by simp [hnotk y hy]⟩
            have hweights :
                ((ks.filter fun b => !(b == k)).eraseDups.map (fun y => xs.count y * f y)) =
                  ((ks.filter fun b => !(b == k)).eraseDups.map (fun y => xs'.count y * f y)) := by
              apply List.map_congr_left
              intro y hy
              have hyks : y ∈ ks.filter (fun b => !(b == k)) := List.mem_eraseDups.mp hy
              have hp : (!(y == k)) = true := (List.mem_filter.mp hyks).2
              have hcf : List.count y (List.filter (fun b : Nat => !(b == k)) xs) = xs.count y :=
                List.count_filter hp
              exact congrArg (fun t => t * f y) hcf.symm
            have hlt : (ks.filter fun b => !(b == k)).length < n := by
              rw [← hlen]
              simp only [List.length_cons]
              exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : Nat => !(b == k)) ks)
                (Nat.lt_succ_self _)
            have hih := ih ((ks.filter fun b => !(b == k)).length) hlt
              (ks.filter fun b => !(b == k)) rfl xs' f hsub'
            rw [hweights, hih]
            exact (split_count xs k f).symm
          · have hzero : xs.count k = 0 := List.count_eq_zero.mpr hk
            have hsub' : ∀ y ∈ xs, y ∈ ks.filter (fun b => !(b == k)) := by
              intro y hy
              have hyks : y ∈ ks := by
                have hyall := hsub y hy
                rcases List.mem_cons.mp hyall with hEq | hTail
                · exact False.elim (hk (by simpa [hEq] using hy))
                · exact hTail
              have hyne : y ≠ k := by
                intro hEq
                apply hk
                simpa [hEq] using hy
              exact List.mem_filter.mpr ⟨hyks, by simp [hyne]⟩
            have hlt : (ks.filter fun b => !(b == k)).length < n := by
              rw [← hlen]
              simp only [List.length_cons]
              exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : Nat => !(b == k)) ks)
                (Nat.lt_succ_self _)
            have hih := ih ((ks.filter fun b => !(b == k)).length) hlt
              (ks.filter fun b => !(b == k)) rfl xs f hsub'
            rw [hzero]
            simp
            exact hih

private theorem sum_count_of_sub (xs ks : List Nat) (f : Nat → Nat)
    (hsub : ∀ x ∈ xs, x ∈ ks) :
    (ks.eraseDups.map (fun k => xs.count k * f k)).sum = (xs.map f).sum := by
  exact sum_count_of_sub_aux ks.length ks rfl xs f hsub

private theorem length_filter_eq_sum (l : List Nat) (p : Nat → Bool) :
    (l.filter p).length = (l.map fun x => if p x then 1 else 0).sum := by
  induction l with
  | nil => simp
  | cons x l ih => by_cases hp : p x <;> simp [hp, ih, Nat.add_comm]

private theorem sum_map_add (l : List Nat) (g h : Nat → Nat) :
    (l.map fun x => g x + h x).sum = (l.map g).sum + (l.map h).sum := by
  induction l with
  | nil => simp
  | cons x l ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem max_one_eq (n : Nat) : max 1 n = n + if n = 0 then 1 else 0 := by
  by_cases h : n = 0
  · simp [h]
  · have hle : 1 ≤ n := by omega
    rw [Nat.max_eq_right hle]
    simp [h]

/-! ## Modular residue counting -/

private theorem residue_iff (d sd nd k : Nat) (hd : 0 < d) (hsd : sd < d)
    (hnd : nd < d) (hk : k < d) :
    ((sd + nd) % d = k) ↔ nd = (k + d - sd) % d := by
  constructor
  · intro h
    by_cases hlt : sd + nd < d
    · have hmod : (sd + nd) % d = sd + nd := Nat.mod_eq_of_lt hlt
      rw [hmod] at h
      have hr : (k + d - sd) % d = k - sd := by
        rw [show k + d - sd = (k - sd) + d by omega, Nat.add_mod_right]
        exact Nat.mod_eq_of_lt (by omega)
      omega
    · have hge : d ≤ sd + nd := by omega
      have hmod : (sd + nd) % d = sd + nd - d := by
        rw [Nat.mod_eq_sub_mod hge]
        exact Nat.mod_eq_of_lt (by omega)
      rw [hmod] at h
      have hr : (k + d - sd) % d = k + d - sd := Nat.mod_eq_of_lt (by omega)
      omega
  · intro h
    by_cases hle : sd ≤ k
    · have hr : (k + d - sd) % d = k - sd := by
        rw [show k + d - sd = (k - sd) + d by omega, Nat.add_mod_right]
        exact Nat.mod_eq_of_lt (by omega)
      rw [hr] at h
      have hlt : sd + nd < d := by omega
      rw [Nat.mod_eq_of_lt hlt]
      omega
    · have hlt : k < sd := by omega
      have hr : (k + d - sd) % d = k + d - sd := Nat.mod_eq_of_lt (by omega)
      rw [hr] at h
      have hge : d ≤ sd + nd := by omega
      rw [Nat.mod_eq_sub_mod hge]
      rw [Nat.mod_eq_of_lt (by omega : sd + nd - d < d)]
      omega

private theorem mod_step (d n s k : Nat) (hd : 0 < d) (hk : k < d) :
    (n + 1) / d + (if (k + d - s % d) % d < (n + 1) % d then 1 else 0) =
      n / d + (if (k + d - s % d) % d < n % d then 1 else 0) +
        (if (s + n) % d = k then 1 else 0) := by
  let sd := s % d
  let nd := n % d
  let r := (k + d - sd) % d
  have hsd : sd < d := Nat.mod_lt s hd
  have hnd : nd < d := Nat.mod_lt n hd
  have hr : r < d := Nat.mod_lt _ hd
  have ht : (s + n) % d = (sd + nd) % d := by
    dsimp [sd, nd]
    rw [Nat.add_mod]
  have hres : ((sd + nd) % d = k) ↔ nd = r := residue_iff d sd nd k hd hsd hnd hk
  have hn : n = d * (n / d) + nd := by
    dsimp [nd]
    exact (Nat.div_add_mod n d).symm
  have hn1 : n + 1 = (n / d) * d + (nd + 1) := by
    rw [Nat.mul_comm]
    omega
  change (n + 1) / d + (if r < (n + 1) % d then 1 else 0) =
    n / d + (if r < nd then 1 else 0) + (if (s + n) % d = k then 1 else 0)
  have hdivgen : ((n / d) * d + (nd + 1)) / d = n / d + (nd + 1) / d := by
    rw [Nat.mul_comm (n / d) d]
    exact Nat.mul_add_div hd (n / d) (nd + 1)
  have hmodgen : ((n / d) * d + (nd + 1)) % d = (nd + 1) % d := by
    rw [Nat.mul_comm (n / d) d]
    exact Nat.mul_add_mod d (n / d) (nd + 1)
  by_cases hlt : nd + 1 < d
  · have hdiv1 : (n + 1) / d = n / d := by
      rw [hn1, hdivgen, Nat.div_eq_of_lt hlt]
      omega
    have hmod1 : (n + 1) % d = nd + 1 := by
      rw [hn1, hmodgen, Nat.mod_eq_of_lt hlt]
    rw [hdiv1, hmod1, ht]
    by_cases hndr : r = nd
    · have htk : (sd + nd) % d = k := hres.mpr hndr.symm
      simp [hndr, htk]
    · have htk : (sd + nd) % d ≠ k := fun h => hndr (hres.mp h).symm
      simp [htk]
      by_cases hrltnd : r < nd
      · have hrltnd1 : r < nd + 1 := by omega
        simp [hrltnd, hrltnd1]
      · have hrge : nd ≤ r := by omega
        have hnot : ¬r < nd + 1 := by omega
        simp [hrltnd, hnot]
  · have hnde : nd + 1 = d := by omega
    have hdiv1 : (n + 1) / d = n / d + 1 := by
      rw [hn1, hdivgen]
      rw [show nd + 1 = d by omega]
      rw [Nat.div_self hd]
    have hmod1 : (n + 1) % d = 0 := by
      rw [hn1, hmodgen]
      rw [show nd + 1 = d by omega]
      rw [Nat.mod_self]
    rw [hdiv1, hmod1, ht]
    by_cases hndr : r = nd
    · have htk : (sd + nd) % d = k := hres.mpr hndr.symm
      simp [hndr, htk]
    · have htk : (sd + nd) % d ≠ k := fun h => hndr (hres.mp h).symm
      have hrlt : r < nd := by omega
      simp [htk, hrlt]

private theorem count_range_mod (n m s k : Nat) (hk : k < m + 1) :
    ((List.range n).map (fun i => (s + i) % (m + 1))).count k =
      n / (m + 1) + (if (k + (m + 1) - s % (m + 1)) % (m + 1) < n % (m + 1) then 1 else 0) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.count_append, ih]
      have hsingle : (List.map (fun i => (s + i) % (m + 1)) [n]).count k =
          (if (s + n) % (m + 1) = k then 1 else 0) := by
        simp [List.count_cons, beq_iff_eq]
      rw [hsingle]
      exact (mod_step (m + 1) n s k (by omega) hk).symm

/-! ## RotationCycle bag facts -/

private theorem mass_eq_count (p : Nat × Nat × Nat) (k : Nat) :
    bag.mass p k = (bag.expand p).count k := by
  rcases p with ⟨n, m, s⟩
  change (if k ≤ m then n / (m + 1) +
      (decide ((k + (m + 1) - s % (m + 1)) % (m + 1) < n % (m + 1))).toNat else 0) =
    ((List.range n).map (fun i => (s + i) % (m + 1))).count k
  by_cases hk : k ≤ m
  · simp only [hk, ite_true]
    rw [count_range_mod n m s k (by omega)]
    by_cases hlt : (k + (m + 1) - s % (m + 1)) % (m + 1) < n % (m + 1) <;> simp [hlt]
  · have hnotmem : ∀ i, (s + i) % (m + 1) ≠ k := by
      intro i h
      have hb : (s + i) % (m + 1) < m + 1 := Nat.mod_lt _ (by omega)
      omega
    have hcount : ((List.range n).map (fun i => (s + i) % (m + 1))).count k = 0 := by
      apply List.count_eq_zero.mpr
      intro hmem
      rw [List.mem_map] at hmem
      rcases hmem with ⟨i, hi, hi_eq⟩
      exact hnotmem i hi_eq
    simp [hk, hcount]

private theorem mem_keys_of_mem_expand (n m s k : Nat) :
    k ∈ bag.expand (n, m, s) → k ∈ bag.keys (n, m, s) := by
  intro h
  rw [show bag.expand (n, m, s) = (List.range n).map (fun i => (s + i) % (m + 1)) by rfl] at h
  rw [List.mem_map] at h
  rcases h with ⟨i, hi, hik⟩
  rw [show bag.keys (n, m, s) = List.range (m + 1) by rfl]
  rw [List.mem_range]
  have hb : (s + i) % (m + 1) < m + 1 := Nat.mod_lt _ (by omega)
  omega

private theorem weighted_eq_sum_expand (p : Nat × Nat × Nat) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  rcases p with ⟨n, m, s⟩
  unfold weighted
  change (((bag.keys (n, m, s)).eraseDups.map fun k => bag.mass (n, m, s) k * f k).sum) =
    ((bag.expand (n, m, s)).map f).sum
  have hmass :
      ((bag.keys (n, m, s)).eraseDups.map fun k => bag.mass (n, m, s) k * f k) =
        ((bag.keys (n, m, s)).eraseDups.map fun k => (bag.expand (n, m, s)).count k * f k) := by
    apply List.map_congr_left
    intro k hk
    rw [mass_eq_count]
  rw [hmass]
  exact sum_count_of_sub (bag.expand (n, m, s)) (bag.keys (n, m, s)) f (by
    intro x hx
    exact mem_keys_of_mem_expand n m s x hx)

/-! ## Goal -/

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  let xs := bag.expand a
  let ys := bag.expand c
  have hleft : weighted bag a (fun x => max 1 (bag.mass c x)) =
      ((xs.map fun x => max 1 (ys.count x)).sum) := by
    rw [weighted_eq_sum_expand]
    change ((xs.map fun x => max 1 (bag.mass c x)).sum) =
      ((xs.map fun x => max 1 (ys.count x)).sum)
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    rw [mass_eq_count]
  have hpairs : (pairs xs ys (· == ·)).length = (xs.map fun x => ys.count x).sum := by
    unfold pairs
    rw [List.length_flatMap]
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    rw [List.length_map]
    simpa [BEq.comm] using (List.count_eq_length_filter (a := x) (l := ys)).symm
  have hunmatched : (xs.filter fun x => !(ys.contains x)).length =
      (xs.map fun x => if ys.count x = 0 then 1 else 0).sum := by
    rw [length_filter_eq_sum]
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    by_cases h : ys.contains x = true
    · have hmem : x ∈ ys := List.contains_iff_mem.mp h
      have hnzero : ys.count x ≠ 0 := by
        intro hz
        exact (List.count_eq_zero.mp hz) hmem
      simp [hnzero, hmem]
    · have hnotmem : x ∉ ys := by
        intro hmem
        exact h (List.contains_iff_mem.mpr hmem)
      have hz : ys.count x = 0 := List.count_eq_zero.mpr hnotmem
      simp [hz, hnotmem]
  have hdecomp : (xs.map fun x => max 1 (ys.count x)).sum =
      (xs.map fun x => ys.count x + if ys.count x = 0 then 1 else 0).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact max_one_eq (ys.count x)
  rw [hleft, hdecomp, sum_map_add, ← hpairs, ← hunmatched]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_OuterWeight
