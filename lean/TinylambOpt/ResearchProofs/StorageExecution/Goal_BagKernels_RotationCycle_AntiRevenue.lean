import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `RotationCycle.AntiRevenue` — Compute revenue of all left rows whose key is absent on
  the right.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_AntiRevenue

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

/-- 小さい入力での真偽確認（n, m, s < 6）。 -/
private def checkAntiRevenue (a c : Nat × Nat × Nat) : Bool :=
  weighted bag a (fun x => if bag.mass c x == 0 then x else 0) =
    ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).sum

#eval (List.range 6).all (fun n => (List.range 5).all (fun m => (List.range 6).all (fun s =>
  (List.range 6).all (fun n2 => (List.range 5).all (fun m2 => (List.range 6).all (fun s2 =>
    checkAntiRevenue (n, m, s) (n2, m2, s2)))))))

private theorem toNat_decide_eq_if (p : Prop) [Decidable p] :
    (decide p).toNat = if p then 1 else 0 := by
  by_cases h : p <;> simp [h]

private theorem eraseDups_eq_self_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (hl : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hx : x ∉ xs := (List.nodup_cons.mp hl).1
      have hxs : xs.Nodup := (List.nodup_cons.mp hl).2
      rw [List.eraseDups_cons]
      have hfilter : List.filter (fun b : α => !b == x) xs = xs := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hne : b ≠ x := by
          intro h
          apply hx
          simpa [h] using hb
        simp [hne]
      rw [hfilter, ih hxs]

private theorem eraseDups_range (P : Nat) : (List.range P).eraseDups = List.range P :=
  eraseDups_eq_self_of_nodup (List.range P) List.nodup_range

private theorem sum_eq_zero_of_pointwise {α : Type} (l : List α) (g : α → Nat)
    (h : ∀ x ∈ l, g x = 0) : (l.map g).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem sum_map_add {α : Type} (l : List α) (g h : α → Nat) :
    (l.map (fun x => g x + h x)).sum = (l.map g).sum + (l.map h).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_range_singleton (P x : Nat) (hx : x < P) (f : Nat → Nat) :
    ((List.range P).map (fun k => List.count k [x] * f k)).sum = f x := by
  induction P with
  | zero => omega
  | succ P ih =>
      by_cases h : x == P
      · have heq : x = P := by simpa using h
        subst x
        have hnot : ∀ k ∈ List.range P, List.count k [P] = 0 := by
          intro k hk
          apply List.count_eq_zero_of_not_mem
          have hklt : k < P := by simpa using hk
          have hkne : k ≠ P := Nat.ne_of_lt hklt
          simp [hkne]
        have hsum0 : ((List.range P).map (fun k => List.count k [P] * f k)).sum = 0 := by
          apply sum_eq_zero_of_pointwise
          intro k hk
          simp [hnot k hk]
        simp [List.range_succ, hsum0]
      · have hne : x ≠ P := by simpa using h
        have hxP : x < P := by omega
        have hcnt : List.count P [x] = 0 := by simp [hne]
        rw [List.range_succ, List.map_append, List.sum_append, ih hxP]
        simp [hcnt]

private theorem sum_count_add (P : Nat) (xs ys : List Nat) (f : Nat → Nat) :
    ((List.range P).map (fun k => (xs.count k + ys.count k) * f k)).sum =
      ((List.range P).map (fun k => xs.count k * f k)).sum +
      ((List.range P).map (fun k => ys.count k * f k)).sum := by
  induction P with
  | zero => simp
  | succ P ih =>
      simp [List.range_succ, Nat.add_mul, sum_map_add, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]

private theorem sum_map_eq_range_count (P : Nat) (xs : List Nat)
    (hxs : ∀ x ∈ xs, x < P) (f : Nat → Nat) :
    (xs.map f).sum = ((List.range P).map (fun k => xs.count k * f k)).sum := by
  induction xs with
  | nil =>
      simpa using (sum_eq_zero_of_pointwise (List.range P) (fun _ => (0 : Nat)) (by simp)).symm
  | cons x xs ih =>
      have hxlt : x < P := hxs x (by simp)
      have hxs' : ∀ y ∈ xs, y < P := by
        intro y hy
        exact hxs y (by simp [hy])
      have hcnt : ∀ k : Nat, List.count k (x :: xs) = List.count k [x] + List.count k xs := by
        intro k
        simpa using (List.count_append (a := k) (l₁ := [x]) (l₂ := xs))
      calc
        (List.map f (x :: xs)).sum
            = f x + (List.map f xs).sum := by simp
        _ = ((List.range P).map (fun k => List.count k [x] * f k)).sum +
            ((List.range P).map (fun k => List.count k xs * f k)).sum := by
              rw [sum_range_singleton P x hxlt f, ih hxs']
        _ = ((List.range P).map (fun k => (List.count k [x] + List.count k xs) * f k)).sum := by
              rw [sum_count_add]
        _ = ((List.range P).map (fun k => List.count k (x :: xs) * f k)).sum := by
              simp [hcnt, Nat.add_mul]

private theorem mod_eq_iff_pos (P s n k : Nat) (hP : 0 < P) (hk : k < P) :
    (s + n) % P = k ↔ (k + P - s % P) % P = n % P := by
  have hr : s % P < P := Nat.mod_lt s hP
  have ht : n % P < P := Nat.mod_lt n hP
  have hsn : (s + n) % P = (s % P + n % P) % P := Nat.add_mod s n P
  have hpos_le : s % P ≤ k → (k + P - s % P) % P = k - s % P := by
    intro hsk
    calc
      (k + P - s % P) % P = (k + P - s % P - P) % P :=
        Nat.mod_eq_sub_mod (by omega : P ≤ k + P - s % P)
      _ = k + P - s % P - P := Nat.mod_eq_of_lt (by omega : k + P - s % P - P < P)
      _ = k - s % P := by omega
  have hpos_gt : k < s % P → (k + P - s % P) % P = k + P - s % P := by
    intro hks
    exact Nat.mod_eq_of_lt (by omega)
  constructor
  · intro h
    have hsum : (s % P + n % P) % P = k := by
      rw [← hsn]
      exact h
    by_cases hsk : s % P ≤ k
    · rw [hpos_le hsk]
      by_cases hlt : s % P + n % P < P
      · have heq : s % P + n % P = k := by
          rw [Nat.mod_eq_of_lt hlt] at hsum
          exact hsum
        omega
      · have hsumeq : s % P + n % P - P = k := by
          rw [Nat.mod_eq_sub_mod (by omega : P ≤ s % P + n % P)] at hsum
          rw [Nat.mod_eq_of_lt (by omega : s % P + n % P - P < P)] at hsum
          exact hsum
        have hlt2 : s % P + n % P - P < k := by omega
        omega
    · have hks : k < s % P := by omega
      rw [hpos_gt hks]
      by_cases hlt : s % P + n % P < P
      · have heq : s % P + n % P = k := by
          rw [Nat.mod_eq_of_lt hlt] at hsum
          exact hsum
        omega
      · have hsumeq : s % P + n % P - P = k := by
          rw [Nat.mod_eq_sub_mod (by omega : P ≤ s % P + n % P)] at hsum
          rw [Nat.mod_eq_of_lt (by omega : s % P + n % P - P < P)] at hsum
          exact hsum
        omega
  · intro h
    rw [hsn]
    by_cases hsk : s % P ≤ k
    · rw [hpos_le hsk] at h
      have heq : s % P + n % P = k := by omega
      rw [Nat.mod_eq_of_lt (by omega : s % P + n % P < P)]
      exact heq
    · have hks : k < s % P := by omega
      rw [hpos_gt hks] at h
      have heq : s % P + n % P = k + P := by omega
      rw [heq]
      rw [Nat.mod_eq_sub_mod (by omega : P ≤ k + P)]
      simpa using (Nat.mod_eq_of_lt hk : k % P = k)

private theorem count_mod_eq (n P s k : Nat) (hP : 0 < P) (hk : k < P) :
    ((List.range n).map (fun i => (s + i) % P)).count k =
      n / P + (if (k + P - s % P) % P < n % P then 1 else 0) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.count_append, ih]
      simp only [List.map_singleton, List.count_singleton, beq_iff_eq]
      have hiff := mod_eq_iff_pos P s n k hP hk
      have hdecomp : P * (n / P) + n % P = n := Nat.div_add_mod n P
      by_cases ht : n % P + 1 < P
      · have hdiv : (n + 1) / P = n / P := by
          have hn1 : n + 1 = P * (n / P) + (n % P + 1) := by omega
          rw [hn1, Nat.mul_add_div hP, Nat.div_eq_of_lt ht, Nat.add_zero]
        have hmod : (n + 1) % P = n % P + 1 := by
          have hn1 : n + 1 = (n % P + 1) + P * (n / P) := by omega
          rw [hn1, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt ht]
        rw [hdiv, hmod]
        by_cases hlt : (k + P - s % P) % P < n % P
        · have hne : ¬ (k + P - s % P) % P = n % P := by omega
          have hmn : ¬ (s + n) % P = k := by
            intro hm
            exact hne (hiff.mp hm)
          simp [hlt, hmn]
          omega
        · by_cases heq : (k + P - s % P) % P = n % P
          · have hmn : (s + n) % P = k := hiff.mpr heq
            simp [heq, hmn]
          · have hmn : ¬ (s + n) % P = k := by
              intro hm
              exact heq (hiff.mp hm)
            have hnlt : ¬ (k + P - s % P) % P < n % P + 1 := by omega
            simp [hlt, hmn, hnlt]
      · have htge : n % P + 1 = P := by
          have hlt : n % P < P := Nat.mod_lt n hP
          omega
        have hdiv : (n + 1) / P = n / P + 1 := by
          have hn1 : n + 1 = P * (n / P) + P := by omega
          rw [hn1, Nat.mul_add_div hP, Nat.div_self hP, Nat.add_comm]
        have hmod : (n + 1) % P = 0 := by
          have hn1' : n + 1 = P * (n / P) + P := by omega
          have hn1 : n + 1 = P * (n / P + 1) := by
            rw [hn1', Nat.mul_add, Nat.mul_one]
          rw [hn1, Nat.mul_mod_right]
        rw [hdiv, hmod]
        simp only [Nat.not_lt_zero, ite_false, Nat.add_zero]
        have hposlt : (k + P - s % P) % P < P := Nat.mod_lt _ hP
        by_cases hlt : (k + P - s % P) % P < n % P
        · have hne : ¬ (k + P - s % P) % P = n % P := by omega
          have hmn : ¬ (s + n) % P = k := by
            intro hm
            exact hne (hiff.mp hm)
          simp [hlt, hmn]
        · have heq : (k + P - s % P) % P = n % P := by omega
          have hmn : (s + n) % P = k := hiff.mpr heq
          simp [heq, hmn]

private theorem count_expand_eq_mass (n m s k : Nat) :
    (bag.expand (n, m, s)).count k = bag.mass (n, m, s) k := by
  unfold bag
  simp only
  by_cases hk : k < m + 1
  · have hkle : k ≤ m := by omega
    rw [count_mod_eq n (m + 1) s k (by omega) hk]
    simp [hkle]
    rw [toNat_decide_eq_if]
  · have hnotk : ¬ k ≤ m := by omega
    have hnot : ∀ i ∈ List.range n, (s + i) % (m + 1) ≠ k := by
      intro i hi heq
      have hlt : (s + i) % (m + 1) < m + 1 := Nat.mod_lt _ (by omega)
      omega
    have hcnt : ((List.range n).map (fun i => (s + i) % (m + 1))).count k = 0 := by
      apply List.count_eq_zero_of_not_mem
      intro hmem
      rw [List.mem_map] at hmem
      rcases hmem with ⟨i, hi, hval⟩
      exact hnot i hi hval
    rw [hcnt]
    simp [hnotk]

private theorem weighted_eq_expand_sum (p : Nat × Nat × Nat) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  rcases p with ⟨n, m, s⟩
  have hkeys : bag.keys (n, m, s) = List.range (m + 1) := by rfl
  unfold weighted
  rw [hkeys, eraseDups_range]
  rw [sum_map_eq_range_count (m + 1) (bag.expand (n, m, s))]
  · simp [count_expand_eq_mass]
  · intro x hx
    simp only [bag] at hx
    rw [List.mem_map] at hx
    rcases hx with ⟨i, hi, rfl⟩
    exact Nat.mod_lt _ (by omega)

private theorem filter_sum_eq_map_ite (l : List Nat) (p : Nat → Bool) :
    (l.filter p).sum = (l.map (fun x => if p x then x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h, ih]

theorem sound_AntiRevenue : RotationCycle.AntiRevenue.Goal := by
  intro a c
  rcases a with ⟨na, ma, sa⟩
  rcases c with ⟨nc, mc, sc⟩
  rw [weighted_eq_expand_sum]
  have hbool : ∀ x : Nat, (bag.mass (nc, mc, sc) x == 0) =
      (!(bag.expand (nc, mc, sc)).contains x) := by
    intro x
    rw [← count_expand_eq_mass nc mc sc x]
    by_cases hmem : x ∈ bag.expand (nc, mc, sc)
    · have hcnt : (bag.expand (nc, mc, sc)).count x ≠ 0 := by
        intro hz
        have hpos : 0 < (bag.expand (nc, mc, sc)).count x := List.count_pos_iff.mpr hmem
        omega
      simp [hcnt, hmem]
    · have hcnt : (bag.expand (nc, mc, sc)).count x = 0 := List.count_eq_zero_of_not_mem hmem
      simp [hcnt, hmem]
  rw [filter_sum_eq_map_ite]
  simp [hbool]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_AntiRevenue
