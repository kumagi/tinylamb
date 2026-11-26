import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `CoGroup.JoinFanout` — Evaluate band join count from the two weighted supports.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_JoinFanout

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

private theorem nodup_eraseDups : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by rw [List.eraseDups, List.eraseDupsBy_nil]; exact .nil
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro hm
        rw [List.mem_eraseDups] at hm
        rw [List.mem_filter] at hm
        have hbad : (!a == a) = true := hm.2
        simp at hbad
      · exact nodup_eraseDups (as.filter (fun b => !b == a))
termination_by l => l.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem not_mem_erase_self_of_nodup : ∀ (l : List Nat), l.Nodup → ∀ a, a ∉ l.erase a := by
  intro l
  induction l with
  | nil => simp [List.erase]
  | cons b bs ih =>
      intro hn a
      rw [List.nodup_cons] at hn
      by_cases hba : b = a
      · subst b
        simp [List.erase_cons_head, hn.1]
      · have hne : ¬(b == a) = true := by simp [hba]
        rw [List.erase_cons_tail hne]
        have hab' : ¬a = b := fun h => hba h.symm
        simp [List.mem_cons, hab', ih hn.2]

private theorem sum_perm {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons x p ih => simp [ih]
  | swap x y l => simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem perm_erase_of_mem {l : List Nat} {x : Nat} (hx : x ∈ l) :
    l.Perm (x :: l.erase x) := by
  induction l with
  | nil => cases hx
  | cons a as ih =>
      by_cases hax : a == x
      · have hax' : a = x := eq_of_beq hax
        subst x
        simp [List.erase_cons_head]
      · rw [List.erase_cons_tail hax]
        have hxas : x ∈ as := by
          rw [List.mem_cons] at hx
          rcases hx with h | h
          · have : (a == x) = true := by rw [h]; simp
            exact False.elim (hax this)
          · exact h
        exact (List.Perm.cons a (ih hxas)).trans (List.Perm.swap x a (as.erase x))

private theorem perm_of_nodup_mem_iff : ∀ n (l₁ l₂ : List Nat), l₁.length ≤ n →
    l₁.Nodup → l₂.Nodup → (∀ k, k ∈ l₁ ↔ k ∈ l₂) → l₁.Perm l₂ := by
  intro n
  induction n with
  | zero =>
      intro l₁ l₂ hle h1 h2 hmem
      have : l₁ = [] := by cases l₁ <;> simp at hle ⊢
      subst l₁
      cases l₂ with
      | nil => exact .nil
      | cons b bs =>
          have : b ∈ ([] : List Nat) := (hmem b).2 (by simp)
          simp at this
  | succ n ih =>
      intro l₁ l₂ hle h1 h2 hmem
      cases l₁ with
      | nil =>
          cases l₂ with
          | nil => exact .nil
          | cons b bs =>
              have : b ∈ ([] : List Nat) := (hmem b).2 (by simp)
              simp at this
      | cons a as =>
          cases l₂ with
          | nil =>
              have : a ∈ ([] : List Nat) := (hmem a).1 (by simp)
              simp at this
          | cons b bs =>
              rw [List.nodup_cons] at h1 h2
              by_cases hab : a = b
              · subst b
                apply List.Perm.cons a
                have hle' : as.length ≤ n := by
                  have hle'' : as.length + 1 ≤ n + 1 := by simpa using hle
                  omega
                apply ih as bs hle' h1.2 h2.2
                intro k
                have hk := hmem k
                simp only [List.mem_cons] at hk
                constructor
                · intro hk_as
                  rcases hk.1 (Or.inr hk_as) with hka | hkbs
                  · subst k
                    exact False.elim (h1.1 hk_as)
                  · exact hkbs
                · intro hk_bs
                  rcases hk.2 (Or.inr hk_bs) with hkb | hkas
                  · subst k
                    exact False.elim (h2.1 hk_bs)
                  · exact hkas
              · have ha_in_bs : a ∈ bs := by
                  have ha_mem : a ∈ a :: as := by simp
                  have ha_or := (hmem a).1 ha_mem
                  simp only [List.mem_cons] at ha_or
                  rcases ha_or with h | h
                  · exact False.elim (hab h)
                  · exact h
                have hb_in_as : b ∈ as := by
                  have hb_mem : b ∈ b :: bs := by simp
                  have hb_or := (hmem b).2 hb_mem
                  simp only [List.mem_cons] at hb_or
                  rcases hb_or with h | h
                  · exact False.elim (hab h.symm)
                  · exact h
                have p₁ : (a :: as).Perm (a :: b :: as.erase b) :=
                  List.Perm.cons a (perm_erase_of_mem hb_in_as)
                have p₂ : (b :: bs).Perm (b :: a :: bs.erase a) :=
                  List.Perm.cons b (perm_erase_of_mem ha_in_bs)
                have hp_rest : (as.erase b).Perm (bs.erase a) := by
                  apply ih (as.erase b) (bs.erase a)
                  · have hlen : (as.erase b).length ≤ as.length := List.length_erase_le
                    have hle_as : as.length ≤ n := by
                      have hle'' : as.length + 1 ≤ n + 1 := by simpa using hle
                      omega
                    omega
                  · exact h1.2.erase b
                  · exact h2.2.erase a
                  · intro k
                    by_cases hka : k = a
                    · subst k
                      constructor
                      · intro hk
                        exact False.elim (h1.1 (List.mem_of_mem_erase hk))
                      · intro hk
                        exact False.elim (not_mem_erase_self_of_nodup bs h2.2 a hk)
                    · by_cases hkb : k = b
                      · subst k
                        constructor
                        · intro hk
                          exact False.elim (not_mem_erase_self_of_nodup as h1.2 b hk)
                        · intro hk
                          exact False.elim (h2.1 (List.mem_of_mem_erase hk))
                      · rw [List.mem_erase_of_ne hkb, List.mem_erase_of_ne hka]
                        have hk := hmem k
                        simp only [List.mem_cons] at hk
                        constructor
                        · intro hk_as
                          rcases hk.1 (Or.inr hk_as) with hkb' | hkbs
                          · exact False.elim (hkb hkb')
                          · exact hkbs
                        · intro hk_bs
                          rcases hk.2 (Or.inr hk_bs) with hka' | hkas
                          · exact False.elim (hka hka')
                          · exact hkas
                exact (p₁.trans (List.Perm.swap a b (as.erase b)).symm).trans
                  ((List.Perm.cons b (List.Perm.cons a hp_rest)).trans p₂.symm)

private theorem eraseDups_cons_mem_sum (ks : List Nat) (x : Nat) (hx : x ∈ ks) (G : Nat → Nat) :
    (ks.eraseDups.map G).sum =
      G x + (((ks.filter (fun b => !b == x)).eraseDups.map G).sum) := by
  have hp : ks.eraseDups.Perm (x :: (ks.filter (fun b => !b == x)).eraseDups) := by
    apply perm_of_nodup_mem_iff ks.eraseDups.length
    · omega
    · exact nodup_eraseDups ks
    · rw [List.nodup_cons]
      constructor
      · intro hm
        rw [List.mem_eraseDups] at hm
        rw [List.mem_filter] at hm
        have hbad : (!x == x) = true := hm.2
        simp at hbad
      · exact nodup_eraseDups _
    · intro k
      simp only [List.mem_cons, List.mem_eraseDups, List.mem_filter]
      constructor
      · intro hk
        by_cases hkx : k == x
        · exact Or.inl (eq_of_beq hkx)
        · exact Or.inr ⟨hk, by simpa using hkx⟩
      · intro hk
        rcases hk with hkx | hk
        · subst k
          exact hx
        · exact hk.1
  have hs := sum_perm (hp.map G)
  simpa using hs

-- A list can be re-summed over its distinct keys with the key multiplicity as coefficient.
private theorem sum_map_eq_eraseDups_count : ∀ (l : List Nat) (f : Nat → Nat),
    (l.map f).sum = ((l.eraseDups.map (fun k => l.count k * f k)).sum) := by
  intro l
  induction l with
  | nil => intro f; simp
  | cons a as ih =>
      intro f
      have hte : ∀ k ∈ as.filter (fun b => !b == a), (a :: as).count k = as.count k := by
        intro k hk
        rw [List.mem_filter] at hk
        have hka : k ≠ a := by
          intro h
          subst k
          simp at hk
        have hak : a ≠ k := fun h => hka h.symm
        simp [hak]
      have hrest :
          (((as.filter (fun b => !b == a)).eraseDups.map (fun k => (a :: as).count k * f k)).sum) =
            (((as.filter (fun b => !b == a)).eraseDups.map (fun k => as.count k * f k)).sum) := by
        congr 1
        apply List.map_congr_left
        intro k hk
        rw [List.mem_eraseDups] at hk
        rw [hte k hk]
      have hhead : (a :: as).count a * f a = f a + as.count a * f a := by
        simp [Nat.succ_mul, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
      rw [List.eraseDups_cons]
      change f a + (as.map f).sum =
        (a :: as).count a * f a +
          (((as.filter (fun b => !b == a)).eraseDups.map (fun k => (a :: as).count k * f k)).sum)
      rw [hrest, hhead]
      by_cases ha : a ∈ as
      · have hsum := eraseDups_cons_mem_sum as a ha (fun k => as.count k * f k)
        have hsum' : as.count a * f a +
            (((as.filter (fun b => !b == a)).eraseDups.map (fun k => as.count k * f k)).sum) =
            (as.eraseDups.map (fun k => as.count k * f k)).sum := by
          simpa using hsum.symm
        rw [ih f]
        rw [Nat.add_assoc, hsum']
      · have hfilter : as.filter (fun b => !b == a) = as := by
          apply List.filter_eq_self.mpr
          intro b hb
          have hba : b ≠ a := by
            intro h
            subst b
            exact ha hb
          simp [hba]
        have hzero : as.count a * f a = 0 := by
          simp [List.count_eq_zero_of_not_mem ha]
        rw [ih f, hfilter, hzero]
        simp [Nat.add_assoc]

private theorem beq_comm_nat (a b : Nat) : (a == b) = (b == a) := by
  by_cases h : a = b
  · subst b
    rfl
  · have h' : b ≠ a := by intro hba; exact h hba.symm
    rw [beq_eq_false_iff_ne.mpr h, beq_eq_false_iff_ne.mpr h']

private theorem count_filter_beq (ys : List Nat) (x k : Nat) :
    (ys.filter (fun y => y == x)).count k = (k == x).toNat * ys.count k := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases hyx : (y == x) = true
      · have hy_eq_x : y = x := eq_of_beq hyx
        subst y
        rw [show (List.filter (fun z : Nat => z == x) (x :: ys)) =
            x :: (ys.filter (fun z : Nat => z == x)) by simp]
        rw [List.count_cons, ih, List.count_cons]
        cases hkx : (k == x) <;>
          simp [hkx, beq_comm_nat x k, Nat.add_mul, Nat.add_comm, Nat.add_left_comm]
      · rw [show (List.filter (fun z : Nat => z == x) (y :: ys)) =
            ys.filter (fun z : Nat => z == x) by simp [hyx]]
        rw [ih, List.count_cons]
        by_cases hkx : (k == x) = true
        · have hyk_ne : y ≠ k := by
            intro hyk_eq
            have hy_eq_x : y = x := hyk_eq.trans (eq_of_beq hkx)
            exact hyx (by simp [hy_eq_x])
          simp [hkx, beq_comm_nat x k, Nat.mul_add, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc, hyk_ne]
        · simp [hkx, beq_comm_nat x k, Nat.mul_add, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

private theorem count_expand_eq_mass (xs ys : List Nat) (k : Nat) :
    (bag.expand (xs, ys)).count k = bag.mass (xs, ys) k := by
  induction xs with
  | nil => simp [bag]
  | cons x xs ih =>
      dsimp [bag] at ih ⊢
      rw [List.flatMap_cons, List.count_append, ih, List.count_cons]
      rw [count_filter_beq]
      rw [beq_comm_nat x k]
      cases h : (k == x) <;> simp [h, Nat.add_mul, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

private theorem mem_expand_subset (xs ys : List Nat) {k : Nat}
    (h : k ∈ bag.expand (xs, ys)) : k ∈ xs := by
  induction xs with
  | nil => simp [bag] at h
  | cons x xs ih =>
      change k ∈ (ys.filter (fun y : Nat => y == x)) ++
          xs.flatMap (fun x => ys.filter (fun y : Nat => y == x)) at h
      rw [List.mem_append] at h
      rcases h with h | h
      · have hx : k = x := eq_of_beq ((List.mem_filter.mp h).2)
        simp [hx]
      · exact List.mem_cons_of_mem x (ih h)

private theorem sum_filter_mem (ks : List Nat) (l : List Nat) (f : Nat → Nat) :
    (ks.map (fun k => l.count k * f k)).sum =
      ((ks.filter (fun k => k ∈ l)).map (fun k => l.count k * f k)).sum := by
  induction ks with
  | nil => simp
  | cons a as ih =>
      by_cases h : a ∈ l
      · simp [h, ih]
      · have h0 : l.count a = 0 := List.count_eq_zero_of_not_mem h
        simp [h, h0, ih]

private theorem perm_support (ks l : List Nat) (hks : ks.Nodup)
    (hsub : ∀ k, k ∈ l → k ∈ ks) :
    (ks.filter (fun k => k ∈ l)).Perm l.eraseDups := by
  apply perm_of_nodup_mem_iff ks.length
  · have hle : (ks.filter (fun k => k ∈ l)).length ≤ ks.length :=
      List.length_filter_le _ _
    omega
  · exact hks.filter _
  · exact nodup_eraseDups l
  · intro k
    rw [List.mem_filter, List.mem_eraseDups]
    constructor
    · intro hk
      simpa using (hk.2 : decide (k ∈ l) = true)
    · intro hk
      exact ⟨hsub k hk, by simp [hk]⟩

-- The histogram of distinct keys agrees with summing the function over the expanded bag.
private theorem weighted_flat (p : List Nat × List Nat) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  rcases p with ⟨xs, ys⟩
  unfold weighted
  change ((xs.eraseDups.map (fun k => bag.mass (xs, ys) k * f k)).sum) =
    ((bag.expand (xs, ys)).map f).sum
  have hmass : ∀ k, bag.mass (xs, ys) k = (bag.expand (xs, ys)).count k := by
    intro k
    exact (count_expand_eq_mass xs ys k).symm
  simp only [hmass]
  rw [sum_map_eq_eraseDups_count (bag.expand (xs, ys)) f]
  rw [sum_filter_mem xs.eraseDups (bag.expand (xs, ys)) f]
  have hper : (xs.eraseDups.filter (fun k => k ∈ bag.expand (xs, ys))).Perm
      (bag.expand (xs, ys)).eraseDups := by
    apply perm_support
    · exact nodup_eraseDups xs
    · intro k hk
      rw [List.mem_eraseDups]
      exact mem_expand_subset xs ys hk
  exact sum_perm (hper.map (fun k => (bag.expand (xs, ys)).count k * f k))

private theorem bool_sum_filter (ys : List Nat) (P : Nat → Bool) :
    ((ys.filter P).map (fun y => (P y).toNat)).sum = (ys.filter P).length := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases h : P y
      · simp [h, ih, Nat.add_comm]
      · simp [h, ih]

private theorem sum_bool_toNat_filter (ys : List Nat) (P : Nat → Bool) :
    (ys.map (fun y => (P y).toNat)).sum = ((ys.filter P).map (fun y => (P y).toNat)).sum := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases h : P y
      · simp [h, ih]
      · simp [h, ih]

private theorem length_pairs (xs ys : List Nat) (P : Nat → Nat → Bool) :
    (pairs xs ys P).length =
      (xs.map (fun x => ((ys.filter (P x)).map (fun y => (P x y).toNat)).sum)).sum := by
  induction xs with
  | nil => simp [pairs]
  | cons x xs ih =>
      simp [pairs, ih, bool_sum_filter, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c s
  let P : Nat → Nat → Bool := fun x y => x ≤ y && y ≤ x + s
  have hleft :
      weighted bag a (fun x => weighted bag c (fun y => (decide (x ≤ y ∧ y ≤ x + s)).toNat)) =
        ((bag.expand a).map (fun x =>
          (((bag.expand c).filter (P x)).map (fun y => (P x y).toNat)).sum)).sum := by
    rw [weighted_flat]
    congr 1
    apply List.map_congr_left
    intro x hx
    rw [weighted_flat]
    have hinner :
        ((bag.expand c).map (fun y => (decide (x ≤ y ∧ y ≤ x + s)).toNat)).sum =
          ((bag.expand c).map (fun y => (P x y).toNat)).sum := by
      simp [P]
    rw [hinner]
    exact sum_bool_toNat_filter (bag.expand c) (P x)
  rw [hleft, length_pairs]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_JoinFanout
