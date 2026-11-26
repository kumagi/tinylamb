import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `MultiplicityRuns.JoinFanout` — Evaluate band join count from the two weighted
  supports.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_JoinFanout

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns

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

private theorem mass_cons (x n k : Nat) (rs : List (Nat × Nat)) :
    bag.mass ((x, n) :: rs) k = (if k == x then n else 0) + bag.mass rs k := by
  by_cases h : k == x
  · have hkx : k = x := eq_of_beq h
    subst k
    simp [bag]
  · have hkx : k ≠ x := by
      intro h'
      apply h
      simp [h']
    simp [bag, h, hkx, beq_eq_false_iff_ne, Ne.symm hkx]

private theorem mass_zero_of_not_mem (rs : List (Nat × Nat)) {x : Nat}
    (hx : x ∉ rs.map Prod.fst) : bag.mass rs x = 0 := by
  induction rs with
  | nil => simp [bag]
  | cons p rs ih =>
      rcases p with ⟨y, m⟩
      have hyx : y ≠ x := by
        intro h
        subst h
        exact hx (by simp)
      have hyx' : (y == x) = false := by simp [hyx]
      have hxrs : x ∉ rs.map Prod.fst := by
        intro h
        exact hx (by simp [h])
      have hfilter : List.filter (fun r => r.fst == x) ((y, m) :: rs) =
          List.filter (fun r => r.fst == x) rs := by
        simp [hyx']
      change bag.mass ((y, m) :: rs) x = 0
      rw [bag]
      simp [hfilter]
      change bag.mass rs x = 0
      exact ih hxrs

private theorem weighted_runs (rs : List (Nat × Nat)) (f : Nat → Nat) :
    weighted bag rs f = (rs.map (fun p => p.2 * f p.1)).sum := by
  induction rs with
  | nil => simp [weighted, bag]
  | cons p rs ih =>
      rcases p with ⟨x, n⟩
      let ks := rs.map Prod.fst
      let G := fun k => bag.mass rs k * f k
      have hweighted : weighted bag rs f = (ks.eraseDups.map G).sum := by
        simp [weighted, bag, ks, G]
      have hcons_mass : ∀ k, bag.mass ((x, n) :: rs) k =
          (if k == x then n else 0) + bag.mass rs k := fun k => mass_cons x n k rs
      have hte_mass : ∀ k ∈ (ks.filter (fun b => !b == x)), bag.mass ((x, n) :: rs) k = bag.mass rs k := by
        intro k hk
        rw [hcons_mass]
        rw [List.mem_filter] at hk
        have hkx : k ≠ x := by
          intro h
          subst k
          simp at hk
        simp [hkx]
      have hte_map : (((ks.filter (fun b => !b == x)).eraseDups.map
          (fun k => bag.mass ((x, n) :: rs) k * f k)).sum) =
          (((ks.filter (fun b => !b == x)).eraseDups.map G).sum) := by
        congr 1
        apply List.map_congr_left
        intro k hk
        rw [List.mem_eraseDups] at hk
        rw [hte_mass k hk]
      change (((x :: ks).eraseDups.map
          (fun k => bag.mass ((x, n) :: rs) k * f k)).sum) =
        (n * f x + (rs.map (fun p => p.2 * f p.1)).sum)
      rw [List.eraseDups_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [hcons_mass x, hte_map]
      simp only [beq_self_eq_true, ↓reduceIte]
      by_cases hx : x ∈ ks
      · have hsum := eraseDups_cons_mem_sum ks x hx G
        have hsum' :
            bag.mass rs x * f x +
                (List.map G (List.filter (fun b => !b == x) ks).eraseDups).sum =
              (List.map G ks.eraseDups).sum := by
          simpa [G] using hsum.symm
        rw [Nat.add_mul, Nat.add_assoc, hsum', ← hweighted, ih]
      · have hfilter : ks.filter (fun b => !b == x) = ks := by
          apply List.filter_eq_self.mpr
          intro b hb
          have hbx : b ≠ x := by
            intro h
            subst b
            exact hx hb
          simp [hbx]
        have hmassx : bag.mass rs x = 0 := mass_zero_of_not_mem rs (by simpa [ks] using hx)
        rw [hfilter, hmassx]
        simp
        rw [← hweighted, ih]

private theorem sum_replicate (n v : Nat) : (List.replicate n v).sum = n * v := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.sum_cons, ih, Nat.succ_mul, Nat.add_comm]

private theorem weighted_flat (rs : List (Nat × Nat)) (f : Nat → Nat) :
    weighted bag rs f = ((bag.expand rs).map f).sum := by
  rw [weighted_runs]
  induction rs with
  | nil => simp [bag]
  | cons p rs ih =>
      rcases p with ⟨x, n⟩
      simp [bag, List.map_append, List.sum_append, List.map_replicate, sum_replicate, ih]

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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_JoinFanout
