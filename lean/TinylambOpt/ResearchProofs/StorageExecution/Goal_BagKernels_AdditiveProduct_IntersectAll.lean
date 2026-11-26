import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `AdditiveProduct.IntersectAll` — Intersect two bags by the minimum multiplicity at
  each key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_IntersectAll

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct

/-! ## Generic list lemmas -/

private theorem sum_map_eq_count_head {α : Type} [BEq α] [LawfulBEq α]
    (k : α) (ks : List α) (f : α → Nat) :
    (ks.map f).sum = ks.count k * f k + ((ks.filter (fun b => !(b == k))).map f).sum := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxeq : x = k := beq_iff_eq.mp hx
        subst x
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm]
      · have hkx : (k == x) = false := Bool.eq_false_of_ne_true (by
          intro h
          exact hx (beq_iff_eq.mpr (beq_iff_eq.mp h).symm))
        simp [List.count_cons, hx, ih, Nat.add_left_comm]

private theorem dedup_weight_sum (f : Nat → Nat) (keys : List Nat) :
    ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum := by
  let motive : Nat → Prop := fun n => ∀ keys : List Nat, keys.length = n →
      ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum
  have H : motive keys.length := by
    apply Nat.strongRecOn
    intro n ih keys hlen
    cases keys with
    | nil => simp
    | cons k ks =>
        let rest := ks.filter (fun b => !(b == k))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : Nat => !(b == k)) ks)
            (Nat.lt_succ_self _)
        have ihrest := ih rest.length hrestlen rest rfl
        rw [List.eraseDups_cons]
        simp only [List.map_cons, List.sum_cons]
        have hhead : (List.count k (k :: ks)) * f k = f k + ks.count k * f k := by
          rw [List.count_cons]
          simp [Nat.add_mul, Nat.add_comm]
        rw [hhead]
        have hrest_count : ∀ a ∈ rest.eraseDups, (List.count a (k :: ks)) = rest.count a := by
          intro a ha
          have hane : (a == k) = false := by
            apply Bool.eq_false_of_ne_true
            intro h
            have hamem : a ∈ rest := List.mem_eraseDups.mp ha
            have hamem' : a ∈ ks.filter (fun b : Nat => !(b == k)) := by simpa [rest] using hamem
            have hff := (List.mem_filter.mp hamem').2
            simp [h] at hff
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hka : (k == a) = false := by
            apply Bool.eq_false_of_ne_true
            intro ht
            have hkea : k = a := beq_iff_eq.mp ht
            have htrue : (a == k) = true := beq_iff_eq.mpr hkea.symm
            rw [hane] at htrue
            cases htrue
          rw [List.count_cons, hka, hrestc]
          simp
        rw [show ((rest.eraseDups.map fun a => List.count a (k :: ks) * f a).sum) =
             ((rest.eraseDups.map fun a => rest.count a * f a).sum) by
             apply congrArg List.sum
             apply List.map_congr_left
             intro a ha
             rw [hrest_count a ha]]
        rw [ihrest]
        have hks : (ks.map f).sum = ks.count k * f k + (rest.map f).sum := by
          simpa [rest] using sum_map_eq_count_head k ks f
        rw [hks]
        omega
  exact H keys rfl

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have h : motive l.length := Nat.strongRecOn l.length (fun n ih l hl => by
    cases l with
    | nil => simp [List.eraseDups_nil]
    | cons b bs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro hb
          rw [List.mem_eraseDups] at hb
          have hb' := (List.mem_filter.mp hb).2
          simp at hb'
        · apply ih (bs.filter (fun x => !x == b)).length
          · have hle := List.length_filter_le (fun x : α => !x == b) bs
            simp only [List.length_cons] at hl
            omega
          · rfl)
  exact h l rfl

private theorem count_flatMap {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (g : α → List α) (a : α) :
    (xs.flatMap g).count a = (xs.map fun x => (g x).count a).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.count_append, ih]

private theorem count_add_left (x : Nat) : ∀ (b : List Nat) (k : Nat),
    (b.map fun y => x + y).count k = if x ≤ k then b.count (k - x) else 0 := by
  intro b
  induction b with
  | nil => intro k; simp
  | cons y ys ih =>
      intro k
      by_cases hle : x ≤ k
      · by_cases hy : y = k - x
        · have heq : x + y = k := by omega
          have hbeq1 : (x + y == k) = true := beq_iff_eq.mpr heq
          have hbeq2 : (y == k - x) = true := beq_iff_eq.mpr hy
          simp [List.count_cons, hbeq1, hbeq2, ih, hle]
        · have hybeq : (y == k - x) = false := by simpa [beq_iff_eq] using hy
          have hxyne : x + y ≠ k := by
            intro he
            apply hy
            omega
          have hbeq1 : (x + y == k) = false := by simpa [beq_iff_eq] using hxyne
          simp [List.count_cons, hbeq1, hybeq, ih, hle]
      · have heqfalse : x + y ≠ k := by omega
        have hbeq1 : (x + y == k) = false := by simpa [beq_iff_eq] using heqfalse
        simp [List.count_cons, hbeq1, ih, hle]

private theorem count_flatMap_replicate (xs : List Nat) (f : Nat → Nat) (k : Nat) :
    (xs.flatMap (fun x => List.replicate (f x) x)).count k =
      (xs.map (fun x => if x = k then f x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, List.count_replicate, beq_iff_eq]

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

/-! ## AdditiveProduct bag facts -/

private theorem mass_eq_sum (a b : List Nat) (k : Nat) :
    bag.mass (a, b) k = (a.map fun x => if x ≤ k then b.count (k - x) else 0).sum := by
  change (a.eraseDups.map fun x => if x ≤ k then a.count x * b.count (k - x) else 0).sum =
    (a.map fun x => if x ≤ k then b.count (k - x) else 0).sum
  rw [show (a.eraseDups.map fun x => if x ≤ k then a.count x * b.count (k - x) else 0) =
       (a.eraseDups.map fun x => a.count x * (if x ≤ k then b.count (k - x) else 0)) by
       apply List.map_congr_left
       intro x hx
       by_cases hle : x ≤ k <;> simp [hle]]
  rw [dedup_weight_sum]

private theorem count_expand_eq_mass (a b : List Nat) (k : Nat) :
    (bag.expand (a, b)).count k = bag.mass (a, b) k := by
  rw [show bag.expand (a, b) = a.flatMap (fun x => b.map (fun y => x + y)) by rfl]
  rw [count_flatMap]
  rw [mass_eq_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  rw [count_add_left]

private theorem mem_keys_of_mem_expand (a b : List Nat) (k : Nat) :
    k ∈ bag.expand (a, b) → k ∈ bag.keys (a, b) := by
  intro h
  rw [show bag.expand (a, b) = a.flatMap (fun x => b.map (fun y => x + y)) by rfl] at h
  rw [List.mem_flatMap] at h
  rcases h with ⟨x, hx, hrest⟩
  rw [List.mem_map] at hrest
  rcases hrest with ⟨y, hy, hxy⟩
  rw [show bag.keys (a, b) = a.eraseDups.flatMap (fun x => b.eraseDups.map (fun y => x + y)) by rfl]
  rw [List.mem_flatMap]
  refine ⟨x, ?_, ?_⟩
  · rw [List.mem_eraseDups]
    exact hx
  · rw [List.mem_map]
    refine ⟨y, ?_, ?_⟩
    · rw [List.mem_eraseDups]
      exact hy
    · exact hxy

private theorem nodup_live (a b : List Nat) : (live bag (a, b)).Nodup := by
  unfold live
  exact List.Sublist.nodup List.filter_sublist (eraseDups_nodup (bag.keys (a, b)))

private theorem mem_live_of_mass_pos (a b : List Nat) (k : Nat)
    (h : 0 < bag.mass (a, b) k) : k ∈ live bag (a, b) := by
  have hc : 0 < (bag.expand (a, b)).count k := by
    rw [count_expand_eq_mass]
    exact h
  have hmem : k ∈ bag.expand (a, b) := List.count_pos_iff.mp hc
  have hkeys : k ∈ bag.keys (a, b) := mem_keys_of_mem_expand a b k hmem
  unfold live
  rw [List.mem_filter]
  constructor
  · rw [List.mem_eraseDups]
    exact hkeys
  · simp [h]

/-! ## Goal -/

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  obtain ⟨a₁, a₂⟩ := a
  obtain ⟨c₁, c₂⟩ := c
  rw [count_flatMap_replicate]
  rw [sum_eq_single_of_nodup (live bag (a₁, a₂)) (nodup_live a₁ a₂)]
  rw [count_expand_eq_mass, count_expand_eq_mass]
  by_cases hk : k ∈ live bag (a₁, a₂)
  · simp [hk]
  · have hm0 : bag.mass (a₁, a₂) k = 0 := by
      by_cases hz : bag.mass (a₁, a₂) k = 0
      · exact hz
      · have hpos : 0 < bag.mass (a₁, a₂) k := Nat.pos_of_ne_zero hz
        exact False.elim (hk (mem_live_of_mass_pos a₁ a₂ k hpos))
    simp [hk, hm0]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_IntersectAll
