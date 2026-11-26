import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_SingletonCompletionAfterDuplicateDetection

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
# SingletonCompletionAfterDuplicateDetection

Repeated keys outrank singleton keys. Stable count-descending sorting therefore
places the complete heavy-key prefix first and fills the remaining quota with
singletons in dictionary order, so duplicate detection can be done before the
final top-ten fill.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false


private def insSorted {α : Type} (le : α → α → Bool) (a : α) : List α → List α
  | [] => [a]
  | x :: xs => if le a x = true then a :: x :: xs else x :: insSorted le a xs

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.SingletonCompletionAfterDuplicateDetection.Goal := by
  intro rs
  dsimp
  let cnt : Nat × Nat → Nat := fun k => rs.count k
  let le : Nat × Nat → Nat × Nat → Bool := fun a b => decide (cnt b ≤ cnt a)
  let heavyPred : Nat × Nat → Bool := fun k => decide (1 < cnt k)
  let singlePred : Nat × Nat → Bool := fun k => decide (cnt k == 1)
  have htrans : ∀ a b c : Nat × Nat, le a b = true → le b c = true → le a c = true := by
    intro a b c hab hbc
    simp [le] at hab hbc ⊢
    omega
  have htotal : ∀ a b : Nat × Nat, (le a b || le b a) = true := by
    intro a b
    simp [le]
    omega
  have hcons : ∀ a : Nat × Nat, ∀ xs : List (Nat × Nat),
      (a :: xs).mergeSort le = insSorted le a (xs.mergeSort le) := by
    intro a xs
    obtain ⟨l1, l2, hfull, hsplit, hl1⟩ := List.mergeSort_cons htrans htotal a xs
    rw [hfull, hsplit]
    have hsorted : (l1 ++ a :: l2).Pairwise (fun x y => le x y = true) := by
      rw [← hfull]
      exact List.pairwise_mergeSort htrans htotal (a :: xs)
    have hl2_all : ∀ (p q : List (Nat × Nat)),
        (p ++ a :: q).Pairwise (fun x y => le x y = true) →
        ∀ z ∈ q, le a z = true := by
      intro p
      induction p with
      | nil =>
          intro q hs z hz
          exact (List.pairwise_cons.mp hs).1 z hz
      | cons x xs ih =>
          intro q hs z hz
          exact ih q ((List.pairwise_cons.mp hs).2) z hz
    have hl2 : ∀ z ∈ l2, le a z = true := hl2_all l1 l2 hsorted
    have hins : ∀ (l1 l2 : List (Nat × Nat)),
        (∀ z ∈ l1, (!le a z) = true) → (∀ z ∈ l2, le a z = true) →
        insSorted le a (l1 ++ l2) = l1 ++ a :: l2 := by
      intro l1
      induction l1 with
      | nil =>
          intro l2 hl1 hl2
          cases l2 with
          | nil => simp [insSorted]
          | cons z zs =>
              have hz := hl2 z (by simp)
              simp [insSorted, hz]
      | cons z zs ih =>
          intro l2 hl1 hl2
          have hznot : le a z = false := by
            have hz := hl1 z (by simp)
            cases h : le a z <;> simp [h] at hz ⊢
          have htail : ∀ y ∈ zs, (!le a y) = true := by
            intro y hy
            exact hl1 y (by simp [hy])
          simp [insSorted, hznot, ih l2 htail hl2]
    exact (hins l1 l2 hl1 hl2).symm
  have hins_append_true : ∀ (p q : List (Nat × Nat)) (a : Nat × Nat),
      (∀ z ∈ q, le a z = true) → insSorted le a (p ++ q) = insSorted le a p ++ q := by
    intro p
    induction p with
    | nil =>
        intro q a hq
        cases q with
        | nil => simp [insSorted]
        | cons z zs =>
            have hz := hq z (by simp)
            simp [insSorted, hz]
    | cons x xs ih =>
        intro q a hq
        by_cases hx : le a x = true
        · simp [insSorted, hx]
        · simp [insSorted, hx, ih q a hq]
  have hins_append_mid : ∀ (p q : List (Nat × Nat)) (a : Nat × Nat),
      (∀ x ∈ p, le a x = false) → (∀ z ∈ q, le a z = true) →
      insSorted le a (p ++ q) = p ++ a :: q := by
    intro p
    induction p with
    | nil =>
        intro q a hp hq
        cases q with
        | nil => simp [insSorted]
        | cons z zs =>
            have hz := hq z (by simp)
            simp [insSorted, hz]
    | cons x xs ih =>
        intro q a hp hq
        have hx := hp x (by simp)
        have htail : ∀ y ∈ xs, le a y = false := by
          intro y hy
          exact hp y (by simp [hy])
        simp [insSorted, hx, ih q a htail hq]
  have hcnt_pos : ∀ k ∈ rs.eraseDups, 1 ≤ cnt k := by
    intro k hk
    have hkrs : k ∈ rs := List.mem_eraseDups.mp hk
    have hne : cnt k ≠ 0 := by
      intro hzero
      have hnz : ¬ k ∈ rs := (List.count_eq_zero.mp hzero)
      exact hnz hkrs
    omega
  have hsorted_eq : ∀ xs : List (Nat × Nat),
      (∀ k ∈ xs, 1 ≤ cnt k) →
      xs.mergeSort le = (xs.filter heavyPred).mergeSort le ++ xs.filter singlePred := by
    intro xs
    induction xs with
    | nil => simp
    | cons a as ih =>
        intro hpos
        have hapos : 1 ≤ cnt a := hpos a (by simp)
        have htailpos : ∀ k ∈ as, 1 ≤ cnt k := by
          intro k hk
          exact hpos k (by simp [hk])
        have ih' := ih htailpos
        rw [hcons a as, ih']
        by_cases haheavy : 1 < cnt a
        · have hasingle : (cnt a == 1) = false := by simp; omega
          have hsingle_true : ∀ z ∈ as.filter singlePred, le a z = true := by
            intro z hz
            have hzsp := (List.mem_filter.mp hz).2
            simp [singlePred] at hzsp
            have hz1 : cnt z = 1 := hzsp
            have hle : cnt z ≤ cnt a := by omega
            simp [le, hle]
          have hhp_a : heavyPred a = true := by simp [heavyPred, haheavy]
          have hsp_a : singlePred a = false := by
            simp [singlePred, hasingle]
            omega
          simp only [List.filter_cons]
          rw [hhp_a, hsp_a]
          simp
          rw [hcons a (as.filter heavyPred)]
          rw [hins_append_true _ _ a hsingle_true]
        · have hasingle : (cnt a == 1) = true := by
            simp
            omega
          have hheavy_false : ∀ x ∈ (as.filter heavyPred).mergeSort le, le a x = false := by
            intro x hx
            have hxmem : x ∈ as.filter heavyPred := (List.mergeSort_perm _ le).mem_iff.mp hx
            have hxhp := (List.mem_filter.mp hxmem).2
            simp [heavyPred] at hxhp
            have hxgt : 1 < cnt x := hxhp
            have hnotle : ¬ cnt x ≤ cnt a := by omega
            simp [le, hnotle]
          have hsingle_true : ∀ z ∈ as.filter singlePred, le a z = true := by
            intro z hz
            have hzsp := (List.mem_filter.mp hz).2
            simp [singlePred] at hzsp
            have hz1 : cnt z = 1 := hzsp
            have hle : cnt z ≤ cnt a := by omega
            simp [le, hle]
          have hhp_a : heavyPred a = false := by
            simp [heavyPred, haheavy]
          have hsp_a : singlePred a = true := by
            simp [singlePred, hasingle]
            omega
          simp only [List.filter_cons]
          rw [hhp_a, hsp_a]
          simp
          rw [hins_append_mid _ _ a hheavy_false hsingle_true]
  let heavy := rs.eraseDups.filter heavyPred
  let single := rs.eraseDups.filter singlePred
  have hmain := hsorted_eq rs.eraseDups hcnt_pos
  change ((rs.eraseDups.filter heavyPred).mergeSort le).take 10 ++
      (rs.eraseDups.filter singlePred).take (10 - (rs.eraseDups.filter heavyPred).length) =
    (rs.eraseDups.mergeSort le).take 10
  rw [hmain]
  rw [List.take_append]
  have hlenperm := (List.mergeSort_perm (rs.eraseDups.filter heavyPred) le).length_eq
  rw [hlenperm]
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_SingletonCompletionAfterDuplicateDetection
