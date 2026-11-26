import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_SkipStrictlyHigherRankPrefix

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
# SkipStrictlyHigherRankPrefix

All groups with a count strictly above the cutoff sort before the remaining
groups. If their number fits inside the page offset, the same page is obtained
by dropping the shorter prefix from the filtered lower-count groups.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

private def insSorted {α : Type} (le : α → α → Bool) (a : α) : List α → List α
  | [] => [a]
  | x :: xs => if le a x = true then a :: x :: xs else x :: insSorted le a xs

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.SkipStrictlyHigherRankPrefix.Goal := by
  intro groups cut hhi hcmp hhiLe
  let hiCount : Nat := hhi
  have hhiCount : hiCount ≤ 1000 := by simpa [hiCount] using hhiLe
  let cnt : Nat × Nat → Nat := fun r => r.2
  let le : Nat × Nat → Nat × Nat → Bool := fun a b => decide (cnt b ≤ cnt a)
  let hiPred : Nat × Nat → Bool := fun r => decide (cut < cnt r)
  let loPred : Nat × Nat → Bool := fun r => decide (cnt r ≤ cut)
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
          intro l2 hl1 hl2'
          cases l2 with
          | nil => simp [insSorted]
          | cons z zs =>
              have hz := hl2' z (by simp)
              simp [insSorted, hz]
      | cons z zs ih =>
          intro l2 hl1 hl2'
          have hznot : le a z = false := by
            have hz := hl1 z (by simp)
            cases h : le a z <;> simp [h] at hz ⊢
          have htail : ∀ y ∈ zs, (!le a y) = true := by
            intro y hy
            exact hl1 y (by simp [hy])
          simp [insSorted, hznot, ih l2 htail hl2']
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
  have hins_append_false_prefix : ∀ (p q : List (Nat × Nat)) (a : Nat × Nat),
      (∀ x ∈ p, le a x = false) → insSorted le a (p ++ q) = p ++ insSorted le a q := by
    intro p
    induction p with
    | nil =>
        intro q a hp
        simp
    | cons x xs ih =>
        intro q a hp
        have hx := hp x (by simp)
        have htail : ∀ y ∈ xs, le a y = false := by
          intro y hy
          exact hp y (by simp [hy])
        simp [insSorted, hx, ih q a htail]
  have hsorted_eq : ∀ xs : List (Nat × Nat),
      xs.mergeSort le = (xs.filter hiPred).mergeSort le ++ (xs.filter loPred).mergeSort le := by
    intro xs
    induction xs with
    | nil => simp
    | cons a as ih =>
        rw [hcons a as, ih]
        by_cases hahi : cut < cnt a
        · have halow : ¬ cnt a ≤ cut := by omega
          have hlo_true : ∀ z ∈ as.filter loPred, le a z = true := by
            intro z hz
            have hzlp := (List.mem_filter.mp hz).2
            have hzle : cnt z ≤ cut := by simpa [loPred] using hzlp
            have hle : cnt z ≤ cnt a := by omega
            simp [le, hle]
          have hhp_a : hiPred a = true := by simp [hiPred, hahi]
          have hlp_a : loPred a = false := by simp [loPred, halow]
          simp only [List.filter_cons]
          rw [hhp_a, hlp_a]
          simp
          rw [hcons a (as.filter hiPred)]
          rw [hins_append_true _ ((as.filter loPred).mergeSort le) a (by
            intro z hz
            have hzmem : z ∈ as.filter loPred := (List.mergeSort_perm _ le).mem_iff.mp hz
            exact hlo_true z hzmem)]
        · have hahi' : ¬ cut < cnt a := hahi
          have halow : cnt a ≤ cut := by omega
          have hhi_false : ∀ x ∈ (as.filter hiPred).mergeSort le, le a x = false := by
            intro x hx
            have hxmem : x ∈ as.filter hiPred := (List.mergeSort_perm _ le).mem_iff.mp hx
            have hxhp := (List.mem_filter.mp hxmem).2
            have hxgt : cut < cnt x := by simpa [hiPred] using hxhp
            have hnotle : ¬ cnt x ≤ cnt a := by omega
            simp [le, hnotle]
          have hhp_a : hiPred a = false := by simp [hiPred, hahi']
          have hlp_a : loPred a = true := by simp [loPred, halow]
          simp only [List.filter_cons]
          rw [hhp_a, hlp_a]
          simp
          rw [hcons a (as.filter loPred)]
          rw [hins_append_false_prefix _ _ a hhi_false]
  let hiList := groups.filter hiPred
  let loList := groups.filter loPred
  have hmain := hsorted_eq groups
  change ((groups.mergeSort le).drop 1000).take 10 =
    ((loList.mergeSort le).drop (1000 - hiCount)).take 10
  rw [hmain]
  rw [List.drop_append]
  have hlenperm := (List.mergeSort_perm hiList le).length_eq
  have hilen : hiList.length = hiCount := by
    change (groups.filter hiPred).length = hhi
    dsimp [hiPred, cnt, hhi, countWhere]
  have hdrop : (hiList.mergeSort le).drop 1000 = [] := by
    rw [List.drop_eq_nil_iff]
    rw [hlenperm, hilen]
    exact hhiCount
  rw [hdrop]
  simp only [List.nil_append]
  change ((loList.mergeSort le).drop (1000 - (hiList.mergeSort le).length)).take 10 =
    ((loList.mergeSort le).drop (1000 - hiCount)).take 10
  rw [hlenperm, hilen]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_SkipStrictlyHigherRankPrefix
