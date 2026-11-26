import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Any pair whose count reaches a certified threshold also reaches it in both
one-dimensional marginals. Restricting rows by that marginal screen therefore
keeps every pair that survives the final histogram threshold, with its count
unchanged; the two surviving histograms are equal.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.HierarchicalPairFrequencyScreen

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates



set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.QueryRewrites

namespace EncodedScanAggregatePairScreen

private theorem filter_filter_comm {α : Type} (l : List α) (p q : α → Bool) :
    (l.filter p).filter q = (l.filter q).filter p := by
  rw [List.filter_filter, List.filter_filter]
  apply List.filter_congr
  intro x hx
  cases hp : p x <;> cases hq : q x <;> simp [hp, hq, Bool.and_comm]

private theorem eraseDups_filter {α : Type} [BEq α] [LawfulBEq α] :
    ∀ l : List α, ∀ p : α → Bool, (l.filter p).eraseDups = l.eraseDups.filter p
  | [], p => rfl
  | a :: l, p => by
      by_cases hp : p a = true
      · rw [List.filter_cons_of_pos hp, List.eraseDups_cons]
        rw [List.eraseDups_cons]
        rw [List.filter_cons_of_pos hp]
        simp only [List.cons.injEq]
        rw [filter_filter_comm]
        exact ⟨trivial, eraseDups_filter (l.filter (fun x => !x == a)) p⟩
      · have hp_false : p a = false := by simpa using hp
        have hp_of_eq : ∀ x, (x == a) = true → p x = false := by
          intro x hxa
          have hxeq : x = a := beq_iff_eq.mp hxa
          simp [hxeq, hp_false]
        have hf : (l.filter p).filter (fun x => !x == a) = l.filter p := by
          apply List.filter_eq_self.mpr
          intro x hx
          have hxp : p x = true := (List.mem_filter.mp hx).2
          have hxa_false : (x == a) = false := by
            by_cases hxa : x == a
            · have hfalse : p x = false := hp_of_eq x hxa
              simp [hfalse] at hxp
            · simpa using hxa
          simp [hxa_false]
        rw [List.eraseDups_cons]
        simp only [List.filter_cons_of_neg hp]
        have hrec := eraseDups_filter (l.filter (fun x => !x == a)) p
        rw [← hrec, filter_filter_comm, hf]
termination_by l => l.length
decreasing_by
  · have hle := List.length_filter_le (fun x => !x == a) l
    simp
    omega
  · have hle := List.length_filter_le (fun x => !x == a) l
    simp
    omega

private theorem count_filter_of_pred_true {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (p : α → Bool) (x : α) (h : p x = true) :
    (xs.filter p).count x = xs.count x := by
  induction xs with
  | nil => simp
  | cons y ys ih =>
      by_cases hyx : y == x
      · have heq : y = x := beq_iff_eq.mp hyx
        have hpy : p y = true := by simp [heq, h]
        simp [List.count_cons, hpy, hyx, ih]
      · by_cases hpy : p y
        · simp [List.count_cons, hpy, hyx, ih]
        · simp [List.count_cons, hpy, hyx, ih]

private theorem count_pair_le_fst : ∀ (rs : List (Nat × Nat)) a b,
    rs.count (a, b) ≤ (rs.map Prod.fst).count a
  | [], a, b => by simp
  | (c, d) :: rs, a, b => by
      by_cases hca : c == a
      · have heq : c = a := beq_iff_eq.mp hca
        have hrec := count_pair_le_fst rs a b
        by_cases hdb : d == b
        · have heq2 : d = b := beq_iff_eq.mp hdb
          simp [List.count_cons, heq, heq2]
          omega
        · have hne : d ≠ b := by
            intro hd
            exact hdb (by simp [hd])
          simp [List.count_cons, heq, hne]
          omega
      · have hne : c ≠ a := by
          intro hc
          exact hca (by simp [hc])
        simp [List.count_cons, hne]
        have hrec := count_pair_le_fst rs a b
        omega

private theorem count_pair_le_snd : ∀ (rs : List (Nat × Nat)) a b,
    rs.count (a, b) ≤ (rs.map Prod.snd).count b
  | [], a, b => by simp
  | (c, d) :: rs, a, b => by
      by_cases hda : d == b
      · have heq : d = b := beq_iff_eq.mp hda
        have hrec := count_pair_le_snd rs a b
        by_cases hca : c == a
        · have heq2 : c = a := beq_iff_eq.mp hca
          simp [List.count_cons, heq, heq2]
          omega
        · have hne : c ≠ a := by
            intro hc
            exact hca (by simp [hc])
          simp [List.count_cons, heq, hne]
          omega
      · have hne : d ≠ b := by
          intro hd
          exact hda (by simp [hd])
        simp [List.count_cons, hne]
        have hrec := count_pair_le_snd rs a b
        omega

end EncodedScanAggregatePairScreen

open EncodedScanAggregatePairScreen

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.HierarchicalPairFrequencyScreen.Goal := by
  intro rs t
  dsimp only
  let viable : Nat × Nat → Bool := fun p =>
    decide (t ≤ min ((rs.map Prod.fst).count p.1) ((rs.map Prod.snd).count p.2))
  let high : Nat × Nat → Bool := fun p => decide (t ≤ rs.count p)
  change (frequency (rs.filter viable)).filter (fun r => t ≤ r.2) =
    (frequency rs).filter (fun r => t ≤ r.2)
  have hviable_of_high : ∀ p : Nat × Nat, high p = true → viable p = true := by
    rintro ⟨a, b⟩ hp
    have ht : t ≤ rs.count (a, b) := by simpa [high] using hp
    have hfst := count_pair_le_fst rs a b
    have hsnd := count_pair_le_snd rs a b
    have hmin : t ≤ min ((rs.map Prod.fst).count a) ((rs.map Prod.snd).count b) := by
      omega
    simp [viable, hmin]
  have hcount_eq : ∀ p : Nat × Nat, viable p = true → (rs.filter viable).count p = rs.count p := by
    intro p hp
    exact count_filter_of_pred_true rs viable p hp
  have hR : (frequency rs).filter (fun r => t ≤ r.2) =
      (rs.eraseDups.filter high).map (fun p => (p, rs.count p)) := by
    unfold frequency
    rw [List.filter_map]
    rfl
  have hL : (frequency (rs.filter viable)).filter (fun r => t ≤ r.2) =
      ((rs.filter viable).eraseDups.filter (fun p => decide (t ≤ (rs.filter viable).count p))).map
        (fun p => (p, (rs.filter viable).count p)) := by
    unfold frequency
    rw [List.filter_map]
    rfl
  rw [hR, hL, eraseDups_filter rs viable]
  rw [List.filter_filter]
  have hpred : (fun p : Nat × Nat => decide (t ≤ (rs.filter viable).count p) && viable p) = high := by
    funext p
    by_cases hv : viable p
    · have hc := hcount_eq p hv
      by_cases ht : t ≤ rs.count p
      · have ht' : t ≤ (rs.filter viable).count p := by simpa [hc] using ht
        simp [hv, high, ht, ht']
      · have ht' : ¬ t ≤ (rs.filter viable).count p := by
          intro h
          exact ht (by simpa [hc] using h)
        simp [hv, high, ht, ht']
    · have htfalse : ¬ high p = true := by
        intro ht
        exact hv (hviable_of_high p ht)
      simp [hv, htfalse]
  rw [hpred]
  apply List.map_congr_left
  intro p hp
  have hvhigh : high p = true := (List.mem_filter.mp hp).2
  have hv : viable p = true := hviable_of_high p hvhigh
  exact congrArg (fun n : Nat => (p, n)) (hcount_eq p hv)
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.HierarchicalPairFrequencyScreen
