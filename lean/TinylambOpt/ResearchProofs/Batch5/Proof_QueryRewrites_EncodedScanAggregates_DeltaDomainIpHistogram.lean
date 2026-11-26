import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_DeltaDomainIpHistogram

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
Delta-domain frequency: translating integer keys by a fixed base preserves
frequency and first-occurrence order. This permits group counting in the
smaller residual domain and decoding afterwards without changing GROUP BY
results.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

private theorem sub_eq_sub_iff (y x base : Int) :
    ((y - base == x - base) = true ↔ (y == x) = true) := by
  constructor <;> intro h
  · simp only [beq_iff_eq] at h ⊢
    omega
  · simp only [beq_iff_eq] at h ⊢
    omega

private theorem count_translate (xs : List Int) (x base : Int) :
    (xs.map (fun y => y - base)).count (x - base) = xs.count x := by
  induction xs with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons]
      rw [List.count_cons, List.count_cons, ih]
      by_cases h : (y == x) = true
      · simp [h, (sub_eq_sub_iff y x base).2 h]
      · have hsub : (y - base == x - base) = false := by
          cases hs : (y - base == x - base) <;> simp_all [sub_eq_sub_iff y x base]
        simp [h, hsub]

private theorem eraseDups_map_translate_aux : ∀ (n : Nat) (base : Int) (xs : List Int),
    xs.length ≤ n →
      (xs.map (fun y => y - base)).eraseDups = xs.eraseDups.map (fun y => y - base) := by
  intro n
  induction n with
  | zero =>
      intro base xs hlen
      cases xs with
      | nil => simp
      | cons y ys => simp at hlen
  | succ n ih =>
      intro base xs hlen
      cases xs with
      | nil => simp
      | cons y ys =>
          have hys : ys.length ≤ n := by simp at hlen; omega
          rw [List.map_cons, List.eraseDups_cons, List.eraseDups_cons, List.map_cons]
          have hfilt :
              (ys.map (fun z => z - base)).filter (fun b => ! (b == y - base)) =
                (ys.filter (fun z => ! (z == y))).map (fun z => z - base) := by
            rw [List.filter_map]
            apply congrArg (List.map (fun z => z - base))
            apply List.filter_congr
            intro z hz
            simp only [Function.comp_apply]
            by_cases hh : (z - base == y - base) = true
            · have hzey : (z == y) = true := (sub_eq_sub_iff z y base).1 hh
              simp [hh, hzey]
            · have hzey : (z == y) = false := by
                cases h' : (z == y) <;> simp_all [sub_eq_sub_iff z y base]
              simp [hh, hzey]
          rw [hfilt]
          have hA : (ys.filter (fun z => ! (z == y))).length ≤ n := by
            have := List.length_filter_le (fun z : Int => ! (z == y)) ys
            omega
          exact congrArg (fun t => (y - base) :: t)
            (ih base (ys.filter (fun z => ! (z == y))) hA)

private theorem eraseDups_map_translate (xs : List Int) (base : Int) :
    (xs.map (fun y => y - base)).eraseDups = xs.eraseDups.map (fun y => y - base) := by
  exact eraseDups_map_translate_aux xs.length base xs (Nat.le_refl _)

/-- Translation by a fixed base is injective and preserves first-occurrence
order, so frequency tables can be computed in the residual domain and decoded. -/
theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.DeltaDomainIpHistogram.Goal := by
  intro ips base
  unfold TinylambOpt.ResearchGoals.QueryRewrites.frequency
  rw [eraseDups_map_translate ips base]
  simp only [List.map_map]
  apply List.map_congr_left
  intro y hy
  have hc := count_translate ips y base
  simp [hc]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.EncodedScanAggregates.Goal_DeltaDomainIpHistogram
