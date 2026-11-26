import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
A phrase omitted from every page shortlist still occurs only among the
rejected lanes. Each such occurrence contributes the whole page count of the
phrase to that page's omitted maximum, so the sum of page maxima bounds the
global phrase frequency. SQL may bound the global count from shortlists alone.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.GlobalPhraseFrequencyBoundFromOmittedPageMaxima

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates



set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace EncodedScanAggregatePageMaxima

private theorem le_maxGetD_of_mem (ys : List Nat) (a : Nat) (h : a ∈ ys) :
    a ≤ ys.max?.getD 0 := by
  induction ys with
  | nil => simp at h
  | cons y ys ih =>
      rw [List.max?_cons]
      simp only [Option.getD_some]
      cases hys : ys.max? with
      | none =>
          cases h with
          | head => simp
          | tail _ ht =>
              have ha0 : a ≤ 0 := by simpa [hys] using ih ht
              simp [hys]
              omega
      | some v =>
          cases h with
          | head =>
              simp [hys]
              omega
          | tail _ ht =>
              have ha : a ≤ v := by simpa [hys] using ih ht
              simp [hys]
              omega

private theorem page_bound (xs keep : List Nat) (k : Nat) (hk : ¬ k ∈ keep) :
    xs.count k ≤ (((xs.filter (!keep.contains ·)).map xs.count).max?).getD 0 := by
  by_cases hmem : k ∈ xs
  · have hkf : k ∈ xs.filter (!keep.contains ·) := by
      simp [hmem, hk]
    exact le_maxGetD_of_mem ((xs.filter (!keep.contains ·)).map xs.count) (xs.count k)
      (List.mem_map.mpr ⟨k, hkf, rfl⟩)
  · have hzero : xs.count k = 0 := List.count_eq_zero.mpr hmem
    simp [hzero]

end EncodedScanAggregatePageMaxima

private theorem count_flatMapFst (ps : List (List Nat × List Nat)) (k : Nat) :
    (ps.flatMap Prod.fst).count k = (ps.map (fun p => p.1.count k)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih => simp [ih]

open EncodedScanAggregatePageMaxima

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.GlobalPhraseFrequencyBoundFromOmittedPageMaxima.Goal := by
  intro pages k hk
  induction pages with
  | nil => simp
  | cons p ps ih =>
      have hp : ¬ k ∈ p.2 := hk p (by simp)
      have hps : ∀ q ∈ ps, ¬ k ∈ q.2 := fun q hq => hk q (by simp [hq])
      have hi := ih hps
      have hb := page_bound p.1 p.2 k hp
      calc
        ((p :: ps).flatMap Prod.fst).count k =
            p.1.count k + (ps.flatMap Prod.fst).count k := by simp
        _ ≤ (((p.1.filter (!p.2.contains ·)).map p.1.count).max?).getD 0 +
              (ps.map (fun x =>
                (((x.1.filter (!x.2.contains ·)).map x.1.count).max?).getD 0)).sum :=
          Nat.add_le_add hb hi
        _ = ((p :: ps).map (fun x =>
              (((x.1.filter (!x.2.contains ·)).map x.1.count).max?).getD 0)).sum := by simp
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.GlobalPhraseFrequencyBoundFromOmittedPageMaxima
