import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_SparseQualifyingTimestampSeek

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
# SparseQualifyingTimestampSeek

The first ten entries of the nonempty-position index are exactly the first ten
nonempty phrases in timestamp order. The executor can seek the sparse position
index and read those phrases directly instead of scanning every timestamp.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.QueryRewrites

def keepIdx {α : Type} (P : α → Bool) : List α → List Nat
  | [] => []
  | a :: as => if P a then 0 :: (keepIdx P as).map (fun i => i + 1) else (keepIdx P as).map (fun i => i + 1)

theorem zipIdx_filter_snd_eq {α : Type} (P : α → Bool) : ∀ (xs : List α) (n : Nat),
    ((xs.zipIdx n).filter (fun r => P r.1)).map Prod.snd = (keepIdx P xs).map (fun i => i + n) := by
  intro xs
  induction xs with
  | nil => intro n; simp [keepIdx]
  | cons a as ih =>
      intro n
      rw [List.zipIdx_cons]
      simp only [List.filter_cons]
      by_cases hp : P a = true
      · simp [keepIdx, hp, ih, ih (n + 1), Nat.add_comm, Nat.add_left_comm]
      · have hpf : P a = false := by cases h : P a <;> simp [h] at hp ⊢
        simp [keepIdx, hpf, ih (n + 1), Nat.add_comm, Nat.add_left_comm]

theorem valuesAt_cons_succ {α : Type} (a : α) (as : List α) (idxs : List Nat) :
    valuesAt (a :: as) (idxs.map (fun i => i + 1)) = valuesAt as idxs := by
  unfold valuesAt
  rw [List.filterMap_map]
  have hf : ((fun i => (a :: as)[i]?) ∘ fun i => i + 1) = (fun i => as[i]?) := by
    funext i
    simp [Function.comp_def]
  rw [hf]

theorem valuesAt_cons_zero_succ {α : Type} (a : α) (as : List α) (idxs : List Nat) :
    valuesAt (a :: as) (0 :: idxs.map (fun i => i + 1)) = a :: valuesAt as idxs := by
  unfold valuesAt
  rw [List.filterMap_cons, List.getElem?_cons_zero]
  simp only
  rw [List.filterMap_map]
  have hf : ((fun i => (a :: as)[i]?) ∘ fun i => i + 1) = (fun i => as[i]?) := by
    funext i
    simp
  rw [hf]

theorem valuesAt_keepIdx_take {α : Type} (P : α → Bool) : ∀ (xs : List α) (k : Nat),
    valuesAt xs ((keepIdx P xs).take k) = (xs.filter P).take k := by
  intro xs
  induction xs with
  | nil => intro k; simp [valuesAt, keepIdx]
  | cons a as ih =>
      intro k
      by_cases hp : P a = true
      · cases k with
        | zero => simp [valuesAt]
        | succ k =>
            simp only [keepIdx, hp, List.take_succ_cons, List.filter_cons, hp, Bool.true_and]
            change valuesAt (a :: as) (0 :: List.take k (List.map (fun i => i + 1) (keepIdx P as))) =
              a :: List.take k (List.filter P as)
            rw [← List.map_take]
            rw [valuesAt_cons_zero_succ]
            exact congrArg (fun xs => a :: xs) (ih k)
      · have hpf : P a = false := by cases h : P a <;> simp [h] at hp ⊢
        cases k with
        | zero => simp [valuesAt]
        | succ k =>
            simp [keepIdx, hpf]
            rw [← List.map_take]
            rw [valuesAt_cons_succ]
            exact ih (k + 1)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.SparseQualifyingTimestampSeek.Goal := by
  intro phrases
  dsimp
  let P : String → Bool := fun s => s != ""
  have hpos : ((phrases.zipIdx.filter (fun r => r.1 != "")).map Prod.snd) = keepIdx P phrases := by
    simpa [P] using (zipIdx_filter_snd_eq P phrases 0)
  rw [hpos]
  exact valuesAt_keepIdx_take P phrases 10
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_SparseQualifyingTimestampSeek
