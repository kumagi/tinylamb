import TinylambOpt.ResearchGoals.BenchmarkRematch.Storage

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_TpchClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Storage

theorem find_lemma : ∀ (runs : List (Nat × Nat)) (s i : Nat), s ≤ i →
    ((((((runs.map Prod.snd).scanl (·+·) s).drop 1).zip (runs.map Prod.fst)).find?
      (fun r => i < r.1)).map Prod.snd) =
      (runs.flatMap (fun (key,n) => List.replicate n key))[i - s]? := by
  intro runs
  induction runs with
  | nil => intro s i h; simp
  | cons r rest ih =>
    intro s i h
    obtain ⟨k, n⟩ := r
    obtain ⟨t, ht⟩ : ∃ t, List.scanl (·+·) (s+n) (rest.map Prod.snd) = (s+n) :: t := by
      cases rest.map Prod.snd <;> simp [List.scanl]
    have ih2 := ih (s+n) i
    rw [ht] at ih2
    simp only [List.map_cons, List.scanl_cons, ht, List.drop_succ_cons, List.drop_zero, List.zip_cons_cons,
      List.find?_cons, List.flatMap_cons]
    by_cases hi : i < s + n
    · have : i - s < n := by omega
      simp [hi, List.getElem?_append_left, List.length_replicate, this, List.getElem?_replicate]
    · have h2 : s + n ≤ i := by omega
      have h3 : ¬ i - s < n := by omega
      have := ih2 h2
      simp only [hi, decide_false, Bool.false_eq_true, ↓reduceIte]
      simp only [List.drop_succ_cons, List.drop_zero] at this
      rw [this, List.getElem?_append_right (by simp; omega)]
      simp only [List.length_replicate]
      congr 1; omega

theorem sound : TpchClickHouse.Goal := by
  intro runs i
  dsimp only
  have := find_lemma runs 0 i (Nat.zero_le _)
  simpa using this

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_TpchClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_TpchClickHouse.sound
