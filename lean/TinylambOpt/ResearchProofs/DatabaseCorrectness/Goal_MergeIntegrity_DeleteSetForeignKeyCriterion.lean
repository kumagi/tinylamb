import TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_DeleteSetForeignKeyCriterion

open TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity


theorem count_partition (rows : List Row) (gone : List Nat) (p : Nat) :
    (rows.map Row.key).count p = ((survivors rows gone).map Row.key).count p +
      ((rows.filter (fun x => gone.contains x.id)).map Row.key).count p := by
  induction rows with
  | nil => simp [survivors]
  | cons r rs ih =>
      by_cases h : gone.contains r.id = true
      · have hdel : (r :: rs).filter (fun x => gone.contains x.id) =
            r :: (rs.filter (fun x => gone.contains x.id)) :=
          List.filter_cons_of_pos (p := fun x : Row => gone.contains x.id) h
        have hm : r.id ∈ gone := by simpa using h
        have hn : ¬ (fun x : Row => !gone.contains x.id) r = true := by
          intro hn
          simp at hn
          exact hn hm
        have hsurv : survivors (r :: rs) gone = survivors rs gone := by
          unfold survivors
          exact List.filter_cons_of_neg (p := fun x : Row => !gone.contains x.id) hn
        rw [hdel, hsurv]
        rw [List.map_cons, List.map_cons, List.count_cons, List.count_cons, ih]
        omega
      · have hdel : (r :: rs).filter (fun x => gone.contains x.id) =
            rs.filter (fun x => gone.contains x.id) :=
          List.filter_cons_of_neg (p := fun x : Row => gone.contains x.id) h
        have hmnot : r.id ∉ gone := by
          intro hr
          apply h
          simpa using hr
        have hp : (fun x : Row => !gone.contains x.id) r = true := by
          simpa using hmnot
        have hsurv : survivors (r :: rs) gone = r :: survivors rs gone := by
          unfold survivors
          exact List.filter_cons_of_pos (p := fun x : Row => !gone.contains x.id) hp
        rw [hdel, hsurv]
        rw [List.map_cons, List.map_cons, List.count_cons, List.count_cons, ih]
        omega

theorem mem_mapKey_iff_count_pos (l : List Row) (p : Nat) :
    p ∈ l.map Row.key ↔ 0 < (l.map Row.key).count p := by
  constructor
  · intro h
    have hn : (l.map Row.key).count p ≠ 0 := by
      intro hz
      exact (List.count_eq_zero.mp hz) h
    omega
  · intro hpos
    by_cases hmem : p ∈ l.map Row.key
    · exact hmem
    · have hz : (l.map Row.key).count p = 0 := List.count_eq_zero.mpr hmem
      omega

theorem sound : DeleteSetForeignKeyCriterion.Goal := by
  intro rows gone hvalid
  constructor
  · intro hv r hr p hp
    have hmem := hv r hr p hp
    have hpos := (mem_mapKey_iff_count_pos (survivors rows gone) p).mp hmem
    have hpart := count_partition rows gone p
    rw [hpart]
    simpa using (Nat.add_lt_add_right hpos
      (List.count p (List.map Row.key (rows.filter (fun x => gone.contains x.id)))))
  · intro hcount r hr p hp
    apply (mem_mapKey_iff_count_pos (survivors rows gone) p).mpr
    have hpart := count_partition rows gone p
    have hc := hcount r hr p hp
    have hlt : List.count p (List.map Row.key (rows.filter (fun x => gone.contains x.id))) <
        List.count p (List.map Row.key (survivors rows gone)) +
          List.count p (List.map Row.key (rows.filter (fun x => gone.contains x.id))) := by
      simpa [hpart] using hc
    exact (Nat.add_lt_add_iff_right
      (k := List.count p (List.map Row.key (rows.filter (fun x => gone.contains x.id))))).mp
      (by simpa using hlt)

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_DeleteSetForeignKeyCriterion
