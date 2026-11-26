import TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_RekeyLastReferencedParent

open TinylambOpt.ResearchGoals.DatabaseCorrectness.MergeIntegrity

theorem unique_id (rows : List Row) (hnod : (rows.map Row.id).Nodup) {x y : Row}
    (hx : x ∈ rows) (hy : y ∈ rows) (hid : x.id = y.id) : x = y := by
  induction rows with
  | nil => simp at hx
  | cons a tl ih =>
      have hp := List.nodup_cons.mp hnod
      simp at hx hy
      rcases hx with rfl | hx
      · rcases hy with rfl | hy
        · rfl
        · have hzin : y.id ∈ tl.map Row.id := List.mem_map.mpr ⟨y, hy, rfl⟩
          rw [← hid] at hzin
          exact (hp.1 hzin).elim
      · rcases hy with rfl | hy
        · have hxin : x.id ∈ tl.map Row.id := List.mem_map.mpr ⟨x, hx, rfl⟩
          rw [hid] at hxin
          exact (hp.1 hxin).elim
        · exact ih hp.2 hx hy

theorem count_gt_one_witness (rows : List Row) (r : Row) (hr : r ∈ rows)
    (hnod : (rows.map Row.id).Nodup) (hgt : (rows.map Row.key).count r.key > 1) :
    ∃ z ∈ rows, z.key = r.key ∧ z.id ≠ r.id := by
  induction rows with
  | nil => simp at hr
  | cons a tl ih =>
      have hp := List.nodup_cons.mp hnod
      simp at hr
      rcases hr with rfl | hrt
      · have hpos : (tl.map Row.key).count r.key > 0 := by
          have hgt2 : (tl.map Row.key).count r.key + 1 > 1 := by
            simpa [List.count_cons] using hgt
          simpa using hgt2
        have hmem : r.key ∈ tl.map Row.key := by
          by_cases hm : r.key ∈ tl.map Row.key
          · exact hm
          · have hz : (tl.map Row.key).count r.key = 0 := List.count_eq_zero.mpr hm
            omega
        rcases List.mem_map.mp hmem with ⟨z, hzmem, hzkey⟩
        refine ⟨z, by simp [hzmem], hzkey, ?_⟩
        intro hid
        have hzin : z.id ∈ tl.map Row.id := List.mem_map.mpr ⟨z, hzmem, rfl⟩
        rw [hid] at hzin
        exact hp.1 hzin
      · by_cases hak : a.key = r.key
        · have hane : a.id ≠ r.id := by
            intro hid
            have hrmem : r ∈ a :: tl := by simp [hrt]
            have haeq : a = r := unique_id (a :: tl) hnod (by simp) hrmem hid
            have hatl : a ∈ tl := by simpa [haeq] using hrt
            have hidin : a.id ∈ tl.map Row.id := List.mem_map.mpr ⟨a, hatl, rfl⟩
            exact (hp.1 hidin).elim
          exact ⟨a, by simp, hak, hane⟩
        · have hgt' : (tl.map Row.key).count r.key > 1 := by
            simpa [List.count_cons, hak] using hgt
          rcases ih hrt hp.2 hgt' with ⟨z, hz, hzk, hzid⟩
          exact ⟨z, by simp [hz], hzk, hzid⟩

theorem two_members_count (rows : List Row) (r z : Row) (p : Nat) (hr : r ∈ rows) (hz : z ∈ rows)
    (hrz : r ≠ z) (hrk : r.key = p) (hzk : z.key = p) : (rows.map Row.key).count p > 1 := by
  induction rows with
  | nil => simp at hr
  | cons a tl ih =>
      simp at hr hz
      rcases hr with rfl | hrt
      · rcases hz with rfl | hzt
        · exact (hrz rfl).elim
        · have hpos : (tl.map Row.key).count p > 0 := by
            by_cases hm : p ∈ tl.map Row.key
            · have hn : (tl.map Row.key).count p ≠ 0 := by
                intro hz0
                exact (List.count_eq_zero.mp hz0) hm
              omega
            · have hzmem : p ∈ tl.map Row.key := List.mem_map.mpr ⟨z, hzt, hzk⟩
              exact (hm hzmem).elim
          simpa [List.map_cons, List.count_cons, hrk] using (Nat.succ_lt_succ hpos)
      · rcases hz with rfl | hzt
        · have hpos : (tl.map Row.key).count p > 0 := by
            by_cases hm : p ∈ tl.map Row.key
            · have hn : (tl.map Row.key).count p ≠ 0 := by
                intro hz0
                exact (List.count_eq_zero.mp hz0) hm
              omega
            · have hzmem : p ∈ tl.map Row.key := List.mem_map.mpr ⟨r, hrt, hrk⟩
              exact (hm hzmem).elim
          simpa [List.map_cons, List.count_cons, hzk] using (Nat.succ_lt_succ hpos)
        · have hgt' : (tl.map Row.key).count p > 1 := ih hrt hzt
          by_cases hap : a.key = p
          · have hn : 0 < List.count p (List.map Row.key tl) := by omega
            simpa [List.map_cons, List.count_cons, hap] using (Nat.succ_lt_succ hn)
          · simpa [List.count_cons, hap] using hgt'

theorem afterRekey_single_key (x : Row) (id k : Nat) :
    (if x.id == id then {x with key := k} else x).key = if x.id = id then k else x.key := by
  by_cases h : x.id = id <;> simp [h, beq_iff_eq]

theorem afterRekey_keys (rows : List Row) (id k : Nat) :
    (afterRekey rows id k).map Row.key = rows.map (fun x => if x.id = id then k else x.key) := by
  induction rows with
  | nil => simp [afterRekey]
  | cons a tl ih =>
      by_cases h : a.id = id
      · simp [afterRekey, h, ih, beq_iff_eq]
        intro x hx
        by_cases hxi : x.id = id <;> simp [hxi]
      · simp [afterRekey, h, ih, beq_iff_eq]
        intro x hx
        by_cases hxi : x.id = id <;> simp [hxi]

theorem afterRekey_mem_iff (rows : List Row) (id k p : Nat) :
    p ∈ (afterRekey rows id k).map Row.key ↔
      ∃ x ∈ rows, (if x.id = id then k else x.key) = p := by
  rw [afterRekey_keys]
  simp [List.mem_map]

theorem rekey_key_mem_iff (rows : List Row) (r : Row) (k : Nat) (hr : r ∈ rows)
    (hnod : (rows.map Row.id).Nodup) (hne : k ≠ r.key) :
    r.key ∈ (afterRekey rows r.id k).map Row.key ↔
      ∃ z ∈ rows, z.key = r.key ∧ z.id ≠ r.id := by
  constructor
  · intro h
    rcases (afterRekey_mem_iff rows r.id k r.key).mp h with ⟨x, hx, hxk⟩
    have hneid : x.id ≠ r.id := by
      intro hid
      have : k = r.key := by simpa [hid] using hxk
      exact hne this
    refine ⟨x, hx, ?_, hneid⟩
    simpa [hneid] using hxk
  · rintro ⟨z, hz, hzk, hzid⟩
    apply (afterRekey_mem_iff rows r.id k r.key).mpr
    exact ⟨z, hz, by simpa [hzid] using hzk⟩

theorem mem_rows_to_after (rows : List Row) (r : Row) (k p : Nat) (hr : r ∈ rows)
    (hnod : (rows.map Row.id).Nodup) (hp : p ≠ r.key) :
    p ∈ rows.map Row.key → p ∈ (afterRekey rows r.id k).map Row.key := by
  intro hpmem
  rcases List.mem_map.mp hpmem with ⟨x, hx, hxkey⟩
  apply (afterRekey_mem_iff rows r.id k p).mpr
  refine ⟨x, hx, ?_⟩
  by_cases hidb : x.id == r.id
  · have hid : x.id = r.id := beq_iff_eq.mp hidb
    have hxr : x = r := unique_id rows hnod hx hr hid
    subst x
    exact (hp hxkey.symm).elim
  · have hneid : x.id ≠ r.id := by
      intro h
      exact hidb (beq_iff_eq.mpr h)
    simpa [hneid] using hxkey

theorem sound : RekeyLastReferencedParent.Goal := by
  intro rows r k hvalid hr hnod hne
  constructor
  · intro hv
    by_cases hgt : (rows.map Row.key).count r.key > 1
    · exact Or.inl hgt
    · right
      intro x hx hpkey
      let y := if x.id == r.id then {x with key := k} else x
      have hy : y ∈ afterRekey rows r.id k := by
        rw [afterRekey]
        exact List.mem_map.mpr ⟨x, hx, rfl⟩
      have hypar : y.parent = some r.key := by
        by_cases hidb : x.id == r.id <;> simp [y, hidb] at hpkey ⊢ <;> exact hpkey
      have hpkey' : r.key ∈ (afterRekey rows r.id k).map Row.key := hv y hy r.key hypar
      have hwit := (rekey_key_mem_iff rows r k hr hnod hne).mp hpkey'
      rcases hwit with ⟨z, hz, hzk, hzid⟩
      have hgt' : (rows.map Row.key).count r.key > 1 := by
        have hzr : z ≠ r := by
          intro heq
          subst z
          exact hzid rfl
        exact two_members_count rows r z r.key hr hz (fun h => hzr h.symm) rfl hzk
      exact (hgt hgt').elim
  · intro hrhs
    intro x hx p hpar
    rcases List.mem_map.mp hx with ⟨y, hy, hyx⟩
    have hpar_y : y.parent = some p := by
      rw [← hyx] at hpar
      by_cases hidb : y.id == r.id <;> simp [hidb] at hpar ⊢ <;> exact hpar
    have hpmem : p ∈ rows.map Row.key := hvalid y hy p hpar_y
    rcases hrhs with hgt | hnone
    · by_cases hp : p = r.key
      · subst p
        apply (rekey_key_mem_iff rows r k hr hnod hne).mpr
        exact count_gt_one_witness rows r hr hnod hgt
      · exact mem_rows_to_after rows r k p hr hnod hp hpmem
    · have hp : p ≠ r.key := by
        intro heq
        subst p
        exact hnone y hy hpar_y
      exact mem_rows_to_after rows r k p hr hnod hp hpmem

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_MergeIntegrity_RekeyLastReferencedParent
