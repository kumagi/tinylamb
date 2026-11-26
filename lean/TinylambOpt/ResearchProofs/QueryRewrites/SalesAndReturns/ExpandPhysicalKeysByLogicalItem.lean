import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# ExpandPhysicalKeysByLogicalItem

Selecting by logical item IDs obtained from the specified physical keys agrees with
expanding those IDs back to all matching physical keys.

The sales filter can use an integer semi-join and retain historical versions that share
the same logical item ID.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ExpandPhysicalKeysByLogicalItem
open TinylambOpt.ResearchGoals.QueryRewrites

theorem filter_key_length_le_one (versions : List (Nat × Option String))
    (hnd : (versions.map Prod.fst).Nodup) (k : Nat) :
    (versions.filter (fun r => r.1 == k)).length ≤ 1 := by
  induction versions with
  | nil => simp
  | cons r rs ih =>
    have hnd_tail : (rs.map Prod.fst).Nodup := (List.nodup_cons.mp hnd).2
    have hnot : r.1 ∉ rs.map Prod.fst := (List.nodup_cons.mp hnd).1
    by_cases hk : r.1 == k
    · have htail : rs.filter (fun x => x.1 == k) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro x hx hxk
        have hxkEq : x.1 = k := beq_iff_eq.mp hxk
        have hrkEq : r.1 = k := beq_iff_eq.mp hk
        apply hnot
        rw [hrkEq.trans hxkEq.symm]
        exact List.mem_map.mpr ⟨x, hx, rfl⟩
      simp [hk, htail]
    · simp [hk, ih hnd_tail]

theorem gather_eq (versions : List (Nat × Option String))
    (hnd : (versions.map Prod.fst).Nodup) (names : List String) (k : Nat) :
    (versions.filter (fun r => r.1 == k)).filter (fun r => r.2.any names.contains) =
      if ((versions.filter (fun r => r.2.any names.contains)).map Prod.fst).contains k
        then versions.filter (fun r => r.1 == k) else [] := by
  generalize hj : versions.filter (fun r => r.1 == k) = js
  have hlen : js.length ≤ 1 := by
    rw [← hj]
    exact filter_key_length_le_one versions hnd k
  cases js with
  | nil =>
      have hnmem : k ∉ (versions.filter (fun r => r.2.any names.contains)).map Prod.fst := by
        rw [List.mem_map]
        rintro ⟨x, hxmem, hxk⟩
        have hxkBool : (x.1 == k) = true := by simpa [hxk]
        have hxjoin : x ∈ versions.filter (fun y => y.1 == k) :=
          List.mem_filter.mpr ⟨(List.mem_filter.mp hxmem).1, hxkBool⟩
        rw [hj] at hxjoin
        simp at hxjoin
      have hnotcontain :
          ((versions.filter (fun r => r.2.any names.contains)).map Prod.fst).contains k = false := by
        simp [hnmem]
      simp [hj, hnotcontain]
  | cons r tl =>
      have htlLen : tl.length = 0 := by
        simp only [List.length_cons] at hlen
        omega
      have htl : tl = [] := List.eq_nil_of_length_eq_zero htlLen
      subst tl
      have hr_mem : r ∈ versions.filter (fun x => x.1 == k) := by simp [hj]
      have hr_versions : r ∈ versions := (List.mem_filter.mp hr_mem).1
      have hrkEq : r.1 = k := beq_iff_eq.mp (List.mem_filter.mp hr_mem).2
      have hcont :
          ((versions.filter (fun r => r.2.any names.contains)).map Prod.fst).contains k = true ↔
            (r.2.any names.contains) = true := by
        rw [List.contains_eq_mem, decide_eq_true_eq, List.mem_map]
        constructor
        · rintro ⟨x, hxmem, hxk⟩
          have hxkBool : (x.1 == k) = true := by simpa [hxk]
          have hxjoin : x ∈ versions.filter (fun y => y.1 == k) :=
            List.mem_filter.mpr ⟨(List.mem_filter.mp hxmem).1, hxkBool⟩
          rw [hj] at hxjoin
          simp at hxjoin
          subst x
          exact (List.mem_filter.mp hxmem).2
        · intro hpr
          refine ⟨r, List.mem_filter.mpr ⟨hr_versions, hpr⟩, ?_⟩
          exact hrkEq
      by_cases hpr : (r.2.any names.contains) = true
      · have hc : ((versions.filter (fun r => r.2.any names.contains)).map Prod.fst).contains k = true :=
          hcont.mpr hpr
        rw [if_pos hc]
        simp [hpr]
      · have hcfalse :
            ((versions.filter (fun r => r.2.any names.contains)).map Prod.fst).contains k = false := by
          cases hcval : ((versions.filter (fun r => r.2.any names.contains)).map Prod.fst).contains k with
          | false => rfl
          | true => exact False.elim (hpr (hcont.mp hcval))
        have hcnot : ¬ ((versions.filter (fun r => r.2.any names.contains)).map Prod.fst).contains k = true := by
          intro hc
          rw [hcfalse] at hc
          cases hc
        rw [if_neg hcnot]
        simp [hpr]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.ExpandPhysicalKeysByLogicalItem.Goal := by
  intro versions seed facts hnd
  dsimp
  induction facts with
  | nil => rfl
  | cons f fs ih =>
    rcases f with ⟨k, z⟩
    cases z with
    | true =>
      simp only [List.flatMap_cons, List.filter_cons, ih]
      simp
    | false =>
      simp only [List.flatMap_cons, List.filter_cons, ih, Bool.false_or]
      rw [gather_eq versions hnd _ k]
      split <;> simp
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ExpandPhysicalKeysByLogicalItem
