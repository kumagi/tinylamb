import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_NameDayBitmapTriplePopulation

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# NameDayBitmapTriplePopulation

Within a bounded date dictionary, counting dates whose customer, customer-day and
warehouse bitmaps are all set gives the same cardinality as building the distinct
day set by successive semijoins. The bound lets the scan represent the day dictionary
as a dense bitmap while preserving the distinct day count of the SQL query.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

namespace NameDayHelpers

theorem contains_true {α : Type} [BEq α] [LawfulBEq α] (l : List α) (x : α) :
    l.contains x = true ↔ x ∈ l := by
  induction l with
  | nil => simp
  | cons y l ih =>
      by_cases h : y == x
      · have hyx : y = x := beq_iff_eq.mp h
        subst hyx
        simp
      · simp

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => .nil
  | a :: l => by
      rw [List.eraseDups_cons]
      constructor
      · intro h hmem
        rw [List.mem_eraseDups, List.mem_filter] at hmem
        intro heq
        rw [← heq] at hmem
        simp at hmem
      · exact nodup_eraseDups (l.filter (fun b => !b == a))
termination_by l => l.length
decreasing_by
  have := List.length_filter_le (fun b => !b == a) l
  simp
  omega

end NameDayHelpers

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.NameDayBitmapTriplePopulation.Goal := by
  intro s c w n hbound
  let q : Nat → Bool := fun d => s.contains d && c.contains d && w.contains d
  let A := (List.range n).filter q
  let B := members (members s c) w
  have hnodA : A.Nodup := by
    dsimp [A]
    exact List.nodup_range.filter q
  have hnodB : B.Nodup := by
    dsimp [B, members]
    exact (NameDayHelpers.nodup_eraseDups (s.eraseDups.filter c.contains)).filter w.contains
  have hmem : ∀ d, d ∈ A ↔ d ∈ B := by
    intro d
    constructor
    · intro hd
      have hd' := List.mem_filter.mp hd
      have hdlt : d < n := by simpa using hd'.1
      have hq : q d = true := hd'.2
      have hscw := Bool.and_eq_true_iff.mp hq
      have hsc := Bool.and_eq_true_iff.mp hscw.1
      have hs : s.contains d = true := hsc.1
      have hc : c.contains d = true := hsc.2
      have hw : w.contains d = true := hscw.2
      have hsd : d ∈ s := (NameDayHelpers.contains_true s d).mp hs
      have hinner : d ∈ s.eraseDups.filter c.contains :=
        List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hsd, hc⟩
      dsimp [B, members]
      exact List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hinner, hw⟩
    · intro hd
      dsimp [B, members] at hd
      have hinnerDedup : d ∈ (s.eraseDups.filter c.contains).eraseDups := (List.mem_filter.mp hd).1
      have hw : w.contains d = true := (List.mem_filter.mp hd).2
      have hinner : d ∈ s.eraseDups.filter c.contains := List.mem_eraseDups.mp hinnerDedup
      have hsc' := List.mem_filter.mp hinner
      have hsd : d ∈ s := List.mem_eraseDups.mp hsc'.1
      have hc : c.contains d = true := hsc'.2
      have hs : s.contains d = true := (NameDayHelpers.contains_true s d).mpr hsd
      have hdlt : d < n := hbound d hsd
      dsimp [A, q]
      exact List.mem_filter.mpr
        ⟨by simpa using hdlt, Bool.and_eq_true_iff.mpr ⟨Bool.and_eq_true_iff.mpr ⟨hs, hc⟩, hw⟩⟩
  have hperm : A.Perm B := (List.perm_ext_iff_of_nodup hnodA hnodB).2 hmem
  have hlen := hperm.length_eq
  change A.length = B.length
  exact hlen
end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_NameDayBitmapTriplePopulation
