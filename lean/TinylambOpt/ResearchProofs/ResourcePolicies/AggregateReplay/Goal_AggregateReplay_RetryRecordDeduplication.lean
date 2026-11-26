import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Retry records denote immutable partials whose identities already exist in the first
attempt's spill: with distinct ids in `rs`, every record of `retry` whose id occurs in
`rs` is dropped by `eraseDupsBy`, so deduplicating the concatenation recovers `rs`
exactly and the group merge is unchanged. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_RetryRecordDeduplication
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

private theorem beq_false {α : Type} [BEq α] [LawfulBEq α] {a b : α}
    (h : ¬ ((a == b) = true)) : (a == b) = false := by
  cases hab : a == b with
  | true => exact absurd hab (by simpa using h)
  | false => exact rfl

/-- Every trailing record whose id already appears in the leading nodup prefix is erased. -/
private theorem eraseApp : ∀ (rs : List Record) (l : List Record),
    (rs.map Record.id).Nodup → (∀ x ∈ l, ∃ y ∈ rs, y.id == x.id) →
    (rs ++ l).eraseDupsBy (fun a b => a.id == b.id) = rs := by
  intro rs
  induction rs with
  | nil =>
      intro l _ hsub
      by_cases hle : l = []
      · subst hle; rfl
      · obtain ⟨x, hx⟩ := List.exists_mem_of_ne_nil l hle
        obtain ⟨y, hy, _⟩ := hsub x hx
        exact absurd hy List.not_mem_nil
  | cons r rest ih =>
      intro l hnd hsub
      rw [List.map_cons] at hnd
      obtain ⟨hnin, hnod⟩ := List.nodup_cons.mp hnd
      have gtrue : ∀ b ∈ rest, (b.id == r.id) = false := by
        intro b hb
        refine beq_false ?_
        intro hbeq
        have hEq : b.id = r.id := LawfulBEq.eq_of_beq hbeq
        exact hnin (by rw [← hEq]; exact List.mem_map.mpr ⟨b, hb, rfl⟩)
      rw [List.cons_append, List.eraseDupsBy_cons]
      congr 1
      rw [List.filter_append]
      have h1 : List.filter (fun x => decide ((x.id == r.id) = false)) rest = rest := by
        refine List.filter_eq_self.mpr ?_
        intro b hb
        simpa using (gtrue b hb)
      rw [h1]
      refine ih _ hnod ?_
      intro x hx
      have hx' : x ∈ l := (List.mem_filter.mp hx).1
      obtain ⟨y, hy, hey⟩ := hsub x hx'
      rcases List.mem_cons.mp hy with rfl | hrest
      · exfalso
        have hg : (x.id == y.id) = false := by simpa using (List.mem_filter.mp hx).2
        have hge : y.id = x.id := LawfulBEq.eq_of_beq (by simpa using hey)
        rw [← hge] at hg
        have hs : (y.id == y.id) = true := beq_self_eq_true y.id
        simp_all
      · exact ⟨y, hrest, hey⟩

theorem sound : RetryRecordDeduplication.Goal := by
  intro rs retry k hnd hsub
  simp only [uniqueRecords]
  have hpre : ∀ x ∈ retry, ∃ y ∈ rs, y.id == x.id := by
    intro x hx
    exact ⟨x, hsub x hx, by simp⟩
  rw [eraseApp rs retry hnd hpre]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_RetryRecordDeduplication
