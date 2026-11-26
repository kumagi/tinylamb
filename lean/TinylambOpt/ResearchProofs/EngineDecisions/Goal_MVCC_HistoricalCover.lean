import TinylambOpt.ResearchGoals.EngineDecisions.MVCC

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `HistoricalCover` — Recover a historical covering scan from today's index plus the
  first before-image per touched row. This restores keys moved out of range as well
  as changed INCLUDE values, without a historical full-table scan.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.MVCC_HistoricalCover

open TinylambOpt.ResearchGoals.EngineDecisions.MVCC.HistoricalCover

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons]
      simp [List.mem_eraseDups, nodup_eraseDups (as.filter fun b => ! b == a)]
termination_by l.length
decreasing_by simp_wf
              have hle := List.length_filter_le (fun b : α => ! b == a) as
              omega

private theorem first_before (cs : List Change) : ∀ (s : Nat → Image) k,
    consistent s cs → (∃ c ∈ cs, (c.rid == k) = true) →
      ((cs.find? fun c => c.rid == k).map Change.before) = some (s k) := by
  induction cs with
  | nil => simp
  | cons c cs ih =>
      intro s k h hmem
      have hc : c.before = s c.rid := h.1
      have hs' := h.2
      by_cases hck : c.rid == k
      · have heq : c.rid = k := by simpa using hck
        subst k
        simp [hc]
      · rcases hmem with ⟨d, hdmem, hdk⟩
        rcases List.mem_cons.mp hdmem with hd_eq | hd_tail
        · subst d
          exact False.elim (hck hdk)
        · have hmem' : ∃ d ∈ cs, d.rid == k := ⟨d, hd_tail, hdk⟩
          have ih' := ih (fun x => if x == c.rid then c.afterwards else s x) k hs' hmem'
          have hneq : c.rid ≠ k := by
            intro heq
            apply hck
            simpa [heq]
          have hneq' : k ≠ c.rid := Ne.symm hneq
          simpa [hck, hneq'] using ih'

private theorem applyLog_not_dirty (s : Nat → Image) (cs : List Change) (k : Nat)
    (h : (cs.map Change.rid).eraseDups.contains k = false) : applyLog s cs k = s k := by
  unfold applyLog
  have hnone : (cs.filter fun c => c.rid == k) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro c hc
    simp
    intro hrid
    have hmem : k ∈ (cs.map Change.rid).eraseDups := by
      rw [List.mem_eraseDups]
      exact List.mem_map.mpr ⟨c, hc, hrid⟩
    simp [hmem] at h
  simp [hnone]

private theorem hits_fst (p : Nat → Bool) (k : Nat) (img : Image) :
    (hits p k img).all (fun row => row.1 == k) = true := by
  unfold hits
  cases img with
  | none => simp
  | some pr =>
      cases pr with
      | mk k' v =>
          by_cases hp : p k' <;> simp [hp]

private theorem filter_filterMap_eq (q : Nat → Bool) (f : Nat → Option (Nat × Nat × Int))
    (hfst : ∀ k row, f k = some row → row.1 = k) : ∀ l : List Nat,
    (l.filter q).filterMap f = (l.filterMap f).filter (fun row => q row.1) := by
  intro l
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases hqa : q a
      · cases hfa : f a with
        | none => simp [hqa, hfa, ih]
        | some row =>
            have hrow := hfst a row hfa
            simp [hqa, hfa, hrow, ih]
      · cases hfa : f a with
        | none => simp [hqa, hfa, ih]
        | some row =>
            have hrow := hfst a row hfa
            simp [hqa, hfa, hrow, ih]

private theorem filterMap_congr_left {α β : Type} (l : List α) (f g : α → Option β)
    (h : ∀ a ∈ l, f a = g a) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha := h a (by simp)
      have htail := ih (by intro x hx; exact h x (by simp [hx]))
      cases hfa : f a with
      | none =>
          cases hga : g a with
          | none => simpa [hfa, hga] using htail
          | some w => simp [hfa, hga] at ha
      | some v =>
          cases hga : g a with
          | none => simp [hfa, hga] at ha
          | some w =>
              simp [hfa, hga] at ha
              subst w
              simpa [hfa, hga] using htail

private theorem mem_eraseDups_map_rid (cs : List Change) (k : Nat)
    (h : k ∈ (cs.map Change.rid).eraseDups) : ∃ d ∈ cs, (d.rid == k) = true := by
  rw [List.mem_eraseDups] at h
  rcases (List.mem_map.mp h : ∃ d, d ∈ cs ∧ Change.rid d = k) with ⟨d, hd, hdk⟩
  exact ⟨d, hd, by simpa [hdk]⟩

theorem sound : Goal := by
  intro p s cs n hcons hrids
  let ids := List.range n
  let dirty := (cs.map Change.rid).eraseDups
  let currentHits := ids.filterMap fun k => hits p k (applyLog s cs k)
  let origHits := ids.filterMap fun k => hits p k (s k)
  have hdirty : dirty.Nodup := by dsimp [dirty]; exact nodup_eraseDups _
  have hids : ids.Nodup := by dsimp [ids]; exact List.nodup_range (n := n)
  have hdmem : ∀ k ∈ dirty, k ∈ ids := by
    intro k hk
    dsimp [dirty, ids] at hk ⊢
    rw [List.mem_eraseDups] at hk
    rcases List.mem_map.mp hk with ⟨c, hc, rfl⟩
    exact List.mem_range.mpr (hrids c hc)
  have hfst_current (f : Nat → Option (Nat × Nat × Int))
      (hf : ∀ k row, f k = some row → row.1 = k) := hf
  have hfc : ∀ k row, hits p k (applyLog s cs k) = some row → row.1 = k := by
    intro k row h
    have := hits_fst p k (applyLog s cs k)
    simp [h] at this
    simpa using this
  have hfo : ∀ k row, hits p k (s k) = some row → row.1 = k := by
    intro k row h
    have := hits_fst p k (s k)
    simp [h] at this
    simpa using this
  have hnot_part : (currentHits.filter fun row => !dirty.contains row.1).Perm
      (origHits.filter fun row => !dirty.contains row.1) := by
    dsimp [currentHits, origHits]
    have hq : (fun k : Nat => ! dirty.contains k) = (fun k => !(k ∈ dirty)) := by funext k; simp
    rw [← filter_filterMap_eq (fun k => ! dirty.contains k) (fun k => hits p k (applyLog s cs k)) hfc ids]
    rw [← filter_filterMap_eq (fun k => ! dirty.contains k) (fun k => hits p k (s k)) hfo ids]
    apply List.Perm.of_eq
    apply filterMap_congr_left
    intro k hk
    have hd : dirty.contains k = false := by
      simpa using (List.mem_filter.mp hk).2
    rw [applyLog_not_dirty s cs k hd]
  have hperm : dirty.Perm (ids.filter fun k => dirty.contains k) := by
    rw [List.perm_iff_count]
    intro a
    rw [hdirty.count]
    have hfilt : (ids.filter fun k => dirty.contains k).Nodup := List.Pairwise.filter _ hids
    rw [hfilt.count]
    have hiff : a ∈ dirty ↔ a ∈ ids ∧ dirty.contains a = true := by
      constructor
      · intro ha
        exact ⟨hdmem a ha, by simpa using ha⟩
      · intro h
        simpa using h.2
    by_cases ha : a ∈ dirty
    · have haid : a ∈ ids := hdmem a ha
      simp [ha, haid]
    · by_cases haid : a ∈ ids <;> simp [ha, haid]
  have hfilter : ((ids.filter fun k => dirty.contains k).filterMap fun k => hits p k (s k)) =
      (origHits.filter fun row => dirty.contains row.1) := by
    dsimp [origHits]
    exact filter_filterMap_eq (fun k => dirty.contains k) (fun k => hits p k (s k)) hfo ids
  have hdirty_part : (dirty.filterMap fun k => (cs.find? fun c => c.rid == k).bind fun c => hits p k c.before).Perm
      (origHits.filter fun row => dirty.contains row.1) := by
    have hfirst : ∀ k ∈ dirty,
        (cs.find? fun c => c.rid == k).bind (fun c => hits p k c.before) = hits p k (s k) := by
      intro k hk
      have hmemc : ∃ d ∈ cs, (d.rid == k) = true := by
        dsimp [dirty] at hk
        exact mem_eraseDups_map_rid cs k hk
      have hb := first_before cs s k hcons hmemc
      cases hf : cs.find? (fun c => c.rid == k) with
      | none => simp [hf] at hb
      | some c =>
          have hcbefore : c.before = s k := by simpa [hf] using hb
          simp [hf, hcbefore]
    have hcongr : (dirty.filterMap fun k => (cs.find? fun c => c.rid == k).bind fun c => hits p k c.before) =
        (dirty.filterMap fun k => hits p k (s k)) := by
      apply filterMap_congr_left
      intro k hk
      exact hfirst k hk
    rw [hcongr, ← hfilter]
    exact List.Perm.filterMap (fun k => hits p k (s k)) hperm
  unfold repair
  dsimp only
  have hjoin : ((origHits.filter fun row => !dirty.contains row.1) ++
        (origHits.filter fun row => dirty.contains row.1)).Perm origHits := by
    simpa using ((List.perm_append_comm (l₁ := origHits.filter fun row => !dirty.contains row.1)
      (l₂ := origHits.filter fun row => dirty.contains row.1)).trans
        (List.filter_append_perm (fun row => dirty.contains row.1) origHits))
  exact (List.Perm.append hnot_part hdirty_part).trans hjoin

end TinylambOpt.ResearchProofs.EngineDecisions.MVCC_HistoricalCover

-- C++ references to this model or its proof.
-- transaction/mvcc_decisions.hpp
