import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_CompactedBailoutOrdinal

theorem sound : TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.CompactedBailoutOrdinal.Goal := by
  intro xs
  have findSome_spec : ∀ {α β : Type} (f : α → Option β) (ys : List α) (y : β),
      ys.findSome? f = some y → ∃ a ∈ ys, f a = some y := by
    intro α β f ys
    induction ys with
    | nil => simp
    | cons a as ih =>
        intro y h
        cases hfa : f a with
        | none =>
            simp [hfa] at h
            rcases ih y h with ⟨b, hb, hby⟩
            exact ⟨b, by simp [hb], hby⟩
        | some z =>
            simp [hfa] at h
            subst y
            exact ⟨a, by simp, hfa⟩
  have first_spec : ∀ (ys : List TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.Cell) (j e : Nat),
      TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.first ys = some (j, e) →
      ys[j]? = some (.error e) := by
    intro ys j e h
    unfold TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.first at h
    rcases findSome_spec (fun p : TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.Cell × Nat =>
        match p.1 with | .error z => some (p.2, z) | _ => none) ys.zipIdx (j, e) h with ⟨p, hp, hpf⟩
    rcases p with ⟨c, i⟩
    cases c with
    | error z =>
        simp at hpf
        rcases hpf with ⟨rfl, rfl⟩
        simpa using (List.mem_zipIdx_iff_getElem?.mp hp)
    | ok _ => simp at hpf
  have gather_spec : ∀ (sel : List Nat), (∀ i ∈ sel, i < xs.length) →
      ∀ (j : Nat) (c : TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.Cell),
        (TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.gather xs sel)[j]? = some c →
        ∃ i, sel[j]? = some i ∧ xs[i]? = some c := by
    intro sel
    induction sel with
    | nil =>
        intro hbound j c h
        simp [TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.gather] at h
    | cons i is ih =>
        intro hbound j c h
        have hi : i < xs.length := hbound i (by simp)
        have hxi : xs[i]? = some (xs[i]'hi) := List.getElem?_eq_getElem hi
        cases j with
        | zero =>
            simp [TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.gather, hxi] at h
            subst c
            exact ⟨i, rfl, hxi⟩
        | succ j =>
            have h' : (TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.gather xs is)[j]? = some c := by
              simpa [TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.gather, hxi] using h
            rcases ih (by intro k hk; exact hbound k (by simp [hk])) j c h' with ⟨k, hsel, hxsk⟩
            exact ⟨k, by simpa using hsel, hxsk⟩
  intro sel j e hbound hfirst
  have hcell := first_spec (TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.gather xs sel) j e hfirst
  exact gather_spec sel hbound j (.error e) hcell

end TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_CompactedBailoutOrdinal
