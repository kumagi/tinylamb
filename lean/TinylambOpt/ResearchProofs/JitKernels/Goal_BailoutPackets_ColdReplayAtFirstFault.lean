import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_ColdReplayAtFirstFault

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

theorem sound : TinylambOpt.ResearchGoals.JitKernels.BailoutPackets.ColdReplayAtFirstFault.Goal := by
  let f : Cell × Nat → Option (Nat × Nat) := fun p =>
    match p.1 with | .error z => some (p.2, z) | _ => none
  have hfirstAux : ∀ (xs : List Cell) (off : Nat),
      (xs.zipIdx off).findSome? f =
        ((xs.zipIdx 0).findSome? f).map (fun (p : Nat × Nat) => (off + p.1, p.2)) := by
    intro xs
    induction xs with
    | nil => intro off; rfl
    | cons c xs ih =>
        intro off
        cases c with
        | error z =>
            simp [List.zipIdx_cons, f]
        | ok v =>
            simp only [List.zipIdx_cons]
            simp only [List.findSome?_cons, f]
            rw [ih (off + 1), ih 1]
            cases h : (xs.zipIdx 0).findSome? f with
            | none => simp [h]
            | some p => simp [h, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
  have hfirst : ∀ xs, first xs = (xs.zipIdx 0).findSome? f := by
    intro xs
    rfl
  have hcons : ∀ (c : Cell) (xs : List Cell), !bad c →
      first (c :: xs) = (first xs).map (fun (p : Nat × Nat) => (p.1 + 1, p.2)) := by
    intro c xs hc
    cases c with
    | ok v =>
        rw [hfirst, hfirst]
        rw [List.zipIdx_cons]
        simp only [List.findSome?_cons]
        rw [hfirstAux xs 1]
        cases h : (xs.zipIdx 0).findSome? f with
        | none => simp [f, h]
        | some p => simp [f, h, Nat.add_comm]
    | error z => simp [bad] at hc
  have hchar : ∀ (xs : List Cell) (i e : Nat), first xs = some (i, e) →
      ∃ (pre suf : List Cell), xs = pre ++ .error e :: suf ∧ pre.length = i := by
    intro xs
    induction xs with
    | nil => intro i e h; simp [first] at h
    | cons c xs ih =>
        intro i e h
        cases c with
        | error z =>
            simp [first] at h
            rcases h with ⟨rfl, rfl⟩
            exact ⟨[], xs, rfl, rfl⟩
        | ok v =>
            rw [hcons (.ok v) xs (by simp [bad])] at h
            cases hfirst' : first xs with
            | none => simp [hfirst'] at h
            | some p =>
                simp [hfirst'] at h
                rcases h with ⟨hi, he⟩
                rcases ih p.1 p.2 hfirst' with ⟨pre, suf, hxs, hlen⟩
                refine ⟨.ok v :: pre, suf, ?_, ?_⟩
                · simp [hxs, he]
                · simp [hlen, hi]
  intro xs i e h
  rcases hchar xs i e h with ⟨pre, suf, hxs, hlen⟩
  rw [hxs]
  simp [first, hlen]

end TinylambOpt.ResearchProofs.JitKernels.Goal_BailoutPackets_ColdReplayAtFirstFault
