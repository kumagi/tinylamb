import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! Mirroring the control ring lets any home slot issue one contiguous read.
Every lane of the modular walk is valid, so the filtered index range is exactly
the corresponding window of the doubled array; SQL can use the unaligned load
for the whole ring probe prefix. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_RingMirrorUnalignedFetch

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-- Indexing a `filterMap` whose function always returns `some`. -/
theorem filterMapGetElemOfSome {α β : Type} (f : α → Option β) (l : List α)
    (hall : ∀ a ∈ l, (f a).isSome) :
    ∀ i : Nat, (l.filterMap f)[i]? = l[i]?.bind f := by
  induction l with
  | nil => intro i; simp
  | cons a l ih =>
      intro i
      have ha : (f a).isSome := hall a (by simp)
      have ht : ∀ b ∈ l, (f b).isSome := by
        intro b hb
        exact hall b (by simp [hb])
      cases hfa : f a with
      | none => simp [hfa] at ha
      | some b =>
          cases i with
          | zero => simp [hfa, Option.bind]
          | succ i => simp [hfa, ih ht i]

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.RingMirrorUnalignedFetch.Goal := by
  intro xs h n hh hn
  let len := xs.length
  have hlen : 0 < len := by omega
  apply List.ext_getElem?
  intro i
  simp only [List.getElem?_take]
  by_cases hin : i < n
  · have hil : i < len := by omega
    simp [hin]
    unfold ring
    have hall : ∀ j ∈ List.range len, (xs[(h + j) % len]?).isSome := by
      intro j hj
      simp at hj
      have hjlt : j < len := hj
      have hmod : (h + j) % len < len := Nat.mod_lt _ hlen
      have hsome : xs[(h + j) % len]? = some xs[(h + j) % len] :=
        List.getElem?_eq_getElem (by simpa [len] using hmod)
      simp [len, hsome]
    have hidx := filterMapGetElemOfSome (fun j => xs[(h + j) % len]?) (List.range len) hall i
    rw [hidx]
    simp [List.getElem?_range hil, Option.bind]
    rw [List.getElem?_append]
    by_cases hsum : h + i < len
    · simp [len, hsum, Nat.mod_eq_of_lt hsum]
    · have hsum2 : h + i < 2 * len := by omega
      have hmod : (h + i) % len = h + i - len := by
        rw [Nat.mod_eq_sub_mod (by omega)]
        exact Nat.mod_eq_of_lt (by omega)
      simp [len, hsum, hmod]
  · simp [hin]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_RingMirrorUnalignedFetch
