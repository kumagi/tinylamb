import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Soundness of "a single injectivity check over the finite dictionary licenses sample
equality": if every signature key in the build pool occurs at most once, then two pool
members with equal signatures are the same string — the hash hit can be verified once at
build time and later groupings may collapse sample-equal dictionary entries without
re-comparing bytes.
Claim: for all pools and members `a b ∈ pool`, `(keys pool).Nodup → sig a = sig b → a = b`.
Proof: membership gives indices `pool[i]? = some a`, `pool[j]? = some b`; mapping `sig`
gives `keys[i]? = some (sig a) = some (sig b) = keys[j]?`, and uniqueness of the key list
forces `i = j`, hence `a = b`.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.SampledEquality_ClosedDictionarySampleEquality
open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

private theorem mem_getElem? :
    ∀ (l : List Bytes) (x : Bytes), x ∈ l → ∃ i : Nat, l[i]? = some x := by
  intro l
  induction l with
  | nil => intro x h; exact absurd h (List.not_mem_nil)
  | cons a as ih =>
      intro x hmem
      rcases List.mem_cons.mp hmem with heq | hmem'
      · exact ⟨0, by rw [heq]; simp [List.getElem?_cons_zero]⟩
      · obtain ⟨i, hi⟩ := ih x hmem'
        exact ⟨i + 1, by simpa using hi⟩

private def Sig := Nat × (List UInt8 × List UInt8 × List UInt8)

private theorem getElem?_mem : ∀ (l : List Sig) (i : Nat) (x : Sig), l[i]? = some x → x ∈ l := by
  intro l
  induction l with
  | nil =>
      intro i x h
      cases i <;> cases h
  | cons a as ih =>
      intro i x h
      cases i with
      | zero =>
          rw [List.getElem?_cons_zero] at h
          obtain rfl : a = x := Option.some.inj h
          exact List.Mem.head _
      | succ i =>
          rw [List.getElem?_cons_succ] at h
          exact List.Mem.tail _ (ih i x h)

private theorem nodup_getEq : ∀ (l : List Sig) (x : Sig) (i j : Nat),
    l.Nodup → l[i]? = some x → l[j]? = some x → i = j := by
  intro l
  induction l with
  | nil =>
      intro x i j _ h1 h2
      cases i <;> cases h1
  | cons a as ih =>
      intro x i j hnd h1 h2
      rw [List.nodup_cons] at hnd
      obtain ⟨hna, hnd⟩ := hnd
      cases i with
      | zero =>
          rw [List.getElem?_cons_zero] at h1
          obtain rfl : a = x := Option.some.inj h1
          cases j with
          | zero => rfl
          | succ j =>
              rw [List.getElem?_cons_succ] at h2
              exact absurd (getElem?_mem as j a h2) hna
      | succ i =>
          rw [List.getElem?_cons_succ] at h1
          cases j with
          | zero =>
              rw [List.getElem?_cons_zero] at h2
              obtain rfl : a = x := Option.some.inj h2
              exact absurd (getElem?_mem as i a h1) hna
          | succ j =>
              rw [List.getElem?_cons_succ] at h2
              have : i = j := ih x i j hnd h1 h2
              omega

theorem sound : ClosedDictionarySampleEquality.Goal := by
  intro pool a b hnd ha hb hs
  obtain ⟨i, hi⟩ := mem_getElem? pool a ha
  obtain ⟨j, hj⟩ := mem_getElem? pool b hb
  have ki : (keys pool)[i]? = some (sig a) := by
    show (pool.map sig)[i]? = some (sig a)
    rw [List.getElem?_map, hi]
    rfl
  have kj : (keys pool)[j]? = some (sig a) := by
    show (pool.map sig)[j]? = some (sig a)
    rw [List.getElem?_map, hj, hs]
    rfl
  have ijeq : i = j := nodup_getEq (keys pool) (sig a) i j hnd ki kj
  subst ijeq
  exact Option.some.inj (hi.symm.trans hj)

end TinylambOpt.ResearchProofs.JitKernels.SampledEquality_ClosedDictionarySampleEquality
