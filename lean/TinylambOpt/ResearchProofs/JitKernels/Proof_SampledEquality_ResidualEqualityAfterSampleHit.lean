import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Soundness of "once all sample windows match, compare only the omitted bytes": when the
lengths and the three raw sample windows of `a` and `b` agree (`sig a = sig b`), the two
strings are equal exactly when their residuals agree — the residual lists enumerate the
same unsampled positions of both strings in index order, so residual equality plus window
agreement (which fixes every sampled position) determines every byte. This lets a
grouping kernel recheck only the bytes the SIMD windows never read.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.SampledEquality_ResidualEqualityAfterSampleHit
open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

private theorem lt_of_getElem?_some :
    ∀ (l : List α) (i : Nat) {x : α}, l[i]? = some x → i < l.length := by
  intro l
  induction l with
  | nil => intro i x h; cases i <;> cases h
  | cons a as ih =>
      intro i x h
      cases i with
      | zero => simp at h; exact h ▸ Nat.zero_lt_succ _
      | succ i =>
          rw [List.getElem?_cons_succ] at h
          exact Nat.succ_lt_succ (ih i h)

private theorem agreeWin (a b : Bytes) (hl : a.length = b.length)
    (he1 : List.take 4 a = List.take 4 b)
    (he2 : List.take 4 (List.drop ((a.length - 4) / 2) a) =
      List.take 4 (List.drop ((b.length - 4) / 2) b))
    (he3 : List.take 4 (List.drop (a.length - 4) a) =
      List.take 4 (List.drop (b.length - 4) b)) :
    ∀ i, covered a.length i = true → a[i]? = b[i]? := by
  intro i hcov
  rw [covered, offsets, List.any_eq_true] at hcov
  obtain ⟨o, ho, hp⟩ := hcov
  simp at hp ho
  obtain ⟨hle, hlt⟩ := hp
  rcases ho with rfl | rfl | rfl
  · have h := congrArg (fun l => l[i]?) he1
    simp only [List.getElem?_take] at h
    rw [if_pos hlt] at h
    rw [if_pos hlt] at h
    exact h
  · have h := congrArg (fun l => l[i - (a.length - 4) / 2]?) he2
    simp only [List.getElem?_take] at h
    rw [if_pos (by omega : i - (a.length - 4) / 2 < 4)] at h
    rw [if_pos (by omega : i - (a.length - 4) / 2 < 4)] at h
    simp only [List.getElem?_drop] at h
    rw [← hl] at h
    rw [Nat.add_sub_of_le hle] at h
    exact h
  · have h := congrArg (fun l => l[i - (a.length - 4)]?) he3
    simp only [List.getElem?_take] at h
    rw [if_pos (by omega : i - (a.length - 4) < 4)] at h
    rw [if_pos (by omega : i - (a.length - 4) < 4)] at h
    simp only [List.getElem?_drop] at h
    rw [← hl] at h
    rw [Nat.add_sub_of_le hle] at h
    exact h

/-- If two equal-length strings agree at every position their index-only filter keeps
(equal filtered first-projection lists, aligned inductively) and at every dropped
position (agreement supplied through `hc`), then they are equal. -/
private theorem agree : ∀ (x y : Bytes), x.length = y.length →
    ∀ (q : Nat → Bool) (p : Nat),
    ((x.zipIdx p).filter (fun t => q t.2)).map Prod.fst =
      ((y.zipIdx p).filter (fun t => q t.2)).map Prod.fst →
    (∀ i v, x[i]? = some v → q (p + i) = false → y[i]? = some v) →
    x = y := by
  intro x
  induction x with
  | nil =>
      intro y hlen q p hr hc
      cases y with
      | nil => rfl
      | cons b bs => exact absurd hlen (by simp)
  | cons a as ih =>
      intro y hlen q p hr hc
      cases y with
      | nil => exact absurd hlen (by simp)
      | cons b bs =>
          have hl' : as.length = bs.length := by simpa using hlen
          by_cases hq : q p = true
          · -- both filters keep the head, so the first bytes are equal
            simp only [List.zipIdx_cons, List.filter_cons, Prod.snd, hq, reduceIte,
              List.map_cons] at hr
            rw [List.cons.injEq] at hr
            obtain ⟨hab, hrs⟩ := hr
            obtain rfl : b = a := hab.symm
            refine congrArg (List.cons b) (ih bs hl' q (p + 1) hrs ?_)
            intro i v hmem hqv
            have := hc (i + 1) v (by simpa using hmem)
                (by rw [show p + (i + 1) = p + 1 + i by omega]; exact hqv)
            simpa using this
          · -- both filters drop the head: it agrees via `hc`, tails by IH
            have hq' : q p = false := by
              cases hqp : q p
              · rfl
              · exact absurd hqp hq
            have h0 : (a :: as)[0]? = some a := by simp
            have hb : b = a :=
              Option.some.inj
                ((by simp : (b :: bs)[0]? = some b).symm.trans
                  (hc 0 a h0 (by rw [Nat.add_zero]; exact hq')))
            subst hb
            simp only [List.zipIdx_cons, List.filter_cons, Prod.snd, hq', reduceIte,
              List.map_nil] at hr
            refine congrArg (List.cons b) (ih bs hl' q (p + 1) hr ?_)
            intro i v hmem hqv
            have := hc (i + 1) v (by simpa using hmem)
                (by rw [show p + (i + 1) = p + 1 + i by omega]; exact hqv)
            simpa using this

theorem sound : ResidualEqualityAfterSampleHit.Goal := by
  intro a b hs
  constructor
  · intro hab
    exact congrArg residual hab
  · intro hres
    rw [sig, sig, Prod.mk.injEq] at hs
    obtain ⟨hl, hw⟩ := hs
    dsimp only [TinylambOpt.SimdGroupHash.byteSamples] at hw
    rw [Prod.mk.injEq, Prod.mk.injEq] at hw
    obtain ⟨he1, he2, he3⟩ := hw
    unfold residual at hres
    rw [← hl] at hres
    refine agree a b hl (fun k => !covered a.length k) 0 ?_ ?_
    · exact hres
    · intro i v hmem hqv
      have hcov : covered a.length i = true := by simpa using hqv
      have hxy := agreeWin a b hl he1 he2 he3 i hcov
      rw [← hxy]
      exact hmem

end TinylambOpt.ResearchProofs.JitKernels.SampledEquality_ResidualEqualityAfterSampleHit
