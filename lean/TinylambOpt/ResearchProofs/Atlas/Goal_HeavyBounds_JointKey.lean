import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_HeavyBounds_JointKey

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.HeavyBounds

/-! Exhaustive check on all lists of length ≤ 3 over the alphabet 0..3 and a
    small family of hash pairs (identity, mod 2, div 2, constant).  The guard
    passes, so no counterexample occurs in this finite search. -/
private def genLists : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => (genLists n).flatMap (fun xs => (List.range 4).map (fun x => x :: xs))

private def bruteJointKey : Bool :=
  let hs : List (Nat → Nat) := [(fun x => x), (fun x => x % 2), (fun x => x / 2), (fun _ => 0)]
  let lss := (List.range 4).flatMap genLists
  lss.all (fun a => lss.all (fun b => hs.all (fun h => hs.all (fun g =>
    let keyed := a.all (fun x => b.all (fun y => h x == h y → g x == g y → x == y))
    !keyed || joinSize a b ==
      (a.map (fun x => b.countP (fun y => h x == h y && g x == g y))).sum))))

#guard bruteJointKey

/-! On the hash pair determined by an `x ∈ a`, the two predicates "y equals x"
    and "h x = h y ∧ g x = g y" agree for every y ∈ b: the forward implication
    is substitution and the reverse one is exactly the injectivity hypothesis.
    `List.countP_congr` then turns the equality of predicates into equality of
    the per-element counts, and summing over `a` gives the claim. -/
theorem sound_HeavyBounds_JointKey : JointKey.Goal := by
  intro a b h g hkey
  unfold joinSize
  induction a with
  | nil => simp
  | cons x xs ih =>
      have hkey' : ∀ y ∈ xs, ∀ z ∈ b, h y = h z → g y = g z → y = z := by
        intro y hy z hz hhy hgz
        exact hkey y (by simp [hy]) z hz hhy hgz
      simp only [List.map_cons, List.sum_cons]
      rw [ih hkey']
      congr 1
      rw [List.count_eq_countP]
      apply List.countP_congr
      intro y hy
      constructor
      · intro hyx
        have heq : y = x := by simpa [Nat.beq_eq] using hyx
        subst y
        simp
      · intro hh
        have h1b : (h x == h y) = true := by
          simpa using (Bool.and_eq_true_iff.mp hh).1
        have h2b : (g x == g y) = true := by
          simpa using (Bool.and_eq_true_iff.mp hh).2
        have heq1 : h x = h y := by simpa [Nat.beq_eq] using h1b
        have heq2 : g x = g y := by simpa [Nat.beq_eq] using h2b
        have hxy : x = y := hkey x (by simp) y hy heq1 heq2
        subst y
        simp

end TinylambOpt.ResearchProofs.Atlas.Goal_HeavyBounds_JointKey
