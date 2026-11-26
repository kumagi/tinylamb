import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Manhattan.DominatedTop` — A bounded page cannot change the descending top k once k
  strictly better witnesses are known.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_DominatedTop

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan

attribute [local instance] boolRelToRel

private def geRel (a b : Nat) : Bool := decide (b ≤ a)

private theorem ge_trans : ∀ a b c : Nat, geRel a b = true → geRel b c = true → geRel a c = true := by
  intro a b c h₁ h₂
  simp [geRel] at h₁ h₂ ⊢
  omega

private theorem ge_total (a b : Nat) : (geRel a b || geRel b a) = true := by
  simp [geRel]
  omega

/-- For a list sorted in descending order, the elements above a threshold form a prefix. -/
private theorem filter_prefix (hi : Nat) : ∀ L : List Nat, L.Pairwise geRel →
    L.filter (fun x => decide (hi < x)) = L.take (L.countP (fun x => decide (hi < x))) := by
  intro L hs
  induction L with
  | nil => simp
  | cons a l ih =>
      have hs_tail : l.Pairwise geRel := hs.tail
      have ih' := ih hs_tail
      by_cases ha : hi < a
      · simp [ha, ih']
      · have hall : ∀ b ∈ l, b ≤ a := by
          intro b hb
          have hge : geRel a b = true := List.rel_of_pairwise_cons hs hb
          simpa [geRel] using hge
        have hfilt : l.filter (fun x => decide (hi < x)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro b hb
          have hb_le := hall b hb
          simp [show ¬ hi < b by omega]
        have hcnt0 : l.countP (fun x => decide (hi < x)) = 0 := by
          rw [List.countP_eq_zero]
          intro b hb
          have hb_le := hall b hb
          simp [show ¬ hi < b by omega]
        simp [ha, hfilt, hcnt0]

/-- Two descending sorted lists with the same above-threshold multiset have the same top k. -/
private theorem topk_eq_of_perm (hi k : Nat) (L M : List Nat)
    (hL : L.Pairwise geRel) (hM : M.Pairwise geRel)
    (hkL : k ≤ L.countP (fun x => decide (hi < x)))
    (hkM : k ≤ M.countP (fun x => decide (hi < x)))
    (hperm : (L.filter (fun x => decide (hi < x))).Perm (M.filter (fun x => decide (hi < x)))) :
    L.take k = M.take k := by
  let p : Nat → Bool := fun x => decide (hi < x)
  have hfL : L.filter p = L.take (L.countP p) := filter_prefix hi L hL
  have hfM : M.filter p = M.take (M.countP p) := filter_prefix hi M hM
  have hfeq : L.filter p = M.filter p := by
    apply List.Perm.eq_of_pairwise (le := fun a b : Nat => geRel a b = true)
    · intro x y hx hy hxy hyx
      simp [geRel] at hxy hyx
      omega
    · exact hL.filter p
    · exact hM.filter p
    · exact hperm
  have hkL' : k ≤ L.countP p := by simpa [p] using hkL
  have hkM' : k ≤ M.countP p := by simpa [p] using hkM
  calc
    L.take k = (L.take (L.countP p)).take k := by
      rw [List.take_take, Nat.min_eq_left hkL']
    _ = (L.filter p).take k := by rw [← hfL]
    _ = (M.filter p).take k := by rw [hfeq]
    _ = (M.take (M.countP p)).take k := by rw [← hfM]
    _ = M.take k := by
      rw [List.take_take, Nat.min_eq_left hkM']

/-- Every non-NULL Manhattan score of a covered page is at most the interval maximum. -/
private theorem score_le (t : Nat) (p : List (Option Row)) (a b : Bounds) (u : Nat)
    (hu : u ∈ scores kernel t p) (hc : covers p a b) : u ≤ (kernel.bounds t a b).hi := by
  rw [scores] at hu
  rw [List.mem_filterMap] at hu
  rcases hu with ⟨r, hr, hru⟩
  cases r with
  | none => simp at hru
  | some xy =>
      rcases xy with ⟨x,y⟩
      simp [kernel] at hru
      subst u
      have hcxy := hc (some (x,y)) hr x y rfl
      simp only [kernel, inside, gapMin, gapMax, gap] at hcxy ⊢
      omega

theorem sound_DominatedTop : Manhattan.DominatedTop.Goal := by
  intro t p a b seen k hcovers hk
  let hi := (kernel.bounds t a b).hi
  let good : Nat → Bool := fun x => decide (hi < x)
  let L := (seen ++ scores kernel t p).mergeSort geRel
  let M := seen.mergeSort geRel
  have hLsort : L.Pairwise geRel := by
    dsimp [L]
    exact List.pairwise_mergeSort ge_trans ge_total _
  have hMsort : M.Pairwise geRel := by
    dsimp [M]
    exact List.pairwise_mergeSort ge_trans ge_total _
  have hscores0 : (scores kernel t p).countP good = 0 := by
    rw [List.countP_eq_zero]
    intro u hu
    have hule := score_le t p a b u hu hcovers
    simp [good, show ¬ hi < u by omega]
  have hkM : k ≤ M.countP good := by
    dsimp [M]
    rw [List.Perm.countP_eq good (List.mergeSort_perm seen geRel)]
    simpa [good, hi] using hk
  have hkL : k ≤ L.countP good := by
    dsimp [L]
    rw [List.Perm.countP_eq good (List.mergeSort_perm (seen ++ scores kernel t p) geRel)]
    rw [List.countP_append, hscores0, Nat.add_zero]
    simpa [good, hi] using hk
  have hfilterScores : (scores kernel t p).filter good = [] := by
    rw [List.filter_eq_nil_iff]
    intro u hu
    have hule := score_le t p a b u hu hcovers
    simp [good, show ¬ hi < u by omega]
  have hpermFilter : (L.filter good).Perm (M.filter good) := by
    have h1 : (L.filter good).Perm ((seen ++ scores kernel t p).filter good) := by
      dsimp [L]
      exact (List.mergeSort_perm (seen ++ scores kernel t p) geRel).filter good
    have h2 : (M.filter good).Perm (seen.filter good) := by
      dsimp [M]
      exact (List.mergeSort_perm seen geRel).filter good
    rw [List.filter_append, hfilterScores, List.append_nil] at h1
    exact h1.trans h2.symm
  have htop := topk_eq_of_perm hi k L M hLsort hMsort hkL hkM hpermFilter
  change L.take k = M.take k
  exact htop

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_DominatedTop
