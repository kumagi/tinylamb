import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Submask.DistinctPostings` — `∀ u t x, ((search.spans u t x).map Prod.fst).Nodup ∧
  (search.spans u t x).length≤min u (t+1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_DistinctPostings

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem submasks_pairwise_lt (x : Nat) : (submasks x).Pairwise (· < ·) := by
  unfold submasks
  suffices h : ∀ (bs : List Nat) (acc : List Nat),
      acc.Pairwise (· < ·) →
      (∀ b ∈ bs, ∀ z ∈ acc, z < 2 ^ b) →
      bs.Pairwise (· < ·) →
      (bs.foldl (fun acc bit => acc ++ acc.map (· + 2 ^ bit)) acc).Pairwise (· < ·) by
    refine h _ [0] (by simp) ?_ ?_
    · intro b hb z hz; simp at hz; subst hz; exact Nat.pow_pos (by decide)
    · have hrange : (List.range (width x)).Pairwise (fun a b => a < b) := by
        rw [List.pairwise_iff_getElem]
        intro i j hi hj hij
        rw [List.getElem_range hi, List.getElem_range hj]
        exact hij
      exact List.Pairwise.filter x.testBit hrange
  intro bs acc hacc hbound hbs
  induction bs generalizing acc with
  | nil => simpa
  | cons b bs ih =>
    simp only [List.foldl_cons]
    apply ih
    · rw [List.pairwise_append]
      refine ⟨hacc, ?_, ?_⟩
      · rw [List.pairwise_map]; exact List.Pairwise.imp (fun {a b} h => by omega) hacc
      · intro a ha c hc
        simp only [List.mem_map] at hc
        rcases hc with ⟨a', ha', rfl⟩
        have : a < 2 ^ b := hbound b List.mem_cons_self a ha
        omega
    · intro b' hb' z hz
      simp only [List.mem_append, List.mem_map] at hz
      have hbb' : b < b' := (List.pairwise_cons.mp hbs).1 b' hb'
      rcases hz with hz | ⟨z', hz', rfl⟩
      · have : z < 2 ^ b := hbound b List.mem_cons_self z hz
        exact Nat.lt_of_lt_of_le this (Nat.pow_le_pow_right (by decide) (by omega))
      · have : z' < 2 ^ b := hbound b List.mem_cons_self z' hz'
        calc z' + 2 ^ b < 2 ^ b + 2 ^ b := by omega
          _ = 2 ^ (b + 1) := by rw [Nat.pow_succ]; omega
          _ ≤ 2 ^ b' := Nat.pow_le_pow_right (by decide) hbb'
    · exact (List.pairwise_cons.mp hbs).2

theorem sound_DistinctPostings : Submask.DistinctPostings.Goal := by
  intro u t x
  simp only [search]
  let ys := (submasks x).filter (fun y => y < u && y ≤ t)
  change ((ys.map (fun y => (y, y + 1))).map Prod.fst).Nodup ∧
    (ys.map (fun y => (y, y + 1))).length ≤ min u (t + 1)
  have hpair : ys.Pairwise (· < ·) :=
    List.Pairwise.sublist (List.filter_sublist (p := fun y => y < u && y ≤ t) (l := submasks x))
      (submasks_pairwise_lt x)
  have hnodup : ys.Nodup := List.Pairwise.imp (fun {a b} h => by omega) hpair
  constructor
  · have hfst : (ys.map (fun y => y)).Nodup :=
      List.Pairwise.map (fun y => y) (by intro a b h; simpa using h) hnodup
    have hlists : (ys.map (fun y => (y, y + 1))).map Prod.fst = ys.map (fun y => y) := by
      induction ys with
      | nil => rfl
      | cons y ys ih => simp [ih]
    rwa [hlists]
  · have hsub1 : ys ⊆ List.range u := by
      intro y hy
      simp only [ys, List.mem_filter, decide_eq_true_eq, Bool.and_eq_true] at hy
      simp only [List.mem_range]
      exact hy.2.1
    have hsub2 : ys ⊆ List.range (t + 1) := by
      intro y hy
      simp only [ys, List.mem_filter, decide_eq_true_eq, Bool.and_eq_true] at hy
      simp only [List.mem_range]
      omega
    apply Nat.le_min.mpr
    constructor
    · calc (ys.map (fun y => (y, y + 1))).length = ys.length := by simp
        _ ≤ (List.range u).length := List.Nodup.length_le_of_subset hnodup hsub1
        _ = u := by simp
    · calc (ys.map (fun y => (y, y + 1))).length = ys.length := by simp
        _ ≤ (List.range (t + 1)).length := List.Nodup.length_le_of_subset hnodup hsub2
        _ = t + 1 := by simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_DistinctPostings
