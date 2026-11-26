import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_ModalBeatsFirst

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.ConstantExceptions

private theorem uleb_length_pos (m : Nat) : 0 < (uleb m).length := by
  unfold uleb
  split <;> simp

private theorem uleb_one : (uleb 1).length = 1 := by
  rw [uleb]
  simp

private theorem little_four (x : Nat) : (little x 4).length = 4 := by
  simp [little]

private theorem filter_zipIdx_all (b : Nat) : ∀ (xs : List Nat) (s : Nat),
    (∀ x ∈ xs, x = b) →
    (List.filter (fun x : Nat × Nat => x.1 != b) (xs.zipIdx s)) = [] := by
  intro xs
  induction xs with
  | nil => intro s h; simp
  | cons x xs ih =>
      intro s h
      have hx : x = b := h x (by simp)
      have htail : ∀ y ∈ xs, y = b := by
        intro y hy
        exact h y (by simp [hy])
      subst x
      simpa [List.zipIdx] using ih (s+1) htail

private theorem filter_zipIdx_all_ne {a b : Nat} (hab : a ≠ b) : ∀ (xs : List Nat) (s : Nat),
    (∀ x ∈ xs, x = b) →
    (List.filter (fun x : Nat × Nat => x.1 != a) (xs.zipIdx s)) = xs.zipIdx s := by
  intro xs
  induction xs with
  | nil => intro s h; simp
  | cons x xs ih =>
      intro s h
      have hx : x = b := h x (by simp)
      have htail : ∀ y ∈ xs, y = b := by
        intro y hy
        exact h y (by simp [hy])
      subst x
      have hba : (b != a) = true := by
        by_cases hEq : b = a
        · exact False.elim (hab hEq.symm)
        · simp [hEq]
      simpa [List.zipIdx, hba] using ih (s+1) htail

private theorem sum_exception_lengths (l : List (Nat × Nat)) :
    5 * l.length ≤ (l.map (fun p => (uleb p.2).length + (little p.1 4).length)).sum := by
  induction l with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, Nat.mul_succ]
      have hp : 0 < (uleb p.2).length := uleb_length_pos p.2
      have hl : (little p.1 4).length = 4 := little_four p.1
      omega

theorem sound_ModalBeatsFirst : ConstantExceptions.ModalBeatsFirst.Goal := by
  intro a b n hab hn
  have hleft : (write (b::a::List.replicate (n-1) b)).length = 10 := by
    have hf : (List.filter (fun x : Nat × Nat => x.1 != b)
        ((List.replicate (n-1) b).zipIdx 2)) = [] := by
      apply filter_zipIdx_all
      intro x hx
      simpa using (List.eq_of_mem_replicate hx)
    simp [write, hab, List.zipIdx, hf, little_four, uleb_one]
  have hright : 10 < (write (a::List.replicate n b)).length := by
    have hf : (List.filter (fun x : Nat × Nat => x.1 != a)
        ((List.replicate n b).zipIdx 1)) = (List.replicate n b).zipIdx 1 := by
      apply filter_zipIdx_all_ne hab
      intro x hx
      simpa using (List.eq_of_mem_replicate hx)
    have hlen : ((List.replicate n b).zipIdx 1).length = n := by
      simp
    have hsum := sum_exception_lengths ((List.replicate n b).zipIdx 1)
    have hulen : 0 < (uleb n).length := uleb_length_pos n
    simp [write, List.zipIdx, hf, little_four, hlen] at *
    omega
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_ModalBeatsFirst
