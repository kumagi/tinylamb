import TinylambOpt.ResearchGoals.BenchmarkRematch.Common

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
open TinylambOpt.ResearchGoals.BenchmarkRematch

abbrev Sorted (l : List Nat) : Prop := l.Pairwise (· ≤ ·)

theorem S_sorted (l : List Nat) : Sorted l.mergeSort := by
  have := List.pairwise_mergeSort (le := fun a b : Nat => decide (a ≤ b))
    (by intro a b c h1 h2; simp at *; omega) (by intro a b; simp; omega) l
  exact this.imp (by intro a b h; simpa using h)

theorem S_perm (l : List Nat) : l.mergeSort.Perm l := List.mergeSort_perm _ _

theorem S_eq (l m : List Nat) (hp : m.Perm l) (hs : Sorted m) : l.mergeSort = m :=
  List.Perm.eq_of_pairwise (le := (· ≤ ·)) (fun a b _ _ h1 h2 => Nat.le_antisymm h1 h2)
    (S_sorted l) hs ((S_perm l).trans hp.symm)

theorem S_perm_inv (l1 l2 : List Nat) (h : l1.Perm l2) : l1.mergeSort = l2.mergeSort :=
  S_eq l1 _ ((S_perm l2).trans h.symm) (S_sorted l2)

def ins (a : Nat) : List Nat → List Nat
  | [] => [a]
  | b :: l => if a ≤ b then a :: b :: l else b :: ins a l

theorem perm_ins (a : Nat) : ∀ l, (ins a l).Perm (a :: l)
  | [] => List.Perm.refl _
  | b :: l => by
    unfold ins
    split
    · exact List.Perm.refl _
    · exact ((perm_ins a l).cons b).trans (List.Perm.swap a b l)

theorem sorted_ins (a : Nat) : ∀ l, Sorted l → Sorted (ins a l)
  | [], _ => by simp [ins, Sorted]
  | b :: l, h => by
    unfold ins
    split
    · rename_i hab
      refine List.pairwise_cons.mpr ⟨?_, h⟩
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx
      · exact hab
      · exact Nat.le_trans hab ((List.pairwise_cons.mp h).1 x hx)
    · rename_i hab
      have hl := (List.pairwise_cons.mp h)
      have ih := sorted_ins a l hl.2
      refine List.pairwise_cons.mpr ⟨?_, ih⟩
      intro x hx
      have hx' := (perm_ins a l).mem_iff.mp hx
      rcases List.mem_cons.mp hx' with rfl | hx2
      · omega
      · exact hl.1 x hx2

theorem S_cons (a : Nat) (l : List Nat) : (a :: l).mergeSort = ins a l.mergeSort :=
  S_eq (a :: l) (ins a l.mergeSort) ((perm_ins a _).trans ((S_perm l).cons a)) (sorted_ins a _ (S_sorted l))

theorem take_cons_take (b : Nat) (l : List Nat) (n : Nat) :
    (b :: l).take n = (b :: l.take n).take n := by
  cases n with
  | zero => simp
  | succ n => simp [List.take_take]

theorem take_ins (a : Nat) : ∀ (l : List Nat) (n : Nat), (ins a l).take n = (ins a (l.take n)).take n
  | [], n => by cases n <;> simp [ins]
  | b :: l, n => by
    cases n with
    | zero => simp
    | succ n =>
      by_cases hab : a ≤ b
      · have e := take_cons_take b l n
        simp only [List.take_succ_cons, ins, hab, ite_true, List.take_succ_cons]
        rw [e]
      · have ih := take_ins a l n
        simp only [List.take_succ_cons, ins, hab, ite_false, List.take_succ_cons]
        rw [ih]

theorem best_def (n : Nat) (l : List Nat) : best n l = l.mergeSort.take n := rfl

theorem best_K (n a : Nat) (L : List Nat) : best n (a :: L) = best n (a :: best n L) := by
  rw [best_def, best_def, S_cons, S_cons, take_ins, best_def]
  have : (L.mergeSort.take n).mergeSort = L.mergeSort.take n :=
    S_eq _ _ (List.Perm.refl _) (List.Pairwise.take (S_sorted L))
  rw [this]

theorem best_idem (n : Nat) (L : List Nat) : best n (best n L) = best n L := by
  rw [best_def, best_def]
  have : (L.mergeSort.take n).mergeSort = L.mergeSort.take n :=
    S_eq _ _ (List.Perm.refl _) (List.Pairwise.take (S_sorted L))
  rw [this, List.take_take, Nat.min_self]

theorem best_Hr (n : Nat) (A : List Nat) : ∀ L : List Nat, best n (A ++ L) = best n (A ++ best n L) := by
  induction A with
  | nil => intro L; simp [best_idem]
  | cons a A ih =>
    intro L
    rw [List.cons_append, best_K, ih, ← best_K, ← List.cons_append]

theorem best_perm (n : Nat) (l1 l2 : List Nat) (h : l1.Perm l2) : best n l1 = best n l2 := by
  rw [best_def, best_def, S_perm_inv l1 l2 h]

theorem best_Hl (n : Nat) (A L : List Nat) : best n (A ++ L) = best n (best n A ++ L) := by
  rw [best_perm n (A ++ L) (L ++ A) List.perm_append_comm, best_Hr, best_perm n (best n A ++ L) (L ++ best n A) List.perm_append_comm]

theorem topn_flatMap (n : Nat) : ∀ shards : List (List Nat),
    best n (shards.flatMap (best n)) = best n shards.flatten := by
  intro shards
  induction shards with
  | nil => rfl
  | cons s ss ih =>
    rw [List.flatMap_cons, List.flatten_cons, ← best_Hl, best_Hr, ih, ← best_Hr]

theorem ranges_best (xs : List Nat) (s l n : Nat) (h : s + l ≤ n) :
    ranges xs s l = ((best n xs).drop s).take l := by
  unfold ranges
  rw [best_def, List.drop_take, List.take_take]
  congr 1
  omega

end TinylambOpt.ResearchProofs.BenchmarkRematch.Topn
