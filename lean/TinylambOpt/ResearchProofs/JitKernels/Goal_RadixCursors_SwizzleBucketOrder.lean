import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
Swapping adjacent lanes cannot change the multiset of emitted scatter addresses:
bucket bases depend only on bucket counts, ranks on the same bucket commute, and
inactive lanes emit no address. Permutation induction then proves that any lane
swizzle preserves the address multiset. SQL may therefore reorder a radix
scatter vector before histogram construction without changing which rows land
in which destination slots.
-/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_SwizzleBucketOrder

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

set_option maxHeartbeats 0

theorem filterMap_range_congr {β : Type} (f g : Nat → Option β) : ∀ n : Nat,
    (∀ i, i < n → f i = g i) →
    (List.range n).filterMap f = (List.range n).filterMap g
  | 0, h => by simp
  | n + 1, h => by
    simp only [List.range_succ_eq_map, List.filterMap_cons, List.filterMap_map]
    rw [filterMap_range_congr (f ∘ Nat.succ) (g ∘ Nat.succ) n
      (fun i hi => by simpa [Function.comp_def] using h (i+1) (by omega))]
    simp [h 0 (by omega)]

theorem option_toList_perm {α : Type} (o1 o2 : Option α) (t : List α) :
    (o1.toList ++ o2.toList ++ t).Perm (o2.toList ++ o1.toList ++ t) := by
  cases o1 with
  | none => cases o2 <;> simp
  | some a =>
    cases o2 with
    | none => simp
    | some b =>
      simp
      exact List.Perm.swap b a t

theorem addresses_append_cons_cons (pre : List (Option Nat)) (x y : Option Nat)
    (l : List (Option Nat)) :
    addresses (pre ++ x::y::l) =
      (List.range pre.length).filterMap (fun i => dst (pre ++ x::y::l) i) ++
      (dst (pre ++ x::y::l) pre.length).toList ++
      (dst (pre ++ x::y::l) (pre.length + 1)).toList ++
      (List.range l.length).filterMap (fun i => dst (pre ++ x::y::l) (pre.length + (i+2))) := by
  unfold addresses
  rw [show (pre ++ x :: y :: l).length = pre.length + (l.length + 2) by simp]
  rw [List.range_add]
  rw [List.filterMap_append]
  rw [List.filterMap_map]
  rw [show l.length + 2 = (l.length + 1) + 1 by omega]
  rw [List.range_succ_eq_map]
  rw [List.filterMap_cons]
  rw [List.filterMap_map]
  rw [List.range_succ_eq_map]
  rw [List.filterMap_cons]
  rw [List.filterMap_map]
  cases h0 : dst (pre ++ x :: y :: l) pre.length <;>
    cases h1 : dst (pre ++ x :: y :: l) (pre.length + 1) <;>
    simp [h0, h1, Function.comp_def, Nat.add_comm, Nat.add_left_comm]

theorem dst_prefix_swap (pre : List (Option Nat)) (x y : Option Nat)
    (l : List (Option Nat)) (i : Nat) (hi : i < pre.length) :
    dst (pre ++ x::y::l) i = dst (pre ++ y::x::l) i := by
  have hle : i ≤ pre.length := Nat.le_of_lt hi
  cases x with
  | none =>
    cases y <;> simp [dst, base, rank, active, List.getElem?_append, hi,
      List.take_append_of_le_length hle, List.countP_append, List.count_append,
      Nat.add_comm, Nat.add_assoc]
  | some p =>
    cases y with
    | none =>
      simp [dst, base, rank, active, List.getElem?_append, hi,
        List.take_append_of_le_length hle, List.countP_append, List.count_append,
        Nat.add_comm, Nat.add_left_comm]
    | some q =>
      simp [dst, base, rank, active, List.getElem?_append, hi,
        List.take_append_of_le_length hle, List.countP_append, List.count_append,
        List.countP_cons, List.count_cons, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

theorem dst_pre_tail_swap (pre : List (Option Nat)) (x y : Option Nat)
    (l : List (Option Nat)) (i : Nat) :
    dst (pre ++ x::y::l) (pre.length + (i+2)) =
      dst (pre ++ y::x::l) (pre.length + (i+2)) := by
  have hlt : ¬ i + (pre.length + 2) < pre.length := by omega
  have hlt' : ¬ pre.length + (i + 2) < pre.length := by omega
  have hsub : i + (pre.length + 2) - pre.length = i + 2 := by omega
  have hsub' : pre.length + (i + 2) - pre.length = i + 2 := by omega
  cases x with
  | none =>
    cases y <;> simp [dst, base, rank, active, List.countP_append, List.count_append,
      List.getElem?_append, List.take_append, hlt, hlt', hsub, hsub']
  | some p =>
    cases y with
    | none =>
      simp [dst, base, rank, active, List.countP_append, List.count_append,
        List.getElem?_append, List.take_append, hlt', hsub']
    | some q =>
      simp [dst, base, rank, active, List.countP_append, List.count_append,
        List.countP_cons, List.count_cons, List.getElem?_append, List.take_append,
        hlt, hsub, Nat.add_comm, Nat.add_left_comm]

theorem dst_pre_head01 (pre : List (Option Nat)) (p q : Nat)
    (l : List (Option Nat)) (hpq : p ≠ q) :
    dst (pre ++ some p :: some q :: l) pre.length =
      dst (pre ++ some q :: some p :: l) (pre.length + 1) := by
  have hlt : ¬ pre.length + 1 < pre.length := by omega
  have hsub : pre.length + 1 - pre.length = 1 := by omega
  have htake : List.take (pre.length + 1) pre = pre := List.take_of_length_le (by omega)
  have hqp : q ≠ p := Ne.symm hpq
  simp [dst, base, rank, active, List.take_append, List.countP_append,
    List.count_append, List.countP_cons, List.count_cons, hlt, hsub, htake, hqp,
    Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

theorem dst_pre_head10 (pre : List (Option Nat)) (p q : Nat)
    (l : List (Option Nat)) (hpq : p ≠ q) :
    dst (pre ++ some p :: some q :: l) (pre.length + 1) =
      dst (pre ++ some q :: some p :: l) pre.length := by
  have hlt : ¬ pre.length + 1 < pre.length := by omega
  have hsub : pre.length + 1 - pre.length = 1 := by omega
  have htake : List.take (pre.length + 1) pre = pre := List.take_of_length_le (by omega)
  have hqp : q ≠ p := Ne.symm hpq
  simp [dst, base, rank, active, List.take_append, List.countP_append,
    List.count_append, List.countP_cons, List.count_cons, hlt, hsub, htake, hpq,
    Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

theorem addresses_swap_pre (pre : List (Option Nat)) (x y : Option Nat)
    (l : List (Option Nat)) :
    (addresses (pre ++ x::y::l)).Perm (addresses (pre ++ y::x::l)) := by
  rw [addresses_append_cons_cons, addresses_append_cons_cons]
  have hpre :
      (List.range pre.length).filterMap (fun i => dst (pre ++ x :: y :: l) i) =
      (List.range pre.length).filterMap (fun i => dst (pre ++ y :: x :: l) i) :=
    filterMap_range_congr _ _ pre.length (fun i hi => dst_prefix_swap pre x y l i hi)
  have htail :
      (List.range l.length).filterMap (fun i => dst (pre ++ x :: y :: l) (pre.length + (i+2))) =
      (List.range l.length).filterMap (fun i => dst (pre ++ y :: x :: l) (pre.length + (i+2))) :=
    filterMap_range_congr _ _ l.length (fun i hi => dst_pre_tail_swap pre x y l i)
  rw [hpre, htail]
  have htake : List.take (pre.length + 1) pre = pre := List.take_of_length_le (by omega)
  cases x with
  | none =>
    cases y with
    | none => simp
    | some p => simp [dst, base, rank, active, List.take_append, List.countP_append, htake]
  | some p =>
    cases y with
    | none => simp [dst, base, rank, active, List.take_append, List.countP_append, htake]
    | some q =>
      by_cases hpq : p = q
      · subst q
        simp
      · have h01 := dst_pre_head01 pre p q l hpq
        have h10 := dst_pre_head10 pre p q l hpq
        rw [h01, h10]
        have hmid := option_toList_perm
          (dst (pre ++ some q :: some p :: l) pre.length)
          (dst (pre ++ some q :: some p :: l) (pre.length + 1))
          ((List.range l.length).filterMap (fun i => dst (pre ++ some q :: some p :: l) (pre.length + (i+2))))
        simpa [List.append_assoc] using
          (List.Perm.append_left
            ((List.range pre.length).filterMap (fun i => dst (pre ++ some q :: some p :: l) i)) hmid.symm)

theorem sound : TinylambOpt.ResearchGoals.JitKernels.RadixCursors.SwizzleBucketOrder.Goal := by
  intro a b hab
  have h : ∀ pre, (addresses (pre ++ a)).Perm (addresses (pre ++ b)) := by
    induction hab with
    | nil => intro pre; simp
    | cons x h ih =>
      intro pre
      simpa [List.append_assoc] using ih (pre ++ [x])
    | swap x y l =>
      intro pre
      simpa [List.nil_append] using (addresses_swap_pre pre x y l).symm
    | trans h1 h2 ih1 ih2 =>
      intro pre
      exact (ih1 pre).trans (ih2 pre)
  simpa using h []

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_SwizzleBucketOrder
