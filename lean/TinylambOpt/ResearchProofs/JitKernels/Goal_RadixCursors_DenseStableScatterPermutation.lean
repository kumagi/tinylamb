import TinylambOpt.ResearchGoals.JitKernels.RadixCursors
import TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_NarrowOffsetAfterRebase
import TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_BucketSliceStableOrdinals

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_DenseStableScatterPermutation

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-- Bucket bases and exclusive same-bucket ranks are a permutation of the dense
output window. The proof counts lower buckets for the base, counts preceding
same-bucket lanes for the rank, and shows that inserting one live lane shifts
exactly the destination ordinals at or above its bucket base. This lets a SQL
radix scatter reuse the same output window without holes or duplicate stores. -/
theorem sound : TinylambOpt.ResearchGoals.JitKernels.RadixCursors.DenseStableScatterPermutation.Goal := by
  have countP_mono : ∀ (l : List Nat) (a b : Nat), a ≤ b →
      l.countP (fun x => x < a) ≤ l.countP (fun x => x < b) := by
    intro l
    induction l with
    | nil => intro a b h; simp
    | cons x ys ih =>
        intro a b h
        simp only [List.countP_cons]
        have hi := ih a b h
        by_cases hxa : x < a
        · have hxb : x < b := by omega
          simp [hxa, hxb]
          omega
        · simp [hxa]
          omega
  have count_add_le : ∀ (l : List Nat) (c b : Nat), c < b →
      l.countP (fun x => x < c) + l.count c ≤ l.countP (fun x => x < b) := by
    intro l
    induction l with
    | nil => intro c b h; simp
    | cons x ys ih =>
        intro c b hcb
        simp only [List.countP_cons, List.count_cons]
        by_cases hxc : x < c
        · have hxb : x < b := by omega
          have hne : ¬ x = c := by omega
          simp [hxc, hxb, hne]
          specialize ih c b hcb
          omega
        · by_cases hxc_eq : x = c
          · subst x
            simp [hxc, hcb]
            specialize ih c b hcb
            omega
          · by_cases hxb : x < b
            · simp [hxc, hxc_eq, hxb]
              specialize ih c b hcb
              omega
            · simp [hxc, hxc_eq, hxb]
              specialize ih c b hcb
              omega
  have tail_dst : ∀ (b : Nat) (xs : List (Option Nat)) (i : Nat),
      dst (some b :: xs) (i+1) =
        (dst xs i).map (fun a => if base xs b ≤ a then a+1 else a) := by
    intro b xs i
    cases hopt : xs[i]? with
    | none => simp [dst, hopt]
    | some ob =>
        cases ob with
        | none => simp [dst, hopt]
        | some c =>
            have hbase_new :
                base (some b :: xs) c = base xs c + (if b < c then 1 else 0) := by
              unfold base active
              simp [List.countP_cons]
            have hrank_new : rank (some b :: xs) (i+1) c =
                (if b = c then 1 else 0) + rank xs i c := by
              simpa using TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_BucketSliceStableOrdinals.rank_succ_cons (some b) xs i c
            have hiff : (base xs b ≤ base xs c + rank xs i c) ↔ b ≤ c := by
              constructor
              · intro hle
                by_cases hbc : b ≤ c
                · exact hbc
                · exfalso
                  have hcb : c < b := by omega
                  have hr := TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_NarrowOffsetAfterRebase.rank_lt_count xs i c hopt
                  have hadd := count_add_le (active xs) c b hcb
                  unfold base at hle hadd
                  omega
              · intro hbc
                have hmono := countP_mono (active xs) b c hbc
                unfold base at hmono ⊢
                omega
            by_cases hbc : b ≤ c
            · by_cases hlt : b < c
              · have hne : b ≠ c := by omega
                simp [dst, hopt, hbase_new, hrank_new, hiff, hbc, hlt, hne]
                omega
              · have heq : b = c := by omega
                subst c
                simp [dst, hopt, hbase_new, hrank_new, hiff]
                omega
            · have hcb : c < b := by omega
              have hnlt : ¬ b < c := by omega
              have hne : b ≠ c := by omega
              simp [dst, hopt, hbase_new, hrank_new, hiff, hbc, hnlt, hne]
  have shift_range_perm : ∀ (t n : Nat), t ≤ n →
      (t :: (List.range n).map (fun a => if t ≤ a then a + 1 else a)).Perm
        (List.range (n + 1)) := by
    intro t n ht
    have hperm0_all : ∀ t : Nat, (t :: List.range t).Perm (List.range t ++ [t]) := by
      intro t
      induction t with
      | zero => simp
      | succ t ih =>
          rw [List.range_succ_eq_map]
          have hmap := ih.map Nat.succ
          simp only [List.map_cons, List.map_append] at hmap
          have h0 := hmap.cons 0
          exact (List.Perm.swap (t+1) 0 (List.map Nat.succ (List.range t))).symm.trans h0
    have hperm0 : (t :: List.range t).Perm (List.range t ++ [t]) := hperm0_all t
    have hrange : List.range n = List.range t ++ (List.range (n - t)).map (fun j => t + j) := by
      have hn : t + (n - t) = n := by omega
      calc
        List.range n = List.range (t + (n - t)) := by rw [hn]
        _ = List.range t ++ (List.range (n - t)).map (fun j => t + j) := by rw [List.range_add]
    have hlow : (List.range t).map (fun a => if t ≤ a then a + 1 else a) = List.range t := by
      calc
        (List.range t).map (fun a => if t ≤ a then a + 1 else a) = (List.range t).map id := by
          apply List.map_congr_left
          intro a ha
          have hat : a < t := by simpa using ha
          simp [show ¬ t ≤ a by omega]
        _ = List.range t := by simp
    have hhigh : ((List.range (n - t)).map (fun j => t + j)).map
          (fun a => if t ≤ a then a + 1 else a) =
        (List.range (n - t)).map (fun j => t + j + 1) := by
      rw [List.map_map]
      apply List.map_congr_left
      intro j hj
      simp [show t ≤ t + j by omega]
    have hmap : (List.range n).map (fun a => if t ≤ a then a + 1 else a) =
        List.range t ++ (List.range (n - t)).map (fun j => t + j + 1) := by
      rw [hrange, List.map_append, hlow, hhigh]
    rw [hmap]
    have hrange1 : List.range (n + 1) =
        List.range (t + 1) ++ (List.range (n - t)).map (fun j => (t + 1) + j) := by
      have hn1 : (t + 1) + (n - t) = n + 1 := by omega
      calc
        List.range (n + 1) = List.range ((t + 1) + (n - t)) := by rw [hn1]
        _ = List.range (t + 1) ++ (List.range (n - t)).map (fun j => (t + 1) + j) := by rw [List.range_add]
    rw [hrange1]
    rw [List.range_succ]
    have hp' := hperm0.append (List.Perm.refl ((List.range (n - t)).map (fun j => t + 1 + j)))
    simpa [List.append_assoc, Nat.succ_eq_add_one, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hp'
  intro xs
  induction xs with
  | nil => simp [addresses, active]
  | cons a xs ih =>
      cases a with
      | none =>
          have hnone0 : dst (none :: xs) 0 = none := by simp [dst]
          have hnonesucc : ∀ i, dst (none :: xs) (i+1) = dst xs i := by
            intro i
            simp [dst, base, rank, active]
          have haddr : addresses (none :: xs) = addresses xs := by
            unfold addresses
            rw [show (none :: xs).length = xs.length + 1 by simp, List.range_succ_eq_map]
            simp [List.filterMap_map, Function.comp_def, hnone0, hnonesucc]
          rw [haddr]
          simpa [active] using ih
      | some b =>
          have hhead : dst (some b :: xs) 0 = some (base xs b) := by
            simp [dst, base, rank, active]
          let t := base xs b
          let f : Nat → Nat := fun a => if t ≤ a then a + 1 else a
          have haddr : addresses (some b :: xs) = t :: (addresses xs).map f := by
            unfold addresses
            rw [show (some b :: xs).length = xs.length + 1 by simp, List.range_succ_eq_map]
            simp [List.filterMap_map, Function.comp_def, hhead, tail_dst b xs, t, f]
            rw [List.map_filterMap]
          have ht : t ≤ (active xs).length := by
            unfold t base
            simpa [t, base] using (List.countP_le_length (p := fun x : Nat => x < b) (l := active xs))
          rw [haddr]
          have hmapped := ih.map f
          have h1 : (t :: (addresses xs).map f).Perm
              (t :: (List.range (active xs).length).map f) := hmapped.cons t
          have h2 : (t :: (List.range (active xs).length).map f).Perm
              (List.range ((active xs).length + 1)) := by
            simpa [f] using shift_range_perm t (active xs).length ht
          simpa [active] using h1.trans h2

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_DenseStableScatterPermutation
