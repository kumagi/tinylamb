import TinylambOpt.ResearchProofs.StorageExecution.SE_FrameSelection_Common
import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-!
Storage Execution / Frame Selection research batch — model `SparsePlanes`.
Seven shared properties are proved once in `Common.lean` and instantiated
here; `OneHotOutlier` and `ShiftedOutlier` hold; `greedyCut` subadditivity
fails for a concrete witness.
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.SparsePlanes

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SparsePlanes

theorem testBit_zero_left : ∀ (i : Nat), Nat.testBit 0 i = false := by
  intro i; induction i with
  | zero => rfl
  | succ i ih => rw [Nat.testBit_add_one]; exact ih

theorem width_nonzero : ∀ (n : Nat), n ≠ 0 → width n = n.log2 + 1 := by
  intro n h
  cases n with
  | zero => exact absurd rfl h
  | succ m =>
      unfold width
      rw [show (Nat.succ m == 0) = false from rfl]
      simp

-- sums over mapped lists (self-contained toolkit)

theorem map_sum_zero : ∀ (l : List Nat) (f : Nat → Nat), (∀ x ∈ l, f x = 0) →
    (List.map f l).sum = 0 := by
  intro l
  induction l with
  | nil => intro f h; rfl
  | cons x xs ih =>
      intro f h
      rw [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih f (fun y hy => h y (by simp [hy])), Nat.add_zero]

theorem map_sum_add : ∀ (l : List Nat) (f g : Nat → Nat),
    (List.map (fun x => f x + g x) l).sum = (List.map f l).sum + (List.map g l).sum := by
  intro l
  induction l with
  | nil => intro f g; rfl
  | cons x xs ih =>
      intro f g
      rw [List.map_cons, List.sum_cons, ih f g, List.map_cons, List.sum_cons, List.map_cons,
        List.sum_cons]
      omega

theorem map_sum_const : ∀ (l : List Nat) (c : Nat),
    (List.map (fun _ => c) l).sum = l.length * c := by
  intro l
  induction l with
  | nil => intro c; rw [List.map_nil, List.sum_nil, List.length_nil, Nat.zero_mul]
  | cons x xs ih =>
      intro c
      rw [List.map_cons, List.sum_cons, ih, List.length_cons, Nat.succ_mul]
      omega

theorem map_sum_congr : ∀ (l : List Nat) (f g : Nat → Nat), (∀ x ∈ l, f x = g x) →
    (List.map f l).sum = (List.map g l).sum := by
  intro l
  induction l with
  | nil => intro f g h; rfl
  | cons x xs ih =>
      intro f g h
      simp only [List.map_cons, List.sum_cons, ih f g (fun y hy => h y (by simp [hy])),
        h x (by simp)]

theorem map_sum_ite1 : ∀ (N k : Nat) (f : Nat → Nat), k < N →
    (List.map (fun i => if i = k then f i else 0) (List.range N)).sum = f k := by
  intro N
  induction N with
  | zero => intro k f h; exact absurd h (by omega)
  | succ m ih =>
      intro k f h
      rw [List.range_succ, List.map_append, List.sum_append, List.map_singleton, List.sum_cons,
        List.sum_nil, Nat.add_zero]
      by_cases hk : k = m
      · have hz : ∀ i ∈ List.range m, (if i = k then f i else 0) = 0 := by
          intro i hi
          rw [List.mem_range] at hi
          rw [if_neg (by omega : i ≠ k)]
        rw [map_sum_congr (List.range m) (fun i => if i = k then f i else 0) (fun _ => 0) hz,
          map_sum_const, Nat.mul_zero, Nat.zero_add, if_pos hk.symm,
          show f m = f k from congrArg f hk.symm]
      · rw [ih k f (by omega : k < m), if_neg (fun hh => hk hh.symm), Nat.add_zero]

-- bitplane structure of `List.replicate n 0 ++ [x]`

theorem uleb_zero : uleb 0 = [0] := by simp [uleb]
theorem uleb_one : uleb 1 = [1] := by simp [uleb]

theorem zipIdx_rep_append_last : ∀ (x : Nat) (n : Nat) (s : Nat),
    ((List.replicate n 0 ++ [x]).zipIdx s) = (List.replicate n 0).zipIdx s ++ [(x, s + n)] := by
  intro x n
  induction n with
  | zero =>
      intro s
      rw [show (List.replicate 0 0 : List Nat) = [] from rfl, List.nil_append,
        show List.zipIdx ([] : List Nat) s = [] from rfl, List.nil_append, Nat.add_zero]
      rfl
  | succ m ih =>
      intro s
      rw [show List.replicate (Nat.succ m) 0 ++ [x] = 0 :: (List.replicate m 0 ++ [x]) from rfl,
        List.zipIdx_cons, ih (s + 1),
        show List.replicate (Nat.succ m) 0 = 0 :: List.replicate m 0 from rfl,
        List.zipIdx_cons, List.cons_append]
      congr 1
      · rw [show s + 1 + m = s + (m + 1) from by omega]

theorem zeros_plane : ∀ (j s n : Nat),
    ((List.replicate n 0).zipIdx s).filter (fun p : Nat × Nat => Nat.testBit p.1 j) = [] := by
  intro j s n
  induction n generalizing s with
  | zero => rfl
  | succ m ih =>
      rw [show List.replicate (Nat.succ m) 0 = 0 :: List.replicate m 0 from rfl,
        List.zipIdx_cons, List.filter_cons, ih (s + 1)]
      simp [testBit_zero_left]

theorem plane_pairs : ∀ (b j s n : Nat),
    ((List.replicate n 0 ++ [2 ^ b]).zipIdx s).filter (fun p : Nat × Nat => Nat.testBit p.1 j) =
      if j = b then [(2 ^ b, s + n)] else [] := by
  intro b j s n
  rw [zipIdx_rep_append_last, List.filter_append, zeros_plane, List.nil_append,
    List.filter_cons, List.filter_nil]
  rw [show (2 ^ b, s + n).fst = 2 ^ b from rfl, Nat.testBit_two_pow]
  by_cases h : j = b
  · rw [decide_eq_true h.symm, if_pos h]
    rfl
  · rw [decide_eq_false (fun hh => h hh.symm), if_neg h]
    rfl

private def pfn (xs : List Nat) (x : Nat) : List (Nat × Nat) :=
  (xs.zipIdx 0).filter (fun p : Nat × Nat => Nat.testBit p.1 x)

theorem onehot_pfn : ∀ (b n x : Nat),
    (uleb (pfn (List.replicate n 0 ++ [2 ^ b]) x).length ++
        (pfn (List.replicate n 0 ++ [2 ^ b]) x).flatMap (fun p : Nat × Nat => uleb p.2)).length =
      1 + (if x = b then ubytes n else 0) := by
  intro b n x
  simp only [pfn, plane_pairs b x 0 n]
  by_cases h : x = b
  · simp [h, uleb_one, List.append_nil, List.length_append, List.length_cons, List.length_nil,
      Nat.add_zero, Nat.zero_add, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len]
    try omega
  · simp [h, uleb_zero, List.append_nil, List.length_cons, List.length_nil, Nat.add_zero,
    Nat.zero_add]
    try omega

theorem plane_fn_onehot (n b : Nat) :
    (fun a => ((fun x =>
        have ps :=
          List.filter ((fun (v, _) => Nat.testBit v x) : Nat × Nat → Bool)
            ((List.replicate n 0 ++ [2 ^ b]).zipIdx 0);
        uleb ps.length ++ ps.flatMap (fun (_, i) => uleb i)) a).length) =
      (fun x => 1 + (if x = b then ubytes n else 0)) := by
  funext a
  show (uleb (pfn (List.replicate n 0 ++ [2 ^ b]) a).length ++
        (pfn (List.replicate n 0 ++ [2 ^ b]) a).flatMap (fun p : Nat × Nat => uleb p.2)).length =
      1 + (if a = b then ubytes n else 0)
  exact onehot_pfn b n a

theorem hpeak : ∀ (b n : Nat), peak (List.replicate n 0 ++ [2 ^ b]) = 2 ^ b := by
  intro b n
  have hmax : List.max? (List.replicate n 0 ++ [2 ^ b]) = some (2 ^ b) :=    List.max?_eq_some_iff.mpr ⟨List.mem_append_right _ (List.mem_singleton (a := 2 ^ b) |>.mpr rfl),
      fun y hy => by
        rw [List.mem_append] at hy
        rcases hy with hy | hy
        · rw [List.mem_replicate] at hy
          obtain ⟨_, rfl⟩ := hy
          exact Nat.zero_le _
        · simp only [List.mem_singleton] at hy
          exact Nat.le_of_eq hy⟩
  show (List.max? (List.replicate n 0 ++ [2 ^ b])).getD 0 = 2 ^ b
  rw [hmax, Option.getD_some]

theorem sound_OneHotOutlier : OneHotOutlier.Goal := by
  intro n b
  rw [SparsePlanes.write]
  rw [hpeak]
  rw [width_nonzero _ (Nat.ne_of_gt (Nat.pow_pos (by decide : (0 : Nat) < 2))),
    Nat.log2_two_pow]
  rw [List.length_append, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len, List.length_flatMap]
  rw [plane_fn_onehot n b]
  have hsplit : (List.map (fun x => 1 + (if x = b then ubytes n else 0))
      (List.range (b + 1))).sum =
      (List.map (fun _ => 1) (List.range (b + 1))).sum +
        (List.map (fun x => if x = b then ubytes n else 0) (List.range (b + 1))).sum := by
    rw [show (fun x => 1 + (if x = b then ubytes n else 0)) =
        (fun x => (fun _ => 1) x + (fun x => if x = b then ubytes n else 0) x) from rfl]
    exact map_sum_add (List.range (b + 1)) (fun _ => 1)
      (fun x => if x = b then ubytes n else 0)
  rw [hsplit, map_sum_const, List.length_range, Nat.mul_one,
    map_sum_ite1 (b + 1) b (fun _ => ubytes n) (by omega : b < b + 1)]
  omega

theorem sp_bit : ∀ (a b j : Nat),
    Nat.testBit (2 ^ a + 2 ^ (a + b + 1)) j =
      if j = a then true else if j = a + b + 1 then true else false := by
  intro a b j
  rw [show 2 ^ a + 2 ^ (a + b + 1) = 2 ^ a * (1 + 2 ^ (b + 1)) from by
        rw [show a + b + 1 = a + (b + 1) from by omega, Nat.pow_add, Nat.mul_add, Nat.mul_one]]
  rw [(Nat.add_zero (2 ^ a * (1 + 2 ^ (b + 1)))).symm]
  rw [Nat.testBit_two_pow_mul_add (1 + 2 ^ (b + 1)) (b := 0) (i := a)
      (Nat.pow_pos (by decide : (0 : Nat) < 2)) j]
  rw [show (1 + 2 ^ (b + 1) : Nat) = 2 ^ (b + 1) * 1 + 1 from by
        rw [Nat.add_comm, Nat.mul_one]]
  rw [Nat.testBit_two_pow_mul_add 1 (b := 1) (i := b + 1)
      (Nat.one_lt_two_pow (by omega : (b + 1) ≠ 0)) (j := j - a)]
  rw [show (1 : Nat).testBit (j - a) = decide (0 = j - a) from by
        rw [show (1 : Nat) = 2 ^ 0 from rfl, Nat.testBit_two_pow],
      show (1 : Nat).testBit (j - a - (b + 1)) = decide (0 = j - a - (b + 1)) from by
        rw [show (1 : Nat) = 2 ^ 0 from rfl, Nat.testBit_two_pow]]
  by_cases hj : j < a
  · rw [if_pos hj]
    have h1 : j ≠ a := by omega
    have h2 : j ≠ a + b + 1 := by omega
    simp [h1, h2]
  · rw [if_neg hj]
    by_cases ha : j = a
    · subst ha
      rw [Nat.sub_self]
      simp
    · by_cases hb : j = a + b + 1
      · subst hb
        rw [show a + b + 1 - a = b + 1 from by omega]
        simp
      · by_cases hc : j - a < b + 1
        · rw [if_pos hc, decide_eq_false (by omega : ¬(0 = j - a)), if_neg ha, if_neg hb]
        · rw [if_neg hc, decide_eq_false (by omega : ¬(0 = j - a - (b + 1))), if_neg ha,
            if_neg hb]

theorem plane_pairs_shift : ∀ (a b j s n : Nat),
    ((List.replicate n 0 ++ [2 ^ a + 2 ^ (a + b + 1)]).zipIdx s).filter
        (fun p : Nat × Nat => Nat.testBit p.1 j) =
      if j = a then [(2 ^ a + 2 ^ (a + b + 1), s + n)]
      else if j = a + b + 1 then [(2 ^ a + 2 ^ (a + b + 1), s + n)] else [] := by
  intro a b j s n
  rw [zipIdx_rep_append_last, List.filter_append, zeros_plane, List.nil_append,
    List.filter_cons, List.filter_nil]
  rw [show (2 ^ a + 2 ^ (a + b + 1), s + n).fst = 2 ^ a + 2 ^ (a + b + 1) from rfl, sp_bit]
  by_cases h1 : j = a <;> by_cases h2 : j = a + b + 1 <;>
    simp [h1, h2, show ¬(a = a + b + 1) from by omega]

theorem shifted_pfn : ∀ (a b n x : Nat),
    (uleb (pfn (List.replicate n 0 ++ [2 ^ a + 2 ^ (a + b + 1)]) x).length ++
        (pfn (List.replicate n 0 ++ [2 ^ a + 2 ^ (a + b + 1)]) x).flatMap
          (fun p : Nat × Nat => uleb p.2)).length =
      1 + (if x = a then ubytes n else 0) + (if x = a + b + 1 then ubytes n else 0) := by
  intro a b n x
  simp only [pfn, plane_pairs_shift a b x 0 n]
  by_cases h1 : x = a
  · simp [h1, (show ¬(a = a + b + 1) from by omega), uleb_one, List.append_nil,
      List.length_append, List.length_cons, List.length_nil, Nat.add_zero, Nat.zero_add,
      TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len]
    try omega
  · by_cases h2 : x = a + b + 1
    · simp [(show ¬(a + b + 1 = a) from by omega), h2, uleb_one, List.append_nil,
        List.length_append, List.length_cons, List.length_nil, Nat.add_zero, Nat.zero_add,
        TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len]
      try omega
    · simp [h1, h2, uleb_zero, List.append_nil, List.length_cons, List.length_nil,
        Nat.add_zero, Nat.zero_add]
      try omega

theorem plane_fn_shifted (a b n : Nat) :
    (fun c => ((fun x =>
        have ps :=
          List.filter ((fun (v, _) => Nat.testBit v x) : Nat × Nat → Bool)
            ((List.replicate n 0 ++ [2 ^ a + 2 ^ (a + b + 1)]).zipIdx 0);
        uleb ps.length ++ ps.flatMap (fun (_, i) => uleb i)) c).length) =
      (fun x => 1 + (if x = a then ubytes n else 0) +
        (if x = a + b + 1 then ubytes n else 0)) := by
  funext x
  show (uleb (pfn (List.replicate n 0 ++ [2 ^ a + 2 ^ (a + b + 1)]) x).length ++
        (pfn (List.replicate n 0 ++ [2 ^ a + 2 ^ (a + b + 1)]) x).flatMap
          (fun p : Nat × Nat => uleb p.2)).length =
      1 + (if x = a then ubytes n else 0) + (if x = a + b + 1 then ubytes n else 0)
  exact shifted_pfn a b n x

theorem hpeak_shift : ∀ (a b n : Nat),
    peak (List.replicate n 0 ++ [2 ^ a + 2 ^ (a + b + 1)]) = 2 ^ a + 2 ^ (a + b + 1) := by
  intro a b n
  have hmax : List.max? (List.replicate n 0 ++ [2 ^ a + 2 ^ (a + b + 1)]) =
      some (2 ^ a + 2 ^ (a + b + 1)) :=
    List.max?_eq_some_iff.mpr ⟨List.mem_append_right _
        (List.mem_singleton (a := 2 ^ a + 2 ^ (a + b + 1)) |>.mpr rfl),
      fun y hy => by
        rw [List.mem_append] at hy
        rcases hy with hy | hy
        · rw [List.mem_replicate] at hy
          obtain ⟨_, rfl⟩ := hy
          exact Nat.zero_le _
        · simp only [List.mem_singleton] at hy
          exact Nat.le_of_eq hy⟩
  show (List.max? (List.replicate n 0 ++ [2 ^ a + 2 ^ (a + b + 1)])).getD 0 =
    2 ^ a + 2 ^ (a + b + 1)
  rw [hmax, Option.getD_some]

theorem sound_ShiftedOutlier : ShiftedOutlier.Goal := by
  intro a b n
  have hX0 : 2 ^ a + 2 ^ (a + b + 1) ≠ 0 := by
    intro hc
    have hpos : 0 < 2 ^ a := Nat.pow_pos (by decide : (0 : Nat) < 2)
    have hle : 2 ^ a ≤ 2 ^ a + 2 ^ (a + b + 1) := Nat.le_add_right _ _
    rw [hc] at hle
    omega
  have hlt : 2 ^ a + 2 ^ (a + b + 1) < 2 ^ (a + b + 2) := calc
    2 ^ a + 2 ^ (a + b + 1)
        < 2 ^ (a + b + 1) + 2 ^ (a + b + 1) :=
          Nat.add_lt_add_right
            (Nat.pow_lt_pow_right (by decide : 1 < 2) (by omega : a < a + b + 1)) _
    _ = 2 ^ (a + b + 2) := by
          show 2 ^ (a + b + 1) + 2 ^ (a + b + 1) = 2 ^ (a + b + 1) * 2
          rw [Nat.mul_two]
  have hlog : (2 ^ a + 2 ^ (a + b + 1)).log2 = a + b + 1 :=
    Nat.log2_eq_iff (h := hX0) |>.mpr ⟨Nat.le_add_left _ _, hlt⟩
  rw [SparsePlanes.write]
  rw [hpeak_shift]
  rw [width_nonzero _ hX0, hlog]
  rw [show (a + b + 1) + 1 = a + b + 2 from rfl]
  rw [List.length_append, TinylambOpt.ResearchProofs.StorageExecution.SECommon.uleb_len, List.length_flatMap]
  rw [plane_fn_shifted a b n]
  have hsplit : (List.map (fun x => 1 + (if x = a then ubytes n else 0) +
      (if x = a + b + 1 then ubytes n else 0)) (List.range (a + b + 2))).sum =
      (List.map (fun _ => 1) (List.range (a + b + 2))).sum +
        ((List.map (fun x => if x = a then ubytes n else 0) (List.range (a + b + 2))).sum +
          (List.map (fun x => if x = a + b + 1 then ubytes n else 0)
            (List.range (a + b + 2))).sum) := by
    rw [map_sum_add (List.range (a + b + 2)) (fun x => 1 + (if x = a then ubytes n else 0))
        (fun x => if x = a + b + 1 then ubytes n else 0),
      map_sum_add (List.range (a + b + 2)) (fun _ => 1)
        (fun x => if x = a then ubytes n else 0), Nat.add_assoc]
  rw [hsplit, map_sum_const, List.length_range, Nat.mul_one,
    map_sum_ite1 (a + b + 2) a (fun _ => ubytes n) (by omega : a < a + b + 2),
    map_sum_ite1 (a + b + 2) (a + b + 1) (fun _ => ubytes n)
      (by omega : a + b + 1 < a + b + 2)]
  omega

theorem sound_ByteFrontier : ByteFrontier.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.byteFrontier_gen write
theorem sound_StrictSavings : StrictSavings.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.strictSavings_holds write
theorem sound_HotPointReads : HotPointReads.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.hotPointReads_holds write
theorem sound_SeekCap : SeekCap.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.seekCap_holds write
theorem sound_ParallelEncode : ParallelEncode.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.parallelEncode_holds write
theorem sound_MandatoryCuts : MandatoryCuts.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.mandatoryCuts_holds write
theorem sound_BoundedRaw : BoundedRaw.Goal := TinylambOpt.ResearchProofs.StorageExecution.SECommon.boundedRaw_gen write

theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro hall
  exact absurd (hall [0] [0, 128] [255]) (by native_decide)

end TinylambOpt.ResearchProofs.StorageExecution.FrameSelection.SparsePlanes
