import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFootersB

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ## Generic consequences of a stitching fold -/

section Generic

variable {σ : Type} (f : Fold σ)

private theorem foldl_reference (h : stitch f) (l : List (List Nat)) (acc : List Nat) :
    List.foldl f.join (f.reference acc) (l.map f.reference) =
      f.reference (acc ++ l.flatten) := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.foldl, List.flatten_cons]
      rw [h acc p, ih (acc ++ p)]
      simp only [List.append_assoc]

private theorem combine_reference (h : stitch f) (ps : List (List Nat)) :
    combine f ps = f.reference ps.flatten := by
  simpa [combine] using foldl_reference f h ps []

private theorem treeReduce_of_stitch (h : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b iha ihb =>
      change f.join (rootState f a) (rootState f b) =
        f.reference (plain a ++ plain b)
      rw [iha, ihb, h]

private theorem flatten_replicate_add (n m : Nat) (xs : List Nat) :
    (List.replicate (n + m) xs).flatten =
      (List.replicate n xs).flatten ++ (List.replicate m xs).flatten := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 + m = (n + m) + 1 by omega, List.replicate_succ,
        List.replicate_succ, List.flatten_cons, ih, List.flatten_cons]
      exact (List.append_assoc _ _ _).symm

private theorem flatten_replicate_double (n : Nat) (xs : List Nat) :
    (List.replicate (n * 2) xs).flatten =
      (List.replicate n xs).flatten ++ (List.replicate n xs).flatten := by
  rw [← flatten_replicate_add]
  apply congrArg List.flatten
  apply congrArg (fun n => List.replicate n xs)
  omega

private theorem flatten_replicate_rotate (n : Nat) (xs : List Nat) :
    (List.replicate n xs).flatten ++ xs = xs ++ (List.replicate n xs).flatten := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate_succ, List.flatten_cons]
      calc
        (xs ++ (List.replicate n xs).flatten) ++ xs =
            xs ++ ((List.replicate n xs).flatten ++ xs) := List.append_assoc _ _ _
        _ = xs ++ (xs ++ (List.replicate n xs).flatten) := by rw [ih]

private theorem flatten_replicate_double_succ (n : Nat) (xs : List Nat) :
    ((List.replicate n xs).flatten ++ (List.replicate n xs).flatten) ++ xs =
      (List.replicate (n * 2 + 1) xs).flatten := by
  rw [show n * 2 + 1 = (n + n) + 1 by omega, List.replicate_succ,
    List.flatten_cons, flatten_replicate_add]
  let a := (List.replicate n xs).flatten
  calc
    (a ++ a) ++ xs = a ++ (a ++ xs) := List.append_assoc _ _ _
    _ = a ++ (xs ++ a) := by rw [flatten_replicate_rotate]
    _ = xs ++ (a ++ a) := by
      rw [← List.append_assoc, flatten_replicate_rotate, List.append_assoc]

private theorem repeatPower_of_stitch (h : stitch f) : repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ k ih =>
      by_cases hk : k = 0
      · subst k
        simp [repeatState]
      · rw [repeatState.eq_1, dite_eq_right hk]
        have hdiv : k / 2 < k := Nat.div_lt_self (by omega) (by decide)
        have ih2 := ih (k / 2) hdiv
        have hsq : f.join (repeatState f (f.reference xs) (k / 2))
              (repeatState f (f.reference xs) (k / 2)) =
            f.reference ((List.replicate (k / 2) xs).flatten ++
              (List.replicate (k / 2) xs).flatten) := by
          simp only [ih2]
          exact h _ _
        have hdecomp : k / 2 * 2 + k % 2 = k := by
          rw [Nat.mul_comm]
          exact Nat.div_add_mod k 2
        by_cases he : k % 2 = 0
        · simp only [he, beq_self_eq_true, ite_true]
          rw [hsq]
          apply congrArg f.reference
          calc
            (List.replicate (k / 2) xs).flatten ++
                (List.replicate (k / 2) xs).flatten =
                (List.replicate (k / 2 * 2) xs).flatten :=
                  (flatten_replicate_double (k / 2) xs).symm
            _ = (List.replicate k xs).flatten :=
                  congrArg (fun n => (List.replicate n xs).flatten)
                    (by simpa [he] using hdecomp)
        · have ho : k % 2 = 1 := by omega
          simp only [ho, show (1 == 0) = false by decide, Bool.false_eq_true, ite_false]
          rw [hsq, h]
          apply congrArg f.reference
          calc
            ((List.replicate (k / 2) xs).flatten ++
                (List.replicate (k / 2) xs).flatten) ++ xs =
                (List.replicate (k / 2 * 2 + 1) xs).flatten :=
                  flatten_replicate_double_succ (k / 2) xs
            _ = (List.replicate k xs).flatten :=
                  congrArg (fun n => (List.replicate n xs).flatten)
                    (by simpa [ho] using hdecomp)

private theorem slice_concat (a p z : List Nat) (lo hi : Nat) (hlo : lo ≤ a.length) :
    a.drop lo ++ p ++ z.take hi =
      ((a ++ p ++ z).drop lo).take (a.length - lo + p.length + hi) := by
  rw [show a ++ p ++ z = a ++ (p ++ z) by simp only [List.append_assoc],
    List.drop_append_of_le_length hlo, List.take_append]
  have hd : (a.drop lo).length = a.length - lo := by simp
  rw [hd]
  have hN : a.length - lo + p.length + hi - (a.length - lo) = p.length + hi := by omega
  rw [hN, List.take_append]
  have hp : p.length + hi - p.length = hi := by omega
  rw [hp]
  have htakeA : List.take (a.length - lo + p.length + hi) (a.drop lo) = a.drop lo :=
    List.take_of_length_le (by simp [hd]; omega)
  have htakeP : List.take (p.length + hi) p = p :=
    List.take_of_length_le (by omega)
  rw [htakeA, htakeP]
  exact List.append_assoc _ _ _

private theorem frameSlice_of_stitch (h : stitch f) : frameSlice f := by
  intro a ps z lo hi hlo
  rw [combine_reference f h, h, h]
  refine congrArg f.reference ?_
  simpa only [List.append_assoc] using slice_concat a ps.flatten z lo hi hlo

private theorem prefixCheckpoint_of_stitch (h : stitch f) : prefixCheckpoint f := by
  intro init ps
  apply List.ext_getElem
  · simp
  · intro i hi₁ hi₂
    rw [List.getElem_map (fun i => f.reference (init ++ (ps.take i).flatten)),
      List.getElem_range, List.getElem_scanl]
    rw [← List.map_take]
    exact foldl_reference f h (ps.take i) init

private theorem flatten_set (ps : List (List Nat)) (i : Nat) (xs : List Nat)
    (hi : i < ps.length) :
    (ps.set i xs).flatten =
      (ps.take i).flatten ++ xs ++ (ps.drop (i + 1)).flatten := by
  induction ps generalizing i with
  | nil => simp at hi
  | cons p ps ih =>
      cases i with
      | zero => simp
      | succ j =>
          have hj : j < ps.length := by simpa using hi
          simp only [List.set_cons_succ, List.take_succ_cons, List.drop_succ_cons,
            List.flatten_cons, ih j hj, List.append_assoc]

private theorem amendLeaf_of_stitch (h : stitch f) : amendLeaf f := by
  intro ps i xs hi
  simp only [combine_reference f h]
  rw [flatten_set ps i xs hi, h, h]
  exact congrArg f.reference (by simp only [List.append_assoc])

private theorem runInput_of_stitch (h : stitch f) : runInput f := by
  intro rs
  have gen : ∀ (l : List (Nat × Nat)) (acc : List Nat),
      List.foldl f.join (f.reference acc)
        (l.map (fun (v, n) => repeatState f (f.reference [v]) n)) =
      f.reference (acc ++ l.flatMap (fun (v, n) => List.replicate n v)) := by
    intro l
    induction l with
    | nil => intro acc; simp
    | cons r rs ih =>
        obtain ⟨v, n⟩ := r
        intro acc
        simp only [List.map_cons, List.foldl, List.flatMap_cons]
        rw [repeatPower_of_stitch f h]
        have hflat : (List.replicate n [v]).flatten = List.replicate n v := by
          induction n with
          | zero => simp
          | succ n ihn => simp [List.replicate_succ, ihn]
        rw [hflat, h, ih]
        simp only [List.append_assoc]
  simpa using gen rs []

private theorem reference_append (h : stitch f) (a b : List Nat) :
    f.reference (a ++ b) = f.join (f.reference a) (f.reference b) := (h a b).symm

private theorem reference_join_assoc (h : stitch f) (a b c : List Nat) :
    f.join (f.join (f.reference a) (f.reference b)) (f.reference c) =
      f.join (f.reference a) (f.join (f.reference b) (f.reference c)) := by
  rw [← reference_append f h a b, ← reference_append f h b c,
    ← reference_append f h (a ++ b) c, ← reference_append f h a (b ++ c)]
  exact congrArg f.reference (List.append_assoc _ _ _)

private theorem reorderNeighbors_of_stitch (h : stitch f) : reorderNeighbors f := by
  intro pre a b suf hcomm
  have hab : f.reference (a ++ b) = f.join (f.reference a) (f.reference b) :=
    reference_append f h a b
  have hba : f.reference (b ++ a) = f.join (f.reference b) (f.reference a) :=
    reference_append f h b a
  calc
    f.reference (pre ++ a ++ b ++ suf) =
        f.join (f.reference (pre ++ (a ++ b))) (f.reference suf) := by
          rw [show pre ++ a ++ b ++ suf = (pre ++ (a ++ b)) ++ suf by
            simp only [List.append_assoc]]
          rw [reference_append f h (pre ++ (a ++ b)) suf]
    _ = f.join (f.reference pre)
          (f.join (f.reference (a ++ b)) (f.reference suf)) := by
            rw [reference_append f h pre (a ++ b)]
            exact reference_join_assoc f h pre (a ++ b) suf
    _ = f.join (f.reference pre)
          (f.join (f.join (f.reference a) (f.reference b)) (f.reference suf)) := by
            rw [hab]
    _ = f.join (f.reference pre)
          (f.join (f.join (f.reference b) (f.reference a)) (f.reference suf)) := by
            rw [hcomm]
    _ = f.join (f.reference pre)
          (f.join (f.reference (b ++ a)) (f.reference suf)) := by rw [← hba]
    _ = f.join (f.reference (pre ++ (b ++ a))) (f.reference suf) := by
            rw [reference_append f h pre (b ++ a)]
            exact (reference_join_assoc f h pre (b ++ a) suf).symm
    _ = f.reference (pre ++ b ++ a ++ suf) := by
          rw [show pre ++ b ++ a ++ suf = (pre ++ (b ++ a)) ++ suf by
            simp only [List.append_assoc]]
          rw [reference_append f h (pre ++ (b ++ a)) suf]

end Generic

/-! ## RollingFingerprint -/

namespace RollingFingerprint

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

private def M : Nat := 65521
private def B : Nat := 257
private def hpow (xs : List Nat) : Nat := B ^ xs.length % M
private def hval (xs : List Nat) : Nat := xs.foldl (fun h x => (B * h + x) % M) 0

private theorem hpow_append (a b : List Nat) : hpow (a ++ b) = hpow a * hpow b % M := by
  simp [hpow, List.length_append, Nat.pow_add, Nat.mul_mod]

private theorem hval_snoc (xs : List Nat) (x : Nat) : hval (xs ++ [x]) = (B * hval xs + x) % M := by
  simp [hval, List.foldl_append]

private theorem hpow_snoc (xs : List Nat) (x : Nat) : hpow (xs ++ [x]) = hpow xs * (B % M) % M := by
  simp [hpow, List.length_append, Nat.pow_add, Nat.pow_one, Nat.mul_mod, Nat.mul_comm]

private theorem hval_lt (xs : List Nat) : hval xs < M := by
  unfold hval
  suffices ∀ (l : List Nat) (acc : Nat), acc < M → List.foldl (fun h x => (B * h + x) % M) acc l < M from
    this xs 0 (by decide)
  intro l; induction l with
  | nil => intro acc h; exact h
  | cons x xs ih => intro acc h; exact ih _ (Nat.mod_lt _ (by decide))

private theorem hval_append (a b : List Nat) : hval (a ++ b) = (hval a * hpow b % M + hval b) % M := by
  induction b using List.reverseRecOn with
  | nil =>
      simp [hval, hpow]
      rw [Nat.mod_eq_of_lt (hval_lt a)]
  | append_singleton bs x ih =>
      rw [List.append_assoc, hval_snoc, ih, hval_snoc, hpow_snoc]
      have hM : 0 < M := by decide
      refine Nat.eq_of_mod_eq_of_lt ?_ (Nat.mod_lt _ hM)
      calc (B * ((hval a * hpow bs % M + hval bs) % M) + x) % M
          = (B * (hval a * hpow bs % M + hval bs) + x) % M := by
              rw [Nat.add_mod, Nat.mul_mod, Nat.mod_mod, ← Nat.mul_mod, ← Nat.add_mod]
        _ = (hval a * hpow bs % M * B + B * hval bs + x) % M := by ring_nf
        _ = (hval a * hpow bs * B + B * hval bs + x) % M := by
              rw [Nat.add_mod, Nat.add_mod (hval a * hpow bs % M * B) _, Nat.mul_mod, Nat.mul_mod (hval a * hpow bs % M) _, Nat.mod_mod, ← Nat.mul_mod, ← Nat.mul_mod, ← Nat.add_mod, ← Nat.add_mod]
        _ = (hval a * (hpow bs * B) + (B * hval bs + x)) % M := by ring_nf
        _ = (hval a * (hpow bs * (B % M) % M) % M + (B * hval bs + x) % M) % M := by
              symm
              calc (hval a * (hpow bs * (B % M) % M) % M + (B * hval bs + x) % M) % M
                  = (hval a * (hpow bs * (B % M) % M) + (B * hval bs + x)) % M := by
                      rw [Nat.add_mod, Nat.mod_mod, Nat.mod_mod, ← Nat.add_mod]
                _ = (hval a * (hpow bs * B) + (B * hval bs + x)) % M := by
                      rw [Nat.add_mod (hval a * (hpow bs * (B % M) % M)) _, Nat.mul_mod, Nat.mod_mod,
                          Nat.mul_mod (hpow bs) (B % M), Nat.mod_mod, ← Nat.mul_mod, ← Nat.mul_mod, ← Nat.add_mod]

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg]
  exact Prod.ext (hpow_append a b) (hval_append a b)

theorem sound_RollingFingerprint_Stitch :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint.Stitch.Goal :=
  stitch_agg

theorem sound_RollingFingerprint_TreeReduce :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint.TreeReduce.Goal :=
  treeReduce_of_stitch _ stitch_agg

theorem sound_RollingFingerprint_RepeatPower :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint.RepeatPower.Goal :=
  repeatPower_of_stitch _ stitch_agg

theorem sound_RollingFingerprint_FrameSlice :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint.FrameSlice.Goal :=
  frameSlice_of_stitch _ stitch_agg

theorem sound_RollingFingerprint_PrefixCheckpoint :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch _ stitch_agg

theorem sound_RollingFingerprint_EqualWindowResidues :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint.EqualWindowResidues.Goal := by
  intro a b pre z hlen hres
  have h1 := stitch_agg pre a; have h2 := stitch_agg pre b
  have h3 := stitch_agg (pre ++ a) z; have h4 := stitch_agg (pre ++ b) z
  simp only [stitch, agg] at h1 h2 h3 h4 hres ⊢
  rw [h3, h4, h1, h2]
  have hvala : hval a = hval b := hres
  have hpow_eq : hpow a = hpow b := by simp [hpow, hlen]
  simp [hvala, hpow_eq, hpow, hval, M, B]

theorem sound_RollingFingerprint_AmendLeaf :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint.AmendLeaf.Goal :=
  amendLeaf_of_stitch _ stitch_agg

theorem sound_RollingFingerprint_RunInput :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint.RunInput.Goal :=
  runInput_of_stitch _ stitch_agg

theorem sound_RollingFingerprint_ReorderNeighbors :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch _ stitch_agg

theorem sound_RollingFingerprint_CollisionBoundary :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint.CollisionBoundary.Goal := by
  decide

#guard agg.reference [0] == agg.reference [65521]
#guard agg.join (agg.reference [1, 2]) (agg.reference [3]) == agg.reference [1, 2, 3]

end RollingFingerprint

/-! ## BalanceFloor -/

namespace BalanceFloor

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

private def step (x : Nat) : Int :=
  if x % 2 == 0 then ((x / 2 : Nat) : Int) else -(((x / 2 + 1 : Nat) : Int))
private def steps (xs : List Nat) : List Int := xs.map step

private theorem scanl_append (a b : List Int) (init : Int) :
    (a ++ b).scanl (· + ·) init = a.scanl (· + ·) init ++ (b.scanl (· + ·) (init + a.sum)).tail := by
  induction a generalizing init with
  | nil => simp [List.scanl]
  | cons x xs ih =>
      simp only [List.cons_append, List.scanl, List.sum_cons, List.tail_cons]
      rw [ih (init + x)]; congr 1; congr 1; ring

private theorem scanl_shift (b : List Int) (s : Int) :
    b.scanl (· + ·) s = (b.scanl (· + ·) 0).map (· + s) := by
  induction b generalizing s with
  | nil => simp [List.scanl]
  | cons x xs ih =>
      simp only [List.scanl, List.map_cons, zero_add]
      constructor
      · ring
      · rw [ih (s + x)]; congr 1; ext y; ring

private theorem foldl_min_map_add (l : List Int) (s init : Int) :
    (l.map (· + s)).foldl min (init + s) = l.foldl min init + s := by
  induction l generalizing init with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.foldl_cons]
      have h : min (init + s) (x + s) = min init x + s := by omega
      rw [h, ih (min init x)]

private theorem min?_getD_eq_foldl (l : List Int) :
    l.min?.getD 0 = l.foldl min 0 := by
  cases l with
  | nil => simp
  | cons x xs => simp [List.min?_cons]

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg]
  constructor
  · simp [steps, List.map_append, List.sum_append]
  · simp only [steps, List.map_append]
    rw [scanl_append, scanl_shift (b.map step) ((a.map step).sum)]
    simp only [min?_getD_eq_foldl]
    rw [List.foldl_append]
    cases hb : (List.scanl (· + ·) 0 (b.map step)) with
    | nil => simp [List.scanl] at hb
    | cons b0 bs =>
        have hb0 : b0 = 0 := by simp [List.scanl] at hb; exact hb.1
        subst hb0
        simp only [List.map_cons, List.tail_cons, List.foldl_cons]
        rw [show (0 : Int) + (a.map step).sum = (a.map step).sum by ring]
        have hbs : (bs.map (· + (a.map step).sum)).foldl min
              (min (((a.map step).scanl (· + ·) 0).foldl min 0) ((a.map step).sum)) =
            min (((a.map step).scanl (· + ·) 0).foldl min 0)
              ((bs.map (· + (a.map step).sum)).foldl min ((a.map step).sum)) := by
          have gen : ∀ (l : List Int) (x y : Int), l.foldl min (min x y) = min x (l.foldl min y) := by
            intro l; induction l with
            | nil => intro x y; simp
            | cons z zs ih => intro x y; simp only [List.foldl_cons]; rw [show min (min x y) z = min x (min y z) by omega]; exact ih x (min y z)
          exact gen _ _ _
        rw [hbs]
        congr 1
        simp only [List.foldl_cons, min_self]
        have := foldl_min_map_add bs ((a.map step).sum) 0
        linarith [this]

theorem sound_BalanceFloor_Stitch :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor.Stitch.Goal :=
  stitch_agg

theorem sound_BalanceFloor_TreeReduce :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor.TreeReduce.Goal :=
  treeReduce_of_stitch _ stitch_agg

theorem sound_BalanceFloor_RepeatPower :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor.RepeatPower.Goal :=
  repeatPower_of_stitch _ stitch_agg

theorem sound_BalanceFloor_FrameSlice :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor.FrameSlice.Goal :=
  frameSlice_of_stitch _ stitch_agg

theorem sound_BalanceFloor_PrefixCheckpoint :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch _ stitch_agg

theorem sound_BalanceFloor_ShardAdmission :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor.ShardAdmission.Goal := by
  intro a b credit
  have hst := stitch_agg a b
  simp only [stitch, agg] at hst
  obtain ⟨hs, hm⟩ := Prod.mk.inj hst
  constructor
  · intro h; constructor <;> omega
  · intro ⟨h1, h2⟩; omega

theorem sound_BalanceFloor_AmendLeaf :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor.AmendLeaf.Goal :=
  amendLeaf_of_stitch _ stitch_agg

theorem sound_BalanceFloor_RunInput :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor.RunInput.Goal :=
  runInput_of_stitch _ stitch_agg

theorem sound_BalanceFloor_ReorderNeighbors :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch _ stitch_agg

theorem sound_BalanceFloor_CreditLimit :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor.CreditLimit.Goal := by
  intro xs credit
  simp only [agg]
  constructor
  · intro h i hi
    simp only [min?_getD_eq_foldl]
    have hmem : ((steps (xs.take i)).sum) ∈ (steps xs).scanl (· + ·) 0 := by
      have hscanl : ∀ (l : List Int) (init : Int) (j : Nat) (hj : j ≤ l.length),
          ((l.take j).sum + init) ∈ l.scanl (· + ·) init := by
        intro l init j hj
        induction l generalizing init j with
        | nil => simp at hj; subst hj; simp [List.scanl]
        | cons x xs ih =>
            cases j with
            | zero => simp [List.scanl]
            | succ j =>
                simp only [List.take_succ_cons, List.sum_cons, List.scanl, List.mem_cons]
                right
                have hj' : j ≤ xs.length := by simp at hj; omega
                have := ih (init + x) j hj'
                simpa [List.sum_cons, add_assoc, add_left_comm, add_comm] using this
      have := hscanl (steps xs) 0 i (by simp [steps, List.length_map]; exact hi)
      simpa [steps, List.map_take, List.sum_map] using this
    have hle : ∀ (l : List Int) (init : Int) (x : Int), x ∈ l → l.foldl min init ≤ min init x := by
      intro l init x hx
      induction l generalizing init with
      | nil => cases hx
      | cons y ys ih =>
          simp only [List.foldl_cons, List.mem_cons] at hx ⊢
          rcases hx with rfl | hx
          · exact le_trans (List.foldl_min_le ..) (le_rfl)
          · exact ih (min init y) hx
    have := hle _ 0 _ hmem
    omega
  · intro hall
    simp only [min?_getD_eq_foldl]
    have hfold : ∀ (l : List Int) (init : Int), (-credit ≤ init) →
        (∀ x ∈ l, -credit ≤ x) → -credit ≤ l.foldl min init := by
      intro l; induction l with
      | nil => intro init h1 h2; simp [h1]
      | cons x xs ih =>
          intro init h1 h2
          simp only [List.foldl_cons, List.mem_cons] at h2 ⊢
          apply ih
          · exact le_min h1 (h2.1)
          · exact h2.2
    apply hfold
    · omega
    · intro x hx
      have hscanl_mem : ∀ (l : List Int) (init : Int) (x : Int), x ∈ l.scanl (· + ·) init →
          ∃ j, j ≤ l.length ∧ x = (l.take j).sum + init := by
        intro l init x hx
        induction l generalizing init with
        | nil => simp [List.scanl] at hx; exact ⟨0, by simp, by simp [hx]⟩
        | cons y ys ih =>
            simp only [List.scanl, List.mem_cons] at hx
            rcases hx with rfl | hx
            · exact ⟨0, by simp, by simp⟩
            · obtain ⟨j, hj, rfl⟩ := ih (init + y) x hx
              exact ⟨j+1, by simp; omega, by simp [List.sum_cons]; ring⟩
      obtain ⟨j, hj, rfl⟩ := hscanl_mem (steps xs) 0 x hx
      simp at hj
      have hj' : j ≤ xs.length := by simpa [steps, List.length_map] using hj
      have := hall j hj'
      simpa [steps, List.map_take, List.sum_map] using this

#guard agg.reference [2, 1, 4] == (2, 0)
#guard agg.join (agg.reference [1]) (agg.reference [1]) == agg.reference [1, 1]

end BalanceFloor

/-! ## BitOccupancy -/

namespace BitOccupancy

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

private def mask (xs : List Nat) : Nat := xs.foldl (fun a x => a ||| 2 ^ (x % 64)) 0

private theorem fold_or_shift (xs : List Nat) (p q : Nat) :
    xs.foldl (fun a x => a ||| 2 ^ (x % 64)) (p ||| q) =
    p ||| xs.foldl (fun a x => a ||| 2 ^ (x % 64)) q := by
  induction xs generalizing p q with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [show (p ||| q) ||| 2 ^ (x % 64) = p ||| (q ||| 2 ^ (x % 64)) by
        simp [Nat.or_assoc, Nat.or_comm, Nat.or_left_comm]]
      exact ih p (q ||| 2 ^ (x % 64))

private theorem mask_append (a b : List Nat) : mask (a ++ b) = mask a ||| mask b := by
  unfold mask
  rw [List.foldl_append]
  conv_lhs => rw [show List.foldl (fun a x => a ||| 2 ^ (x % 64)) 0 a =
    (List.foldl (fun a x => a ||| 2 ^ (x % 64)) 0 a ||| 0) by simp]
  rw [fold_or_shift]

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg, List.length_append]
  exact Prod.ext rfl (mask_append a b)

theorem sound_BitOccupancy_Stitch :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy.Stitch.Goal :=
  stitch_agg

theorem sound_BitOccupancy_TreeReduce :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy.TreeReduce.Goal :=
  treeReduce_of_stitch _ stitch_agg

theorem sound_BitOccupancy_RepeatPower :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy.RepeatPower.Goal :=
  repeatPower_of_stitch _ stitch_agg

theorem sound_BitOccupancy_FrameSlice :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy.FrameSlice.Goal :=
  frameSlice_of_stitch _ stitch_agg

theorem sound_BitOccupancy_PrefixCheckpoint :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch _ stitch_agg

theorem sound_BitOccupancy_FoldInto32 :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy.FoldInto32.Goal := by
  intro xs k hk
  simp only [agg]
  induction xs with
  | nil => simp [mask]
  | cons x xs ih =>
      unfold mask at ih ⊢
      simp only [List.foldl_cons]
      rw [show (0 : Nat) ||| 2 ^ (x % 64) = 2 ^ (x % 64) ||| 0 by simp [Nat.or_comm]]
      rw [fold_or_shift xs (2 ^ (x % 64)) 0]
      simp only [Nat.testBit_or, Bool.or_eq_true, List.any_cons]
      have rearr : ∀ a b c d : Bool, (a || c) || (b || d) = (a || b) || (c || d) := by decide
      rw [rearr, ih]
      congr 1
      have hk1 : k < 64 := by omega
      have hk2 : k + 32 < 64 := by omega
      rw [Nat.testBit_two_pow, Nat.testBit_two_pow]
      simp only [hk1, decide_true, Bool.and_true, hk2]
      by_cases hxk : x % 32 = k
      · have h64 : x % 64 = k ∨ x % 64 = k + 32 := by omega
        rcases h64 with h64 | h64 <;> simp [h64, hxk, beq_iff_eq]
      · have hne1 : x % 64 ≠ k := by omega
        have hne2 : x % 64 ≠ k + 32 := by omega
        simp [hne1, hne2, hxk, beq_iff_eq]

theorem sound_BitOccupancy_AmendLeaf :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy.AmendLeaf.Goal :=
  amendLeaf_of_stitch _ stitch_agg

theorem sound_BitOccupancy_RunInput :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy.RunInput.Goal :=
  runInput_of_stitch _ stitch_agg

theorem sound_BitOccupancy_ReorderNeighbors :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch _ stitch_agg

theorem sound_BitOccupancy_NegativeTest :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy.NegativeTest.Goal := by
  intro xs x hnot
  simp only [agg] at hnot
  intro hmem
  suffices h : (List.foldl (fun a y => a ||| 2 ^ (y % 64)) 0 xs).testBit (x % 64) = true by
    simp [h] at hnot
  induction xs with
  | nil => cases hmem
  | cons y ys ih =>
      simp only [List.foldl_cons, List.mem_cons] at hmem ⊢
      rw [show (0 : Nat) ||| 2 ^ (y % 64) = 2 ^ (y % 64) ||| 0 by simp [Nat.or_comm]]
      rw [fold_or_shift ys (2 ^ (y % 64)) 0]
      simp only [Nat.testBit_or, Bool.or_eq_true]
      rcases hmem with rfl | hmem
      · left
        rw [Nat.testBit_two_pow]
        simp [Nat.mod_lt x (by decide : 64 > 0)]
      · right
        exact ih hmem

#guard agg.reference [0, 32, 64] == (3, 4294967297)
#guard agg.join (agg.reference [1]) (agg.reference [33]) == agg.reference [1, 33]

end BitOccupancy

/-! ## FirstLastChange -/

namespace FirstLastChange

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg]
  cases ha : a.head? with
  | none => have : a = [] := by cases a <;> simp_all; subst this; simp [optionalJoin]
  | some va =>
      cases hb : b.head? with
      | none => have : b = [] := by cases b <;> simp_all; subst this; simp [optionalJoin]
      | some vb =>
          simp only [ha, hb, Option.map_some, optionalJoin]
          have hhead : (a ++ b).head? = some va := by cases a <;> simp_all
          have hlast : (a ++ b).getLast? = b.getLast? := by
            cases b with
            | nil => simp at hb
            | cons x xs => simp [List.getLast?_append_cons]
          simp [hhead, hlast]
          congr 1
          have hlastA : a.getLast? = some (a.getLast (by cases a <;> simp_all)) := by simp [List.getLast?_eq_getLast]
          simp only [hlastA, Option.getD_some]
          have hedges : edges (a ++ b) = edges a ++ ((a.getLast (by cases a <;> simp_all), vb) :: edges b) := by
            cases a with
            | nil => simp at ha
            | cons x xs =>
                cases b with
                | nil => simp at hb
                | cons y ys => simp [edges, List.zip_append, List.drop_append, List.getLast_cons, List.zip_cons_cons]
          rw [hedges]
          simp [List.countP_append, List.countP_cons]
          have hvb : vb = b.head (by cases b <;> simp_all) := by cases b <;> simp_all
          simp [hvb]
          have hbeq : (decide (a.getLast (by cases a <;> simp_all) != vb)).toNat =
              1 - (decide (a.getLast (by cases a <;> simp_all) == vb)).toNat := by
            by_cases h : a.getLast (by cases a <;> simp_all) == vb <;> simp [h]
          rw [hbeq]; omega

theorem sound_FirstLastChange_Stitch :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange.Stitch.Goal :=
  stitch_agg

theorem sound_FirstLastChange_TreeReduce :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange.TreeReduce.Goal :=
  treeReduce_of_stitch _ stitch_agg

theorem sound_FirstLastChange_RepeatPower :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange.RepeatPower.Goal :=
  repeatPower_of_stitch _ stitch_agg

theorem sound_FirstLastChange_FrameSlice :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange.FrameSlice.Goal :=
  frameSlice_of_stitch _ stitch_agg

theorem sound_FirstLastChange_PrefixCheckpoint :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch _ stitch_agg

theorem sound_FirstLastChange_DuplicateBlock :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange.DuplicateBlock.Goal := by
  intro xs k hx hk
  simp only [agg]
  have hne : xs ≠ [] := hx
  cases k with
  | zero => omega
  | succ k =>
      induction k with
      | zero => simp [List.replicate_succ]
      | succ k ih =>
          have ih' := ih (by omega)
          rw [show List.replicate (k + 2) xs = List.replicate (k + 1) xs ++ [xs] by
            have : k + 2 = (k + 1) + 1 := by omega
            rw [this, List.replicate_add]; simp [List.replicate_one]]
          rw [List.flatten_append, List.flatten_singleton]
          have hst := stitch_agg (List.replicate (k + 1) xs).flatten xs
          simp only [stitch, agg] at hst
          have hflat_ne : (List.replicate (k + 1) xs).flatten ≠ [] := by
            simp [List.flatten_eq_nil_iff, List.mem_replicate]
            intro hall; exact hne (hall xs (by simp))
          have hxs_head : xs.head? = some (xs.head hne) := by simp [List.head?_eq_some_head]
          have hxs_last : xs.getLast? = some (xs.getLast hne) := by simp [List.getLast?_eq_getLast]
          have hflat_head : (List.replicate (k + 1) xs).flatten.head? = some (xs.head hne) := by
            rw [show List.replicate (k + 1) xs = xs :: List.replicate k xs by simp [List.replicate_succ]]
            simp [List.flatten_cons, List.head?_append, hxs_head, hne]
          have hflat_last : (List.replicate (k + 1) xs).flatten.getLast? = some (xs.getLast hne) := by
            induction k with
            | zero => simp [List.replicate_succ, hxs_last]
            | succ k ih2 =>
                rw [show List.replicate (k + 2) xs = List.replicate (k + 1) xs ++ [xs] by
                  have : k + 2 = (k + 1) + 1 := by omega
                  rw [this, List.replicate_add]; simp [List.replicate_one]]
                rw [List.flatten_append, List.flatten_singleton]
                simp [List.getLast?_append, hxs_last]
          rw [hflat_head, hxs_head] at hst
          simp only [Option.map_some, optionalJoin] at hst
          have happ_head : ((List.replicate (k + 1) xs).flatten ++ xs).head? = some (xs.head hne) := by
            simp [List.head?_append, hflat_head, hflat_ne]
          simp [happ_head, hxs_last, hflat_last]
          have hst' := congrArg (fun x => match x with | some r => r.2.2 | none => 0) hst
          simp only [happ_head, hflat_head, hxs_head, hxs_last, hflat_last, Option.map_some, Option.getD_some] at hst' ih' ⊢
          simp only [List.getLast?_eq_getLast, Option.getD_some] at ih' hst' ⊢
          omega

theorem sound_FirstLastChange_AmendLeaf :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange.AmendLeaf.Goal :=
  amendLeaf_of_stitch _ stitch_agg

theorem sound_FirstLastChange_RunInput :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange.RunInput.Goal :=
  runInput_of_stitch _ stitch_agg

theorem sound_FirstLastChange_ReorderNeighbors :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch _ stitch_agg

theorem sound_FirstLastChange_ReverseCount :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange.ReverseCount.Goal := by
  intro xs
  simp only [agg]
  cases h : xs.head? with
  | none => have : xs = [] := by cases xs <;> simp_all; simp [this]
  | some v =>
      have hne : xs ≠ [] := by cases xs <;> simp_all
      have hrev_ne : xs.reverse ≠ [] := by simp [hne]
      have hhead_rev : xs.reverse.head? = xs.getLast? := by rw [List.head?_reverse hrev_ne]
      have hlast_rev : xs.reverse.getLast? = xs.head? := by rw [List.getLast?_reverse hne]
      rw [hhead_rev, hlast_rev]
      simp only [Option.map_some]
      congr 1
      have h1 : edges xs.reverse = (edges xs).reverse.map Prod.swap := by
        simp [edges, List.zip_reverse, List.map_reverse, List.reverse_reverse, Function.comp]
      rw [h1, List.countP_map, List.countP_reverse]
      congr 1
      ext ⟨a, b⟩
      simp [Function.comp, Prod.swap]
      cases hba : b == a <;> cases hab : a == b <;> simp_all [beq_iff_eq]

#guard agg.reference [1, 1, 2] == some (1, 2, 2)
#guard agg.join (agg.reference [1, 2]) (agg.reference [2, 3]) == agg.reference [1, 2, 2, 3]

end FirstLastChange

/-! ## IndexWeighted -/

namespace IndexWeighted

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

private def wsum (xs : List Nat) : Nat := (xs.zipIdx.map (fun (v, i) => i * v)).sum

private theorem wsum_cons (x : Nat) (xs : List Nat) : wsum (x :: xs) = wsum xs + xs.sum := by
  simp [wsum, List.zipIdx_cons, List.map_cons, List.map_map, Function.comp]
  induction xs with
  | nil => simp
  | cons y ys ih => simp [List.zipIdx_cons, List.map_cons, List.map_map, Function.comp, List.sum_cons]; omega

private theorem wsum_append (a b : List Nat) : wsum (a ++ b) = wsum a + wsum b + a.length * b.sum := by
  induction a with
  | nil => simp [wsum]
  | cons x xs ih => simp only [List.cons_append, wsum_cons, List.length_cons, List.sum_append, ih]; ring

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg, List.length_append, List.sum_append]
  refine Prod.ext rfl (Prod.ext rfl ?_)
  exact wsum_append a b

theorem sound_IndexWeighted_Stitch :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted.Stitch.Goal :=
  stitch_agg

theorem sound_IndexWeighted_TreeReduce :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted.TreeReduce.Goal :=
  treeReduce_of_stitch _ stitch_agg

theorem sound_IndexWeighted_RepeatPower :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted.RepeatPower.Goal :=
  repeatPower_of_stitch _ stitch_agg

theorem sound_IndexWeighted_FrameSlice :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted.FrameSlice.Goal :=
  frameSlice_of_stitch _ stitch_agg

theorem sound_IndexWeighted_PrefixCheckpoint :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch _ stitch_agg

theorem sound_IndexWeighted_ReplaceAt :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted.ReplaceAt.Goal := by
  intro xs i x hi
  simp only [agg]
  induction xs generalizing i with
  | nil => cases hi
  | cons y ys ih =>
      cases i with
      | zero => simp [wsum_cons, List.sum_cons]
      | succ j =>
          simp only [List.set_cons_succ, wsum_cons, List.sum_set]
          have hj : j < ys.length := by simpa using hi
          have ih' := ih j hj
          simp only [List.getElem?_eq_getElem, hj, Option.getD_some] at ih' ⊢
          omega

theorem sound_IndexWeighted_AmendLeaf :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted.AmendLeaf.Goal :=
  amendLeaf_of_stitch _ stitch_agg

theorem sound_IndexWeighted_RunInput :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted.RunInput.Goal :=
  runInput_of_stitch _ stitch_agg

theorem sound_IndexWeighted_ReorderNeighbors :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch _ stitch_agg

theorem sound_IndexWeighted_ReverseMoment :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted.ReverseMoment.Goal := by
  intro xs
  simp only [agg]
  induction xs with
  | nil => simp [wsum]
  | cons x xs ih =>
      rw [List.reverse_cons, wsum_append]
      simp only [wsum_cons, wsum, List.zipIdx_cons, List.map_cons, List.sum_cons, Nat.zero_add,
        List.length_reverse, List.sum_reverse, List.length_cons, List.sum_cons, List.length_singleton, List.sum_singleton]
      have hw1 : wsum [x] = 0 := by simp [wsum]
      rw [hw1]; have := ih; omega

#guard agg.reference [10, 20, 30] == (3, 60, 140)
#guard agg.join (agg.reference [2, 3]) (agg.reference [5]) == agg.reference [2, 3, 5]

end IndexWeighted

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFootersB
