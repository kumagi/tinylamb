import TinylambOpt.ResearchGoals.Frontiers.RankedJoins

namespace TinylambOpt.ResearchProofs.Frontiers.RankedJoinsNear

private theorem sortedInt (xs : List Int) : xs.mergeSort.Pairwise (fun a b : Int => a ≤ b) := by
  have h := List.pairwise_mergeSort (le := fun a b : Int => decide (a ≤ b))
    (by intro a b c hab hbc; by_cases h1 : a ≤ b <;> by_cases h2 : b ≤ c <;> simp_all; omega)
    (by intro a b; by_cases h : a ≤ b <;> simp [h] <;> omega) xs
  exact h.imp (by intro a b hab; simpa using hab)

private theorem takeWhile_length_count_gen {s : List Int} (hs : s.Pairwise (fun a b => a ≤ b))
    (p : Int → Bool) (hdown : ∀ y z, p z = true → y ≤ z → p y = true) :
    (s.takeWhile p).length = s.countP p := by
  induction s with
  | nil => simp
  | cons a s ih =>
      have hs' := List.Pairwise.of_cons hs
      by_cases ha : p a
      · simp [ha, ih hs']
      · have hnone : ∀ y ∈ s, ¬ p y = true := by
          intro y hy hpy
          have hay : a ≤ y := (List.pairwise_cons.mp hs).1 y hy
          exact ha (hdown a y hpy hay)
        have hcnt0 : s.countP p = 0 := List.countP_eq_zero.mpr hnone
        simp [ha, hcnt0]

private theorem natAbs_sub_comm (x y : Int) : (x-y).natAbs = (y-x).natAbs := by
  rw [← Int.natAbs_neg (x-y)]
  congr 1 <;> omega

private theorem natAbs_sub_le_iff (x y : Int) (d : Nat) :
    (x-y).natAbs ≤ d ↔ x - (d:Int) ≤ y ∧ y ≤ x + (d:Int) := by
  constructor
  · intro h
    rcases Int.natAbs_eq (x-y) with hpos | hneg
    · omega
    · omega
  · intro h
    rcases Int.natAbs_eq (x-y) with hpos | hneg
    · omega
    · omega

private theorem countP_band (s : List Int) (x : Int) (d : Nat) :
    s.countP (fun y => y ≤ x + (d:Int)) =
      s.countP (fun y => y < x - (d:Int)) + s.countP (fun y => (x-y).natAbs ≤ d) := by
  induction s with
  | nil => simp
  | cons a s ih =>
      by_cases hle : a ≤ x + (d:Int)
      · by_cases hlt : a < x - (d:Int)
        · have hn : ¬ (x-a).natAbs ≤ d := by
             rw [natAbs_sub_le_iff]; intro h; omega
          simp [hle, hlt, hn, ih]
          omega
        · have hy : (x-a).natAbs ≤ d := by
             rw [natAbs_sub_le_iff]; constructor <;> omega
          simp [hle, hlt, hy, ih]
          omega
      · have hnlt : ¬ a < x - (d:Int) := by omega
        have hn : ¬ (x-a).natAbs ≤ d := by rw [natAbs_sub_le_iff]; intro h; omega
        simp [hle, hnlt, hn, ih]

private theorem countP_eq_map_sum (p : Int → Bool) (s : List Int) :
    s.countP p = (s.map fun x => if p x then 1 else 0).sum := by
  induction s with
  | nil => simp
  | cons a s ih => simp [List.countP_cons, ih]; omega

private def near (d : Nat) (x y : Int) : Bool := (x-y).natAbs ≤ d

private def allPairs (d : Nat) (s : List Int) : Nat :=
  (s.map fun x => s.countP (near d x)).sum

private def laterPairs (d : Nat) (s : List Int) : Nat :=
  (s.zipIdx.map fun p => (s.drop (p.2+1)).countP (near d p.1)).sum

private theorem sum_map_add (f g : Int → Nat) : ∀ s : List Int,
    (s.map fun y => f y + g y).sum = (s.map f).sum + (s.map g).sum := by
  intro s
  induction s with
  | nil => simp
  | cons a s ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem zipIdx_shift (f : Int → Nat → Nat) : ∀ (s : List Int) (i : Nat),
    ((List.zipIdx s (i+1)).map (fun p => f p.1 p.2)).sum =
      ((List.zipIdx s i).map (fun p => f p.1 (p.2+1))).sum := by
  intro s
  induction s with
  | nil => simp
  | cons a s ih =>
      intro i
      simp [List.zipIdx_cons, ih (i+1), Nat.add_comm, Nat.add_left_comm]

private theorem laterPairs_cons (d : Nat) (x : Int) (s : List Int) :
    laterPairs d (x::s) = s.countP (near d x) + laterPairs d s := by
  unfold laterPairs
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons]
  change ((x::s).drop (0+1)).countP (near d x) +
      ((List.zipIdx s 1).map (fun p => ((x::s).drop (p.2+1)).countP (near d p.1))).sum =
    s.countP (near d x) + ((List.zipIdx s 0).map (fun p => (s.drop (p.2+1)).countP (near d p.1))).sum
  have hdrop1 : (x::s).drop 1 = s := by simp
  rw [hdrop1]
  congr 1
  calc
    ((List.zipIdx s 1).map (fun p => ((x::s).drop (p.2+1)).countP (near d p.1))).sum =
        ((List.zipIdx s 1).map (fun p => (s.drop p.2).countP (near d p.1))).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro p hp
          rw [List.drop_succ_cons]
    _ = ((List.zipIdx s 0).map (fun p => (s.drop (p.2+1)).countP (near d p.1))).sum := by
          exact zipIdx_shift (fun y i => (s.drop i).countP (near d y)) s 0

private theorem allPairs_eq_laterPairs (d : Nat) : ∀ s : List Int,
    allPairs d s = s.length + 2 * laterPairs d s := by
  intro s
  induction s with
  | nil => simp [allPairs, laterPairs]
  | cons x s ih =>
      have hself : (x::s).countP (near d x) = s.countP (near d x) + 1 := by
        simp [near]
      have hother : ∀ y ∈ s,
          (x::s).countP (near d y) =
            s.countP (near d y) + if near d y x then 1 else 0 := by
        intro y hy
        simp [near, List.countP_cons]
      have hind : (s.map fun y => if near d y x then 1 else 0).sum = s.countP (near d x) := by
        calc
          (s.map fun y => if near d y x then 1 else 0).sum =
              (s.map fun y => if near d x y then 1 else 0).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro y hy
            simp [near, natAbs_sub_comm x y]
          _ = s.countP (fun y => near d x y) := (countP_eq_map_sum (fun y => near d x y) s).symm
      have hmap : (s.map fun y => (x::s).countP (near d y)).sum = allPairs d s + s.countP (near d x) := by
        calc
          (s.map fun y => (x::s).countP (near d y)).sum =
              (s.map fun y => s.countP (near d y) + if near d y x then 1 else 0).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro y hy
            exact hother y hy
          _ = (s.map fun y => s.countP (near d y)).sum +
                (s.map fun y => if near d y x then 1 else 0).sum := by
            exact sum_map_add (fun y => s.countP (near d y)) (fun y => if near d y x then 1 else 0) s
          _ = allPairs d s + s.countP (near d x) := by rw [hind]; rfl
      calc
        allPairs d (x::s) = (x::s).countP (near d x) + (s.map fun y => (x::s).countP (near d y)).sum := by
          simp [allPairs]
        _ = (s.countP (near d x) + 1) + (allPairs d s + s.countP (near d x)) := by rw [hself, hmap]
        _ = (s.length + 1) + 2 * (s.countP (near d x) + laterPairs d s) := by rw [ih]; omega
        _ = (x::s).length + 2 * laterPairs d (x::s) := by
          rw [laterPairs_cons]
          simp

private theorem perm_sum {s t : List Nat} (h : s.Perm t) : s.sum = t.sum := by
  induction h with
  | nil => simp
  | cons a h ih => simpa using ih
  | swap a b l => simp; omega
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem allPairs_perm (d : Nat) {s t : List Int} (h : s.Perm t) :
    allPairs d s = allPairs d t := by
  unfold allPairs
  have hcnt : ∀ x, s.countP (near d x) = t.countP (near d x) := by
    intro x
    exact h.countP_eq (near d x)
  have hmap : (s.map fun x => s.countP (near d x)).Perm (t.map fun x => s.countP (near d x)) := h.map _
  have hmap2 : (t.map fun x => s.countP (near d x)) = (t.map fun x => t.countP (near d x)) := by
    apply List.map_congr_left
    intro x hx
    exact hcnt x
  rw [hmap2] at hmap
  exact perm_sum hmap

theorem NearPairs_sound : TinylambOpt.ResearchGoals.Frontiers.RankedJoins.NearPairs.Goal := by
  intro xs d
  let a := xs.mergeSort
  have hs : a.Pairwise (fun x y : Int => x ≤ y) := by simpa [a] using sortedInt xs
  have hband_le : ∀ x : Int,
      (a.takeWhile (fun y => y ≤ x + (d:Int))).length = a.countP (fun y => y ≤ x+(d:Int)) := by
    intro x
    exact takeWhile_length_count_gen hs (fun y => y ≤ x+(d:Int)) (by intro y z hy hzy; simp at hy ⊢; omega)
  have hband_lt : ∀ x : Int,
      (a.takeWhile (fun y => y < x - (d:Int))).length = a.countP (fun y => y < x-(d:Int)) := by
    intro x
    exact takeWhile_length_count_gen hs (fun y => y < x-(d:Int)) (by intro y z hy hzy; simp at hy ⊢; omega)
  have hband : ∀ x : Int,
      (a.takeWhile (fun y => y ≤ x+(d:Int))).length -
      (a.takeWhile (fun y => y < x-(d:Int))).length = a.countP (near d x) := by
    intro x
    rw [hband_le x, hband_lt x]
    unfold near
    rw [countP_band a x d]
    omega
  have hdir :
      (a.map (fun x => (a.takeWhile (fun y => y ≤ x+(d:Int))).length -
        (a.takeWhile (fun y => y < x-(d:Int))).length)).sum = allPairs d a := by
    unfold allPairs
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact hband x
  have hperm : a.Perm xs := by
    simpa [a] using List.mergeSort_perm xs (fun x y : Int => decide (x ≤ y))
  have hlen : a.length = xs.length := hperm.length_eq
  have hallperm : allPairs d a = allPairs d xs := allPairs_perm d hperm
  have hla : laterPairs d a = (allPairs d a - a.length)/2 := by
    rw [allPairs_eq_laterPairs d a]
    omega
  have hlx : laterPairs d xs = (allPairs d xs - xs.length)/2 := by
    rw [allPairs_eq_laterPairs d xs]
    omega
  have hlater : laterPairs d a = laterPairs d xs := by
    rw [hla, hlx, hallperm, hlen]
  dsimp only
  change ((a.map (fun x => (a.takeWhile (fun y => y ≤ x+(d:Int))).length -
      (a.takeWhile (fun y => y < x-(d:Int))).length)).sum - a.length)/2 = laterPairs d xs
  rw [hdir]
  rw [allPairs_eq_laterPairs d a]
  have hdiv : (a.length + 2 * laterPairs d a - a.length)/2 = laterPairs d a := by
    have heq : a.length + 2 * laterPairs d a - a.length = 2 * laterPairs d a := by omega
    rw [heq]
    exact Nat.mul_div_right (laterPairs d a) (by decide : 0 < 2)
  rw [hdiv]
  exact hlater

end TinylambOpt.ResearchProofs.Frontiers.RankedJoinsNear
