import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PlateauFrames_InjectiveCodes

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private theorem range_succ_cons (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun i => i + 1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      calc
        List.range (Nat.succ n + 1) = List.range (n + 1) ++ [n + 1] := by
          simp [Nat.succ_eq_add_one, List.range_succ]
        _ = (0 :: (List.range n).map (fun i => i + 1)) ++ [n + 1] := by rw [ih]
        _ = 0 :: ((List.range n).map (fun i => i + 1) ++ [n + 1]) := by simp
        _ = 0 :: (List.range (n + 1)).map (fun i => i + 1) := by simp [List.range_succ]

private theorem range_flatMap_succ {α : Type} (n : Nat) (f : Nat → List α) :
    (List.range (n + 1)).flatMap f = f 0 ++ (List.range n).flatMap (fun i => f (i + 1)) := by
  rw [range_succ_cons n]
  simp [List.flatMap_map]

private def prefixWindows (x : Series) := (List.range x.length).map (fun j => x.take (j + 1))

private theorem windows_cons (a : Int) (xs : Series) :
    windows (a :: xs) = prefixWindows (a :: xs) ++ windows xs := by
  unfold windows prefixWindows
  simp only [List.length_cons]
  rw [range_flatMap_succ]
  simp [List.drop_succ_cons]

private theorem matchesFirst_cons (a : Int) (xs : Series) :
    matchesFirst (a :: xs) = (xs.takeWhile (fun c => c == a)).length + 1 := by
  simp [matchesFirst]

private theorem take_all_ne (a b : Int) (bs : List Int) (j : Nat) (h : b ≠ a) :
    ((b :: bs).take (j + 1)).all (fun c => c == a) = false := by
  rw [List.take_succ_cons]
  simp [h]

private theorem count_take_eq (a : Int) (xs : Series) :
    (List.range (xs.length + 1)).countP
        (fun j => ((a :: xs).take (j + 1)).all (fun c => c == a)) =
      (xs.takeWhile (fun c => c == a)).length + 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.length_cons]
      rw [range_succ_cons (bs.length + 1)]
      change List.countP (fun j => ((a :: b :: bs).take (j + 1)).all (fun c => c == a))
          (0 :: (List.range (bs.length + 1)).map (fun i => i + 1)) =
        (List.takeWhile (fun c => c == a) (b :: bs)).length + 1
      rw [List.countP_cons, List.countP_map]
      change List.countP (fun i => ((a :: b :: bs).take ((i + 1) + 1)).all (fun c => c == a))
          (List.range (bs.length + 1)) +
          (if ((a :: b :: bs).take (0 + 1)).all (fun c => c == a) = true then 1 else 0) =
        (List.takeWhile (fun c => c == a) (b :: bs)).length + 1
      have hp0 : ((a :: b :: bs).take (0 + 1)).all (fun c => c == a) = true := by simp
      rw [ite_eq_left hp0]
      have hpred : (fun i => ((a :: b :: bs).take ((i + 1) + 1)).all (fun c => c == a)) =
          (fun i => ((b :: bs).take (i + 1)).all (fun c => c == a)) := by
        funext i
        simp only [Nat.add_assoc, List.take_succ_cons, List.all_cons]
        simp
      rw [hpred]
      by_cases hba : b = a
      · subst b
        rw [ih]
        simp
      · have hshift : (List.range (bs.length + 1)).countP
              (fun i => ((b :: bs).take (i + 1)).all (fun c => c == a)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro i hi
            rw [take_all_ne a b bs i hba]
            simp
        rw [hshift]
        simp [hba]

private theorem countP_prefixWindows (a : Int) (xs : Series) :
    (prefixWindows (a :: xs)).countP (fun w => w.all (fun c => some c == w.head?)) =
      matchesFirst (a :: xs) := by
  unfold prefixWindows
  rw [List.countP_map]
  simp only [List.length_cons]
  change List.countP
      (fun j => ((a :: xs).take (j + 1)).all (fun c => some c == ((a :: xs).take (j + 1)).head?))
      (List.range (xs.length + 1)) = matchesFirst (a :: xs)
  have hhead : ∀ j, ((a :: xs).take (j + 1)).head? = some a := by
    intro j
    simp
  simp only [hhead]
  rw [matchesFirst_cons]
  exact count_take_eq a xs

private theorem measure_cons (a : Int) (xs : Series) :
    PlateauFrames.measure (a :: xs) = matchesFirst (a :: xs) + PlateauFrames.measure xs := by
  unfold PlateauFrames.measure
  rw [windows_cons, List.countP_append, countP_prefixWindows]
  omega

private theorem measure_nil : PlateauFrames.measure ([] : Series) = 0 := by
  simp [PlateauFrames.measure, windows]

/-- f が先頭値 a と xs 内の値の間で単射なら、先頭の等値ラン長は保存される。 -/
private theorem leadCount_map_eq (f : Int → Int) (a : Int) (xs : Series)
    (hhead : ∀ b ∈ xs, f b = f a → b = a) :
    ((xs.map f).takeWhile (fun c => c == f a)).length =
      (xs.takeWhile (fun c => c == a)).length := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      have hhead' : ∀ c ∈ bs, f c = f a → c = a := by
        intro c hc heq
        exact hhead c (by simp [hc]) heq
      by_cases hba : b == a
      · have hbaeq : b = a := by simpa using hba
        subst b
        simp [ih hhead']
      · have hfb : f b ≠ f a := by
          intro heq
          exact hba (by simp [hhead b (by simp) heq])
        simp [hba, hfb]

private theorem matchesFirst_map_eq (f : Int → Int) (a : Int) (xs : Series)
    (hhead : ∀ b ∈ xs, f b = f a → b = a) :
    matchesFirst (f a :: xs.map f) = matchesFirst (a :: xs) := by
  rw [matchesFirst_cons, matchesFirst_cons]
  rw [leadCount_map_eq f a xs hhead]

theorem sound_PlateauFrames_InjectiveCodes : PlateauFrames.InjectiveCodes.Goal := by
  intro x f hf
  induction x with
  | nil => simp [measure_nil]
  | cons a xs ih =>
      have hf' : ∀ b ∈ xs, ∀ c ∈ xs, f b = f c → b = c := by
        intro b hb c hc heq
        exact hf b (by simp [hb]) c (by simp [hc]) heq
      have hhead : ∀ b ∈ xs, f b = f a → b = a := by
        intro b hb heq
        exact hf b (by simp [hb]) a (by simp) heq
      rw [List.map_cons]
      rw [measure_cons, ih hf', measure_cons, matchesFirst_map_eq f a xs hhead]

end TinylambOpt.ResearchProofs.Atlas.Goal_PlateauFrames_InjectiveCodes
