import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_AmendLeaf

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs, 3 :: xs])

private def pages (n : Nat) : List (List (List Nat)) :=
  let ls := gen n
  [[]] ++ ls.map (fun a => [a]) ++ ls.flatMap (fun a => ls.map (fun b => [a, b]))

private def probe (n : Nat) : Bool :=
  let psys := pages n
  let reps : List (List Nat) := [[], [0], [1, 2], [3, 4, 5]]
  psys.all (fun ps => (List.range ps.length).all (fun i =>
    reps.all (fun xs =>
      combine agg (ps.set i xs) ==
        agg.join (combine agg (ps.take i))
          (agg.join (agg.reference xs) (combine agg (ps.drop (i + 1)))))))

#eval probe 3

abbrev State := Int × Int

-- A reachable state has a nonpositive minimum prefix, and that minimum is
-- at most the final balance.
private def Good (s : State) : Prop := s.2 ≤ 0 ∧ s.2 ≤ s.1

private def GoodList (l : List State) : Prop := ∀ x ∈ l, Good x

private def minFrom (start : Int) : List Int → Int
  | [] => start
  | x :: xs => min start (minFrom (start + x) xs)

private theorem scanl_min_cons (start x : Int) (xs : List Int) :
    ((x :: xs).scanl (· + ·) start).min?.getD start =
      min start ((xs.scanl (· + ·) (start + x)).min?.getD (start + x)) := by
  cases xs with
  | nil => simp
  | cons y ys => simp

private theorem minFrom_eq (start : Int) (l : List Int) :
    minFrom start l = (l.scanl (· + ·) start).min?.getD start := by
  induction l generalizing start with
  | nil => simp [minFrom]
  | cons x xs ih =>
      rw [minFrom, ih (start + x), scanl_min_cons]

private theorem minFrom_le_start (start : Int) (l : List Int) : minFrom start l ≤ start := by
  cases l with
  | nil => simp [minFrom]
  | cons x xs =>
      simp only [minFrom]
      omega

private theorem minFrom_le_end (start : Int) (l : List Int) : minFrom start l ≤ start + l.sum := by
  induction l generalizing start with
  | nil => simp [minFrom]
  | cons x xs ih =>
      simp only [minFrom, List.sum_cons]
      have h := ih (start + x)
      omega

private def stepsOf (xs : List Nat) : List Int :=
  xs.map (fun (x : Nat) => if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int))

private theorem good_ref (xs : List Nat) : Good (agg.reference xs) := by
  change Good ((stepsOf xs).sum, ((stepsOf xs).scanl (· + ·) 0).min?.getD 0)
  constructor
  · rw [← minFrom_eq 0 (stepsOf xs)]
    exact minFrom_le_start 0 (stepsOf xs)
  · rw [← minFrom_eq 0 (stepsOf xs)]
    simpa using minFrom_le_end 0 (stepsOf xs)

private theorem good_join {a b : State} (ha : Good a) (hb : Good b) : Good (agg.join a b) := by
  rcases a with ⟨s, m⟩
  rcases b with ⟨t, n⟩
  dsimp [Good, agg] at ha hb ⊢
  omega

private theorem join_left_id {a : State} (ha : Good a) : agg.join (0, 0) a = a := by
  rcases a with ⟨s, m⟩
  dsimp [Good] at ha
  simp [agg]
  omega

private theorem join_right_id {a : State} (ha : Good a) : agg.join a (0, 0) = a := by
  rcases a with ⟨s, m⟩
  dsimp [Good] at ha
  simp [agg]
  omega

private theorem join_assoc (a b c : State) :
    agg.join (agg.join a b) c = agg.join a (agg.join b c) := by
  rcases a with ⟨s, m⟩
  rcases b with ⟨t, n⟩
  rcases c with ⟨u, v⟩
  simp [agg]
  omega

private theorem good_fold (l : List State) (a : State) (ha : Good a) (hl : GoodList l) :
    Good (List.foldl agg.join a l) := by
  induction l generalizing a with
  | nil => simpa using ha
  | cons x xs ih =>
      simp only [List.foldl_cons]
      apply ih
      · exact good_join ha (hl x (by simp))
      · intro y hy
        exact hl y (by simp [hy])

private theorem foldl_eq_join (l : List State) (a : State) (ha : Good a) (hl : GoodList l) :
    List.foldl agg.join a l = agg.join a (List.foldl agg.join (0, 0) l) := by
  induction l generalizing a with
  | nil => simp [join_right_id ha]
  | cons x xs ih =>
      have hx : Good x := hl x (by simp)
      have hl' : GoodList xs := fun y hy => hl y (by simp [hy])
      have hax : Good (agg.join a x) := good_join ha hx
      simp only [List.foldl_cons]
      rw [ih (agg.join a x) hax hl']
      rw [join_assoc]
      rw [join_left_id hx]
      rw [ih x hx hl']

private theorem foldl_append_join (l₁ l₂ : List State) (a : State) (ha : Good a)
    (h₁ : GoodList l₁) (h₂ : GoodList l₂) :
    List.foldl agg.join a (l₁ ++ l₂) =
      agg.join (List.foldl agg.join a l₁) (List.foldl agg.join (0, 0) l₂) := by
  rw [List.foldl_append]
  exact foldl_eq_join l₂ _ (good_fold l₁ a ha h₁) h₂

private theorem combine_eq_fold (ps : List (List Nat)) :
    combine agg ps = List.foldl agg.join (0, 0) (ps.map agg.reference) := by
  simp [combine, agg]

private theorem map_set (ps : List (List Nat)) (i : Nat) (xs : List Nat) (hi : i < ps.length) :
    (ps.set i xs).map agg.reference =
      (ps.take i).map agg.reference ++ [agg.reference xs] ++ (ps.drop (i + 1)).map agg.reference := by
  induction ps generalizing i with
  | nil => simp at hi
  | cons p ps ih =>
      cases i with
      | zero => simp
      | succ j =>
          have hj : j < ps.length := by simpa using hi
          simp [ih j hj, List.append_assoc]

theorem sound_AmendLeaf : BalanceFloor.AmendLeaf.Goal := by
  intro ps i xs hi
  rw [combine_eq_fold, map_set ps i xs hi]
  rw [combine_eq_fold, combine_eq_fold]
  let A := (ps.take i).map agg.reference
  let C := (ps.drop (i + 1)).map agg.reference
  let r := agg.reference xs
  have hA : GoodList A := by
    intro y hy
    dsimp [A] at hy
    rcases List.mem_map.mp hy with ⟨ys, _, rfl⟩
    exact good_ref ys
  have hC : GoodList C := by
    intro y hy
    dsimp [C] at hy
    rcases List.mem_map.mp hy with ⟨ys, _, rfl⟩
    exact good_ref ys
  have hr : Good r := by
    dsimp [r]
    exact good_ref xs
  have he : Good (0, 0) := by simp [Good]
  have hsr : GoodList [r] := by
    intro y hy
    simp at hy
    subst y
    exact hr
  have hAr : GoodList (A ++ [r]) := by
    intro y hy
    simp at hy
    rcases hy with hy | hy
    · exact hA y hy
    · subst y
      exact hr
  have hsplit₁ :
      List.foldl agg.join (0, 0) ((A ++ [r]) ++ C) =
        agg.join (List.foldl agg.join (0, 0) (A ++ [r])) (List.foldl agg.join (0, 0) C) := by
    exact foldl_append_join (A ++ [r]) C (0, 0) he hAr hC
  have hsplit₂ :
      List.foldl agg.join (0, 0) (A ++ [r]) =
        agg.join (List.foldl agg.join (0, 0) A) (List.foldl agg.join (0, 0) [r]) := by
    exact foldl_append_join A [r] (0, 0) he hA hsr
  have hsingle : List.foldl agg.join (0, 0) [r] = r := by
    simpa using join_left_id hr
  change List.foldl agg.join (0, 0) ((A ++ [r]) ++ C) =
    agg.join (List.foldl agg.join (0, 0) A) (agg.join r (List.foldl agg.join (0, 0) C))
  rw [hsplit₁, hsplit₂, hsingle, join_assoc]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_AmendLeaf
