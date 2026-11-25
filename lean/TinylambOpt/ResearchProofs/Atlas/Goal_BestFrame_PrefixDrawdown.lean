import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BestFrame_PrefixDrawdown

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check on all series of length <= 4 over [-3, 3]. -/
#eval do
  let vals : List Int := [-3, -2, -1, 0, 1, 2, 3]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  pure ((lists 4).filter (fun x : Series =>
    BestFrame.measure x != Drawdown.measure ((0 :: prefs x).reverse)) |>.length)

/-! ### List algebra: range, zipIdx, pairs, windows, prefs -/

private theorem range_map_succ {α : Type} (n : Nat) (f : Nat → α) :
    (List.range (n + 1)).map f = f 0 :: (List.range n).map (fun i => f (i + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n.succ + 1 = (n + 1).succ by omega]
      rw [List.range_succ, List.map_append, ih]
      simp [List.range_succ]

private theorem flatMap_range_succ {α : Type} (f : Nat → List α) (n : Nat) :
    (List.range (n + 1)).flatMap f = f 0 ++ (List.range n).flatMap (fun i => f (i + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n.succ + 1 = n.succ.succ by omega, List.range_succ, List.flatMap_append]
      rw [ih]
      rw [List.range_succ, List.flatMap_append]
      simp [List.append_assoc]

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem pairs_nil : pairs [] = [] := by
  unfold pairs
  rw [List.zipIdx_nil]
  rfl

private theorem pairs_singleton (a : Int) : pairs [a] = [] := by
  simpa [pairs_nil] using pairs_cons a []

private theorem pairs_map (f : Int → Int) (x : Series) :
    pairs (x.map f) = (pairs x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      simp only [List.map_cons, pairs_cons, ih, List.map_append, List.map_map,
        Function.comp_def]

private theorem map_add_sub (a : Int) (l : List Int) :
    (l.map (fun s => a + s)).map (fun b : Int => b - a) = l := by
  induction l with
  | nil => rfl
  | cons b bs ih => simp [ih]; omega

private theorem pairs_head_sub (a : Int) (ys : Series) :
    (ys.map (fun b : Int => (a, b))).map (fun p : Int × Int => p.2 - p.1) =
      ys.map (fun b : Int => b - a) := by
  induction ys with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun s => a + s) := by
  unfold prefs
  simp only [List.length_cons]
  rw [range_map_succ]
  simp [List.take_succ_cons, List.map_map]

private theorem windows_cons (a : Int) (xs : Series) :
    windows (a :: xs) =
      (List.range (xs.length + 1)).map (fun j => (a :: xs).take (j + 1)) ++ windows xs := by
  unfold windows
  simp only [List.length_cons]
  rw [flatMap_range_succ]
  have hsub : ∀ i : Nat, xs.length + 1 - (i + 1) = xs.length - i := by intro i; omega
  simp [List.drop_succ_cons, hsub]

private theorem wsums_cons (a : Int) (xs : Series) :
    (windows (a :: xs)).map List.sum =
      (a :: (prefs xs).map (fun s => a + s)) ++ (windows xs).map List.sum := by
  rw [windows_cons, List.map_append]
  apply congrArg (fun l => l ++ (windows xs).map List.sum)
  rw [range_map_succ]
  simp [List.take_succ_cons, prefs, List.map_map]

private theorem pairs_z_map (y : Series) :
    (pairs (0 :: y)).map (fun p : Int × Int => p.2 - p.1) =
      y ++ (pairs y).map (fun p : Int × Int => p.2 - p.1) := by
  rw [pairs_cons, List.map_append]
  have hfirst : (y.map (fun b : Int => (0, b))).map
      (fun p : Int × Int => p.2 - p.1) = y := by
    induction y with
    | nil => rfl
    | cons b bs ih => simp [ih]
  rw [hfirst]

private theorem wsums_perm (x : Series) :
    ((windows x).map List.sum).Perm
      ((pairs (0 :: prefs x)).map (fun p : Int × Int => p.2 - p.1)) := by
  induction x with
  | nil => simp [windows, prefs, pairs]
  | cons a xs ih =>
      rw [wsums_cons, prefs_cons, pairs_z_map]
      have ih' : ((windows xs).map List.sum).Perm
          (prefs xs ++ (pairs (prefs xs)).map (fun p : Int × Int => p.2 - p.1)) := by
        simpa [pairs_z_map] using ih
      have htail : (pairs (a :: (prefs xs).map (fun s => a + s))).map
          (fun p : Int × Int => p.2 - p.1) =
          prefs xs ++ (pairs (prefs xs)).map (fun p : Int × Int => p.2 - p.1) := by
        rw [pairs_cons, List.map_append]
        rw [pairs_head_sub]
        rw [map_add_sub]
        rw [pairs_map]
        rw [List.map_map]
        apply congrArg (fun l : List Int => prefs xs ++ l)
        apply List.map_congr_left
        intro p hp
        rcases p with ⟨u, v⟩
        simp
        omega
      rw [htail]
      simpa [List.append_assoc] using ih'.append_left (a :: (prefs xs).map (fun s => a + s))

/-! ### max0 respects permutations, and the reverse-pair identity -/

private theorem max0_cons (a : Int) (xs : List Int) :
    max0 (a :: xs) = max 0 (max a (max0 xs)) := by
  simp only [max0, List.max?_cons]
  cases hx : xs.max? with
  | none => simp
  | some b =>
      simp only [Option.getD_some, Option.elim_some]
      change max 0 (max a b) = max 0 (max a (max 0 b))
      omega

private theorem max0_nonneg (xs : List Int) : 0 ≤ max0 xs := by
  simp only [max0]
  omega

private theorem le_max0_of_mem {xs : List Int} {a : Int} (ha : a ∈ xs) : a ≤ max0 xs := by
  induction xs with
  | nil => simp at ha
  | cons x xs ih =>
      rw [max0_cons]
      rcases List.mem_cons.mp ha with rfl | ha
      · omega
      · have hi := ih ha
        omega

private theorem max0_le {xs : List Int} {b : Int} (hb : 0 ≤ b)
    (h : ∀ a ∈ xs, a ≤ b) : max0 xs ≤ b := by
  induction xs with
  | nil => simpa [max0] using hb
  | cons x xs ih =>
      rw [max0_cons]
      exact Int.max_le.mpr ⟨hb, Int.max_le.mpr ⟨h x (by simp),
        ih (by intro a ha; exact h a (by simp [ha]))⟩⟩

private theorem max0_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : max0 l₁ = max0 l₂ := by
  apply Int.le_antisymm
  · apply max0_le (max0_nonneg _)
    intro a ha
    exact le_max0_of_mem (p.subset ha)
  · apply max0_le (max0_nonneg _)
    intro a ha
    exact le_max0_of_mem (p.symm.subset ha)

private theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem cart_singleton (xs : Series) (a : Int) :
    cart xs [a] = xs.map (fun b : Int => (b, a)) := by
  induction xs with
  | nil => simp [cart]
  | cons b bs ih => simp [cart_cons, ih]

private theorem pairs_append_perm (x y : Series) :
    (pairs (x ++ y)).Perm (pairs x ++ pairs y ++ cart x y) := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.map_append]
      let A : List (Int × Int) := xs.map (fun b : Int => (a, b))
      let B : List (Int × Int) := y.map (fun b : Int => (a, b))
      let P : List (Int × Int) := pairs xs
      let Q : List (Int × Int) := pairs y
      let C : List (Int × Int) := cart xs y
      have h1 := (List.Perm.append_left (A ++ B)) ih
      have h2 := ((List.perm_append_comm (l₁ := B) (l₂ := P)).append_right (Q ++ C)).append_left A
      have h3 := ((List.perm_append_comm (l₁ := B) (l₂ := Q)).append_right C).append_left (A ++ P)
      have h1' : (A ++ (B ++ pairs (xs ++ y))).Perm (A ++ (B ++ (P ++ (Q ++ C)))) := by
        simpa [List.append_assoc] using h1
      have h2' : (A ++ (B ++ (P ++ (Q ++ C)))).Perm (A ++ (P ++ (B ++ (Q ++ C)))) := by
        simpa [List.append_assoc] using h2
      have h3' : (A ++ (P ++ (B ++ (Q ++ C)))).Perm (A ++ (P ++ (Q ++ (B ++ C)))) := by
        simpa [List.append_assoc] using h3
      simpa [List.append_assoc, A, B, P, Q, C] using (h1'.trans (h2'.trans h3'))

private theorem pairs_reverse_perm (x : Series) :
    (pairs x.reverse).Perm ((pairs x).map (fun p : Int × Int => (p.2, p.1))) := by
  induction x with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.reverse_cons]
      have hp := pairs_append_perm xs.reverse [a]
      simp only [pairs_singleton, List.append_nil] at hp
      rw [cart_singleton] at hp
      have hrev : ((xs.reverse).map (fun b : Int => (b, a))).Perm
          (xs.map (fun b : Int => (b, a))) := by
        simp
      have h1 := List.Perm.append ih hrev
      have h2 := List.perm_append_comm
        (l₁ := (pairs xs).map (fun p : Int × Int => (p.2, p.1)))
        (l₂ := xs.map (fun b : Int => (b, a)))
      simpa [pairs_cons, List.map_append, List.map_map, Function.comp_def] using
        (hp.trans (h1.trans h2))

/-! ### Main theorem -/

theorem sound_BestFrame_PrefixDrawdown : BestFrame.PrefixDrawdown.Goal := by
  intro x
  unfold BestFrame.measure Drawdown.measure
  let z : Series := 0 :: prefs x
  have h₁ : max0 ((windows x).map List.sum) =
      max0 ((pairs z).map (fun p : Int × Int => p.2 - p.1)) := by
    exact max0_perm (wsums_perm x)
  have h₂ : max0 ((pairs z.reverse).map (fun p : Int × Int => p.1 - p.2)) =
      max0 ((pairs z).map (fun p : Int × Int => p.2 - p.1)) := by
    apply max0_perm
    have hp := (pairs_reverse_perm z).map (fun p : Int × Int => p.1 - p.2)
    simpa [List.map_map, Function.comp_def] using hp
  rw [h₁]
  exact h₂.symm

end TinylambOpt.ResearchProofs.Atlas.Goal_BestFrame_PrefixDrawdown
