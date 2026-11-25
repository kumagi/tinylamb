import TinylambOpt.ResearchGoals.Atlas.ExecutorResources
import Std

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.DevinBatch.Atlas_ExecutorResources_A

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges

/-! ===== Pipeline: CriticalCut / BatchSummary / SteadyState / SplitOverlap =====
The proofs below are the verified arguments (private helpers included) for the
four pipeline goals; `TwoStageOrder` (Johnson's rule) is treated separately. -/

private def sumF (xs : List Job) : Nat := (xs.map Prod.fst).sum
private def sumS (xs : List Job) : Nat := (xs.map Prod.snd).sum

/-- The cut values of a list starting with a stage-1 readiness `s`. -/
private def cuts (s : Nat) : List Job → List Nat
  | [] => []
  | (a, b) :: rest => (s + a + b + sumS rest) :: cuts (s + a) rest

private theorem max?_getD_cons (x : Nat) (xs : List Nat) :
    (x :: xs).max?.getD 0 = max x (xs.max?.getD 0) := by
  rw [List.max?_cons]
  cases h : xs.max? <;> simp [h]

private theorem le_max?_getD (x : Nat) (xs : List Nat) : x ≤ (x :: xs).max?.getD 0 := by
  rw [max?_getD_cons]
  exact Nat.le_max_left x _

private theorem max?_getD_map_add (c : Nat) {xs : List Nat} (h : xs ≠ []) :
    (xs.map (fun v => c + v)).max?.getD 0 = c + xs.max?.getD 0 := by
  induction xs with
  | nil => contradiction
  | cons x xs ih =>
      rw [List.map_cons, max?_getD_cons, max?_getD_cons]
      cases xs with
      | nil => simp
      | cons y ys =>
          rw [ih (by simp : y :: ys ≠ [])]
          omega

private theorem cutList_eq (s : Nat) (xs : List Job) :
    xs.zipIdx.map (fun (_, i) =>
      s + ((xs.take (i + 1)).map Prod.fst).sum + ((xs.drop i).map Prod.snd).sum) = cuts s xs := by
  induction xs generalizing s with
  | nil => rfl
  | cons j rest ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons]
      dsimp [cuts]
      congr 1
      · simp [sumS]
        omega
      · rw [show rest.zipIdx 1 = (rest.zipIdx 0).map (fun p : Job × Nat => (p.1, p.2 + 1)) by
          simpa using (List.zipIdx_succ (l := rest) (i := 0))]
        rw [List.map_map]
        rw [← ih (s + j.1)]
        apply List.map_congr_left
        intro p hp
        rcases p with ⟨x, k⟩
        dsimp
        simp
        omega

private theorem fold_bound (s t : Nat) (xs : List Job) :
    (xs.foldl step (s, t)).2 = max (t + sumS xs) ((cuts s xs).max?.getD 0) := by
  induction xs generalizing s t with
  | nil => simp [sumS, cuts]
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons]
      dsimp only [step]
      rw [ih]
      change max (max (s + a) t + b + sumS rest) ((cuts (s + a) rest).max?.getD 0) =
        max (t + (b + sumS rest)) (((s + a + b + sumS rest) :: cuts (s + a) rest).max?.getD 0)
      rw [max?_getD_cons]
      omega

private theorem fold_fst (s t : Nat) (xs : List Job) :
    (xs.foldl step (s, t)).1 = s + sumF xs := by
  induction xs generalizing s t with
  | nil => simp [sumF]
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons]
      dsimp only [step]
      rw [ih]
      simp [sumF]
      omega

private theorem fst_le_snd (s t : Nat) (xs : List Job) (h : s ≤ t) :
    (xs.foldl step (s, t)).1 ≤ (xs.foldl step (s, t)).2 := by
  induction xs generalizing s t with
  | nil => simpa using h
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons]
      dsimp only [step]
      apply ih
      omega

private theorem cuts_add (s c : Nat) (xs : List Job) :
    cuts (s + c) xs = (cuts s xs).map (fun v => c + v) := by
  induction xs generalizing s with
  | nil => rfl
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      dsimp [cuts]
      simp only [List.map_cons]
      rw [← ih (s + a)]
      rw [show s + c + a = s + a + c by omega]
      congr 1
      omega

private theorem cuts_zero (s : Nat) (xs : List Job) :
    cuts s xs = (cuts 0 xs).map (fun v => s + v) := by
  simpa using cuts_add 0 s xs

private theorem finish_single (a b : Nat) : finish [(a, b)] = a + b := by
  simp [finish, step]

private theorem finish_nil : finish [] = 0 := by
  simp [finish]

private theorem replicate_succ_eq_append (n : Nat) (j : Job) :
    List.replicate (n + 1) j = List.replicate n j ++ [j] := by
  induction n with
  | zero => simp
  | succ n ih =>
      simpa [List.replicate_succ] using congrArg (List.cons j) ih

theorem CriticalCut_sound :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline.CriticalCut.Goal := by
  intro xs
  rw [show finish xs = max (sumS xs) ((cuts 0 xs).max?.getD 0) by
    dsimp [finish]
    rw [fold_bound]
    simp]
  have hle : sumS xs ≤ (cuts 0 xs).max?.getD 0 := by
    cases xs with
    | nil => simp [sumS]
    | cons j rest =>
        rcases j with ⟨a, b⟩
        dsimp [cuts]
        rw [max?_getD_cons]
        simp [sumS]
        omega
  rw [← cutList_eq 0 xs] at hle ⊢
  simp at hle ⊢
  omega

theorem BatchSummary_sound :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline.BatchSummary.Goal := by
  intro a b
  let s0 := (a.foldl step (0, 0)).1
  let t0 := (a.foldl step (0, 0)).2
  have hs0 : s0 = sumF a := by
    dsimp [s0]
    rw [fold_fst]
    simp
  have ht0 : t0 = finish a := by
    rfl
  have hst : s0 ≤ t0 := by
    dsimp [s0, t0]
    exact fst_le_snd 0 0 a (by omega)
  have hfin : finish (a ++ b) = max (t0 + sumS b) ((cuts s0 b).max?.getD 0) := by
    dsimp [finish, s0, t0]
    rw [List.foldl_append]
    rw [fold_bound]
  cases b with
  | nil =>
      clear hfin
      rw [List.append_nil]
      simp only [sumS, List.map_nil, List.sum_nil]
      rw [finish_nil]
      simp only [Nat.add_zero]
      have hf_le : sumF a ≤ finish a := by
        rw [← hs0, ← ht0]
        exact hst
      unfold sumF at hf_le
      rw [Nat.max_eq_left hf_le]
  | cons j bs =>
      have hne : cuts 0 (j :: bs) ≠ [] := by simp [cuts]
      change finish (a ++ j :: bs) = max (finish a + sumS (j :: bs)) (sumF a + finish (j :: bs))
      rw [cuts_zero] at hfin
      rw [max?_getD_map_add s0 hne] at hfin
      have hfb : finish (j :: bs) = max (sumS (j :: bs)) ((cuts 0 (j :: bs)).max?.getD 0) := by
        simpa [finish] using (fold_bound 0 0 (j :: bs))
      rw [hfin, hfb]
      rw [hs0, ht0] at *
      simp only [sumS] at *
      omega

theorem SteadyState_sound :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline.SteadyState.Goal := by
  intro n a b hn
  induction n with
  | zero => omega
  | succ n ih =>
      cases n with
      | zero => simp [finish, step]
      | succ n =>
          have hi := ih (by omega : 0 < Nat.succ n)
          rw [show List.replicate (Nat.succ (Nat.succ n)) (a, b) =
              List.replicate (Nat.succ n) (a, b) ++ [(a, b)] by
            rw [show Nat.succ (Nat.succ n) = Nat.succ n + 1 by omega]
            exact replicate_succ_eq_append (Nat.succ n) (a, b)]
          rw [BatchSummary_sound]
          simp [sumF, sumS, finish_single] at hi ⊢
          rw [hi]
          by_cases hab : a ≤ b
          · have hm : max a b = b := Nat.max_eq_right hab
            rw [hm]
            rw [Nat.add_mul]
            simp only [Nat.one_mul]
            have hle : n * a + a + (a + b) ≤ a + b + n * b + b := by
              have hna : n * a ≤ n * b := Nat.mul_le_mul_left n hab
              have haa : a + a ≤ b + b := Nat.add_le_add hab hab
              omega
            rw [Nat.max_eq_left hle]
            rw [Nat.add_mul]
            simp only [Nat.one_mul]
            omega
          · have hba : b ≤ a := by omega
            have hm : max a b = a := Nat.max_eq_left hba
            rw [hm]
            rw [Nat.add_mul]
            simp only [Nat.one_mul]
            have hle : a + b + n * a + b ≤ n * a + a + (a + b) := by
              have hbb : b + b ≤ a + a := Nat.add_le_add hba hba
              omega
            rw [Nat.max_eq_right hle]
            omega

private theorem step_t_mono (s t₁ t₂ : Nat) (h : t₁ ≤ t₂) (xs : List Job) :
    (xs.foldl step (s, t₁)).2 ≤ (xs.foldl step (s, t₂)).2 := by
  induction xs generalizing s t₁ t₂ with
  | nil => simpa using h
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons, List.foldl_cons]
      dsimp only [step]
      apply ih
      omega

private theorem split_pair (s t a b c d : Nat) :
    let p := [(a, b), (c, d)].foldl step (s, t)
    let q := [(a + c, b + d)].foldl step (s, t)
    p.1 = q.1 ∧ p.2 ≤ q.2 := by
  dsimp
  simp [step]
  constructor <;> omega

theorem SplitOverlap_sound :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline.SplitOverlap.Goal := by
  intro l r a b c d
  dsimp [finish]
  rw [show (l ++ [(a, b), (c, d)] ++ r).foldl step (0, 0) =
      r.foldl step ([(a, b), (c, d)].foldl step (l.foldl step (0, 0))) by
    rw [List.foldl_append, List.foldl_append]]
  rw [show (l ++ [(a + c, b + d)] ++ r).foldl step (0, 0) =
      r.foldl step ([(a + c, b + d)].foldl step (l.foldl step (0, 0))) by
    rw [List.foldl_append, List.foldl_append]]
  let s := (l.foldl step (0, 0)).1
  let t := (l.foldl step (0, 0)).2
  change (r.foldl step ([(a, b), (c, d)].foldl step (s, t))).2 ≤
    (r.foldl step ([(a + c, b + d)].foldl step (s, t))).2
  have hgen := split_pair s t a b c d
  dsimp at hgen
  have hmono := step_t_mono (([(a, b), (c, d)].foldl step (s, t)).1) _ _ hgen.2 r
  simpa [step, Nat.add_assoc] using hmono

private theorem max?_elim_eq (d : Nat) (l : List Nat) :
    l.max?.elim d (max d) = max d (l.max?.getD 0) := by
  cases h : l.max? <;> simp [h]

private theorem max?_getD_append (xs ys : List Nat) :
    (xs ++ ys).max?.getD 0 = max (xs.max?.getD 0) (ys.max?.getD 0) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.cons_append, max?_getD_cons, max?_getD_cons, ih]
      omega

private theorem cuts_append (s : Nat) (xs ys : List Job) :
    cuts s (xs ++ ys) = (cuts s xs).map (fun v => v + sumS ys) ++ cuts (s + sumF xs) ys := by
  induction xs generalizing s with
  | nil => simp [cuts, sumF]
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      dsimp [cuts, sumF]
      rw [show sumS (rest ++ ys) = sumS rest + sumS ys by simp [sumS]]
      rw [ih]
      rw [List.map_cons, List.cons_append]
      congr 1
      · omega
      · simp [sumF]
        congr 1
        omega

private def jkey (j : Job) : Nat := if j.1 ≤ j.2 then 0 else 1

/-- Johnson's total preorder: front jobs (a ≤ b) first by increasing a,
    then back jobs (a > b) by decreasing b. -/
private def jle (x y : Job) : Bool :=
  if jkey x < jkey y then true
  else if jkey y < jkey x then false
  else if jkey x == 0 then x.1 ≤ y.1 else y.2 ≤ x.2

private theorem jle_trans : ∀ a b c : Job, jle a b = true → jle b c = true → jle a c = true := by
  rintro ⟨a1, a2⟩ ⟨b1, b2⟩ ⟨c1, c2⟩ hab hbc
  by_cases ha : a1 ≤ a2 <;> by_cases hb : b1 ≤ b2 <;> by_cases hc : c1 ≤ c2 <;>
    simp [jle, jkey, ha, hb, hc] at hab hbc ⊢ <;> omega

private theorem jle_total : ∀ a b : Job, (jle a b || jle b a) = true := by
  rintro ⟨a1, a2⟩ ⟨b1, b2⟩
  by_cases ha : a1 ≤ a2 <;> by_cases hb : b1 ≤ b2 <;>
    simp [jle, jkey, ha, hb] <;> omega

private theorem jle_criterion {x y : Job} (h : jle x y = true) :
    min x.1 y.2 ≤ min y.1 x.2 := by
  rcases x with ⟨a1, a2⟩
  rcases y with ⟨b1, b2⟩
  by_cases ha : a1 ≤ a2 <;> by_cases hb : b1 ≤ b2 <;>
    simp [jle, jkey, ha, hb] at h ⊢ <;> omega

private theorem cutmax_swap (p q : List Job) (x y : Job)
    (hcrit : min y.1 x.2 ≤ min x.1 y.2) :
    (cuts 0 (p ++ [y, x] ++ q)).max?.getD 0 ≤
      (cuts 0 (p ++ [x, y] ++ q)).max?.getD 0 := by
  rcases x with ⟨x1, x2⟩
  rcases y with ⟨y1, y2⟩
  simp at hcrit
  rw [cuts_append 0 (p ++ [(y1, y2), (x1, x2)]) q]
  rw [cuts_append 0 (p ++ [(x1, x2), (y1, y2)]) q]
  rw [cuts_append 0 p [(y1, y2), (x1, x2)]]
  rw [cuts_append 0 p [(x1, x2), (y1, y2)]]
  simp [cuts, sumS, sumF, max?_getD_append, max?_elim_eq]
  rw [show y2 + x2 = x2 + y2 by omega]
  have ht : cuts ((List.map Prod.fst p).sum + (y1 + x1)) q =
      cuts ((List.map Prod.fst p).sum + (x1 + y1)) q := by
    congr 1
    omega
  rw [ht]
  omega

private theorem swap_finish (p q : List Job) (x y : Job) (h : jle y x = true) :
    finish (p ++ [y, x] ++ q) ≤ finish (p ++ [x, y] ++ q) := by
  have hcrit := jle_criterion (x := y) (y := x) h
  have hsum : sumS (p ++ [y, x] ++ q) = sumS (p ++ [x, y] ++ q) := by
    simp [sumS]
    omega
  have hf1 : finish (p ++ [y, x] ++ q) =
      max (sumS (p ++ [y, x] ++ q)) ((cuts 0 (p ++ [y, x] ++ q)).max?.getD 0) := by
    dsimp [finish]
    rw [fold_bound]
    simp
  have hf2 : finish (p ++ [x, y] ++ q) =
      max (sumS (p ++ [x, y] ++ q)) ((cuts 0 (p ++ [x, y] ++ q)).max?.getD 0) := by
    dsimp [finish]
    rw [fold_bound]
    simp
  rw [hf1, hf2, hsum]
  have hc := cutmax_swap p q x y hcrit
  omega

private def ins (j : Job) : List Job → List Job
  | [] => [j]
  | x :: xs => if jle j x then j :: x :: xs else x :: ins j xs

private def isort : List Job → List Job
  | [] => []
  | j :: js => ins j (isort js)

private theorem ins_mem (j y : Job) (l : List Job) : y ∈ ins j l ↔ y = j ∨ y ∈ l := by
  induction l with
  | nil => simp [ins]
  | cons x xs ih =>
      by_cases h : jle j x = true
      · simp [ins, h, ih]
      · simp [ins, h]
        rw [ih]
        constructor
        · intro hm
          rcases hm with hyx | hrest
          · exact Or.inr (Or.inl hyx)
          · rcases hrest with hyj | hyxs
            · exact Or.inl hyj
            · exact Or.inr (Or.inr hyxs)
        · intro hm
          rcases hm with hyj | hrest
          · exact Or.inr (Or.inl hyj)
          · rcases hrest with hyx | hyxs
            · exact Or.inl hyx
            · exact Or.inr (Or.inr hyxs)

private theorem ins_pairwise (j : Job) (l : List Job)
    (hl : l.Pairwise (fun a b => jle a b = true)) :
    (ins j l).Pairwise (fun a b => jle a b = true) := by
  induction l with
  | nil => simp [ins]
  | cons x xs ih =>
      by_cases h : jle j x = true
      · change List.Pairwise (fun a b => jle a b = true)
          (if jle j x = true then j :: x :: xs else x :: ins j xs)
        rw [if_pos h]
        have hcross : ∀ y : Job, y ∈ x :: xs → jle j y = true := by
          intro y hy
          rw [List.mem_cons] at hy
          rcases hy with hxy | hxmem
          · simpa [hxy] using h
          · exact jle_trans j x y h (List.rel_of_pairwise_cons hl hxmem)
        exact List.Pairwise.cons hcross hl
      · have hxj : jle x j = true := by
          have ht := jle_total j x
          simp [h] at ht
          exact ht
        change List.Pairwise (fun a b => jle a b = true)
          (if jle j x = true then j :: x :: xs else x :: ins j xs)
        rw [if_neg h]
        have hcross : ∀ y : Job, y ∈ ins j xs → jle x y = true := by
          intro y hy
          rcases (ins_mem j y xs).mp hy with hyj | hyxs
          · simpa [hyj] using hxj
          · exact List.rel_of_pairwise_cons hl hyxs
        exact List.Pairwise.cons hcross (ih hl.tail)

private theorem isort_pairwise (l : List Job) :
    (isort l).Pairwise (fun a b => jle a b = true) := by
  induction l with
  | nil => simp [isort]
  | cons j js ih => exact ins_pairwise j (isort js) ih

private theorem ins_perm (j : Job) (l : List Job) : (ins j l).Perm (j :: l) := by
  induction l with
  | nil => simp [ins]
  | cons x xs ih =>
      by_cases h : jle j x = true
      · simp [ins, h]
      · change (if jle j x = true then j :: x :: xs else x :: ins j xs).Perm (j :: x :: xs)
        rw [if_neg h]
        exact (List.Perm.cons x ih).trans (List.Perm.swap j x xs)

private theorem isort_perm (l : List Job) : (isort l).Perm l := by
  induction l with
  | nil => simp [isort]
  | cons j js ih =>
      exact (ins_perm j (isort js)).trans (List.Perm.cons j ih)

private theorem ins_improve (j : Job) (l : List Job)
    (hl : l.Pairwise (fun a b => jle a b = true)) (p q : List Job) :
    finish (p ++ ins j l ++ q) ≤ finish (p ++ j :: l ++ q) := by
  induction l generalizing p q with
  | nil => simp [ins]
  | cons x xs ih =>
      by_cases h : jle j x = true
      · change finish (p ++ (if jle j x = true then j :: x :: xs else x :: ins j xs) ++ q) ≤
          finish (p ++ j :: x :: xs ++ q)
        rw [ite_eq_left h]
        omega
      · have hxj : jle x j = true := by
          have ht := jle_total j x
          simp [h] at ht
          exact ht
        change finish (p ++ (if jle j x = true then j :: x :: xs else x :: ins j xs) ++ q) ≤
          finish (p ++ j :: x :: xs ++ q)
        rw [ite_eq_right h]
        have h1 := ih hl.tail (p ++ [x]) q
        have h2 := swap_finish p (xs ++ q) j x hxj
        exact Nat.le_trans (by simpa [List.append_assoc] using h1)
          (by simpa [List.append_assoc] using h2)

private theorem isort_improve (xs : List Job) (p q : List Job) :
    finish (p ++ isort xs ++ q) ≤ finish (p ++ xs ++ q) := by
  induction xs generalizing p q with
  | nil => simp [isort]
  | cons j js ih =>
      have h1 := ins_improve j (isort js) (isort_pairwise js) p q
      have h2 := ih (p ++ [j]) q
      change finish (p ++ ins j (isort js) ++ q) ≤ finish (p ++ j :: js ++ q)
      exact Nat.le_trans h1 (by simpa [List.append_assoc] using h2)

private theorem jle_refl (a : Job) : jle a a = true := by
  have h := jle_total a a
  simpa using h

private theorem mem_split {α : Type} (a : α) : ∀ l : List α, a ∈ l → ∃ u v, l = u ++ a :: v := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro h
      rw [List.mem_cons] at h
      rcases h with hxa | hxs
      · subst x
        exact ⟨[], xs, rfl⟩
      · rcases ih hxs with ⟨u, v, huv⟩
        exact ⟨x :: u, v, by simp [huv]⟩

private theorem perm_move_front {α : Type} (a : α) :
    ∀ u v : List α, (u ++ a :: v).Perm (a :: (u ++ v)) := by
  intro u
  induction u with
  | nil => intro v; rfl
  | cons x xs ih =>
      intro v
      have h1 : (xs ++ a :: v).Perm (a :: xs ++ v) := ih v
      have h2 := List.Perm.cons x h1
      have h3 : (x :: a :: (xs ++ v)).Perm (a :: x :: (xs ++ v)) :=
        List.Perm.swap a x (xs ++ v)
      exact h2.trans h3

private theorem move_front (a : Job) (u v p q : List Job)
    (hcross : ∀ x ∈ u, jle a x = true) :
    finish (p ++ a :: u ++ v ++ q) ≤ finish (p ++ u ++ a :: v ++ q) := by
  induction u generalizing p v q with
  | nil => simp
  | cons x xs ih =>
      have hx : jle a x = true := hcross x (by simp)
      have h1 := ih (p := p ++ [x]) (v := v) (q := q)
        (by intro y hy; exact hcross y (by simp [hy]))
      have h2 := swap_finish p (xs ++ v ++ q) x a hx
      exact Nat.le_trans (by simpa [List.append_assoc] using h2)
        (by simpa [List.append_assoc] using h1)

private theorem sorted_optimal (z : List Job)
    (hz : z.Pairwise (fun a b => jle a b = true)) :
    ∀ w, w.Perm z → ∀ p q, finish (p ++ z ++ q) ≤ finish (p ++ w ++ q) := by
  induction z with
  | nil =>
      intro w hw p q
      cases w with
      | nil => simp
      | cons b bs =>
          have hwlen : (b :: bs).length = 0 := by simpa using hw.length_eq
          simp at hwlen
  | cons a zs ih =>
      intro w hw p q
      have hmem : a ∈ w := (hw.mem_iff).mpr (by simp)
      rcases mem_split a w hmem with ⟨u, v, hwuv⟩
      have hcross : ∀ x ∈ u, jle a x = true := by
        intro x hx
        have hxw : x ∈ w := by
          rw [hwuv, List.mem_append]
          exact Or.inl hx
        have hxz : x ∈ a :: zs := (hw.mem_iff).mp hxw
        rw [List.mem_cons] at hxz
        rcases hxz with hxa | hxzs
        · rw [hxa]
          exact jle_refl a
        · exact List.rel_of_pairwise_cons hz hxzs
      have hw' : (u ++ a :: v).Perm (a :: zs) := by simpa [hwuv] using hw
      have htail : (u ++ v).Perm zs := by
        have hfront : (a :: (u ++ v)).Perm (a :: zs) :=
          (perm_move_front a u v).symm.trans hw'
        exact List.Perm.cons_inv hfront
      have hih := ih hz.tail (u ++ v) htail (p ++ [a]) q
      have hmf := move_front a u v p q hcross
      rw [hwuv]
      exact Nat.le_trans (by simpa [List.append_assoc] using hih)
        (by simpa [List.append_assoc] using hmf)

private theorem pairwise_append {R : Job → Job → Prop} {l₁ l₂ : List Job}
    (h₁ : l₁.Pairwise R) (h₂ : l₂.Pairwise R)
    (hc : ∀ a ∈ l₁, ∀ b ∈ l₂, R a b) : (l₁ ++ l₂).Pairwise R := by
  induction l₁ with
  | nil => simpa using h₂
  | cons x xs ih =>
      exact List.Pairwise.cons
        (by
          intro y hy
          rcases List.mem_append.mp hy with hy | hy
          · exact List.rel_of_pairwise_cons h₁ hy
          · exact hc x (by simp) y hy)
        (ih h₁.tail (by
          intro a ha b hb
          exact hc a (by simp [ha]) b hb))

private theorem front_sorted (l : List Job) (hl : ∀ j ∈ l, j.1 ≤ j.2) :
    (l.mergeSort (fun a b : Job => a.1 ≤ b.1)).Pairwise (fun a b => jle a b = true) := by
  let cmp : Job → Job → Bool := fun a b => a.1 ≤ b.1
  have hperm := List.mergeSort_perm l cmp
  have hs := List.pairwise_mergeSort (le := cmp)
    (trans := by intro a b c hab hbc; simp [cmp] at hab hbc ⊢; omega)
    (total := by intro a b; simp [cmp]; omega) l
  rw [List.pairwise_iff_getElem]
  intro i j hi hj hij
  have ha_ms := List.getElem_mem hi
  have hb_ms := List.getElem_mem hj
  have ha : (l.mergeSort cmp)[i] ∈ l := hperm.mem_iff.mp (by simpa [cmp] using ha_ms)
  have hb : (l.mergeSort cmp)[j] ∈ l := hperm.mem_iff.mp (by simpa [cmp] using hb_ms)
  have hrel := (List.pairwise_iff_getElem.mp hs) i j hi hj hij
  have ha' := hl ((l.mergeSort cmp)[i]) ha
  have hb' := hl ((l.mergeSort cmp)[j]) hb
  simpa [jle, jkey, ha', hb', cmp] using hrel

private theorem back_sorted (l : List Job) (hl : ∀ j ∈ l, ¬ j.1 ≤ j.2) :
    (l.mergeSort (fun a b : Job => a.2 ≥ b.2)).Pairwise (fun a b => jle a b = true) := by
  let cmp : Job → Job → Bool := fun a b => a.2 ≥ b.2
  have hperm := List.mergeSort_perm l cmp
  have hs := List.pairwise_mergeSort (le := cmp)
    (trans := by intro a b c hab hbc; simp [cmp] at hab hbc ⊢; omega)
    (total := by intro a b; simp [cmp]; omega) l
  rw [List.pairwise_iff_getElem]
  intro i j hi hj hij
  have ha_ms := List.getElem_mem hi
  have hb_ms := List.getElem_mem hj
  have ha : (l.mergeSort cmp)[i] ∈ l := hperm.mem_iff.mp (by simpa [cmp] using ha_ms)
  have hb : (l.mergeSort cmp)[j] ∈ l := hperm.mem_iff.mp (by simpa [cmp] using hb_ms)
  have hrel := (List.pairwise_iff_getElem.mp hs) i j hi hj hij
  have ha' := hl ((l.mergeSort cmp)[i]) ha
  have hb' := hl ((l.mergeSort cmp)[j]) hb
  simpa [jle, jkey, ha', hb', cmp, ge_iff_le] using hrel

private theorem johnson_sorted (xs : List Job) :
    (johnson xs).Pairwise (fun a b => jle a b = true) := by
  let p : Job → Bool := fun j => j.1 ≤ j.2
  dsimp [johnson]
  rw [List.partition_eq_filter_filter]
  let front := xs.filter p
  let back := xs.filter (not ∘ p)
  change ((front.mergeSort (fun a b : Job => a.1 ≤ b.1)) ++
    (back.mergeSort (fun a b : Job => a.2 ≥ b.2))).Pairwise (fun a b => jle a b = true)
  have hfront : ∀ j ∈ front, j.1 ≤ j.2 := by
    intro j hj
    have hj' : j ∈ xs.filter p := by simpa [front] using hj
    have hp := (List.mem_filter.mp hj').2
    simpa [p] using hp
  have hback : ∀ j ∈ back, ¬ j.1 ≤ j.2 := by
    intro j hj
    have hj' : j ∈ xs.filter (not ∘ p) := by simpa [back] using hj
    have hp := (List.mem_filter.mp hj').2
    simp only [Function.comp_apply] at hp
    have hpfalse : p j = false := by simpa using hp
    simpa [p] using hpfalse
  have hcross : ∀ a ∈ front, ∀ b ∈ back, jle a b = true := by
    intro a ha b hb
    have ha' := hfront a ha
    have hb' := hback b hb
    simp [jle, jkey, ha', hb']
  have hpF := List.mergeSort_perm front (fun a b : Job => a.1 ≤ b.1)
  have hpB := List.mergeSort_perm back (fun a b : Job => a.2 ≥ b.2)
  have hcross' : ∀ a ∈ front.mergeSort (fun c d : Job => c.1 ≤ d.1),
      ∀ b ∈ back.mergeSort (fun c d : Job => c.2 ≥ d.2), jle a b = true := by
    intro a ha b hb
    exact hcross a (hpF.mem_iff.mp ha) b (hpB.mem_iff.mp hb)
  exact pairwise_append (front_sorted front hfront) (back_sorted back hback) hcross'

private theorem johnson_perm (xs : List Job) : (johnson xs).Perm xs := by
  let p : Job → Bool := fun j => j.1 ≤ j.2
  dsimp [johnson]
  rw [List.partition_eq_filter_filter]
  change ((xs.filter p).mergeSort (fun a b : Job => a.1 ≤ b.1) ++
    (xs.filter (not ∘ p)).mergeSort (fun a b : Job => a.2 ≥ b.2)).Perm xs
  have hf := List.mergeSort_perm (xs.filter p) (fun a b : Job => a.1 ≤ b.1)
  have hb := List.mergeSort_perm (xs.filter (not ∘ p)) (fun a b : Job => a.2 ≥ b.2)
  have happ := hf.append hb
  have hfilt := List.filter_append_perm p xs
  exact happ.trans (by simpa [Function.comp_def] using hfilt)

theorem TwoStageOrder_sound :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline.TwoStageOrder.Goal := by
  intro xs ys hperm
  have hj : (johnson xs).Perm ys := (johnson_perm xs).trans hperm
  simpa using sorted_optimal (johnson xs) (johnson_sorted xs) ys hj.symm [] []

/-! ===== Exchanges helpers (packets / volume / sums) ===== -/

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  induction h : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons a l =>
          rw [List.eraseDups_cons]
          refine List.nodup_cons.mpr ⟨?_, ?_⟩
          · simp
          · apply ih ((l.filter (fun b => !b == a)).length)
            · have hle := List.length_filter_le (l := l) (p := fun b => !b == a)
              have hn : n = l.length + 1 := by simpa using h.symm
              omega
            · rfl

private theorem sum_map_eq_zero_of_all_zero {α : Type} (l : List α) (f : α → Nat)
    (h : ∀ x ∈ l, f x = 0) : (l.map f).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y = 0 := by intro y hy; exact h y (by simp [hy])
      simp only [List.map_cons, List.sum_cons]
      rw [hx]
      simpa using ih ht

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_map_le_sum_map {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y ≤ g y := by intro y hy; exact h y (by simp [hy])
      exact Nat.add_le_add hx (ih ht)

private theorem sum_map_mul_left {α : Type} (l : List α) (f : α → Nat) (n : Nat) :
    (l.map (fun x => n * f x)).sum = n * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [Nat.mul_add]

private theorem sum_map_ite_countP (l : List Nat) (p : Nat → Bool) :
    (l.map (fun d => if p d then 1 else 0)).sum = l.countP p := by
  induction l with
  | nil => simp
  | cons d ds ih =>
      simp only [List.map_cons, List.sum_cons, List.countP_cons]
      by_cases h : p d = true
      · simp [h, ih]
        omega
      · simp [h, ih]

private theorem countP_eq_le_one {ds : List Nat} (hnd : ds.Nodup) (x : Nat) :
    ds.countP (fun d => d == x) ≤ 1 := by
  induction ds with
  | nil => simp
  | cons d ds ih =>
      rw [List.countP_cons]
      by_cases hd : d == x
      · have hne : ∀ y ∈ ds, y ≠ x := by
          intro y hy hyx
          have hdx : d = x := by simpa using hd
          have : d = y := by omega
          exact (List.nodup_cons.mp hnd).1 (by simpa [this] using hy)
        have hzero : ds.countP (fun y => y == x) = 0 := by
          rw [List.countP_eq_zero]
          intro y hy htrue
          have hyx : y = x := by simpa using htrue
          exact hne y hy hyx
        simp [hd, hzero]
      · have hnd' := (List.nodup_cons.mp hnd).2
        have hi := ih hnd'
        simp [hd]
        omega

private theorem packets_zero (cap : Nat) : packets cap 0 = 0 := by
  unfold packets
  cases cap <;> simp

private theorem le_packets_mul (cap n : Nat) (hcap : 0 < cap) :
    n ≤ packets cap n * cap := by
  unfold packets
  have hdecomp0 : cap * ((n + cap - 1) / cap) + (n + cap - 1) % cap = n + cap - 1 :=
    Nat.div_add_mod (n + cap - 1) cap
  have hdecomp : ((n + cap - 1) / cap) * cap + (n + cap - 1) % cap = n + cap - 1 := by
    simpa [Nat.mul_comm] using hdecomp0
  have hmod : (n + cap - 1) % cap < cap := Nat.mod_lt _ hcap
  omega

private theorem packets_le_of_le_mul (cap n k : Nat) (hcap : 0 < cap)
    (h : n ≤ k * cap) : packets cap n ≤ k := by
  unfold packets
  apply Nat.le_of_lt_succ
  apply (Nat.div_lt_iff_lt_mul hcap).mpr
  have hlt : n + cap - 1 < k * cap + cap := by omega
  have heq : (k + 1) * cap = k * cap + cap := by simp [Nat.add_mul]
  rwa [heq]

private theorem packets_add_le_one (cap x y : Nat) (hcap : 0 < cap) :
    packets cap x + packets cap y ≤ packets cap (x + y) + 1 := by
  unfold packets
  let a := x + cap - 1
  let b := y + cap - 1
  have hq : a / cap + b / cap ≤ (a + b) / cap := by
    rw [Nat.add_div hcap]
    omega
  have hle : a + b ≤ (x + y + cap - 1) + cap := by
    dsimp [a, b]
    omega
  have hd := Nat.div_le_div_right hle (c := cap)
  have hadd : ((x + y + cap - 1) + cap) / cap = (x + y + cap - 1) / cap + 1 := by
    rw [show (x + y + cap - 1) + cap = (x + y + cap - 1) + cap * 1 by simp]
    rw [Nat.add_mul_div_left (x + y + cap - 1) 1 hcap]
  dsimp [a, b] at hq hd
  omega

private theorem packets_add (cap x y : Nat) (hcap : 0 < cap) :
    packets cap (x + y) ≤ packets cap x + packets cap y := by
  have hx := le_packets_mul cap x hcap
  have hy := le_packets_mul cap y hcap
  have hxy : x + y ≤ (packets cap x + packets cap y) * cap := by
    rw [Nat.add_mul]
    omega
  exact packets_le_of_le_mul cap (x + y) (packets cap x + packets cap y) hcap hxy

private theorem packets_sum_le (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    packets cap xs.sum ≤ (xs.map (packets cap)).sum := by
  induction xs with
  | nil => simp [packets_zero]
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.le_trans (packets_add cap x xs.sum hcap)
        (Nat.add_le_add_left ih (packets cap x))

private theorem volume_append (a b : List Bucket) (d : Nat) :
    volume (a ++ b) d = volume a d + volume b d := by
  unfold volume
  rw [List.filter_append, List.map_append, List.sum_append]

private theorem volume_not_mem (xs : List Bucket) (d : Nat)
    (hd : d ∉ xs.map Prod.fst) : volume xs d = 0 := by
  unfold volume
  have hf : xs.filter (fun p => p.1 == d) = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro p hp
    have hm := List.mem_filter.mp hp
    have hpd : p.1 = d := by simpa using hm.2
    exact hd (List.mem_map.mpr ⟨p, hm.1, hpd⟩)
  rw [hf]
  simp

private theorem sum_le_length_mul {α : Type} (l : List α) (f : α → Nat) (cap : Nat)
    (h : ∀ x ∈ l, f x ≤ cap) : (l.map f).sum ≤ l.length * cap := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y ≤ cap := by intro y hy; exact h y (by simp [hy])
      have hi := ih ht
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [Nat.succ_mul]
      omega

private theorem volume_le_count_mul (xs : List Bucket) (d cap : Nat)
    (h : ∀ p ∈ xs, p.2 ≤ cap) :
    volume xs d ≤ (xs.filter (fun p => p.1 == d)).length * cap := by
  unfold volume
  apply sum_le_length_mul
  intro p hp
  exact h p (List.mem_filter.mp hp).1

private theorem packets_volume_le_count (xs : List Bucket) (d cap : Nat) (hcap : 0 < cap)
    (h : ∀ p ∈ xs, p.2 ≤ cap) :
    packets cap (volume xs d) ≤ (xs.filter (fun p => p.1 == d)).length := by
  exact packets_le_of_le_mul cap (volume xs d) _ hcap (volume_le_count_mul xs d cap h)

private theorem sum_countDest_le_length (ds : List Nat) (hnd : ds.Nodup) (wire : List Bucket) :
    (ds.map (fun d => (wire.filter (fun p => p.1 == d)).length)).sum ≤ wire.length := by
  induction wire with
  | nil =>
      simpa using sum_map_eq_zero_of_all_zero ds (fun _ => 0) (by simp)
  | cons p wire ih =>
      simp only [List.filter_cons]
      have hstep :
          (ds.map (fun d => (if p.1 == d then p :: wire.filter (fun q => q.1 == d)
              else wire.filter (fun q => q.1 == d)).length)).sum =
            (ds.map (fun d => (wire.filter (fun q => q.1 == d)).length)).sum +
              ds.countP (fun d => p.1 == d) := by
        rw [← sum_map_ite_countP ds (fun d => p.1 == d)]
        rw [← sum_map_add]
        have hmap :
            (ds.map (fun d => (if p.1 == d then p :: wire.filter (fun q => q.1 == d)
                else wire.filter (fun q => q.1 == d)).length)) =
              ds.map (fun d => (wire.filter (fun q => q.1 == d)).length +
                if p.1 == d then 1 else 0) := by
          apply List.map_congr_left
          intro d hdmem
          by_cases h : p.1 == d <;> simp [h]
        rw [hmap]
      rw [hstep]
      have hc : ds.countP (fun d => p.1 == d) ≤ 1 := by
        have hc' := countP_eq_le_one hnd p.1
        have hfuneq : (fun d => p.1 == d) = (fun d => d == p.1) := by
          funext d
          exact Bool.beq_comm
        simpa [hfuneq] using hc'
      rw [List.length_cons]
      omega

theorem PacketLowerBound_sound :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges.PacketLowerBound.Goal := by
  intro cap xs wire hcap hvol hcapw
  let D := (xs.map Prod.fst).eraseDups
  have hD : D.Nodup := nodup_eraseDups _
  have hvolw : ∀ d, volume wire d = volume xs d := hvol
  have hstep : ∀ d, packets cap (volume xs d) ≤ (wire.filter (fun p => p.1 == d)).length := by
    intro d
    rw [← hvolw d]
    exact packets_volume_le_count wire d cap hcap hcapw
  have hsum := sum_map_le_sum_map D (fun d => packets cap (volume xs d))
    (fun d => (wire.filter (fun p => p.1 == d)).length) (fun d _ => hstep d)
  have hlen := sum_countDest_le_length D hD wire
  have htr : traffic cap xs = (D.map (fun d => packets cap (volume xs d))).sum := rfl
  rw [htr]
  exact Nat.le_trans hsum hlen

theorem FrameScale_sound :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges.FrameScale.Goal := by
  intro cap xs n hcap hn
  let D := (xs.map Prod.fst).eraseDups
  have htr1 : traffic (cap * n) xs = (D.map (fun d => packets (cap * n) (volume xs d))).sum := rfl
  have htr2 : traffic cap xs = (D.map (fun d => packets cap (volume xs d))).sum := rfl
  have hcn : 0 < cap * n := Nat.mul_pos hcap hn
  constructor
  · rw [htr1, htr2]
    apply sum_map_le_sum_map
    intro d _
    apply packets_le_of_le_mul (cap * n) (volume xs d) (packets cap (volume xs d)) hcn
    have h1 := le_packets_mul cap (volume xs d) hcap
    have h2 : volume xs d ≤ (packets cap (volume xs d) * cap) * n :=
      Nat.le_trans h1 (Nat.le_mul_of_pos_right (packets cap (volume xs d) * cap) hn)
    rw [← Nat.mul_assoc (packets cap (volume xs d)) cap n]
    exact h2
  · rw [htr1, htr2, ← sum_map_mul_left]
    apply sum_map_le_sum_map
    intro d _
    apply packets_le_of_le_mul cap (volume xs d) (n * packets (cap * n) (volume xs d)) hcap
    have h1 := le_packets_mul (cap * n) (volume xs d) hcn
    have h2 : (n * packets (cap * n) (volume xs d)) * cap =
        packets (cap * n) (volume xs d) * (cap * n) := by
      rw [Nat.mul_assoc, Nat.mul_left_comm n (packets (cap * n) (volume xs d)) cap,
        Nat.mul_comm n cap]
    rw [h2]
    exact h1


private theorem nodup_decomp {l : List Nat} (hnd : l.Nodup) {a : Nat} (ha : a ∈ l) :
    ∃ l', List.Perm l (a :: l') ∧ a ∉ l' ∧ l'.Nodup := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      by_cases hba : b = a
      · subst b
        exact ⟨bs, List.Perm.refl _, (List.nodup_cons.mp hnd).1, (List.nodup_cons.mp hnd).2⟩
      · have habs : a ∈ bs := by
          rcases List.mem_cons.mp ha with h | h
          · exact False.elim (hba h.symm)
          · exact h
        rcases ih (List.nodup_cons.mp hnd).2 habs with ⟨l', hp, hnot, hnd'⟩
        refine ⟨b :: l', ?_, ?_, ?_⟩
        · exact (List.Perm.cons b hp).trans (List.Perm.swap a b l')
        · have hba' : a ≠ b := by intro h; exact hba h.symm
          simp [hba', hnot]
        · refine List.nodup_cons.mpr ⟨?_, hnd'⟩
          intro hbin
          exact (List.nodup_cons.mp hnd).1 ((hp.mem_iff).mpr (by simp [hbin]))

private theorem sum_group_by_key {α : Type} (key : α → Nat) (f : α → Nat)
    (l : List α) (rs : List Nat) (hrnd : rs.Nodup)
    (hcover : ∀ a ∈ l, key a ∈ rs) :
    (l.map f).sum =
      (rs.map (fun r => ((l.filter (fun a => key a == r)).map f).sum)).sum := by
  induction l with
  | nil =>
      symm
      apply sum_map_eq_zero_of_all_zero
      intro r hr
      simp
  | cons a l ih =>
      have hcover_l : ∀ x ∈ l, key x ∈ rs := by intro x hx; exact hcover x (by simp [hx])
      have ihres := ih hcover_l
      let rkey := key a
      rcases nodup_decomp hrnd (hcover a (by simp)) with ⟨rs', hperm, hnot, hnd'⟩
      have hperm1 :
          (rs.map (fun t => (((a :: l).filter (fun x => key x == t)).map f).sum)).sum =
          ((rkey :: rs').map (fun t => (((a :: l).filter (fun x => key x == t)).map f).sum)).sum := by
        exact List.Perm.sum_nat (hperm.map (fun t => (((a :: l).filter (fun x => key x == t)).map f).sum))
      have hperm2 :
          (rs.map (fun t => ((l.filter (fun x => key x == t)).map f).sum)).sum =
          ((rkey :: rs').map (fun t => ((l.filter (fun x => key x == t)).map f).sum)).sum := by
        exact List.Perm.sum_nat (hperm.map (fun t => ((l.filter (fun x => key x == t)).map f).sum))
      have hgroupr :
          (((a :: l).filter (fun x => key x == rkey)).map f).sum =
            f a + ((l.filter (fun x => key x == rkey)).map f).sum := by
        simp [rkey]
      have hgroups' :
          (rs'.map (fun t => (((a :: l).filter (fun x => key x == t)).map f).sum)) =
          (rs'.map (fun t => ((l.filter (fun x => key x == t)).map f).sum)) := by
        apply List.map_congr_left
        intro t ht
        have hne : rkey ≠ t := by
          intro heq
          exact hnot (by simpa [rkey, heq] using ht)
        have hbad : (key a == t) = false := by
          by_cases h : (key a == t) = true
          · have : rkey = t := by simpa [rkey] using h
            exact False.elim (hne this)
          · exact Bool.eq_false_of_ne_true h
        simp [hbad]
      simp only [List.map_cons, List.sum_cons]
      rw [ihres, hperm1, hperm2]
      simp only [List.map_cons, List.sum_cons]
      rw [hgroupr, hgroups']
      omega

private theorem sum_map_filter_eq {α : Type} (l : List α) (f : α → Nat) (p : α → Bool)
    (hz : ∀ x ∈ l, p x = false → f x = 0) :
    (l.map f).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      by_cases h : p x = true
      · simp [h]
        have ht : ∀ y ∈ xs, p y = false → f y = 0 := by
          intro y hy hpy; exact hz y (by simp [hy]) hpy
        rw [ih ht]
      · have hf : p x = false := Bool.eq_false_of_ne_true h
        have hx := hz x (by simp) hf
        simp [h, hx]
        exact ih (by intro y hy hpy; exact hz y (by simp [hy]) hpy)

private theorem sum_eraseDups_eq_of_mem (f : Nat → Nat) (l₁ l₂ : List Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂) : (l₁.map f).sum = (l₂.map f).sum := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hnil : l₂ = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro d hd
        cases ((hmem d).mpr (by simpa using hd))
      subst l₂
      simp
  | cons a l₁ ih =>
      have ha2 : a ∈ l₂ := (hmem a).mp (by simp)
      rcases nodup_decomp hnd₂ ha2 with ⟨l₂', hp, hnot, hnd₂'⟩
      have hmem' : ∀ d, d ∈ l₁ ↔ d ∈ l₂' := by
        intro d
        constructor
        · intro hd
          have hd2 : d ∈ l₂ := (hmem d).mp (by simp [hd])
          have hda : d ∈ a :: l₂' := hp.mem_iff.mp hd2
          rcases List.mem_cons.mp hda with h | h
          · subst h
            exact False.elim ((List.nodup_cons.mp hnd₁).1 hd)
          · exact h
        · intro hd
          have hd2 : d ∈ a :: l₂' := by simp [hd]
          have hd1 : d ∈ a :: l₁ := (hmem d).mpr (hp.mem_iff.mpr hd2)
          rcases List.mem_cons.mp hd1 with h | h
          · subst h
            exact False.elim (hnot hd)
          · exact h
      have ihres := ih l₂' (List.nodup_cons.mp hnd₁).2 hnd₂' hmem'
      have hperm2 : (l₂.map f).sum = ((a :: l₂').map f).sum :=
        List.Perm.sum_nat (hp.map f)
      simp only [List.map_cons, List.sum_cons]
      rw [hperm2]
      simp only [List.map_cons, List.sum_cons]
      rw [ihres]

private theorem sum_eraseDups_eq_of_mem_zero (f : Nat → Nat) (l₁ l₂ : List Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (hsub : ∀ d ∈ l₁, d ∈ l₂) (hzero : ∀ d ∈ l₂, d ∉ l₁ → f d = 0) :
    (l₁.map f).sum = (l₂.map f).sum := by
  let l₂' := l₂.filter (fun d => d ∈ l₁)
  have hnd' : l₂'.Nodup := by
    dsimp [l₂']
    exact List.Nodup.sublist List.filter_sublist hnd₂
  have hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂' := by
    intro d
    constructor
    · intro hd
      exact List.mem_filter.mpr ⟨hsub d hd, by simp [hd]⟩
    · intro hd
      exact of_decide_eq_true (List.mem_filter.mp hd).2
  have heq1 : (l₁.map f).sum = (l₂'.map f).sum := sum_eraseDups_eq_of_mem f l₁ l₂' hnd₁ hnd' hmem
  have heq2 : (l₂.map f).sum = (l₂'.map f).sum := by
    dsimp [l₂']
    apply sum_map_filter_eq
    intro d hd hpd
    exact hzero d hd (of_decide_eq_false hpd)
  omega

private theorem sum_eraseDups_append_left (f : Nat → Nat) (la lb : List Nat)
    (hz : ∀ d ∈ lb, d ∉ la → f d = 0) :
    (la.eraseDups.map f).sum = ((la ++ lb).eraseDups.map f).sum := by
  apply sum_eraseDups_eq_of_mem_zero f la.eraseDups (la ++ lb).eraseDups
      (nodup_eraseDups la) (nodup_eraseDups (la ++ lb))
  · intro d hd
    exact List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_eraseDups.mp hd))
  · intro d hd hnot
    apply hz d
    · have hdab := List.mem_eraseDups.mp hd
      rcases List.mem_append.mp hdab with h | h
      · exact False.elim (hnot (List.mem_eraseDups.mpr h))
      · exact h
    · intro hdla
      exact hnot (List.mem_eraseDups.mpr hdla)

private theorem sum_eraseDups_append_right (f : Nat → Nat) (la lb : List Nat)
    (hz : ∀ d ∈ la, d ∉ lb → f d = 0) :
    (lb.eraseDups.map f).sum = ((la ++ lb).eraseDups.map f).sum := by
  apply sum_eraseDups_eq_of_mem_zero f lb.eraseDups (la ++ lb).eraseDups
      (nodup_eraseDups lb) (nodup_eraseDups (la ++ lb))
  · intro d hd
    exact List.mem_eraseDups.mpr (List.mem_append_right _ (List.mem_eraseDups.mp hd))
  · intro d hd hnot
    apply hz d
    · have hdab := List.mem_eraseDups.mp hd
      rcases List.mem_append.mp hdab with h | h
      · exact h
      · exact False.elim (hnot (List.mem_eraseDups.mpr h))
    · intro hdmem
      exact hnot (List.mem_eraseDups.mpr hdmem)

private theorem sum_filter_map_eq_sum_ite {α : Type} (l : List α) (f : α → Nat) (p : α → Bool) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons, List.map_cons, List.sum_cons]
      by_cases h : p x = true <;> simp [h, ih]

private theorem packets_mono_cap (cap k n : Nat) (hcap : 0 < cap) (hck : cap ≤ k) :
    packets k n ≤ packets cap n := by
  have hkpos : 0 < k := Nat.lt_of_lt_of_le hcap hck
  have hn := le_packets_mul cap n hcap
  have hmul : packets cap n * cap ≤ packets cap n * k := Nat.mul_le_mul_left _ hck
  exact packets_le_of_le_mul k n (packets cap n) hkpos (Nat.le_trans hn hmul)

private theorem packets_cap_scale (cap n v : Nat) (hcap : 0 < cap) (hn : 0 < n) :
    packets cap v ≤ n * packets (cap * n) v := by
  have hmpos : 0 < cap * n := Nat.mul_pos hcap hn
  have hv := le_packets_mul (cap * n) v hmpos
  have hre : packets (cap * n) v * (cap * n) = (n * packets (cap * n) v) * cap := by
    simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]
  have hle : v ≤ (n * packets (cap * n) v) * cap := by simpa [hre] using hv
  exact packets_le_of_le_mul cap v (n * packets (cap * n) v) hcap hle

private theorem volume_route (xs : List Bucket) (route : Nat → Nat) (r : Nat) :
    volume (xs.map (fun (d, w) => (route d, w))) r =
      ((xs.filter (fun p => route p.1 == r)).map Prod.snd).sum := by
  unfold volume
  rw [List.filter_map]
  simp only [Function.comp_apply]
  rw [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨d, w⟩
  simp

theorem FlushDebt_sound :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges.FlushDebt.Goal := by
  intro cap a b hcap
  let la := a.map Prod.fst
  let lb := b.map Prod.fst
  let U := (la ++ lb).eraseDups
  have htrA : traffic cap a = (U.map (fun d => packets cap (volume a d))).sum := by
    unfold traffic
    dsimp [U, la, lb]
    apply sum_eraseDups_append_left
    intro d hd hnot
    have hnota : d ∉ a.map Prod.fst := by simpa [la] using hnot
    rw [volume_not_mem a d hnota, packets_zero]
  have htrB : traffic cap b = (U.map (fun d => packets cap (volume b d))).sum := by
    unfold traffic
    dsimp [U, la, lb]
    apply sum_eraseDups_append_right
    intro d hd hnot
    have hnotb : d ∉ b.map Prod.fst := by simpa [lb] using hnot
    rw [volume_not_mem b d hnotb, packets_zero]
  have htrAB : traffic cap (a ++ b) = (U.map (fun d => packets cap (volume (a ++ b) d))).sum := by
    unfold traffic
    dsimp [U, la, lb]
    rw [List.map_append]
  rw [htrA, htrB, htrAB]
  apply Nat.sub_le_iff_le_add.mpr
  have hper : ∀ d ∈ U,
      packets cap (volume a d) + packets cap (volume b d) ≤
        packets cap (volume (a ++ b) d) + (if d ∈ la ∧ d ∈ lb then 1 else 0) := by
    intro d hd
    by_cases hdA : d ∈ la
    · by_cases hdT : d ∈ lb
      · simp [hdA, hdT, volume_append]
        exact packets_add_le_one cap (volume a d) (volume b d) hcap
      · have hnotb : d ∉ b.map Prod.fst := by simpa [lb] using hdT
        simp [hdA, hdT, hnotb, volume_append, volume_not_mem b d hnotb, packets_zero]
    · have hnota : d ∉ a.map Prod.fst := by simpa [la] using hdA
      simp [hdA, hnota, volume_append, volume_not_mem a d hnota, packets_zero]
  have hsum := sum_map_le_sum_map U
      (fun d => packets cap (volume a d) + packets cap (volume b d))
      (fun d => packets cap (volume (a ++ b) d) + (if d ∈ la ∧ d ∈ lb then 1 else 0)) hper
  have hleft :
      (U.map (fun d => packets cap (volume a d) + packets cap (volume b d))).sum =
        (U.map (fun d => packets cap (volume a d))).sum +
          (U.map (fun d => packets cap (volume b d))).sum := sum_map_add _ _ _
  have hright :
      (U.map (fun d => packets cap (volume (a ++ b) d) + (if d ∈ la ∧ d ∈ lb then 1 else 0))).sum =
        (U.map (fun d => packets cap (volume (a ++ b) d))).sum +
          (U.map (fun d => if d ∈ la ∧ d ∈ lb then 1 else 0)).sum := sum_map_add _ _ _
  have hind :
      (U.map (fun d => if d ∈ la ∧ d ∈ lb then 1 else 0)).sum =
        la.eraseDups.countP (fun d => lb.contains d) := by
    have h1 :
        (U.map (fun d => if d ∈ la ∧ d ∈ lb then 1 else 0)).sum =
          (la.eraseDups.map (fun d => if d ∈ la ∧ d ∈ lb then 1 else 0)).sum := by
      symm
      apply sum_eraseDups_append_left
      intro d hd hnot
      simp [hnot]
    have h2 :
        (la.eraseDups.map (fun d => if d ∈ la ∧ d ∈ lb then 1 else 0)).sum =
          (la.eraseDups.map (fun d => if lb.contains d then 1 else 0)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro d hdmem
      have hdla : d ∈ la := List.mem_eraseDups.mp hdmem
      simp [hdla]
    rw [h1, h2, sum_map_ite_countP]
  rw [hleft, hright, hind] at hsum
  dsimp [la, lb] at hsum ⊢
  omega

theorem ReceiverCoalesce_sound :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges.ReceiverCoalesce.Goal := by
  intro cap xs route hcap
  let routed := xs.map (fun (d, w) => (route d, w))
  let dests := (xs.map Prod.fst).eraseDups
  let rdests := (routed.map Prod.fst).eraseDups
  have hndD : dests.Nodup := nodup_eraseDups _
  have hndR : rdests.Nodup := nodup_eraseDups _
  have hcoverD : ∀ p ∈ xs, p.1 ∈ dests := by
    intro p hp
    exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨p, hp, rfl⟩)
  have hcoverR : ∀ d ∈ dests, route d ∈ rdests := by
    intro d hd
    have hdmem := List.mem_eraseDups.mp hd
    rcases List.mem_map.mp hdmem with ⟨p, hp, hpd⟩
    apply List.mem_eraseDups.mpr
    rw [List.mem_map]
    refine ⟨(route d, p.2), ?_, rfl⟩
    have hpin : (route d, p.2) ∈ routed := by
      dsimp [routed]
      rw [List.mem_map]
      exact ⟨p, hp, by simp [hpd]⟩
    exact hpin
  have hvolroute : ∀ r, volume routed r =
      ((dests.filter (fun d => route d == r)).map (fun d => volume xs d)).sum := by
    intro r
    let lr := xs.filter (fun p => route p.1 == r)
    have hcoverLR : ∀ p ∈ lr, p.1 ∈ dests := by
      intro p hp
      exact hcoverD p (List.mem_filter.mp hp).1
    have hgrp := sum_group_by_key (fun p : Bucket => p.1) Prod.snd lr dests hndD hcoverLR
    have hL : (lr.map Prod.snd).sum = volume routed r := by
      dsimp [lr]
      rw [volume_route]
    have hinner : ∀ d ∈ dests,
        ((lr.filter (fun p => p.1 == d)).map Prod.snd).sum =
          if route d == r then volume xs d else 0 := by
      intro d hd
      by_cases hdr : (route d == r) = true
      · have hfilter : lr.filter (fun p => p.1 == d) = xs.filter (fun p => p.1 == d) := by
          dsimp [lr]
          rw [List.filter_filter]
          apply List.filter_congr
          intro p hp
          by_cases hpd : (p.1 == d) = true
          · have hpd' : p.1 = d := by simpa using hpd
            have hrt : (route p.1 == r) = true := by simpa [hpd'] using hdr
            simp [hrt, hpd]
          · have hpdb : (p.1 == d) = false := Bool.eq_false_of_ne_true hpd
            simp [hpdb]
        rw [hfilter]
        simp [hdr, volume]
      · have hfilter : lr.filter (fun p => p.1 == d) = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro p hp
          have hp1 := List.mem_filter.mp (List.mem_filter.mp hp).1
          have hpd : p.1 = d := by simpa using (List.mem_filter.mp hp).2
          have hrt : (route p.1 == r) = true := by simpa using hp1.2
          have hdr' : (route d == r) = true := by simpa [hpd] using hrt
          exact hdr hdr'
        have hdrf : (route d == r) = false := Bool.eq_false_of_ne_true hdr
        rw [hfilter]
        simp [hdrf]
    have hstep :
        (dests.map (fun d => ((lr.filter (fun p => p.1 == d)).map Prod.snd).sum)).sum =
          (dests.map (fun d => if route d == r then volume xs d else 0)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro d hd
      exact hinner d hd
    have hfiltered :
        (dests.map (fun d => if route d == r then volume xs d else 0)).sum =
          ((dests.filter (fun d => route d == r)).map (fun d => volume xs d)).sum := by
      exact (sum_filter_map_eq_sum_ite dests (fun d => volume xs d) (fun d => route d == r)).symm
    rw [← hL, hgrp, hstep, hfiltered]
  have hper : ∀ r ∈ rdests,
      packets cap (volume routed r) ≤
        ((dests.filter (fun d => route d == r)).map (fun d => packets cap (volume xs d))).sum := by
    intro r hr
    rw [hvolroute r]
    have hp := packets_sum_le cap hcap
        ((dests.filter (fun d => route d == r)).map (fun d => volume xs d))
    rw [List.map_map] at hp
    simpa [Function.comp_def] using hp
  have hsum := sum_map_le_sum_map rdests
      (fun r => packets cap (volume routed r))
      (fun r => ((dests.filter (fun d => route d == r)).map (fun d => packets cap (volume xs d))).sum) hper
  have hgrp := sum_group_by_key (fun d : Nat => route d) (fun d => packets cap (volume xs d))
      dests rdests hndR hcoverR
  rw [← hgrp] at hsum
  dsimp [traffic, dests, routed] at hsum ⊢
  exact hsum


theorem GlobalPacking_disproof :
    ¬ TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges.GlobalPacking.Goal := by
  intro h
  have hh := h 4 [(0, 1), (1, 1)] (by decide)
  have hneq : ¬ (Exchanges.traffic 4 [(0, 1), (1, 1)] =
      Exchanges.packets 4 ([(0, 1), (1, 1)].map Prod.snd).sum) := by
    decide
  exact hneq hh

end TinylambOpt.ResearchProofs.DevinBatch.Atlas_ExecutorResources_A
