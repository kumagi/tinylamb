import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

namespace TinylambOpt.ResearchProofs.Atlas.ExecutorResources.Pipeline

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline

set_option maxHeartbeats 0

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

theorem sound_TwoStageOrder : TwoStageOrder.Goal := by
  intro xs ys hperm
  have hj : (johnson xs).Perm ys := (johnson_perm xs).trans hperm
  simpa using sorted_optimal (johnson xs) (johnson_sorted xs) ys hj.symm [] []

theorem sound_CriticalCut : CriticalCut.Goal := by
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

theorem sound_BatchSummary : BatchSummary.Goal := by
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
      have hf_le' : (List.map Prod.fst a).sum ≤ finish a := by
        simpa [sumF] using hf_le
      rw [Nat.max_eq_left hf_le']
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

theorem sound_SteadyState : SteadyState.Goal := by
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
          rw [sound_BatchSummary]
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

theorem sound_SplitOverlap : SplitOverlap.Goal := by
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

end TinylambOpt.ResearchProofs.Atlas.ExecutorResources.Pipeline
