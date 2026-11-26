import TinylambOpt.ResearchGoals.ExecutionStrategies.RecursiveJoins
import TinylambOpt.ResearchProofs.RecursiveDelta

/-!
These contracts describe the state or ordering needed for alternative SQL execution
strategies.

Proved (10 claims, including the following)

- `TinylambOpt.ResearchGoals.RecursiveJoins.BinaryLifting` — `∀ f k s d, k < 2^d →
  liftJump f k d s = steps f s k`
- `TinylambOpt.ResearchGoals.RecursiveJoins.ButterflyCount` — `∀ (g : Graph), let lefts
  := (g.map Prod.fst).eraseDups let rights := (g.map Prod.snd).eraseDups let covers
  := fun u v w => g.contains (u,w) && g.contains (v,w) ((increasingPairs lefts).map
  fun (u,v) => let n := rights.countP (covers u v) n*(n-1)/2).sum = ((increasingPairs
  lefts).map fun (u,v) => (increasingPairs rights).countP fun (w,z) => covers u v w
  && covers u v z).sum`
- `TinylambOpt.ResearchGoals.RecursiveJoins.InsertClosure` — Incremental transitive
  closure after inserting one edge, even if it closes a cycle. The two factors are
  from the old closure and include zero-length paths.
-/

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.RecursiveJoins

open TinylambOpt.ResearchGoals.RecursiveJoins
open TinylambOpt.ResearchGoals.RecursiveDelta (Edge Reach)
open List

/-! ### Reachability helpers -/

private theorem path_refl (g : Graph) (s : Nat) : path g s s := by
  apply Reach.seed
  simp

private theorem path_trans {g : Graph} {a b c : Nat} (hab : path g a b) (hbc : path g b c) :
    path g a c := by
  induction hbc with
  | seed hs =>
      simp at hs
      cases hs
      simpa using hab
  | step hprev he ih => exact Reach.step ih he

private theorem path_mono {g₁ g₂ : Graph} (hsub : ∀ e ∈ g₁, e ∈ g₂) {s t : Nat}
    (h : path g₁ s t) : path g₂ s t := by
  induction h with
  | seed hs => exact Reach.seed hs
  | step hprev he ih => exact Reach.step ih (hsub _ he)

private theorem path_cons_mono {g : Graph} {e : Edge} {s t : Nat} (h : path g s t) :
    path (e :: g) s t := by
  exact path_mono (by intro x hx; simp [hx]) h

/-! ### InsertClosure -/

theorem InsertClosure_sound : TinylambOpt.ResearchGoals.RecursiveJoins.InsertClosure.Goal := by
  intro g u v s t
  constructor
  · intro h
    induction h with
    | seed hs =>
        simp at hs
        cases hs
        exact Or.inl (path_refl g s)
    | @step x y hprev he ih =>
        simp only [List.mem_cons] at he
        rcases he with heq | heg
        · have hx : x = u := congrArg Prod.fst heq
          have hy : y = v := congrArg Prod.snd heq
          subst x
          subst y
          have hsu : path g s u := by
            cases ih with
            | inl hgu => exact hgu
            | inr hpair => exact hpair.1
          exact Or.inr ⟨hsu, path_refl g v⟩
        · cases ih with
          | inl hgu => exact Or.inl (Reach.step hgu heg)
          | inr hpair => exact Or.inr ⟨hpair.1, Reach.step hpair.2 heg⟩
  · intro h
    rcases h with hgt | ⟨hgu, hgv⟩
    · exact path_cons_mono hgt
    · exact path_trans (Reach.step (path_cons_mono hgu) (by simp)) (path_cons_mono hgv)

/-! ### QuotientClosure -/

theorem QuotientClosure_sound : TinylambOpt.ResearchGoals.RecursiveJoins.QuotientClosure.Goal := by
  intro g label s t hcert
  let edges := (g.map (fun (u, v) => (label u, label v))).eraseDups.filter (fun (u, v) => u != v)
  change path g s t ↔ path edges (label s) (label t)
  have hfwd : ∀ {A B : Nat}, path g A B → path edges (label A) (label B) := by
    intro A B h
    induction h with
    | seed hs =>
        simp at hs
        cases hs
        exact Reach.seed (by simp)
    | @step x y hprev he ih =>
        by_cases hxy : label x == label y
        · have heq : label x = label y := by simpa using hxy
          simpa [heq] using ih
        · have hem1 : (label x, label y) ∈ g.map (fun (u, v) => (label u, label v)) :=
            List.mem_map.mpr ⟨(x, y), he, rfl⟩
          have hem2 : (label x, label y) ∈
              (g.map (fun (u, v) => (label u, label v))).eraseDups :=
            List.mem_eraseDups.mpr hem1
          have hne : (label x != label y) = true := by simpa using hxy
          have hem : (label x, label y) ∈ edges := by
            exact List.mem_filter.mpr ⟨hem2, hne⟩
          exact Reach.step ih hem
  have hback : ∀ {A B : Nat}, path edges A B →
      ∀ x y : Nat, label x = A → label y = B → path g x y := by
    intro A B h
    induction h with
    | seed hs =>
        intro x y hx hy
        simp at hs
        cases hs
        exact hcert x y (hx.trans hy.symm)
    | @step x y hprev he ih =>
        intro a b ha hb
        have hem : (x, y) ∈ (g.map (fun (u, v) => (label u, label v))).eraseDups :=
          (List.mem_filter.mp he).1
        have hmap : (x, y) ∈ g.map (fun (u, v) => (label u, label v)) :=
          List.mem_eraseDups.mp hem
        rcases List.mem_map.mp hmap with ⟨q, huv, hlab⟩
        cases q with
        | mk u v =>
          have hlu : label u = x := by simpa using congrArg Prod.fst hlab
          have hlv : label v = y := by simpa using congrArg Prod.snd hlab
          have hau : path g a u := ih a u ha hlu
          have hav : path g a v := Reach.step hau huv
          have hvb : path g v b := hcert v b (hlv.trans hb.symm)
          exact path_trans hav hvb
  constructor
  · exact hfwd
  · intro h
    exact hback h s t rfl rfl

/-! ### MeetFrontiers -/

private theorem transpose_mem (g : Graph) {a b : Nat} : (a, b) ∈ transpose g ↔ (b, a) ∈ g := by
  constructor
  · intro h
    rcases List.mem_map.mp h with ⟨q, hq, hswap⟩
    cases q with
    | mk x y =>
        have heq : (y, x) = (a, b) := by simpa using hswap
        cases heq
        exact hq
  · intro h
    exact List.mem_map.mpr ⟨(b, a), h, rfl⟩

private theorem mem_endsAt_succ {g : Graph} {s d v : Nat} :
    v ∈ endsAt g s (d + 1) ↔ ∃ u ∈ endsAt g s d, (u, v) ∈ g := by
  simp [endsAt, List.mem_flatMap, List.mem_map, List.mem_filter]

private theorem mem_endsAt_front (g : Graph) (s t d : Nat) :
    t ∈ endsAt g s (d + 1) ↔ ∃ u, (s, u) ∈ g ∧ t ∈ endsAt g u d := by
  induction d generalizing s t with
  | zero =>
      rw [mem_endsAt_succ]
      constructor
      · rintro ⟨w, hw, hwt⟩
        simp [endsAt] at hw
        subst w
        exact ⟨t, hwt, by simp [endsAt]⟩
      · rintro ⟨u, hsu, hut⟩
        simp [endsAt] at hut
        subst u
        exact ⟨s, by simp [endsAt], hsu⟩
  | succ d ih =>
      rw [mem_endsAt_succ]
      constructor
      · rintro ⟨w, hw, hwt⟩
        rcases (ih s w).mp hw with ⟨x, hsx, hxw⟩
        exact ⟨x, hsx, (mem_endsAt_succ).mpr ⟨w, hxw, hwt⟩⟩
      · rintro ⟨x, hsx, hxt⟩
        rcases (mem_endsAt_succ).mp hxt with ⟨w, hxw, hwt⟩
        exact ⟨w, (ih s w).mpr ⟨x, hsx, hxw⟩, hwt⟩

private theorem mem_endsAt_transpose (g : Graph) (s t d : Nat) :
    t ∈ endsAt g s d ↔ s ∈ endsAt (transpose g) t d := by
  induction d generalizing s t with
  | zero =>
      simp [endsAt]
      omega
  | succ d ih =>
      rw [mem_endsAt_front]
      rw [mem_endsAt_succ]
      constructor
      · rintro ⟨u, hsu, hut⟩
        exact ⟨u, (ih u t).mp hut, (transpose_mem g).mpr hsu⟩
      · rintro ⟨u, htu, hus⟩
        exact ⟨u, (transpose_mem g).mp hus, (ih u t).mpr htu⟩

theorem MeetFrontiers_sound : TinylambOpt.ResearchGoals.RecursiveJoins.MeetFrontiers.Goal := by
  intro g s t l r
  induction l generalizing s r with
  | zero =>
      simpa [endsAt] using (mem_endsAt_transpose g s t r)
  | succ l ih =>
      rw [show Nat.succ l + r = l + Nat.succ r by omega]
      rw [ih (r := Nat.succ r)]
      constructor
      · rintro ⟨v, hv, hvtr⟩
        rcases (mem_endsAt_succ).mp hvtr with ⟨w, hwtr, hwv⟩
        refine ⟨w, ?_, hwtr⟩
        exact (mem_endsAt_succ).mpr ⟨v, hv, (transpose_mem g).mp hwv⟩
      · rintro ⟨w, hw, hwtr⟩
        rcases (mem_endsAt_succ).mp hw with ⟨v, hv, hvw⟩
        refine ⟨v, hv, ?_⟩
        exact (mem_endsAt_succ).mpr ⟨w, hwtr, (transpose_mem g).mpr hvw⟩

/-! ### BinaryLifting -/

private theorem steps_add (f : Nat → Option Nat) (s m n : Nat) :
    (steps f s m).bind (fun x => steps f x n) = steps f s (m + n) := by
  induction n with
  | zero => simp [steps]
  | succ n ih =>
      rw [Nat.add_succ]
      change (steps f s m).bind (fun x => (steps f x n).bind f) =
        (steps f s (m + n)).bind f
      rw [← Option.bind_assoc, ih]

private theorem steps_sq (f : Nat → Option Nat) (s q : Nat) :
    steps (fun v => (f v).bind f) s q = steps f s (2 * q) := by
  induction q with
  | zero => simp [steps]
  | succ q ih =>
      rw [Nat.mul_succ]
      change (steps (fun v => (f v).bind f) s q).bind (fun v => (f v).bind f) =
        steps f s (2 * (q + 1))
      rw [ih]
      rw [show 2 * (q + 1) = 2 * q + 2 by omega]
      rw [← steps_add f s (2 * q) 2]
      rfl

theorem BinaryLifting_sound : TinylambOpt.ResearchGoals.RecursiveJoins.BinaryLifting.Goal := by
  intro f k s d hk
  induction d generalizing k s f with
  | zero =>
      have hk0 : k = 0 := by omega
      subst k
      rfl
  | succ d ih =>
      let sq := fun v => (f v).bind f
      rw [Nat.pow_succ] at hk
      have hdiv : k / 2 < 2 ^ d := by
        have hdm := Nat.div_add_mod k 2
        omega
      rcases Nat.mod_two_eq_zero_or_one k with hmod | hmod
      · have hk_eq : k = 2 * (k / 2) := by
          have hdm := Nat.div_add_mod k 2
          omega
        have heven' : ¬ ((k % 2 == 1) = true) := by simp [hmod]
        have hs : liftJump f k (Nat.succ d) s = liftJump sq (k / 2) d s := by
          change (if (k % 2 == 1) then f s else some s).bind
              (fun s' => liftJump (fun v => (f v).bind f) (k / 2) d s') =
            liftJump sq (k / 2) d s
          rw [ite_eq_right heven']
          rfl
        rw [hs, ih sq (k / 2) s hdiv]
        rw [steps_sq]
        exact congrArg (fun n => steps f s n) hk_eq.symm
      · have hk_eq : k = 2 * (k / 2) + 1 := by
          have hdm := Nat.div_add_mod k 2
          omega
        have hodd : (k % 2 == 1) = true := by simp [hmod]
        have hs : liftJump f k (Nat.succ d) s =
            (f s).bind (fun s' => liftJump sq (k / 2) d s') := by
          change (if (k % 2 == 1) then f s else some s).bind
              (fun s' => liftJump (fun v => (f v).bind f) (k / 2) d s') =
            (f s).bind (fun s' => liftJump sq (k / 2) d s')
          rw [ite_eq_left hodd]
        rw [hs]
        have hbind : (f s).bind (fun s' => liftJump sq (k / 2) d s') =
            (f s).bind (fun s' => steps f s' (2 * (k / 2))) := by
          apply Option.bind_congr
          intro s' hs'
          rw [ih sq (k / 2) s' hdiv]
          exact steps_sq f s' (k / 2)
        rw [hbind]
        have hsum : 1 + 2 * (k / 2) = k := by omega
        have hadd := steps_add f s 1 (2 * (k / 2))
        rw [hsum] at hadd
        exact hadd

/-! ### PathMultiplicity -/

private theorem freq_fold_general (g : Graph) (prev : Freq) (h0 : Freq) (t : Nat) :
    (g.foldl (fun h (u, v) => h.insert v (h.getD v 0 + prev.getD u 0)) h0).getD t 0 =
      h0.getD t 0 + ((g.filter (fun e => e.2 == t)).map (fun e => prev.getD e.1 0)).sum := by
  induction g generalizing h0 with
  | nil => simp
  | cons e es ih =>
      cases e with
      | mk u v =>
          rw [List.foldl_cons, ih, Std.HashMap.getD_insert]
          by_cases hvt : v == t
          · have hvt' : v = t := by simpa using hvt
            subst t
            simp [hvt]
            omega
          · simp [hvt]

private theorem count_out_eq_filter (g : Graph) (u t : Nat) :
    ((g.filter fun e => e.1 == u).map Prod.snd).count t =
      (g.filter fun e => e.1 == u && e.2 == t).length := by
  induction g with
  | nil => simp
  | cons e es ih =>
      cases e with
      | mk a b =>
          by_cases hau : (a == u) = true
          · by_cases hbt : (b == t) = true
            · have hab : a = u := beq_iff_eq.mp hau
              have hbt' : b = t := beq_iff_eq.mp hbt
              subst a
              subst b
              simp [ih, Nat.add_comm]
            · have hbtf : (b == t) = false := by
                cases h : b == t <;> simp_all
              simp [List.count_cons, hau, hbtf, ih]
          · have hauf : (a == u) = false := by
              cases h : a == u <;> simp_all
            simp [hauf, ih]

private theorem sum_map_eq_of_mem {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ a ∈ l, f a = g a) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp)]
      exact congrArg (fun n => g a + n) (ih (by intro b hb; exact h b (by simp [hb])))

private theorem beq_comm_point (u x : Nat) : (if u == x then 1 else 0) = (if x == u then 1 else 0) := by
  by_cases h : (u == x) = true
  · have hx : (x == u) = true := beq_iff_eq.mpr (beq_iff_eq.mp h).symm
    simp [h, hx]
  · by_cases hx : (x == u) = true
    · have : (u == x) = true := beq_iff_eq.mpr (beq_iff_eq.mp hx).symm
      exact absurd this h
    · simp [h, hx]

private theorem sum_map_zero {α : Type} (l : List α) : ((l.map fun _ => 0).sum) = 0 := by
  induction l with
  | nil => rfl
  | cons a as ih => simpa using ih

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Nat) :
    ((xs.map fun x => f x + g x).sum) = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem len_filter_and (g : Graph) (x t : Nat) :
    (g.filter fun e => e.1 == x && e.2 == t).length =
      ((g.filter fun e => e.2 == t).filter fun e => e.1 == x).length := by
  simp [List.filter_filter, Bool.and_comm, Bool.and_left_comm, Bool.and_assoc]

private theorem countP_eq_len_filter (l : List Edge) (p : Edge → Bool) :
    l.countP p = (l.filter p).length := List.countP_eq_length_filter

private theorem count_filter_eq_len (g : Graph) (x t : Nat) :
    (g.filter fun e => e.2 == t).countP (fun e => e.1 == x) =
      (g.filter fun e => e.1 == x && e.2 == t).length := by
  rw [countP_eq_len_filter, len_filter_and]

private theorem sum_if_eq_countP (l : List Edge) (p : Edge → Bool) :
    ((l.map fun e => if p e then 1 else 0).sum) = l.countP p := by
  induction l with
  | nil => rfl
  | cons e es ih =>
      simp only [List.map_cons, List.sum_cons, List.countP_cons]
      rw [ih]
      omega

private theorem sum_filter_count (xs : List Nat) (g : Graph) (t : Nat) :
    ((xs.map fun u => (g.filter fun e => e.1 == u && e.2 == t).length).sum) =
      ((g.filter fun e => e.2 == t).map (fun e => xs.count e.1)).sum := by
  induction xs with
  | nil =>
      simp only [List.map_nil, List.sum_nil, List.count_nil]
      exact (sum_map_zero (g.filter fun e => e.2 == t)).symm
  | cons x xs ih =>
      let fg := g.filter fun e => e.2 == t
      let d := (g.filter fun e => e.1 == x && e.2 == t).length
      let A := (fg.map fun e => xs.count e.1).sum
      have hpt : ∀ e : Edge, (x :: xs).count e.1 = xs.count e.1 + (if e.1 == x then 1 else 0) := by
        intro e
        rw [List.count_cons, beq_comm_point x e.1]
      have hsplit : (fg.map fun e => (x :: xs).count e.1).sum = A + d := by
        calc
          (fg.map fun e => (x :: xs).count e.1).sum =
              (fg.map fun e => xs.count e.1 + (if e.1 == x then 1 else 0)).sum := by
                apply sum_map_eq_of_mem
                intro e he
                exact hpt e
          _ = (fg.map fun e => xs.count e.1).sum +
                (fg.map fun e => if e.1 == x then 1 else 0).sum := sum_map_add fg _ _
          _ = A + fg.countP (fun e => e.1 == x) := by
                rw [sum_if_eq_countP]
          _ = A + d := by
                rw [count_filter_eq_len]
      have hL : (((x :: xs).map fun u => (g.filter fun e => e.1 == u && e.2 == t).length).sum) =
          d + ((xs.map fun u => (g.filter fun e => e.1 == u && e.2 == t).length).sum) := by
        simp only [List.map_cons, List.sum_cons]
        rfl
      calc
        ((x :: xs).map fun u => (g.filter fun e => e.1 == u && e.2 == t).length).sum =
            d + ((xs.map fun u => (g.filter fun e => e.1 == u && e.2 == t).length).sum) := hL
        _ = d + A := congrArg (fun n => d + n) ih
        _ = A + d := by omega
        _ = (fg.map fun e => (x :: xs).count e.1).sum := hsplit.symm

theorem PathMultiplicity_sound : TinylambOpt.ResearchGoals.RecursiveJoins.PathMultiplicity.Goal := by
  intro g s d t
  induction d generalizing t with
  | zero =>
      rw [frequencies, Std.HashMap.getD_insert]
      simp [endsAt, List.count_cons, beq_iff_eq]
  | succ d ih =>
      rw [frequencies]
      rw [freq_fold_general]
      simp only [Std.HashMap.getD_empty, Nat.zero_add]
      change ((g.filter fun e => e.2 == t).map (fun e => (frequencies g s d).getD e.1 0)).sum =
        count t ((endsAt g s d).flatMap fun u => (g.filter fun e => e.1 == u).map Prod.snd)
      rw [List.count_flatMap]
      change ((g.filter fun e => e.2 == t).map (fun e => (frequencies g s d).getD e.1 0)).sum =
        ((endsAt g s d).map fun u => ((g.filter fun e => e.1 == u).map Prod.snd).count t).sum
      have hcout : ((endsAt g s d).map fun u => ((g.filter fun e => e.1 == u).map Prod.snd).count t).sum =
          ((endsAt g s d).map fun u => (g.filter fun e => e.1 == u && e.2 == t).length).sum := by
        apply sum_map_eq_of_mem
        intro u hu
        exact count_out_eq_filter g u t
      rw [hcout]
      rw [sum_filter_count]
      exact sum_map_eq_of_mem (g.filter fun e => e.2 == t)
        (fun e => (frequencies g s d).getD e.1 0)
        (fun e => (endsAt g s d).count e.1)
        (by intro e he; exact ih e.1)

/-! ### SuppressConnector -/

private theorem path_map (φ : Nat → Nat) {g : Graph} {s t : Nat} (h : path g s t) :
    path (g.map fun e => (φ e.1, φ e.2)) (φ s) (φ t) := by
  induction h with
  | seed hs =>
      simp at hs
      cases hs
      exact Reach.seed (by simp)
  | @step x y hprev he ih =>
      have hem : (φ x, φ y) ∈ g.map (fun e => (φ e.1, φ e.2)) :=
        List.mem_map.mpr ⟨(x, y), he, rfl⟩
      exact Reach.step ih hem

private theorem path_no_self_loop (g : Graph) (a s t : Nat) :
    path ((a, a) :: g) s t → path g s t := by
  intro h
  induction h with
  | seed hs => exact Reach.seed hs
  | @step x y hprev he ih =>
      simp only [List.mem_cons] at he
      rcases he with heq | heg
      · have hx : x = a := congrArg Prod.fst heq
        have hy : y = a := congrArg Prod.snd heq
        subst x
        subst y
        exact ih
      · exact Reach.step ih heg

private theorem map_phi_eq_self (g : Graph) (φ : Nat → Nat)
    (h : ∀ e ∈ g, φ e.1 = e.1 ∧ φ e.2 = e.2) :
    g.map (fun e => (φ e.1, φ e.2)) = g := by
  induction g with
  | nil => rfl
  | cons e es ih =>
      cases e with
      | mk a b =>
          have ha : φ a = a := (h (a, b) (by simp)).1
          have hb : φ b = b := (h (a, b) (by simp)).2
          have ht : ∀ e ∈ es, φ e.1 = e.1 ∧ φ e.2 = e.2 := by
            intro e he
            exact h e (by simp [he])
          simp [ha, hb, ih ht]

private theorem suppress_backward (g : Graph) (s t u v w : Nat)
    (h : path ((u, v) :: g) s t) : path ((u, w) :: (w, v) :: g) s t := by
  induction h with
  | seed hs => exact Reach.seed hs
  | @step x y hprev he ih =>
      simp only [List.mem_cons] at he
      rcases he with heq | heg
      · have hx : x = u := congrArg Prod.fst heq
        have hy : y = v := congrArg Prod.snd heq
        subst x
        subst y
        have he1 : (u, w) ∈ (u, w) :: (w, v) :: g := by simp
        have he2 : (w, v) ∈ (u, w) :: (w, v) :: g := by simp
        exact Reach.step (Reach.step ih he1) he2
      · have hem : (x, y) ∈ (u, w) :: (w, v) :: g := by simp [heg]
        exact Reach.step ih hem

theorem SuppressConnector_sound : TinylambOpt.ResearchGoals.RecursiveJoins.SuppressConnector.Goal := by
  intro g s t u v w hws hwt hwu hwv hg
  constructor
  · intro h
    let φ := fun x : Nat => if x = w then u else x
    have hφs : φ s = s := by simp [φ, Ne.symm hws]
    have hφt : φ t = t := by simp [φ, Ne.symm hwt]
    have hφu : φ u = u := by simp [φ, Ne.symm hwu]
    have hφv : φ v = v := by simp [φ, Ne.symm hwv]
    have hφw : φ w = u := by simp [φ]
    have hmapg : g.map (fun e => (φ e.1, φ e.2)) = g := by
      apply map_phi_eq_self g φ
      intro e he
      have hew := hg e he
      constructor
      · simp [φ, show ¬ e.1 = w from hew.1]
      · simp [φ, show ¬ e.2 = w from hew.2]
    have hmapped := path_map φ h
    have hgraph : ((u, w) :: (w, v) :: g).map (fun e => (φ e.1, φ e.2)) =
        (u, u) :: (u, v) :: g := by
      simp [hφu, hφw, hφv, hmapg]
    have hmid : path ((u, u) :: (u, v) :: g) s t := by
      simpa [hφs, hφt, hgraph] using hmapped
    exact path_no_self_loop ((u, v) :: g) u s t hmid
  · intro h
    exact suppress_backward g s t u v w h

/-! ### RedundantEdges -/

theorem RedundantEdges_sound : TinylambOpt.ResearchGoals.RecursiveJoins.RedundantEdges.Goal := by
  intro kept removed s t hcert
  constructor
  · intro h
    induction h with
    | seed hs => exact Reach.seed hs
    | @step x y hprev he ih =>
        simp only [List.mem_append] at he
        rcases he with hek | her
        · exact Reach.step ih (by simp [hek])
        · exact path_trans ih (hcert (x, y) (by simpa using her))
  · intro h
    exact path_mono (by intro e he; exact List.mem_append_left _ he) h

/-! ### TargetCone -/

private theorem reach_transpose_path (g : Graph) {s t : Nat} (h : Reach (transpose g) [t] s) :
    path g s t := by
  induction h with
  | seed hs =>
      simp at hs
      cases hs
      exact path_refl g t
  | @step x y hprev he ih =>
      have hyx : (y, x) ∈ g := (transpose_mem g).mp he
      exact path_trans (Reach.step (path_refl g y) hyx) ih

private theorem reach_cons_edge {edges : List Edge} {a b c : Nat}
    (h : Reach edges [a] b) (hca : (c, a) ∈ edges) : Reach edges [c] b := by
  induction h with
  | seed hs =>
      simp at hs
      cases hs
      exact Reach.step (Reach.seed (by simp)) hca
  | step hprev he ih => exact Reach.step ih he

private theorem path_reach_transpose (g : Graph) {s t : Nat} (h : path g s t) :
    Reach (transpose g) [t] s := by
  induction h with
  | seed hs =>
      simp at hs
      cases hs
      exact Reach.seed (by simp)
  | @step x y hprev he ih =>
      have hey : (y, x) ∈ transpose g := (transpose_mem g).mpr he
      exact reach_cons_edge ih hey

private theorem filter_good_filter_dest (g : Graph) (good : List Nat) (v : Nat)
    (hv : good.contains v = true) :
    (g.filter (fun e => good.contains e.2)).filter (fun e => e.2 == v) =
      g.filter (fun e => e.2 == v) := by
  have hvmem : v ∈ good := List.contains_iff_mem.mp hv
  rw [List.filter_filter]
  simp only [List.contains_eq_mem]
  induction g with
  | nil => rfl
  | cons e es ih =>
      cases e with
      | mk a b =>
          by_cases hbv : (b == v) = true
          · have hbmem : b ∈ good := by
              rw [beq_iff_eq.mp hbv]
              exact hvmem
            simp [hbv, hbmem, ih]
          · simp [hbv, ih]

theorem TargetCone_sound : TinylambOpt.ResearchGoals.RecursiveJoins.TargetCone.Goal := by
  intro g s t d
  let good := (TinylambOpt.ResearchGoals.RecursiveDelta.execute (transpose g) [t]).seen
  let gf := g.filter (fun e => good.contains e.2)
  change (endsAt gf s d).count t = (endsAt g s d).count t
  have hsound := TinylambOpt.ResearchProofs.RecursiveDelta.sound (transpose g) [t]
  have hgood : ∀ x : Nat, good.contains x = true ↔ path g x t := by
    intro x
    constructor
    · intro hc
      have hmem : x ∈ good := List.contains_iff_mem.mp hc
      exact reach_transpose_path g ((hsound.1 x).mp hmem)
    · intro hp
      have hmem : x ∈ good := (hsound.1 x).mpr (path_reach_transpose g hp)
      exact List.contains_iff_mem.mpr hmem
  have hfreq : ∀ (n : Nat) (v : Nat), path g v t →
      (frequencies gf s n).getD v 0 = (frequencies g s n).getD v 0 := by
    intro n
    induction n with
    | zero =>
        intro v hv
        simp [frequencies, Std.HashMap.getD_insert]
    | succ n ih =>
        intro v hv
        rw [frequencies, freq_fold_general, frequencies, freq_fold_general]
        simp only [Std.HashMap.getD_empty, Nat.zero_add]
        let fg := g.filter (fun e => e.2 == v)
        have hvgood : good.contains v = true := (hgood v).mpr hv
        have hfil : gf.filter (fun e => e.2 == v) = fg := by
          dsimp [gf, fg]
          exact filter_good_filter_dest g good v hvgood
        have hpoint : ∀ e ∈ fg,
            (frequencies gf s n).getD e.1 0 = (frequencies g s n).getD e.1 0 := by
          intro e he
          have heg : e ∈ g := (List.mem_filter.mp he).1
          have hev : e.2 = v := by
            have := (List.mem_filter.mp he).2
            exact beq_iff_eq.mp this
          have hp2 : path g e.2 t := by rw [hev]; exact hv
          have hp1 : path g e.1 t := path_trans (Reach.step (path_refl g e.1) heg) hp2
          exact ih e.1 hp1
        rw [hfil]
        exact sum_map_eq_of_mem fg
          (fun e => (frequencies gf s n).getD e.1 0)
          (fun e => (frequencies g s n).getD e.1 0) hpoint
  have hpm_gf := PathMultiplicity_sound gf s d t
  have hpm_g := PathMultiplicity_sound g s d t
  rw [← hpm_gf, ← hpm_g]
  exact hfreq d t (path_refl g t)

/-! ### ButterflyCount -/

private theorem nodup_eraseDups : ∀ (l : List Nat), l.eraseDups.Nodup := by
  intro l
  have key : ∀ (n : Nat) (xs : List Nat), xs.length = n → xs.eraseDups.Nodup := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hxn
        cases xs with
        | nil => exact List.Pairwise.nil
        | cons a as =>
            rw [List.eraseDups_cons]
            have hlen : (as.filter fun b => !b == a).length < n := by
              rw [List.length_cons] at hxn
              have hle := List.length_filter_le (fun b => !b == a) as
              omega
            refine List.Pairwise.cons (fun b hb => ?_)
              (ih (as.filter fun b => !b == a).length hlen _ rfl)
            have hb' : (!b == a) = true := (List.mem_filter.mp (List.mem_eraseDups.mp hb)).2
            have hbne : b ≠ a := by
              intro heq
              subst b
              simp at hb'
            exact fun heq => hbne heq.symm
  exact key l.length l rfl

private theorem perm_sum {α : Type} (f : α → Nat) {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp only [List.map_cons, List.sum_cons]; omega
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem fold_max_spec : ∀ (l : List Nat) (b : Nat),
    b ≤ l.foldl max b ∧ ∀ x ∈ l, x ≤ l.foldl max b := by
  intro l
  induction l with
  | nil => intro b; simp
  | cons a as ih =>
      intro b
      dsimp only [List.foldl_cons]
      have h := ih (max b a)
      constructor
      · exact Nat.le_trans (Nat.le_max_left b a) h.1
      · intro x hx
        cases List.mem_cons.mp hx with
        | inl heq =>
            cases heq
            exact Nat.le_trans (Nat.le_max_right b a) h.1
        | inr hx => exact h.2 x hx

private theorem fold_max_mem_nonempty {l : List Nat} (h : l ≠ []) : l.foldl max 0 ∈ l := by
  cases l with
  | nil => contradiction
  | cons a as =>
      dsimp only [List.foldl_cons]
      have h0a : max 0 a = a := Nat.max_eq_right (Nat.zero_le a)
      rw [h0a]
      have general : ∀ (xs : List Nat) (b : Nat), xs.foldl max b ∈ b :: xs := by
        intro xs
        induction xs with
        | nil => intro b; simp
        | cons x xs ih =>
            intro b
            dsimp only [List.foldl_cons]
            have hh := ih (max b x)
            cases List.mem_cons.mp hh with
            | inl heq =>
                rw [heq]
                by_cases hb : b ≤ x
                · rw [Nat.max_eq_right hb]; simp
                · have hx : x ≤ b := by omega
                  rw [Nat.max_eq_left hx]; simp
            | inr hmem => simp [hmem]
      exact general as a

private theorem filter_nil_of_all_false {α : Type} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = false) : l.filter p = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      have ha : p a = false := h a (by simp)
      have ht : ∀ x ∈ as, p x = false := by intro x hx; exact h x (by simp [hx])
      simp [ha, ih ht]

private theorem sum_map_succ (l : List Nat) (f : Nat → Nat) :
    (l.map fun x => f x + 1).sum = (l.map f).sum + l.length := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      omega

private theorem not_mem_erase_of_nodup (l : List Nat) (hn : l.Nodup) (mx : Nat) :
    mx ∉ l.erase mx := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have htail := (List.nodup_cons.mp hn).2
      have hnot_as : a ∉ as := (List.nodup_cons.mp hn).1
      by_cases h : a = mx
      · subst a
        simpa using hnot_as
      · simp [h, Ne.symm h, ih htail]

private theorem incPairs_length_of_max (l : List Nat) (mx : Nat)
    (hmem : mx ∈ l) (hmax : ∀ x ∈ l, x ≤ mx) (hn : l.Nodup) :
    (increasingPairs l).length = (increasingPairs (l.erase mx)).length + (l.erase mx).length := by
  let rest := l.erase mx
  have hp : l.Perm (mx :: rest) := List.perm_cons_erase hmem
  have hlenIP : (increasingPairs l).length =
      (l.map fun u => (l.filter fun z => u < z).length).sum := by
    simp [increasingPairs, List.length_flatMap]
  have hlenRest : (increasingPairs rest).length =
      (rest.map fun u => (rest.filter fun z => u < z).length).sum := by
    simp [increasingPairs, List.length_flatMap]
  rw [hlenIP, hlenRest]
  have hzero : (l.filter fun z => mx < z).length = 0 := by
    have hnil := filter_nil_of_all_false (fun z : Nat => mx < z) l (by
      intro z hz
      exact decide_eq_false (by have := hmax z hz; omega))
    rw [hnil]
    simp
  have hsum1 : (l.map fun u => (l.filter fun z => u < z).length).sum =
      ((mx :: rest).map fun u => (l.filter fun z => u < z).length).sum := perm_sum _ hp
  have hsum2 : ((mx :: rest).map fun u => (l.filter fun z => u < z).length).sum =
      (rest.map fun u => (rest.filter fun z => u < z).length).sum + rest.length := by
    simp only [List.map_cons, List.sum_cons]
    rw [hzero, Nat.zero_add]
    have hpoint : ∀ u ∈ rest, (l.filter fun z => u < z).length =
        (rest.filter fun z => u < z).length + 1 := by
      intro u hu
      have hu_ne : u ≠ mx := by
        intro heq
        subst u
        exact absurd hu (not_mem_erase_of_nodup l hn mx)
      have hu_l : u ∈ l := List.mem_of_mem_erase hu
      have humx : u < mx := by
        have := hmax u hu_l
        omega
      have hpf : (l.filter fun z => u < z).Perm ((mx :: rest).filter fun z => u < z) := hp.filter _
      have hlenf := hpf.length_eq
      have hfmx : (mx :: rest).filter (fun z => u < z) = mx :: (rest.filter fun z => u < z) := by
        simp [humx]
      rw [hfmx] at hlenf
      simpa using hlenf
    calc
      (rest.map fun u => (l.filter fun z => u < z).length).sum =
          (rest.map fun u => (rest.filter fun z => u < z).length + 1).sum :=
            sum_map_eq_of_mem rest _ _ hpoint
      _ = _ := sum_map_succ rest (fun u => (rest.filter fun z => u < z).length)
  exact hsum1.trans hsum2

private theorem choose2_succ (n : Nat) : (n + 1) * n / 2 = n * (n - 1) / 2 + n := by
  rcases Nat.mod_two_eq_zero_or_one n with h0 | h1
  · obtain ⟨k, hk⟩ : ∃ k, n = 2 * k := by
      refine ⟨n / 2, ?_⟩
      have hd := Nat.div_add_mod n 2
      omega
    rw [hk]
    rw [show (2 * k + 1 : Nat) * (2 * k) = 2 * (k * (2 * k + 1)) by ac_rfl]
    rw [Nat.mul_div_right (k * (2 * k + 1)) (by decide)]
    by_cases hk0 : k = 0
    · subst k
      simp
    · have hs : (2 * k + 1 : Nat) = (2 * k - 1) + 2 := by omega
      rw [show (2 * k : Nat) * (2 * k - 1) = 2 * (k * (2 * k - 1)) by ac_rfl]
      rw [Nat.mul_div_right (k * (2 * k - 1)) (by decide)]
      rw [hs, Nat.mul_add]
      omega
  · obtain ⟨k, hk⟩ : ∃ k, n = 2 * k + 1 := by
      refine ⟨n / 2, ?_⟩
      have hd := Nat.div_add_mod n 2
      omega
    rw [hk]
    have hp1 : (2 * k + 1 + 1 : Nat) = 2 * (k + 1) := by omega
    rw [hp1]
    rw [show (2 * (k + 1) : Nat) * (2 * k + 1) = 2 * ((k + 1) * (2 * k + 1)) by ac_rfl]
    rw [Nat.mul_div_right ((k + 1) * (2 * k + 1)) (by decide)]
    have hp2 : (2 * k + 1 : Nat) - 1 = 2 * k := by omega
    rw [hp2]
    rw [show (2 * k + 1 : Nat) * (2 * k) = 2 * ((2 * k + 1) * k) by ac_rfl]
    rw [Nat.mul_div_right ((2 * k + 1) * k) (by decide)]
    rw [Nat.add_mul]
    simp only [Nat.one_mul]
    rw [Nat.mul_comm k (2 * k + 1)]

private theorem incPairs_length_nodup (l : List Nat) (hn : l.Nodup) :
    (increasingPairs l).length = l.length * (l.length - 1) / 2 := by
  have key : ∀ n : Nat, ∀ l : List Nat, l.length = n → l.Nodup →
      (increasingPairs l).length = l.length * (l.length - 1) / 2 := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro l hlen hn
        cases l with
        | nil => simp [increasingPairs]
        | cons a as =>
            cases as with
            | nil => simp [increasingPairs]
            | cons b bs =>
                let l0 := a :: b :: bs
                let mx := l0.foldl max 0
                have hmem : mx ∈ l0 := fold_max_mem_nonempty (by simp [l0])
                have hmax : ∀ x ∈ l0, x ≤ mx := (fold_max_spec l0 0).2
                have hlenErase : (l0.erase mx).length = l0.length - 1 := List.length_erase_of_mem hmem
                have hlen0 : l0.length = n := by simpa [l0] using hlen
                have hnpos : 0 < n := by rw [← hlen]; simp
                have hlt : (l0.erase mx).length < n := by rw [hlenErase, hlen0]; omega
                have hnd : (l0.erase mx).Nodup := hn.erase mx
                have hrec := ih (l0.erase mx).length hlt (l0.erase mx) rfl hnd
                rw [incPairs_length_of_max l0 mx hmem hmax hn, hrec, hlenErase]
                have hcs := choose2_succ (l0.length - 1)
                have hlenpos : 1 ≤ l0.length := by simp [l0]
                rw [Nat.sub_add_cancel hlenpos] at hcs
                exact hcs.symm
  exact key l.length l rfl hn

private theorem incPairs_countP (l : List Nat) (Q : Nat × Nat → Bool) :
    (increasingPairs l).countP Q =
      (l.map fun u => (l.filter (fun z => Q (u, z) && u < z)).length).sum := by
  rw [List.countP_eq_length_filter]
  simp [increasingPairs, List.filter_flatMap, List.length_flatMap,
    List.filter_map, List.filter_filter, Bool.and_comm]

private theorem sum_filter_eq (l : List Nat) (p : Nat → Bool) (f : Nat → Nat) :
    ((l.filter p).map f).sum = (l.map fun x => if p x = true then f x else 0).sum := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      by_cases hpa : p a = true
      · simp [hpa, ih]
      · simp [hpa, ih]

private theorem filter_eq_of_pointwise {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ a ∈ l, p a = q a) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      have ha : p a = q a := h a (by simp)
      have ht : ∀ a ∈ as, p a = q a := by intro a ha'; exact h a (by simp [ha'])
      simp only [List.filter_cons]
      rw [ha]
      simp [ih ht]

theorem ButterflyCount_sound : TinylambOpt.ResearchGoals.RecursiveJoins.ButterflyCount.Goal := by
  intro g
  let lefts := (g.map Prod.fst).eraseDups
  let rights := (g.map Prod.snd).eraseDups
  let covers := fun u v w => g.contains (u, w) && g.contains (v, w)
  change ((increasingPairs lefts).map fun (u, v) =>
    let n := rights.countP (covers u v)
    n * (n - 1) / 2).sum =
  ((increasingPairs lefts).map fun (u, v) =>
    (increasingPairs rights).countP fun (w, z) => covers u v w && covers u v z).sum
  have hrights : rights.Nodup := by
    dsimp [rights]
    exact nodup_eraseDups _
  have hpair : ∀ p ∈ increasingPairs lefts,
      (let n := rights.countP (covers p.1 p.2); n * (n - 1) / 2) =
        (increasingPairs rights).countP (fun (w, z) => covers p.1 p.2 w && covers p.1 p.2 z) := by
    intro p hp
    let cov := covers p.1 p.2
    let S := rights.filter cov
    have hSnd : S.Nodup := hrights.filter _
    have hSlen : S.length = rights.countP cov := by
      dsimp [S]
      exact List.countP_eq_length_filter.symm
    have hleft : rights.countP cov * (rights.countP cov - 1) / 2 = (increasingPairs S).length := by
      rw [← hSlen]
      exact (incPairs_length_nodup S hSnd).symm
    have hright : (increasingPairs rights).countP (fun x => cov x.1 && cov x.2) =
        (increasingPairs S).length := by
      rw [incPairs_countP]
      have hterm : ∀ w : Nat,
          (rights.filter (fun z => cov w && cov z && w < z)).length =
            if cov w = true then (S.filter fun z => w < z).length else 0 := by
        intro w
        by_cases hcw : cov w = true
        · rw [ite_eq_left hcw]
          have heq : rights.filter (fun z => cov w && cov z && w < z) =
              S.filter (fun z => w < z) := by
            dsimp [S]
            rw [List.filter_filter]
            apply filter_eq_of_pointwise
            intro z hz
            by_cases hwz : w < z <;> simp [hcw, hwz, Bool.and_assoc, Bool.and_comm, Bool.and_left_comm]
          rw [heq]
        · rw [ite_eq_right hcw]
          have heq : rights.filter (fun z => cov w && cov z && w < z) = [] := by
            apply filter_nil_of_all_false
            intro z hz
            simp [hcw]
          rw [heq]
          simp
      calc
        (rights.map fun w => (rights.filter (fun z => cov w && cov z && w < z)).length).sum =
            (rights.map fun w => if cov w = true then (S.filter fun z => w < z).length else 0).sum :=
              sum_map_eq_of_mem rights _ _ (by intro w hw; exact hterm w)
        _ = (S.map fun w => (S.filter fun z => w < z).length).sum := by
              rw [sum_filter_eq]
        _ = (increasingPairs S).length := by
              simp [increasingPairs, List.length_flatMap]
    dsimp [cov, S] at hleft hright ⊢
    exact hleft.trans hright.symm
  apply sum_map_eq_of_mem
  intro p hp
  exact hpair p hp

/-! ### TriangleOrientation -/

private theorem sum_map_mul_left (l : List Nat) (f : Nat → Nat) (c : Nat) :
    (l.map fun x => c * f x).sum = c * (l.map f).sum := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, Nat.mul_add]

private def pairSum (l : List Nat) (Q : Nat × Nat → Bool) : Nat :=
  (l.map fun a => (l.filter fun b => Q (a, b)).length).sum

private def tripleSum (l : List Nat) (tri : Nat → Nat → Nat → Bool) : Nat :=
  (l.map fun a => pairSum l (fun p => tri a p.1 p.2)).sum

private def incTripleSum (l : List Nat) (tri : Nat → Nat → Nat → Bool) : Nat :=
  (l.map fun a => ((l.filter fun b => a < b).map fun b =>
    (l.filter fun c => b < c && tri a b c).length).sum).sum

private theorem sum_if_bool (l : List Nat) (p : Nat → Bool) :
    (l.map fun x => if p x then 1 else 0).sum = l.countP p := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons, List.countP_cons]
      rw [ih]
      omega

private theorem pairSum_perm {l₁ l₂ : List Nat} (p : l₁.Perm l₂) (Q : Nat × Nat → Bool) :
    pairSum l₁ Q = pairSum l₂ Q := by
  unfold pairSum
  calc
    (l₁.map fun a => (l₁.filter fun b => Q (a, b)).length).sum =
        (l₁.map fun a => (l₂.filter fun b => Q (a, b)).length).sum := by
          apply sum_map_eq_of_mem
          intro a ha
          exact (p.filter (fun b => Q (a, b))).length_eq
    _ = (l₂.map fun a => (l₂.filter fun b => Q (a, b)).length).sum := perm_sum _ p

private theorem tripleSum_perm {l₁ l₂ : List Nat} (p : l₁.Perm l₂)
    (tri : Nat → Nat → Nat → Bool) : tripleSum l₁ tri = tripleSum l₂ tri := by
  unfold tripleSum
  calc
    (l₁.map fun a => pairSum l₁ (fun q => tri a q.1 q.2)).sum =
        (l₁.map fun a => pairSum l₂ (fun q => tri a q.1 q.2)).sum := by
          apply sum_map_eq_of_mem
          intro a ha
          exact pairSum_perm p _
    _ = (l₂.map fun a => pairSum l₂ (fun q => tri a q.1 q.2)).sum := perm_sum _ p

private theorem incTripleSum_perm {l₁ l₂ : List Nat} (p : l₁.Perm l₂)
    (tri : Nat → Nat → Nat → Bool) : incTripleSum l₁ tri = incTripleSum l₂ tri := by
  unfold incTripleSum
  calc
    (l₁.map fun a => ((l₁.filter fun b => a < b).map fun b =>
        (l₁.filter fun c => b < c && tri a b c).length).sum).sum =
      (l₁.map fun a => ((l₂.filter fun b => a < b).map fun b =>
        (l₁.filter fun c => b < c && tri a b c).length).sum).sum := by
          apply sum_map_eq_of_mem
          intro a ha
          exact perm_sum (fun b => (l₁.filter fun c => b < c && tri a b c).length)
            (p.filter (fun b => a < b))
    _ = (l₁.map fun a => ((l₂.filter fun b => a < b).map fun b =>
        (l₂.filter fun c => b < c && tri a b c).length).sum).sum := by
          apply sum_map_eq_of_mem
          intro a ha
          apply sum_map_eq_of_mem
          intro b hb
          exact (p.filter (fun c => b < c && tri a b c)).length_eq
    _ = (l₂.map fun a => ((l₂.filter fun b => a < b).map fun b =>
        (l₂.filter fun c => b < c && tri a b c).length).sum).sum := perm_sum _ p

private theorem incPairs_countP_perm {l₁ l₂ : List Nat} (p : l₁.Perm l₂)
    (Q : Nat × Nat → Bool) : (increasingPairs l₁).countP Q = (increasingPairs l₂).countP Q := by
  rw [incPairs_countP, incPairs_countP]
  calc
    (l₁.map fun u => (l₁.filter (fun z => Q (u, z) && u < z)).length).sum =
        (l₁.map fun u => (l₂.filter (fun z => Q (u, z) && u < z)).length).sum := by
          apply sum_map_eq_of_mem
          intro u hu
          exact (p.filter (fun z => Q (u, z) && u < z)).length_eq
    _ = (l₂.map fun u => (l₂.filter (fun z => Q (u, z) && u < z)).length).sum := perm_sum _ p

private theorem pairSum_eq_two_incPairs (l : List Nat) (hn : l.Nodup) (Q : Nat × Nat → Bool)
    (hdiag : ∀ a, Q (a, a) = false) (hsym : ∀ a b, Q (a, b) = Q (b, a)) :
    pairSum l Q = 2 * (increasingPairs l).countP Q := by
  have key : ∀ n : Nat, ∀ l : List Nat, l.length = n → l.Nodup →
      pairSum l Q = 2 * (increasingPairs l).countP Q := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro l hlen hn
        cases l with
        | nil => simp [pairSum, increasingPairs]
        | cons a as =>
            cases as with
            | nil => simp [pairSum, increasingPairs, hdiag]
            | cons b bs =>
                let l0 := a :: b :: bs
                let mx := l0.foldl max 0
                let rest := l0.erase mx
                have hmem : mx ∈ l0 := fold_max_mem_nonempty (by simp [l0])
                have hmax : ∀ x ∈ l0, x ≤ mx := (fold_max_spec l0 0).2
                have hlenErase : rest.length = l0.length - 1 := by
                  dsimp [rest]
                  exact List.length_erase_of_mem hmem
                have hlen0 : l0.length = n := by simpa [l0] using hlen
                have hnpos : 0 < n := by rw [← hlen]; simp
                have hlt : rest.length < n := by rw [hlenErase, hlen0]; omega
                have hnd : rest.Nodup := by dsimp [rest]; exact hn.erase mx
                have hrec := ih rest.length hlt rest rfl hnd
                have hp : l0.Perm (mx :: rest) := List.perm_cons_erase hmem
                rw [pairSum_perm hp Q, incPairs_countP_perm hp Q]
                have hhead : (mx :: rest).filter (fun x => Q (mx, x)) =
                    rest.filter (fun x => Q (mx, x)) := by
                  simp [hdiag]
                have htail : ∀ u ∈ rest,
                    ((mx :: rest).filter fun x => Q (u, x)).length =
                      (if Q (u, mx) then 1 else 0) +
                        (rest.filter fun x => Q (u, x)).length := by
                  intro u hu
                  by_cases hq : Q (u, mx) <;> simp [hq] <;> omega
                have hsumA : (rest.map fun u => if Q (u, mx) then 1 else 0).sum =
                    (rest.filter fun x => Q (mx, x)).length := by
                  rw [sum_if_bool, List.countP_eq_length_filter]
                  have heq : rest.filter (fun u => Q (u, mx)) =
                      rest.filter (fun u => Q (mx, u)) := by
                    apply filter_eq_of_pointwise
                    intro u hu
                    exact hsym u mx
                  rw [heq]
                have hpair : pairSum (mx :: rest) Q =
                    pairSum rest Q + 2 * (rest.filter fun x => Q (mx, x)).length := by
                  unfold pairSum
                  simp only [List.map_cons, List.sum_cons]
                  rw [hhead]
                  have hs : (rest.map fun u => ((mx :: rest).filter fun x => Q (u, x)).length).sum =
                      (rest.map fun u => (if Q (u, mx) then 1 else 0) +
                        (rest.filter fun x => Q (u, x)).length).sum :=
                    sum_map_eq_of_mem rest _ _ htail
                  rw [hs, sum_map_add, hsumA]
                  omega
                have hinc : (increasingPairs (mx :: rest)).countP Q =
                    (rest.filter fun x => Q (mx, x)).length +
                      (increasingPairs rest).countP Q := by
                  rw [incPairs_countP]
                  simp only [List.map_cons, List.sum_cons]
                  have hzero : ((mx :: rest).filter fun z => Q (mx, z) && mx < z).length = 0 := by
                    have hnil := filter_nil_of_all_false (fun z : Nat => Q (mx, z) && mx < z)
                      (mx :: rest) (by
                        intro z hz
                        cases List.mem_cons.mp hz with
                        | inl heq =>
                            cases heq
                            simp
                        | inr hzrest =>
                            have hz0 : z ∈ l0 := List.mem_of_mem_erase hzrest
                            have hle := hmax z hz0
                            have hnot : ¬ mx < z := by omega
                            simp [hnot])
                    rw [hnil]
                    simp
                  rw [hzero, Nat.zero_add]
                  have htailInc : ∀ u ∈ rest,
                      ((mx :: rest).filter fun z => Q (u, z) && u < z).length =
                        (if Q (u, mx) then 1 else 0) +
                          (rest.filter fun z => Q (u, z) && u < z).length := by
                    intro u hu
                    have hu_ne : u ≠ mx := by
                      intro heq
                      subst u
                      exact absurd hu (by dsimp [rest]; exact not_mem_erase_of_nodup l0 hn mx)
                    have hu_l : u ∈ l0 := by dsimp [rest] at hu; exact List.mem_of_mem_erase hu
                    have hult : u < mx := by
                      have := hmax u hu_l
                      omega
                    by_cases hq : Q (u, mx) <;> simp [hq, hult] <;> omega
                  calc
                    (rest.map fun u => ((mx :: rest).filter fun z => Q (u, z) && u < z).length).sum =
                        (rest.map fun u => (if Q (u, mx) then 1 else 0) +
                          (rest.filter fun z => Q (u, z) && u < z).length).sum :=
                          sum_map_eq_of_mem rest _ _ htailInc
                    _ = (rest.map fun u => if Q (u, mx) then 1 else 0).sum +
                        (rest.map fun u => (rest.filter fun z => Q (u, z) && u < z).length).sum :=
                          sum_map_add rest _ _
                    _ = (rest.filter fun x => Q (mx, x)).length +
                        (increasingPairs rest).countP Q := by
                          rw [hsumA, incPairs_countP]
                rw [hpair, hinc, hrec]
                omega
  exact key l.length l rfl hn

private theorem incPairs_countP_cons_max (mx : Nat) (rest : List Nat)
    (hmaxrest : ∀ x ∈ rest, x < mx) (Q : Nat × Nat → Bool)
    (hsym : ∀ a b, Q (a, b) = Q (b, a)) :
    (increasingPairs (mx :: rest)).countP Q =
      (rest.filter fun x => Q (mx, x)).length + (increasingPairs rest).countP Q := by
  rw [incPairs_countP]
  simp only [List.map_cons, List.sum_cons]
  have hzero : ((mx :: rest).filter fun z => Q (mx, z) && mx < z).length = 0 := by
    have hnil := filter_nil_of_all_false (fun z : Nat => Q (mx, z) && mx < z) (mx :: rest) (by
      intro z hz
      cases List.mem_cons.mp hz with
      | inl heq => cases heq; simp
      | inr hzrest =>
          have hlt := hmaxrest z hzrest
          have hnot : ¬ mx < z := by omega
          simp [hnot])
    rw [hnil]
    simp
  rw [hzero, Nat.zero_add]
  have htail : ∀ u ∈ rest,
      ((mx :: rest).filter fun z => Q (u, z) && u < z).length =
        (if Q (u, mx) then 1 else 0) +
          (rest.filter fun z => Q (u, z) && u < z).length := by
    intro u hu
    have hult := hmaxrest u hu
    by_cases hq : Q (u, mx) <;> simp [hq, hult] <;> omega
  calc
    (rest.map fun u => ((mx :: rest).filter fun z => Q (u, z) && u < z).length).sum =
        (rest.map fun u => (if Q (u, mx) then 1 else 0) +
          (rest.filter fun z => Q (u, z) && u < z).length).sum :=
          sum_map_eq_of_mem rest _ _ htail
    _ = (rest.map fun u => if Q (u, mx) then 1 else 0).sum +
        (rest.map fun u => (rest.filter fun z => Q (u, z) && u < z).length).sum :=
          sum_map_add rest _ _
    _ = (rest.filter fun x => Q (mx, x)).length +
        (increasingPairs rest).countP Q := by
          rw [sum_if_bool, List.countP_eq_length_filter]
          have heq : rest.filter (fun u => Q (u, mx)) = rest.filter (fun u => Q (mx, u)) := by
            apply filter_eq_of_pointwise
            intro u hu
            exact hsym u mx
          rw [heq, incPairs_countP]

private theorem pairSum_cons_max (mx : Nat) (rest : List Nat)
    (hn : (mx :: rest).Nodup) (hmaxrest : ∀ x ∈ rest, x < mx)
    (Q : Nat × Nat → Bool) (hdiagQ : ∀ a, Q (a, a) = false)
    (hsymQ : ∀ a b, Q (a, b) = Q (b, a)) :
    pairSum (mx :: rest) Q = pairSum rest Q + 2 * (rest.filter fun x => Q (mx, x)).length := by
  have hnd : rest.Nodup := (List.nodup_cons.mp hn).2
  have hdiag_all : ∀ a, Q (a, a) = false := hdiagQ
  rw [pairSum_eq_two_incPairs (mx :: rest) hn Q hdiag_all hsymQ,
      pairSum_eq_two_incPairs rest hnd Q hdiag_all hsymQ,
      incPairs_countP_cons_max mx rest hmaxrest Q hsymQ]
  omega

private theorem tripleSum_eq_six_inc (l : List Nat) (hn : l.Nodup)
    (tri : Nat → Nat → Nat → Bool)
    (h12 : ∀ a b c, tri a b c = tri b a c)
    (h23 : ∀ a b c, tri a b c = tri a c b)
    (hdiag : ∀ a b, tri a a b = false) :
    tripleSum l tri = 6 * incTripleSum l tri := by
  have hdiag2 : ∀ a b, tri a b b = false := by
    intro a b
    rw [h12 a b b, h23 b a b, hdiag b a]
  have hdiag3 : ∀ a b, tri a b a = false := by
    intro a b
    rw [h23 a b a, hdiag a b]
  have hsym_last : ∀ a b c, tri a b c = tri b a c := h12
  have hrot : ∀ a b c, tri a b c = tri a c b := h23
  have hsym_mx : ∀ a b c, tri a b c = tri c a b := by
    intro a b c
    rw [h12 a b c, h23 b a c, h12 b c a, h23 c b a]
  have key : ∀ n : Nat, ∀ l : List Nat, l.length = n → l.Nodup →
      tripleSum l tri = 6 * incTripleSum l tri := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro l hlen hn
        cases l with
        | nil => simp [tripleSum, incTripleSum, pairSum]
        | cons a as =>
            cases as with
            | nil =>
                simp [tripleSum, incTripleSum, pairSum, hdiag, hdiag2, hdiag3]
            | cons b bs =>
                cases bs with
                | nil =>
                    by_cases hab : a < b
                    · have hba : ¬ b < a := by omega
                      simp [tripleSum, incTripleSum, pairSum, hdiag, hdiag2, hdiag3,
                        h12, h23, hab, hba]
                    · by_cases hba : b < a
                      · simp [tripleSum, incTripleSum, pairSum, hdiag, hdiag2, hdiag3,
                          h12, h23, hab, hba]
                      · have heqab : a = b := by omega
                        subst b
                        simp [tripleSum, incTripleSum, pairSum, hdiag, hdiag2, hdiag3]
                | cons c cs =>
                    let l0 := a :: b :: c :: cs
                    let mx := l0.foldl max 0
                    let rest := l0.erase mx
                    have hmem : mx ∈ l0 := fold_max_mem_nonempty (by simp [l0])
                    have hmax : ∀ x ∈ l0, x ≤ mx := (fold_max_spec l0 0).2
                    have hlenErase : rest.length = l0.length - 1 := by
                      dsimp [rest]
                      exact List.length_erase_of_mem hmem
                    have hlen0 : l0.length = n := by simpa [l0] using hlen
                    have hnpos : 0 < n := by rw [← hlen]; simp
                    have hlt : rest.length < n := by rw [hlenErase, hlen0]; omega
                    have hnd : rest.Nodup := by dsimp [rest]; exact hn.erase mx
                    have hrec := ih rest.length hlt rest rfl hnd
                    have hp : l0.Perm (mx :: rest) := List.perm_cons_erase hmem
                    rw [tripleSum_perm hp tri, incTripleSum_perm hp tri]
                    have hmaxrest : ∀ x ∈ rest, x < mx := by
                      intro x hx
                      have hx0 : x ∈ l0 := List.mem_of_mem_erase hx
                      have hxne : x ≠ mx := by
                        intro heq
                        subst x
                        exact absurd hx (by dsimp [rest]; exact not_mem_erase_of_nodup l0 hn mx)
                      have := hmax x hx0
                      omega
                    have hn0 : (mx :: rest).Nodup := by
                      exact hp.nodup (by simpa [l0] using hn)
                    let Qmx := fun q : Nat × Nat => tri q.1 q.2 mx
                    have hQmx_sym : ∀ a b, Qmx (a, b) = Qmx (b, a) := by
                      intro x y
                      dsimp [Qmx]
                      exact h12 x y mx
                    have hQmx_diag : ∀ a, Qmx (a, a) = false := by
                      intro a
                      dsimp [Qmx]
                      exact hdiag a mx
                    have hQa_sym : ∀ (a x y : Nat), tri a x y = tri a y x := by
                      intro a x y
                      exact h23 a x y
                    have hQa_diag : ∀ (a x : Nat), tri a x x = false := by
                      intro a x
                      exact hdiag2 a x
                    have hheadQ : pairSum (mx :: rest) (fun q => tri mx q.1 q.2) =
                        pairSum rest (fun q => tri mx q.1 q.2) := by
                      have hres := pairSum_cons_max mx rest hn0 hmaxrest
                        (fun q => tri mx q.1 q.2)
                        (by intro x; simpa using hdiag2 mx x)
                        (by intro x y; exact h23 mx x y)
                      rw [hres]
                      have hz : rest.filter (fun x => tri mx mx x) = [] := by
                        apply filter_nil_of_all_false
                        intro x hx
                        exact hdiag mx x
                      rw [hz]
                      simp
                    have htailQ : ∀ a ∈ rest,
                        pairSum (mx :: rest) (fun q => tri a q.1 q.2) =
                          pairSum rest (fun q => tri a q.1 q.2) +
                            2 * (rest.filter fun x => tri a x mx).length := by
                      intro a ha
                      have hres := pairSum_cons_max mx rest hn0 hmaxrest
                        (fun q => tri a q.1 q.2) (hQa_diag a) (hQa_sym a)
                      rw [hres]
                      congr 2
                      apply congrArg List.length
                      apply filter_eq_of_pointwise
                      intro x hx
                      exact h23 a mx x
                    have hpair : tripleSum (mx :: rest) tri =
                        tripleSum rest tri + 3 * pairSum rest Qmx := by
                      unfold tripleSum
                      simp only [List.map_cons, List.sum_cons]
                      rw [hheadQ]
                      have hheadQeq : pairSum rest (fun q => tri mx q.1 q.2) = pairSum rest Qmx := by
                        unfold pairSum
                        apply sum_map_eq_of_mem
                        intro a ha
                        apply congrArg List.length
                        apply filter_eq_of_pointwise
                        intro b hb
                        dsimp [Qmx]
                        rw [h12 mx a b, h23 a mx b]
                      rw [hheadQeq]
                      have hs : (rest.map fun a => pairSum (mx :: rest) (fun q => tri a q.1 q.2)).sum =
                          (rest.map fun a => pairSum rest (fun q => tri a q.1 q.2) +
                            2 * (rest.filter fun x => tri a x mx).length).sum :=
                        sum_map_eq_of_mem rest _ _ htailQ
                      rw [hs, sum_map_add]
                      have hsumC : (rest.map fun a => (rest.filter fun x => tri a x mx).length).sum =
                          pairSum rest Qmx := by
                        rfl
                      have hsumC2 : (rest.map fun a => 2 * (rest.filter fun x => tri a x mx).length).sum =
                          2 * pairSum rest Qmx := by
                        rw [show (rest.map fun a => 2 * (rest.filter fun x => tri a x mx).length).sum =
                            2 * (rest.map fun a => (rest.filter fun x => tri a x mx).length).sum from
                              sum_map_mul_left rest (fun a => (rest.filter fun x => tri a x mx).length) 2]
                        rw [hsumC]
                      rw [hsumC2]
                      omega
                    have hinc : incTripleSum (mx :: rest) tri =
                        incTripleSum rest tri + (increasingPairs rest).countP Qmx := by
                      unfold incTripleSum
                      simp only [List.map_cons, List.sum_cons]
                      have hhead0 : ((mx :: rest).filter fun b => mx < b) = [] := by
                        apply filter_nil_of_all_false
                        intro z hz
                        cases List.mem_cons.mp hz with
                        | inl heq => cases heq; simp
                        | inr hzrest =>
                            have hlt := hmaxrest z hzrest
                            have hnot : ¬ mx < z := by omega
                            simp [hnot]
                      rw [hhead0]
                      simp
                      have htail : ∀ a ∈ rest,
                          (((mx :: rest).filter fun b => a < b).map fun b =>
                            ((mx :: rest).filter fun c => b < c && tri a b c).length).sum =
                          (((rest.filter fun b => a < b).map fun b =>
                            (rest.filter fun c => b < c && tri a b c).length).sum) +
                            ((rest.filter fun b => a < b).countP fun b => tri a b mx) := by
                        intro a ha
                        have hfilt : (mx :: rest).filter (fun b => a < b) =
                            mx :: (rest.filter fun b => a < b) := by
                          have halt : a < mx := hmaxrest a ha
                          simp [halt]
                        rw [hfilt]
                        simp only [List.map_cons, List.sum_cons]
                        have hbzero : ((mx :: rest).filter fun c => mx < c && tri a mx c).length = 0 := by
                          have hnil := filter_nil_of_all_false (fun c : Nat => mx < c && tri a mx c)
                            (mx :: rest) (by
                              intro z hz
                              cases List.mem_cons.mp hz with
                              | inl heq => cases heq; simp
                              | inr hzrest =>
                                  have hlt := hmaxrest z hzrest
                                  have hnot : ¬ mx < z := by omega
                                  simp [hnot])
                          rw [hnil]
                          simp
                        rw [hbzero, Nat.zero_add]
                        have hterm : ∀ b ∈ rest.filter (fun b => a < b),
                            ((mx :: rest).filter fun c => b < c && tri a b c).length =
                              (if tri a b mx then 1 else 0) +
                                (rest.filter fun c => b < c && tri a b c).length := by
                          intro b hb
                          have hb_rest : b ∈ rest := (List.mem_filter.mp hb).1
                          have hblt : b < mx := hmaxrest b hb_rest
                          by_cases hq : tri a b mx <;> simp [hq, hblt] <;> omega
                        calc
                          ((rest.filter fun b => a < b).map fun b =>
                              ((mx :: rest).filter fun c => b < c && tri a b c).length).sum =
                            ((rest.filter fun b => a < b).map fun b =>
                              (if tri a b mx then 1 else 0) +
                                (rest.filter fun c => b < c && tri a b c).length).sum :=
                              sum_map_eq_of_mem _ _ _ hterm
                          _ = ((rest.filter fun b => a < b).map fun b =>
                                if tri a b mx then 1 else 0).sum +
                              ((rest.filter fun b => a < b).map fun b =>
                                (rest.filter fun c => b < c && tri a b c).length).sum :=
                              sum_map_add _ _ _
                          _ = ((rest.filter fun b => a < b).countP fun b => tri a b mx) +
                              ((rest.filter fun b => a < b).map fun b =>
                                (rest.filter fun c => b < c && tri a b c).length).sum := by
                              rw [sum_if_bool]
                          _ = _ := by omega
                      have hs : (rest.map fun a =>
                          (((mx :: rest).filter fun b => a < b).map fun b =>
                            ((mx :: rest).filter fun c => b < c && tri a b c).length).sum).sum =
                          (rest.map fun a =>
                            (((rest.filter fun b => a < b).map fun b =>
                              (rest.filter fun c => b < c && tri a b c).length).sum) +
                              ((rest.filter fun b => a < b).countP fun b => tri a b mx)).sum :=
                        sum_map_eq_of_mem rest _ _ htail
                      rw [hs, sum_map_add]
                      have hsumQ : (rest.map fun a =>
                          (rest.filter fun b => a < b).countP fun b => tri a b mx).sum =
                          (increasingPairs rest).countP Qmx := by
                        rw [incPairs_countP]
                        apply sum_map_eq_of_mem
                        intro u hu
                        rw [List.countP_eq_length_filter]
                        rw [List.filter_filter]
                      rw [hsumQ]
                    have hQmx := pairSum_eq_two_incPairs rest hnd Qmx hQmx_diag hQmx_sym
                    rw [hpair, hinc, hrec, hQmx]
                    omega
  exact key l.length l rfl hn

theorem TriangleOrientation_sound : TinylambOpt.ResearchGoals.RecursiveJoins.TriangleOrientation.Goal := by
  intro g
  let vs := (g.map Prod.fst ++ g.map Prod.snd).eraseDups
  let adj := fun a b => a != b && (g.contains (a, b) || g.contains (b, a))
  let tri := fun a b c => adj a b && adj b c && adj c a
  change (vs.map (fun a => (vs.map (fun b => vs.countP (tri a b))).sum)).sum =
    6 * (vs.map (fun a => ((vs.filter (a < ·)).map fun b =>
      (vs.filter (b < ·)).countP (tri a b)).sum)).sum
  have hvn : vs.Nodup := by dsimp [vs]; exact nodup_eraseDups _
  have hadj_sym : ∀ a b, adj a b = adj b a := by
    intro a b
    dsimp [adj]
    by_cases heq : a = b
    · subst b
      simp
    · have hane : a ≠ b := heq
      have hbne : b ≠ a := by intro h; exact heq h.symm
      have hne1 : (a != b) = true := by simp [hane]
      have hne2 : (b != a) = true := by simp [hbne]
      simp [hne1, hne2, Bool.or_comm]
  have hadj_diag : ∀ a, adj a a = false := by
    intro a
    dsimp [adj]
    simp
  have h12 : ∀ a b c, tri a b c = tri b a c := by
    intro a b c
    dsimp [tri]
    rw [hadj_sym a b, hadj_sym c a, hadj_sym b c]
    simp [Bool.and_assoc, Bool.and_comm, Bool.and_left_comm]
  have h23 : ∀ a b c, tri a b c = tri a c b := by
    intro a b c
    dsimp [tri]
    rw [hadj_sym b c, hadj_sym a b, hadj_sym c a]
    simp [Bool.and_assoc, Bool.and_comm, Bool.and_left_comm]
  have hdiag : ∀ a b, tri a a b = false := by
    intro a b
    dsimp [tri]
    simp [hadj_diag a]
  have hs := tripleSum_eq_six_inc vs hvn tri h12 h23 hdiag
  simpa [tripleSum, incTripleSum, pairSum, List.countP_eq_length_filter,
    List.filter_filter, Bool.and_comm] using hs

end TinylambOpt.ResearchProofs.ExecutionStrategies.RecursiveJoins

-- C++ references to this model or its proof.
-- executor/detail/execution_strategies.hpp
-- executor/detail/execution_strategies_test.cpp
