import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

namespace TinylambOpt.ResearchProofs.StorageExecution.GatherPlans.Sector
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

@[simp] theorem covers_nil : covers ([] : List Nat) = [[]] := rfl

@[simp] theorem covers_cons (x : Nat) (xs : List Nat) :
    covers (x :: xs) =
      (covers xs).flatMap (fun ps => match ps with
        | [] => [[(x, x)]]
        | (_, b) :: rest => [(x, x) :: ps, (x, b) :: rest]) := rfl

theorem covers_ne_nil : (ks : List Nat) → covers ks ≠ []
  | [] => by simp
  | [_] => by simp
  | x :: y :: ys => fun h => by
    rw [covers_cons] at h
    rw [List.flatMap_eq_nil_iff] at h
    obtain ⟨p, hp⟩ := List.exists_mem_of_ne_nil (covers (y :: ys)) (covers_ne_nil (y :: ys))
    have hn : (match p with | [] => [[(x, x)]] | (_, b) :: rest => [(x, x) :: p, (x, b) :: rest]) ≠ [] := by
      cases p <;> simp
    exact hn (h p hp)

theorem exists_mem_covers (ks : List Nat) : ∃ p, p ∈ covers ks := by
  cases h : covers ks with
  | nil => exact absurd h (covers_ne_nil ks)
  | cons p rest => exact ⟨p, by simp⟩

/-- The all-singletons plan is always available. -/
theorem mem_covers_singletons (ks : List Nat) : ks.map (fun k => (k, k)) ∈ covers ks := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
    rw [covers_cons]
    simp only [List.mem_flatMap]
    refine ⟨xs.map (fun k => (k, k)), ih, ?_⟩
    cases xs with
    | nil => simp
    | cons y ys => simp

/-- The single bounding-extent plan is available for nonempty inputs. -/
theorem mem_covers_span (ks : List Nat) (h : ks ≠ []) :
    [(ks.head?.getD 0, ks.getLast?.getD 0)] ∈ covers ks := by
  cases ks with
  | nil => exact absurd rfl h
  | cons x xs =>
    cases xs with
    | nil => simp
    | cons y ys =>
      rw [covers_cons]
      simp only [List.mem_flatMap]
      refine ⟨[(y, (y :: ys).getLast?.getD 0)], ?_, ?_⟩
      · exact mem_covers_span _ (by simp)
      · simp

/-- Concatenating plans covers concatenated points. -/
theorem mem_covers_append {xs ys : List Nat} {p q : List Extent}
    (hp : p ∈ covers xs) (hq : q ∈ covers ys) : p ++ q ∈ covers (xs ++ ys) := by
  induction xs generalizing p q ys with
  | nil =>
    cases hp
    · simpa using hq
    · cases ‹List.Mem p []›
  | cons x t ih =>
    rw [List.cons_append]
    rw [covers_cons] at hp
    simp only [List.mem_flatMap] at hp
    obtain ⟨ps, hps, hmem⟩ := hp
    have h1 : ps ++ q ∈ covers (t ++ ys) := ih hps hq
    rw [covers_cons]
    simp only [List.mem_flatMap]
    cases ps with
    | nil =>
      have hp' : p = [(x, x)] := by simpa using hmem
      subst hp'
      exact ⟨q, by simpa using h1, by cases q <;> simp⟩
    | cons e rest =>
      cases e with
      | mk a b =>
        simp only at hmem
        rw [List.mem_cons, List.mem_cons, List.mem_nil_iff, or_false] at hmem
        cases hmem with
        | inl h =>
          subst h
          exact ⟨((a, b) :: rest) ++ q, h1, by simp [List.cons_append]⟩
        | inr h =>
          subst h
          exact ⟨(a, b) :: (rest ++ q), by simpa [List.cons_append] using h1, by simp⟩


/-! Part B: points sortedness, cost algebra. -/

@[simp] theorem mem_points {x : Nat} {xs : List Nat} : x ∈ points xs ↔ x ∈ xs := by
  show x ∈ xs.eraseDups.mergeSort _ ↔ x ∈ xs
  simp [List.mem_eraseDups]

theorem ble_true (a b : Nat) : (Nat.ble a b = true) = (a ≤ b) := by
  cases Nat.le_total a b <;> simp [*]

theorem ble_true_iff {a b : Nat} : Nat.ble a b = true ↔ a ≤ b := by
  rw [ble_true]

theorem ble_total (a b : Nat) : (Nat.ble a b || Nat.ble b a) = true := by
  cases Nat.le_total a b <;> simp [*]

theorem ble_trans {a b c : Nat}
    (h₁ : Nat.ble a b = true) (h₂ : Nat.ble b c = true) : Nat.ble a c = true := by
  rw [ble_true_iff] at *; omega

/-- Rewire a `Pairwise` statement through a pointwise relation equivalence. -/
theorem pairwise_congr {α : Type} {R S : α → α → Prop} (h : ∀ a b, R a b ↔ S a b) :
    ∀ (l : List α), List.Pairwise R l → List.Pairwise S l := by
  intro l hl
  induction l with
  | nil => exact List.Pairwise.nil
  | cons a t ih =>
    cases hl with
    | cons h₁ h₂ =>
      refine List.Pairwise.cons ?_ ?_
      · intro b hb; exact (h a b).mp (h₁ b hb)
      · exact ih h₂

theorem pairwise_points (xs : List Nat) :
    List.Pairwise (fun a b => a ≤ b) (points xs) := by
  have key : ∀ a b : Nat, (fun x y => Nat.ble x y = true) a b ↔ a ≤ b :=
    fun a b => ble_true_iff
  exact pairwise_congr key (points xs)
    (List.pairwise_mergeSort (@ble_trans) (@ble_total) xs.eraseDups)

theorem points_head_le {xs : List Nat} {h : Nat} {t : List Nat}
    (hh : points xs = h :: t) : ∀ v ∈ t, h ≤ v := by
  have hp := pairwise_points xs
  rw [hh] at hp
  exact (List.pairwise_cons.mp hp).1

theorem points_head?_getD {xs : List Nat} {h : Nat} {t : List Nat}
    (hh : points xs = h :: t) : (points xs).head?.getD 0 = h := by
  rw [hh]; simp

theorem points_ne_nil {xs : List Nat} (h : xs ≠ []) : points xs ≠ [] := by
  cases xs with
  | nil => exact absurd rfl h
  | cons a xs =>
    intro h'
    have hm : a ∈ points (a :: xs) := by simp [mem_points]
    rw [h'] at hm
    cases hm

theorem points_singleton (x : Nat) : points [x] = [x] := by
  unfold points
  rw [show ([x].eraseDups : List Nat) = [x] from by
    unfold List.eraseDups
    rfl,
    List.mergeSort_singleton]

/-- Removing elements that are not present does nothing. -/
theorem removeAll_eq_self {xs ys : List Nat} (h : ∀ x ∈ xs, ∀ y ∈ ys, ¬ y = x) :
    ys.removeAll xs = ys := by
  induction xs generalizing ys with
  | nil => simp
  | cons x t ih =>
    rw [List.removeAll_cons]
    show (List.filter (fun x_1 => !x_1 == x) ys).removeAll t = ys
    have he : List.filter (fun x_1 => !x_1 == x) ys = ys := by
      rw [List.filter_eq_self]
      intro a ha
      have hb : !(a == x) = true := by
        cases hax : a == x with
        | true =>
          have : a = x := Nat.eq_of_beq_eq_true hax
          exact absurd this (h x (by simp) a ha)
        | false => decide
      exact hb
    rw [he]
    exact ih fun x' hx' y hy => h x' (by simp [hx']) y hy

/-- Two sorted lists that are permutations of each other are equal. -/
theorem sorted_perm_eq {a b : List Nat} (hp : List.Perm a b)
    (ha : List.Pairwise (fun x y => x ≤ y) a)
    (hb : List.Pairwise (fun x y => x ≤ y) b) : a = b := by
  induction a generalizing b with
  | nil =>
    cases b with
    | nil => rfl
    | cons y bs =>
      have hy : y ∈ ([] : List Nat) := hp.symm.subset List.mem_cons_self
      cases hy
  | cons c cs ih =>
    cases b with
    | nil =>
      have hx : c ∈ ([] : List Nat) := hp.subset List.mem_cons_self
      cases hx
    | cons d ds =>
      have hmem_d : d ∈ c :: cs := hp.symm.subset List.mem_cons_self
      have hcd : c ≤ d := by
        rcases List.mem_cons.mp hmem_d with hd | hd
        · subst hd; exact Nat.le_refl _
        · exact (List.pairwise_cons.mp ha).1 d hd
      have hmem_c : c ∈ d :: ds := hp.subset List.mem_cons_self
      have hdc : d ≤ c := by
        rcases List.mem_cons.mp hmem_c with hc | hc
        · subst hc; exact Nat.le_refl _
        · exact (List.pairwise_cons.mp hb).1 c hc
      have hcd' : c = d := Nat.le_antisymm hcd hdc
      subst hcd'
      have has : List.Perm cs ds := by
        have := List.Perm.erase c hp
        simpa using this
      rw [ih has (List.pairwise_cons.mp ha).2 (List.pairwise_cons.mp hb).2]

/-- Left half and right half points concatenate under a separating gap. -/
theorem points_append {xs ys : List Nat} (h : ∀ x ∈ xs, ∀ y ∈ ys, x < y) :
    points (xs ++ ys) = points xs ++ points ys := by
  have he : (xs ++ ys).eraseDups = xs.eraseDups ++ ys.eraseDups := by
    rw [List.eraseDups_append, removeAll_eq_self (xs := xs) (ys := ys)
      fun x hx y hy hxy => by have := h x hx y hy; omega]
  unfold points
  rw [he]
  refine sorted_perm_eq ?_ ?_ ?_
  · exact (List.mergeSort_perm (xs.eraseDups ++ ys.eraseDups) _).trans
      (List.Perm.append (List.mergeSort_perm xs.eraseDups _).symm
        (List.mergeSort_perm ys.eraseDups _).symm)
  · exact pairwise_congr (fun a b => ble_true_iff) _
      (List.pairwise_mergeSort (@ble_trans) (@ble_total)
        (xs.eraseDups ++ ys.eraseDups))
  · rw [List.pairwise_append]
    refine ⟨pairwise_points xs, pairwise_points ys, fun a ha b hb => ?_⟩
    have hax' : a ∈ xs := (List.mem_eraseDups).mp ((List.mem_mergeSort).mp ha)
    have hby' : b ∈ ys := (List.mem_eraseDups).mp ((List.mem_mergeSort).mp hb)
    exact Nat.le_of_lt (h a hax' b hby')

/-- Cost of concatenated plans. -/
theorem cost_append (f : Price) (p q : List Extent) :
    cost f (p ++ q) = cost f p + cost f q := by
  unfold cost; simp [List.map_append, List.sum_append]

theorem cost_cons (f : Price) (e : Extent) (p : List Extent) :
    cost f (e :: p) = f e.1 e.2 + cost f p := by
  unfold cost
  simp [List.map_cons, List.sum_cons]

/-- The all-singletons plan's cost is `Σ k, f k k`. -/
theorem cost_map_single (f : Price) (ks : List Nat) :
    cost f (ks.map fun k => (k, k)) = (ks.map fun k => f k k).sum := by
  unfold cost
  rw [List.map_map]
  rfl

/-- Adding a constant to every extent price adds `c * ps.length`. -/
theorem cost_add_const (f : Price) (c : Nat) :
    ∀ ps : List Extent, cost (fun a b => c + f a b) ps = c * ps.length + cost f ps := by
  intro ps
  induction ps with
  | nil => simp [cost]
  | cons e t ih =>
    rw [cost_cons, cost_cons, ih, List.length_cons, Nat.mul_add, Nat.mul_one]
    omega

/-- Adding `c * span` to every extent price adds `(map (c * ·.span))`. -/
theorem cost_mul_span (f : Price) (c : Nat) :
    ∀ ps : List Extent, cost (fun a b => c * span a b + f a b) ps =
      (ps.map fun e => c * span e.1 e.2).sum + cost f ps := by
  intro ps
  induction ps with
  | nil => simp [cost]
  | cons e t ih =>
    rw [cost_cons, cost_cons, ih]
    simp only [List.map_cons, List.sum_cons]
    omega
/-! Part C: `choose` is a minimum-cost plan covering every point. -/

theorem mergeSort_ne_nil {α : Type} {le : α → α → Bool} {l : List α}
    (h : l ≠ []) : l.mergeSort le ≠ [] := by
  intro hnil
  exact h (List.eq_nil_of_length_eq_zero
    (by rw [← List.length_mergeSort, hnil]; simp))

/-- Only the empty point list admits the empty plan. -/
theorem mem_covers_nil_iff {ks : List Nat} : ([] : List Extent) ∈ covers ks ↔ ks = [] := by
  constructor
  · intro h
    induction ks with
    | nil => rfl
    | cons y ys ih =>
      rw [covers_cons] at h
      simp only [List.mem_flatMap] at h
      obtain ⟨qs, hqs, hmem⟩ := h
      cases qs with
      | nil => simp at hmem
      | cons e rest => simp at hmem
  · intro he; rw [he]; simp

/-- A nonempty list is a cons. -/
theorem exists_cons_of_ne_nil {l : List α} (h : l ≠ []) : ∃ a as, l = a :: as := by
  cases l with
  | nil => exact absurd rfl h
  | cons a as => exact ⟨a, as, rfl⟩

theorem choose_cost_le (f : Price) (xs : List Nat) :
    ∀ ps, ps ∈ covers (points xs) →
      cost f (choose f xs) ≤ cost f ps := by
  intro ps hps
  unfold choose
  have hn : ((covers (points xs)).mergeSort (fun a b => cost f a ≤ cost f b)) ≠ [] :=
    mergeSort_ne_nil (covers_ne_nil (points xs))
  obtain ⟨hhd, tl, hmeq⟩ := exists_cons_of_ne_nil hn
  have hpw := @List.pairwise_mergeSort (List Extent)
      (fun a b => (cost f a ≤ cost f b : Bool))
      (fun a b c ha hb => decide_eq_true
        (Nat.le_trans (of_decide_eq_true ha) (of_decide_eq_true hb)))
      (fun a b => by cases Nat.le_total (cost f a) (cost f b) <;> simp [*])
      (covers (points xs))
  have keyd : ∀ u v : List Extent,
      ((fun a b => (cost f a ≤ cost f b : Bool)) u v = true) ↔ cost f u ≤ cost f v :=
    fun u v => ⟨fun h => of_decide_eq_true h, fun h => decide_eq_true h⟩
  have hp2 : List.Pairwise (fun a b => (cost f a ≤ cost f b)) (hhd :: tl) :=
    pairwise_congr keyd (hhd :: tl) (by rw [hmeq] at hpw; exact hpw)
  have hhead : ∀ b ∈ tl, cost f hhd ≤ cost f b :=
    fun b hb => (List.pairwise_cons.mp hp2).1 b hb
  have hps2 : ps ∈ hhd :: tl := by
    have h2 : ps ∈ (covers (points xs)).mergeSort (fun a b => cost f a ≤ cost f b) :=
      Iff.mpr List.mem_mergeSort hps
    rw [hmeq] at h2; exact h2
  rw [hmeq]
  simp
  simp only [List.mem_cons] at hps2
  cases hps2 with
  | inl he => rw [← he]; exact Nat.le_refl _
  | inr he => exact hhead _ he

theorem choose_mem_covers (f : Price) (xs : List Nat) :
    choose f xs ∈ covers (points xs) := by
  unfold choose
  have hn : ((covers (points xs)).mergeSort (fun a b => cost f a ≤ cost f b)) ≠ [] :=
    mergeSort_ne_nil (covers_ne_nil (points xs))
  obtain ⟨hhd, tl, hmeq⟩ := exists_cons_of_ne_nil hn
  have hmem : hhd ∈ covers (points xs) := by
    have h2 : hhd ∈ (covers (points xs)).mergeSort (fun a b => cost f a ≤ cost f b) := by
      rw [hmeq]; simp
    rw [List.mem_mergeSort] at h2
    exact h2
  rw [hmeq]
  simp
  exact hmem

theorem choose_cost_min (f : Price) (xs : List Nat) :
    ((covers (points xs)).map (cost f)).min? =
      some (cost f (choose f xs)) := by
  refine List.min?_eq_some_iff.mpr ⟨List.mem_map.mpr ⟨choose f xs,
    choose_mem_covers f xs, rfl⟩, ?_⟩
  intro n hn
  obtain ⟨ps, hps, rfl⟩ := List.mem_map.mp hn
  exact choose_cost_le f xs ps hps

/-- Extent endpoints of a `covers` plan are requested points. -/
theorem plan_pts_mem : ∀ (ks : List Nat) (ps : List Extent), ps ∈ covers ks →
    ∀ e ∈ ps, e.1 ∈ ks ∧ e.2 ∈ ks
  | [], ps, hps, e, he => by
      have hps' : ps = [] := by simpa using hps
      subst hps'; cases he
  | c :: cs, ps, hps, e, he => by
      rw [covers_cons] at hps
      simp only [List.mem_flatMap] at hps
      obtain ⟨qs, hqs, hmem⟩ := hps
      cases qs with
      | nil =>
        have hps' : ps = [(c, c)] := by simpa using hmem
        subst hps'
        rcases List.mem_cons.mp he with rfl | he'
        · exact ⟨by simp, by simp⟩
        · exact absurd he' List.not_mem_nil
      | cons q rest =>
        have hshape : ps = (c, c) :: (q :: rest) ∨ ps = (c, q.snd) :: rest := by
          simpa using hmem
        cases hshape with
        | inl hshape =>
          subst hshape
          rcases List.mem_cons.mp he with rfl | he'
          · exact ⟨by simp, by simp⟩
          · obtain ⟨h1, h2⟩ := plan_pts_mem cs (q :: rest) hqs e he'
            exact ⟨List.Mem.tail _ h1, List.Mem.tail _ h2⟩
        | inr hshape =>
          subst hshape
          rcases List.mem_cons.mp he with rfl | he'
          · exact ⟨by simp, List.Mem.tail _ (plan_pts_mem cs (q :: rest) hqs q (by simp)).2⟩
          · obtain ⟨h1, h2⟩ := plan_pts_mem cs (q :: rest) hqs e (List.Mem.tail _ he')
            exact ⟨List.Mem.tail _ h1, List.Mem.tail _ h2⟩

/-- On sorted points every plan in `covers` covers every point. -/
theorem plan_covers_points : ∀ (ks : List Nat), List.Pairwise (fun a b => a ≤ b) ks →
    ∀ ps ∈ covers ks, ∀ u ∈ ks, ∃ e ∈ ps, e.1 ≤ u ∧ u ≤ e.2
  | [], _, ps, _, u, hu => absurd hu List.not_mem_nil
  | c :: cs, hks, ps, hps, u, hu => by
      have hcs : List.Pairwise (fun a b => a ≤ b) cs := hks.tail
      have cmin : ∀ v ∈ cs, c ≤ v := (List.pairwise_cons.mp hks).1
      rw [covers_cons] at hps
      simp only [List.mem_flatMap] at hps
      obtain ⟨qs, hqs, hmem⟩ := hps
      rcases List.mem_cons.mp hu with rfl | hu'
      · -- `c` was substituted away; the goal now speaks of `u`
        cases qs with
        | nil =>
          have hps' : ps = [(u, u)] := by simpa using hmem
          subst hps'
          exact ⟨(u, u), by simp, Nat.le_refl _, Nat.le_refl _⟩
        | cons q rest =>
          have hshape : ps = (u, u) :: (q :: rest) ∨ ps = (u, q.snd) :: rest := by
            simpa using hmem
          cases hshape with
          | inl hshape =>
            subst hshape
            exact ⟨(u, u), by simp, Nat.le_refl _, Nat.le_refl _⟩
          | inr hshape =>
            subst hshape
            have hq2 : q.snd ∈ cs := (plan_pts_mem cs (q :: rest) hqs q (by simp)).2
            exact ⟨(u, q.snd), by simp, Nat.le_refl _, cmin q.snd hq2⟩
      · -- u ∈ cs
        cases qs with
        | nil =>
          have hcs' : cs = [] := mem_covers_nil_iff.mp hqs
          subst hcs'; cases hu'
        | cons q rest =>
          have hshape : ps = (c, c) :: (q :: rest) ∨ ps = (c, q.snd) :: rest := by
            simpa using hmem
          cases hshape with
          | inl hshape =>
            subst hshape
            obtain ⟨e, he, h1, h2⟩ := plan_covers_points cs hcs (q :: rest) hqs u hu'
            exact ⟨e, List.Mem.tail _ he, h1, h2⟩
          | inr hshape =>
            subst hshape
            obtain ⟨e, he, h1, h2⟩ := plan_covers_points cs hcs (q :: rest) hqs u hu'
            cases List.mem_cons.mp he with
            | inl heq =>
                cases heq
                exact ⟨(c, q.snd), by simp, cmin u hu', h2⟩
            | inr he' =>
                exact ⟨e, List.Mem.tail _ he', h1, h2⟩
/-! Part D: `tagged` lookup and the seven parameter-independent goal engines. -/

theorem tagged_cells_mem (a b i : Nat) (src : Nat → Nat) (h1 : a ≤ i) (h2 : i ≤ b) :
    (i, src i) ∈ (List.range (b+1-a)).map fun j => (a+j, src (a+j)) := by
  refine List.mem_map.mpr ⟨i - a, ?_, ?_⟩
  · rw [List.mem_range]; omega
  · obtain ⟨d, hd⟩ : ∃ d, i = a + d := ⟨i - a, by omega⟩
    subst hd
    have key : a + (a + d - a) = a + d := by omega
    rw [key]

theorem tagged_cells_unique (a b i w : Nat) (src : Nat → Nat)
    (h : (i, w) ∈ (List.range (b+1-a)).map fun j => (a+j, src (a+j))) : w = src i := by
  obtain ⟨j, _, heq⟩ := List.mem_map.mp h
  cases heq
  rfl

theorem lookup_of_first {l : List (Nat × Nat)} {k v : Nat}
    (h1 : (k, v) ∈ l) (h2 : ∀ p ∈ l, p.1 = k → p.2 = v) : l.lookup k = some v := by
  induction l with
  | nil => cases h1
  | cons p t ih =>
      cases p with
      | mk a b =>
          rw [List.lookup_cons]
          cases h : k == a with
          | true =>
              have hak : a = k := Eq.symm ((beq_iff_eq (α := Nat)).mp h)
              exact congrArg some (h2 (a, b) (by simp) hak)
          | false =>
              rcases List.mem_cons.mp h1 with he | hmem
              · cases he
                exact absurd h (by simp)
              · exact ih hmem (fun q hq hq1 => h2 q (List.Mem.tail _ hq) hq1)

theorem mem_tagged : ∀ (ps : List Extent) (src : Nat → Nat) (i : Nat),
    (∃ a b, (a, b) ∈ ps ∧ a ≤ i ∧ i ≤ b) → (i, src i) ∈ tagged ps src := by
  intro ps
  induction ps with
  | nil => intro src i ⟨a, b, hab, _, _⟩; cases hab
  | cons p t ih =>
      intro src i h
      obtain ⟨a, b, hab, hle, hge⟩ := h
      cases List.mem_cons.mp hab with
      | inl he =>
          cases he
          have hcells : tagged ((a, b) :: t) src =
              ((List.range (b+1-a)).map fun j => (a+j, src (a+j))) ++ tagged t src := by
            unfold tagged; rfl
          rw [hcells]
          exact List.mem_append.mpr (Or.inl (tagged_cells_mem a b i src hle hge))
      | inr hmem =>
          have hcells : tagged (p :: t) src =
              ((List.range (p.2+1-p.1)).map fun j => (p.1+j, src (p.1+j))) ++ tagged t src := by
            unfold tagged; rfl
          rw [hcells]
          exact List.mem_append.mpr (Or.inr (ih src i ⟨a, b, hmem, hle, hge⟩))

theorem tagged_unique : ∀ (ps : List Extent) (src : Nat → Nat) (i w : Nat),
    (i, w) ∈ tagged ps src → w = src i := by
  intro ps
  induction ps with
  | nil => intro src i w h; cases h
  | cons p t ih =>
      intro src i w h
      have hcells : tagged (p :: t) src =
          ((List.range (p.2+1-p.1)).map fun j => (p.1+j, src (p.1+j))) ++ tagged t src := by
        unfold tagged; rfl
      rw [hcells] at h
      rcases List.mem_append.mp h with h1 | h2
      · exact tagged_cells_unique p.1 p.2 i w src h1
      · exact ih src i w h2

theorem lookup_tagged : ∀ (ps : List Extent) (src : Nat → Nat) (i : Nat),
    (∃ a b, (a, b) ∈ ps ∧ a ≤ i ∧ i ≤ b) → (tagged ps src).lookup i = some (src i) := by
  intro ps src i h
  refine lookup_of_first (mem_tagged ps src i h) ?_
  intro q hq hq1
  cases hq1
  exact tagged_unique ps src q.1 q.2 hq

/-- Core exchange argument: a steeper per-unit surcharge cannot favour more units. -/
theorem exchange_arg (a c x y X Y : Nat)
    (h1 : (a + c + 1) * x + y ≤ (a + c + 1) * X + Y)
    (h2 : a * X + Y ≤ a * x + y) : x ≤ X := by
  rcases Nat.lt_or_ge X x with hx | hx
  · have h1x : (a + (c + 1)) * x + y ≤ (a + (c + 1)) * X + Y := by
      have hc : a + c + 1 = a + (c + 1) := by omega
      rw [hc] at h1
      exact h1
    have e1 := Nat.add_mul a (c + 1) x
    have e2 := Nat.add_mul a (c + 1) X
    have m1 : (c + 1) * X + (c + 1) ≤ (c + 1) * x :=
      Nat.mul_le_mul_left (c + 1) (Nat.succ_le_of_lt hx)
    have ma : a * X ≤ a * x := Nat.mul_le_mul_left a (Nat.le_of_lt hx)
    omega
  · exact hx

/-- Pointwise `Bool` monotonicity of `List.all`. -/
theorem all_mono {α : Type} {p q : α → Bool} : ∀ (ps : List α),
    (∀ x, x ∈ ps → p x = true → q x = true) →
    ps.all p = true → ps.all q = true := by
  intro ps
  induction ps with
  | nil => exact fun _ _ => by simp
  | cons e t ih =>
      intro h hq
      rw [List.all_cons, Bool.and_eq_true] at hq ⊢
      obtain ⟨h1, h2⟩ := hq
      exact ⟨h e (by simp) h1, ih (fun x hx => h x (List.Mem.tail _ hx)) h2⟩

theorem sum_map_mul_span (n : Nat) : ∀ ps : List Extent,
    (ps.map fun e => n * span e.1 e.2).sum = n * used ps := by
  intro ps
  induction ps with
  | nil => rfl
  | cons e t ih =>
      simp only [List.map_cons, List.sum_cons, used, ih]
      rw [Nat.mul_add]

theorem genericGatherOrder (f : Price) : gatherOrder f := by
  intro xs src
  refine List.map_congr_left (fun x hx => ?_)
  rw [lookup_tagged (choose f xs) src x]
  obtain ⟨e, hem, h1, h2⟩ :=
    plan_covers_points (points xs) (pairwise_points xs) (choose f xs)
      (choose_mem_covers f xs) x (mem_points.mpr hx)
  refine ⟨e.1, e.2, ?_, h1, h2⟩
  cases e
  simpa using hem

theorem genericSparseOrDense (f : Price) : sparseOrDense f := by
  intro xs hxs
  show cost f (choose f xs) ≤
    min ((points xs).map (fun k => f k k)).sum
      (f ((points xs).head?.getD 0) ((points xs).getLast?.getD 0))
  have h1 : cost f (choose f xs) ≤ ((points xs).map (fun k => f k k)).sum := by
    rw [← cost_map_single]
    exact choose_cost_le f xs _ (mem_covers_singletons (points xs))
  have hne : points xs ≠ [] := points_ne_nil hxs
  have h2 : cost f (choose f xs) ≤
      f ((points xs).head?.getD 0) ((points xs).getLast?.getD 0) := by
    have hle := choose_cost_le f xs _ (mem_covers_span (points xs) hne)
    show cost f (choose f xs) ≤
      cost f [( (points xs).head?.getD 0, (points xs).getLast?.getD 0)]
    exact hle
  rcases Nat.le_total _ _ with hlt | hgt
  · rw [Nat.min_eq_left hlt]
    exact h1
  · rw [Nat.min_eq_right hgt]
    exact h2

theorem genericSeekTariff (f : Price) : seekTariff f := by
  intro xs a b hab
  obtain ⟨c, hb⟩ := Nat.exists_eq_add_of_lt hab
  subst hb
  have h1 : cost (fun l r => (a + c + 1) + f l r)
      (choose (fun l r => (a + c + 1) + f l r) xs)
      ≤ cost (fun l r => (a + c + 1) + f l r) (choose (fun l r => a + f l r) xs) :=
    choose_cost_le (fun l r => (a + c + 1) + f l r) xs _ (choose_mem_covers _ xs)
  have h2 : cost (fun l r => a + f l r) (choose (fun l r => a + f l r) xs)
      ≤ cost (fun l r => a + f l r) (choose (fun l r => (a + c + 1) + f l r) xs) :=
    choose_cost_le (fun l r => a + f l r) xs _ (choose_mem_covers _ xs)
  rw [cost_add_const, cost_add_const] at h1
  rw [cost_add_const, cost_add_const] at h2
  exact exchange_arg a c _ _ _ _ h1 h2

theorem genericReadAmplification (f : Price) : readAmplification f := by
  intro xs a b hab
  obtain ⟨c, hb⟩ := Nat.exists_eq_add_of_lt hab
  subst hb
  have h1 : cost (fun l r => (a + c + 1) * span l r + f l r)
      (choose (fun l r => (a + c + 1) * span l r + f l r) xs)
      ≤ cost (fun l r => (a + c + 1) * span l r + f l r)
      (choose (fun l r => a * span l r + f l r) xs) :=
    choose_cost_le (fun l r => (a + c + 1) * span l r + f l r) xs _
      (choose_mem_covers _ xs)
  have h2 : cost (fun l r => a * span l r + f l r)
      (choose (fun l r => a * span l r + f l r) xs)
      ≤ cost (fun l r => a * span l r + f l r)
      (choose (fun l r => (a + c + 1) * span l r + f l r) xs) :=
    choose_cost_le (fun l r => a * span l r + f l r) xs _
      (choose_mem_covers _ xs)
  rw [cost_mul_span, cost_mul_span, sum_map_mul_span, sum_map_mul_span] at h1
  rw [cost_mul_span, cost_mul_span, sum_map_mul_span, sum_map_mul_span] at h2
  exact exchange_arg a c _ _ _ _ h1 h2

theorem genericShardBoundary (f : Price) : shardBoundary f := by
  intro xs ys hsep
  have hm : choose f xs ++ choose f ys ∈ covers (points xs ++ points ys) :=
    mem_covers_append (choose_mem_covers f xs) (choose_mem_covers f ys)
  rw [← points_append hsep] at hm
  have hle := choose_cost_le f (xs ++ ys) _ hm
  rw [cost_append] at hle
  exact hle

theorem genericLatencyCap (f : Price) : latencyCap f := by
  intro xs a b n hab hn
  simp only [bestCapped] at hn ⊢
  obtain ⟨hnm, hnmin⟩ := List.min?_eq_some_iff.mp hn
  obtain ⟨ps, hps, hceq⟩ := List.mem_map.mp hnm
  have hpsb : ps ∈ (covers (points xs)).filter
      fun qs => qs.all fun (x, y) => span x y ≤ b := by
    rw [List.mem_filter] at hps ⊢
    refine ⟨hps.1, all_mono _ ?_ hps.2⟩
    intro z hz hz'
    cases z with
    | mk l r =>
        exact decide_eq_true (Nat.le_trans (of_decide_eq_true hz') hab)
  cases hm : ((covers (points xs)).filter
      fun qs => qs.all fun (x, y) => span x y ≤ b).map (cost f) |>.min? with
  | none =>
      exfalso
      rw [List.min?_eq_none_iff] at hm
      have hmem2 := List.mem_map.mpr ⟨ps, hpsb, hceq⟩
      rw [hm] at hmem2
      exact absurd hmem2 (by simp)
  | some m =>
      refine ⟨m, ?_, rfl⟩
      rw [List.min?_eq_some_iff] at hm
      exact hm.2 n (List.mem_map.mpr ⟨ps, hpsb, hceq⟩)

theorem covers_pair {x y : Nat} :
    covers [x, y] = [[(x, x), (y, y)], [(x, y)]] := by
  rw [covers_cons, covers_cons, covers_nil]
  rfl

theorem points_pair {x y : Nat} (h : x < y) : points [x, y] = [x, y] := by
  have hcond : !y == x = true := by
    rw [Bool.not_eq_true', beq_eq_false_iff_ne]
    omega
  have hf : ([y].filter fun b => !b == x : List Nat) = [y] := by
    rw [List.filter_cons, List.filter_nil, hcond]
    rfl
  have he : ([x, y].eraseDups : List Nat) = [x, y] := by
    rw [List.eraseDups_cons, hf, List.eraseDups_cons, List.filter_nil,
        List.eraseDups_nil]
  unfold points
  rw [he]
  have hxy : decide (x ≤ y) = true := decide_eq_true (Nat.le_of_lt h)
  rw [List.mergeSort_pair]
  show (if decide (x ≤ y) = true then [x, y] else [y, x]) = [x, y]
  rw [if_pos hxy]

theorem genericTwoRequests (f : Price) : twoRequests f := by
  intro x y hlt
  have hcov : covers (points [x, y]) = [[(x, x), (y, y)], [(x, y)]] := by
    rw [points_pair hlt, covers_pair]
  have h11 := choose_cost_le f [x, y] [(x, x), (y, y)] (by rw [hcov]; simp)
  have h1 := choose_cost_le f [x, y] [(x, y)] (by rw [hcov]; simp)
  have c11 : cost f [(x, x), (y, y)] = f x x + f y y := by unfold cost; simp
  have c1 : cost f [(x, y)] = f x y := by unfold cost; simp
  have hmem := choose_mem_covers f [x, y]
  rw [hcov] at hmem
  simp only [List.mem_cons, List.mem_nil_iff, or_false] at hmem
  cases hmem with
  | inl he =>
      rw [he] at h1 ⊢
      rw [c11] at h1 ⊢
      rw [c1] at h1
      rw [Nat.min_eq_left h1]
  | inr he =>
      rw [he] at h11 ⊢
      rw [c1] at h11 ⊢
      rw [c11] at h11
      rw [Nat.min_eq_right h11]


/-! Part E: `dp` (via `boundaryCost`) equals the `covers`-optimum, hence `leastCost`. -/

theorem drop_length_nil (l : List Nat) : l.drop l.length = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      show (a :: as).drop (as.length + 1) = _
      rw [List.drop_succ_cons]
      exact ih

/-- Minimum cost over all covering plans of `ks`. -/
def M (f : Price) (ks : List Nat) : Nat :=
  ((covers ks).map (cost f)).min?.getD 0

theorem M_nil (f : Price) : M f [] = 0 := rfl

theorem covers_map_ne_nil (f : Price) (ks : List Nat) :
    (covers ks).map (cost f) ≠ [] := by
  intro h
  rw [List.map_eq_nil_iff] at h
  exact covers_ne_nil ks h

/-- The two defining properties of `M f ks` as a minimum. -/
theorem M_spec (f : Price) (ks : List Nat) :
    (∃ p, p ∈ covers ks ∧ cost f p = M f ks) ∧
      (∀ p, p ∈ covers ks → M f ks ≤ cost f p) := by
  cases hmin : ((covers ks).map (cost f)).min? with
  | none =>
      have h0 : (covers ks).map (cost f) = [] := by
        rw [← List.min?_eq_none_iff]
        exact hmin
      exact absurd h0 (covers_map_ne_nil f ks)
  | some m =>
      have hM : M f ks = m := by
        show ((covers ks).map (cost f)).min?.getD 0 = m
        rw [hmin]
        rfl
      rw [List.min?_eq_some_iff] at hmin
      refine ⟨?_, ?_⟩
      · obtain ⟨p, hp, hceq⟩ := List.mem_map.mp hmin.1
        exact ⟨p, hp, hceq.trans hM.symm⟩
      · intro p hp
        have := hmin.2 (cost f p) (List.mem_map.mpr ⟨p, hp, rfl⟩)
        rw [hM]
        exact this

theorem min?_some_of_ne_nil {l : List Nat} (h : l ≠ []) : ∃ m, l.min? = some m := by
  cases hl : l with
  | nil => exact absurd hl h
  | cons a as => exact ⟨List.foldl min a as, rfl⟩

theorem min?_getD_le {l : List Nat} (h : l ≠ []) : ∀ z ∈ l, l.min?.getD 0 ≤ z := by
  obtain ⟨m, hm⟩ := min?_some_of_ne_nil h
  rw [hm]
  exact (List.min?_eq_some_iff.mp hm).2

theorem getD_range_in (k n : Nat) (hk : k < n) : (List.range n).getD k 0 = k := by
  rw [List.getD_eq_getElem?_getD, List.getElem?_range hk]
  rfl

theorem getD_map_in (f : α → β) (a : α) (b : β) {l : List α} :
    ∀ {i : Nat}, i < l.length → (l.map f).getD i b = f (l.getD i a) := by
  induction l with
  | nil => intro i hi; exact absurd hi (by simp)
  | cons x xs ih =>
      intro i hi
      cases i with
      | zero => rfl
      | succ j =>
          rw [List.map_cons, List.getD_cons_succ, List.getD_cons_succ]
          exact ih (by simpa using hi)

theorem getElem?_drop (l : List Nat) : ∀ i e, (l.drop i)[e]? = l[i + e]? := by
  induction l with
  | nil => intro i; cases i <;> intro e <;> rfl
  | cons a as ih =>
      intro i e
      cases i with
      | zero => rw [Nat.zero_add]; rfl
      | succ j =>
          rw [show j + 1 + e = j + e + 1 from by omega]
          exact ih j e

theorem drop_getD_cons {l : List Nat} :
    ∀ {i : Nat}, i < l.length → l.drop i = l.getD i 0 :: l.drop (i + 1) := by
  induction l with
  | nil => intro i h; exact absurd h (by simp)
  | cons a as ih =>
      intro i hi
      cases i with
      | zero => rfl
      | succ j =>
          have hj : j < as.length := by simpa using hi
          rw [List.drop_succ_cons, List.getD_cons_succ, List.drop_succ_cons]
          exact ih hj

theorem drop_getD_eq_getD (l : List Nat) (i e : Nat) :
    (l.drop i).getD e 0 = l.getD (i + e) 0 := by
  rw [List.getD_eq_getElem?_getD, List.getD_eq_getElem?_getD, getElem?_drop]

/-- Every plan in `covers ks` is either empty or starts with one extent from the first
key of `ks` to some key, followed by a plan covering the remaining keys. -/
theorem mem_covers_head_cases (ks : List Nat) :
    ∀ (w : List Extent), w ∈ covers ks →
      w = [] ∨ ∃ (d : Nat) (q : List Extent),
        w = (ks.getD 0 0, ks.getD d 0) :: q ∧ d < ks.length ∧
          q ∈ covers (ks.drop (d + 1)) := by
  induction ks with
  | nil =>
      intro w h
      rw [covers_nil] at h
      rw [List.mem_singleton] at h
      exact Or.inl h
  | cons x xs ih =>
      intro w h
      rw [covers_cons] at h
      obtain ⟨u, hu, hm⟩ := List.mem_flatMap.mp h
      cases u with
      | nil =>
          have hw : w = [(x, x)] := by simpa using hm
          subst hw
          exact Or.inr ⟨0, [], rfl, by simp, hu⟩
      | cons c r =>
          rcases c with ⟨c1, c2⟩
          simp only [List.mem_cons, List.mem_singleton, List.mem_nil_iff] at hm
          rcases hm with h1 | h2 | hnil
          · subst h1
            exact Or.inr ⟨0, (c1, c2) :: r, rfl, by simp, hu⟩
          · subst h2
            rcases ih ((c1, c2) :: r) hu with hnil' | ⟨e, q, heq, hd, hq⟩
            · cases hnil'
            · cases heq
              refine Or.inr ⟨e + 1, r, ?_, by simpa using hd, ?_⟩
              · simp only [List.getD_cons_succ, List.getD_cons_zero]
              · rw [List.drop_succ_cons]
                exact hq
          · simp at hnil

/-- Conversely, any plan of the keys after a cut point extends with the spanning first
extent to a plan of all the keys. -/
theorem mem_covers_block (ks : List Nat) :
    ∀ (d : Nat), d < ks.length → ∀ (q : List Extent),
      q ∈ covers (ks.drop (d + 1)) → (ks.getD 0 0, ks.getD d 0) :: q ∈ covers ks := by
  induction ks with
  | nil => intro d hd; exact absurd hd (by simp)
  | cons x xs ih =>
      intro d hd q hq
      rw [covers_cons]
      cases d with
      | zero =>
          refine List.mem_flatMap.mpr ⟨q, hq, ?_⟩
          cases q with
          | nil => exact List.Mem.head _
          | cons c r => exact List.Mem.head _
      | succ e =>
          refine List.mem_flatMap.mpr
            ⟨(xs.getD 0 0, xs.getD e 0) :: q, ih e (by simpa using hd) q hq, ?_⟩
          exact List.Mem.tail _ (List.Mem.head _)

/-- First-extent decomposition of `covers`. -/
theorem mem_covers_firstBlock {a : Nat} {as : List Nat} {ps : List Extent} :
    ps ∈ covers (a :: as) ↔
      ∃ d q, d ≤ as.length ∧ q ∈ covers (as.drop d) ∧
        ps = (a, (a :: as).getD d 0) :: q := by
  constructor
  · intro h
    rcases mem_covers_head_cases (a :: as) ps h with rfl | ⟨d, q, heq, hd, hq⟩
    · rw [covers_cons] at h
      obtain ⟨u, hu, hm⟩ := List.mem_flatMap.mp h
      cases u with
      | nil => simp at hm
      | cons c r =>
          rcases c with ⟨c1, c2⟩
          simp at hm
    · refine ⟨d, q, ?_, ?_, ?_⟩
      · rw [List.length_cons] at hd
        omega
      · rw [List.drop_succ_cons] at hq
        exact hq
      · exact heq
  · intro hex
    obtain ⟨d, q, hd, hq, rfl⟩ := hex
    refine mem_covers_block (a :: as) d ?_ q ?_
    · rw [List.length_cons]
      omega
    · rw [List.drop_succ_cons]
      exact hq

theorem M_cons (f : Price) (a : Nat) (as : List Nat) :
    M f (a :: as) =
      ((List.range (as.length + 1)).map fun d =>
        f a ((a :: as).getD d 0) + M f (as.drop d)).min?.getD 0 := by
  have hT : ((List.range (as.length + 1)).map fun d =>
      f a ((a :: as).getD d 0) + M f (as.drop d)) ≠ [] := by
    intro h
    rw [List.map_eq_nil_iff] at h
    rw [List.eq_nil_iff_length_eq_zero, List.length_range] at h
    omega
  have hstep_ge : ∀ d, d < as.length + 1 →
      M f (a :: as) ≤ f a ((a :: as).getD d 0) + M f (as.drop d) := by
    intro d hd
    obtain ⟨q', hq', hceq'⟩ := (M_spec f (as.drop d)).1
    have hps : (a, (a :: as).getD d 0) :: q' ∈ covers (a :: as) :=
      (mem_covers_firstBlock (a := a) (as := as)).2 ⟨d, q', by omega, hq', rfl⟩
    have hle := (M_spec f (a :: as)).2 _ hps
    have hcost : cost f ((a, (a :: as).getD d 0) :: q') =
        f a ((a :: as).getD d 0) + cost f q' := cost_cons f _ _
    rw [hcost] at hle
    rw [hceq'] at hle
    exact hle
  have hmemT : ∀ z ∈ ((List.range (as.length + 1)).map fun d =>
      f a ((a :: as).getD d 0) + M f (as.drop d)),
      M f (a :: as) ≤ z := by
    intro z hz
    obtain ⟨d, hd, rfl⟩ := List.mem_map.mp hz
    rw [List.mem_range] at hd
    exact hstep_ge d hd
  have hmemS : ∀ z ∈ (covers (a :: as)).map (cost f),
      ∃ w ∈ ((List.range (as.length + 1)).map fun d =>
        f a ((a :: as).getD d 0) + M f (as.drop d)), w ≤ z := by
    intro z hz
    obtain ⟨ps, hps, hceq⟩ := List.mem_map.mp hz
    obtain ⟨d, q, hd, hq, heq⟩ := (mem_covers_firstBlock (a := a) (as := as)).1 hps
    refine ⟨f a ((a :: as).getD d 0) + M f (as.drop d),
      List.mem_map.mpr ⟨d, by rw [List.mem_range]; omega, rfl⟩, ?_⟩
    have hle := (M_spec f (as.drop d)).2 q hq
    rw [heq] at hceq
    rw [cost_cons] at hceq
    have hceq' : f a ((a :: as).getD d 0) + cost f q = z := hceq
    omega
  obtain ⟨tm, htm⟩ := min?_some_of_ne_nil hT
  obtain ⟨sm, hsm⟩ := min?_some_of_ne_nil (covers_map_ne_nil f (a :: as))
  have hMsm : M f (a :: as) = sm := by
    show ((covers (a :: as)).map (cost f)).min?.getD 0 = sm
    rw [hsm]
    rfl
  have hTm : ((List.range (as.length + 1)).map fun d =>
      f a ((a :: as).getD d 0) + M f (as.drop d)).min?.getD 0 = tm := by
    show ((List.range (as.length + 1)).map fun d =>
        f a ((a :: as).getD d 0) + M f (as.drop d)).min?.getD 0 = tm
    rw [htm]
    rfl
  have h1 : M f (a :: as) ≤ tm := hmemT tm ((List.min?_eq_some_iff.mp htm).1)
  rw [hMsm] at h1
  obtain ⟨w, hwT, hwsm⟩ := hmemS sm ((List.min?_eq_some_iff.mp hsm).1)
  have h3 : tm ≤ w := by
    rw [← hTm]
    exact min?_getD_le hT w hwT
  rw [hMsm, hTm]
  omega

theorem array_getD_toList (A : Array Nat) (i : Nat) :
    (A[i]?).getD 0 = A.toList.getD i 0 := by
  rw [← Array.getElem?_toList, List.getD_eq_getElem?_getD]

theorem back?_getL (A : Array Nat) : A.back?.getD 0 = A.toList.getLast?.getD 0 := by
  obtain ⟨l⟩ := A
  change ((List.toArray l).back?.getD 0) = ((List.toArray l).toList.getLast?.getD 0)
  rw [List.toList_toArray, List.back?_toArray]

theorem bc_foldl (n : Nat) (edge : Nat → Nat → Nat) :
    boundaryCost n edge =
      (((List.range n).foldl (fun (s : Array Nat) done =>
          s.push (((List.range (done+1)).map fun d =>
            edge (n-done-1) (n-done-1+d+1) + (s[done-d]?.getD 0)).min?.getD 0))
        #[0]).back?.getD 0) := by
  unfold boundaryCost
  simp only [Id.run, bind_pure, pure_bind, StateT.pure]
  rw [List.forIn_pure_yield_eq_foldl]
  simp only [pure_bind, bind_pure]
  rfl

abbrev BCStep (n : Nat) (K : List Nat) (f : Price) (s : Array Nat) (d : Nat) : Array Nat :=
  s.push (((List.range (d + 1)).map fun e =>
    f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e + 1 - 1) 0) +
      (s[d - e]?).getD 0).min?.getD 0)

abbrev BCVal (n : Nat) (K : List Nat) (f : Price) (i : Nat) : Nat :=
  M f (K.drop (n - i))

theorem bc_arr_val (n : Nat) (K : List Nat) (hK : K.length = n) (f : Price) :
    ∀ (done : Nat), done ≤ n →
      ((List.range done).foldl (BCStep n K f) #[0]).toList =
        (List.range (done + 1)).map (BCVal n K f) := by
  intro done
  induction done with
  | zero =>
      intro _
      show (#[0] : Array Nat).toList = (List.range (0 + 1)).map (BCVal n K f)
      have hb : (#[0] : Array Nat).toList = [0] := rfl
      have h1 : (List.range (0 + 1)).map (BCVal n K f) = [BCVal n K f 0] := rfl
      have h0 : BCVal n K f 0 = 0 := by
        show M f (K.drop (n - 0)) = 0
        rw [Nat.sub_zero, ← hK, drop_length_nil, M_nil]
      rw [hb, h1, h0]
  | succ d ih =>
      intro hd
      -- unfold one foldl iteration
      have hstep : ((List.range (d + 1)).foldl (BCStep n K f) #[0]).toList =
          ((List.range d).foldl (BCStep n K f) #[0]).toList ++
            [((List.range (d + 1)).map fun e =>
                f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e + 1 - 1) 0) +
                  (((List.range d).foldl (BCStep n K f) #[0])[d - e]?).getD 0).min?.getD 0] := by
        simp only [List.range_succ, List.foldl_append, List.foldl_cons, List.foldl_nil,
          Array.toList_push, BCStep, List.map_append, List.map_singleton]
      have hmap : (List.range (d + 1 + 1)).map (BCVal n K f) =
          (List.range (d + 1)).map (BCVal n K f) ++ [BCVal n K f (d + 1)] := by
        simp only [List.range_succ, List.map_append, List.map_singleton]
      rw [hstep, ih (by omega), hmap]
      refine congrArg (fun x => List.map (BCVal n K f) (List.range (d + 1)) ++ [x]) ?_
      show ((List.range (d + 1)).map fun e =>
          f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e + 1 - 1) 0) +
            (((List.range d).foldl (BCStep n K f) #[0])[d - e]?).getD 0).min?.getD 0
        = BCVal n K f (d + 1)
      -- convert internal array reads to list reads of the IH value
      have heq1 : (fun e => f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e + 1 - 1) 0) +
            (((List.range d).foldl (BCStep n K f) #[0])[d - e]?).getD 0)
          = (fun e => f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e) 0) +
            ((List.range (d + 1)).map (BCVal n K f)).getD (d - e) 0) := by
        refine funext (fun e => congrArg (fun z =>
          f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e) 0) + z) ?_)
        · rw [array_getD_toList]
          have hin : ((List.range d).foldl (BCStep n K f) #[0]).toList =
              (List.range (d + 1)).map (BCVal n K f) := ih (by omega)
          rw [hin]
      rw [heq1]
      have heq2 : (fun e => f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e) 0) +
            ((List.range (d + 1)).map (BCVal n K f)).getD (d - e) 0)
          = (fun e => f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e) 0) +
            M f (K.drop (n - d + e))) := by
        refine funext (fun e => congrArg (fun z =>
          f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e) 0) + z) ?_)
        · show ((List.range (d + 1)).map (BCVal n K f)).getD (d - e) 0
            = M f (K.drop (n - d + e))
          rw [getD_map_in (BCVal n K f) 0 0
            (show d - e < (List.range (d + 1)).length by
              rw [List.length_range]; omega),
            getD_range_in (d - e) (d + 1) (by omega)]
          show M f (K.drop (n - (d - e))) = M f (K.drop (n - d + e))
          by_cases he : e ≤ d
          · refine congrArg (M f) (congrArg (fun z => K.drop z) ?_)
            omega
          · have h1 : K.drop (n - (d - e)) = [] := by
              rw [show n - (d - e) = n from by omega, ← hK]
              exact drop_length_nil K
            have h2 : K.drop (n - d + e) = [] := by
              refine (List.drop_eq_nil_of_le ?_)
              rw [← hK]
              omega
            rw [h1, h2, M_nil]
      rw [heq2]
      -- now the entry is a clean minimum over prefixes; identify it with M f (K.drop (n-d-1))
      have hlt : n - d - 1 < K.length := by rw [hK]; omega
      have hsplit := @drop_getD_cons K (n - d - 1) hlt
      have hlen : (K.drop (n - d - 1 + 1)).length = d := by
        rw [List.length_drop, hK]
        omega
      have hgoal : M f (K.drop (n - (d + 1))) =
          ((List.range (d + 1)).map fun e =>
            f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e) 0) +
              M f (K.drop (n - d + e))).min?.getD 0 := by
        have h1 : n - (d + 1) = n - d - 1 := by omega
        rw [h1]
        have h2 : K.drop (n - d - 1) = K.getD (n - d - 1) 0 :: K.drop (n - d - 1 + 1) := hsplit
        rw [h2, M_cons]
        refine congrArg (fun l : List Nat => l.min?.getD 0) ?_
        rw [hlen]
        refine congrArg (fun g : Nat → Nat => List.map g (List.range (d + 1))) ?_
        refine funext (fun e => ?_)
        rw [← hsplit, drop_getD_eq_getD]
        refine congrArg (fun z =>
          f (K.getD (n - d - 1) 0) (K.getD (n - d - 1 + e) 0) + z) ?_
        refine congrArg (M f) ?_
        rw [List.drop_drop]
        refine congrArg (fun z => K.drop z) ?_
        omega
      exact hgoal.symm

theorem getLast?_range_succ (n : Nat) : (List.range (n + 1)).getLast? = some n := by
  induction n with
  | zero => rfl
  | succ m ih =>
      rw [List.range_succ, List.getLast?_append, ih]
      rfl

theorem boundaryCost_M (n : Nat) (K : List Nat) (hK : K.length = n) (f : Price) :
    boundaryCost n (fun a b => f (K.getD a 0) (K.getD (b - 1) 0)) = M f K := by
  rw [bc_foldl, back?_getL, bc_arr_val n K hK f n (by omega)]
  have h2 : ((List.range (n + 1)).map (BCVal n K f)).getLast? = some (BCVal n K f n) := by
    rw [List.getLast?_map, getLast?_range_succ]
    rfl
  rw [h2]
  have h3 : BCVal n K f n = M f (K.drop (n - n)) := rfl
  rw [h3, Nat.sub_self, List.drop_zero]
  rfl

theorem dp_eq_M (f : Price) (xs : List Nat) : dp f (points xs) = M f (points xs) := by
  have hdpe : dp f (points xs)
      = boundaryCost (points xs).toArray.size
          (fun a b => f ((points xs).toArray[a]?.getD 0) ((points xs).toArray[b-1]?.getD 0)) := rfl
  have he : (fun a b => f ((points xs).toArray[a]?.getD 0) ((points xs).toArray[b-1]?.getD 0))
      = (fun a b => f ((points xs).getD a 0) ((points xs).getD (b-1) 0)) := by
    refine funext (fun a => funext (fun b => ?_))
    rw [array_getD_toList, array_getD_toList, List.toList_toArray]
  rw [hdpe, List.size_toArray, he, boundaryCost_M ((points xs).length) (points xs) rfl f]

theorem genericLeastCost (f : Price) : leastCost f := by
  intro xs
  show dp f (points xs) = cost f (choose f xs)
  rw [dp_eq_M]
  show ((covers (points xs)).map (cost f)).min?.getD 0 = cost f (choose f xs)
  rw [choose_cost_min]
  rfl

theorem sound_LeastCost : Sector.LeastCost.Goal := fun shift startup => genericLeastCost (Sector.price shift startup)
theorem sound_GatherOrder : Sector.GatherOrder.Goal := fun shift startup => genericGatherOrder (Sector.price shift startup)
theorem sound_SparseOrDense : Sector.SparseOrDense.Goal := fun shift startup => genericSparseOrDense (Sector.price shift startup)
theorem sound_SeekTariff : Sector.SeekTariff.Goal := fun shift startup => genericSeekTariff (Sector.price shift startup)
theorem sound_ReadAmplification : Sector.ReadAmplification.Goal := fun shift startup => genericReadAmplification (Sector.price shift startup)
theorem sound_ShardBoundary : Sector.ShardBoundary.Goal := fun shift startup => genericShardBoundary (Sector.price shift startup)
theorem sound_LatencyCap : Sector.LatencyCap.Goal := fun shift startup => genericLatencyCap (Sector.price shift startup)
theorem sound_TwoRequests : Sector.TwoRequests.Goal := fun shift startup => genericTwoRequests (Sector.price shift startup)

/-! ── Part F: PositiveGapRule plumbing (chunk 1) ── -/

/-- Consecutive pairs, defined by structural recursion. -/
def pg_pair : List Nat → List (Nat × Nat)
  | [] => []
  | [_] => []
  | a :: b :: l => (a, b) :: pg_pair (b :: l)

def pg_D (l : List Nat) : List Nat := (pg_pair l).map fun p => p.2 - p.1 - 1
def pg_ezs (l : List Nat) : List Nat := (pg_pair l).map fun p => p.2 - p.1
def pg_raw (fee : Nat) (l : List Nat) : List Nat := (pg_D l).map (fun g => min fee g)

theorem pg_pair_nil : pg_pair [] = [] := rfl
theorem pg_pair_singleton (a : Nat) : pg_pair [a] = [] := rfl
theorem pg_pair_cons_cons (a b : Nat) (ys : List Nat) :
    pg_pair (a :: b :: ys) = (a, b) :: pg_pair (b :: ys) := rfl

theorem pg_pair_eq_zip (l : List Nat) : pg_pair l = l.zip (l.drop 1) := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases xs with
      | nil => rfl
      | cons y ys => rw [pg_pair_cons_cons, ih]; rfl

theorem pg_getD_oob : ∀ (l : List Nat) (d z : Nat), l.length ≤ d → l.getD d z = z := by
  intro l
  induction l with
  | nil => intro d z _; rfl
  | cons a as ih =>
      intro d z hd
      cases d with
      | zero =>
          exact absurd hd (by simp)
      | succ e =>
          rw [List.getD_cons_succ]
          exact ih e z (by simpa using hd)

theorem pg_getD_mem : ∀ (D : List Nat) (g : Nat), g ∈ D → ∃ j, j < D.length ∧ D.getD j 0 = g := by
  intro D
  induction D with
  | nil => intro g hg; exact absurd hg (by simp)
  | cons x xs ih =>
      intro g hg
      rw [List.mem_cons] at hg
      cases hg with
      | inl h => subst h; exact ⟨0, by simp, rfl⟩
      | inr h2 =>
          obtain ⟨j, hj, heq⟩ := ih g h2
          exact ⟨j + 1, by simpa using hj, by rw [List.getD_cons_succ, heq]⟩

theorem pg_head_le_getD : ∀ (l : List Nat) (i : Nat), i < l.length →
    (∀ p ∈ pg_pair l, p.1 < p.2) → l.getD 0 0 ≤ l.getD i 0 := by
  intro l
  induction l with
  | nil => intro i hi _; exact absurd hi (by simp)
  | cons a as ih =>
      intro i hi hd
      cases i with
      | zero => rw [List.getD_cons_zero]; exact Nat.le_refl _
      | succ j =>
          cases as with
          | nil => exact absurd hi (by simp)
          | cons b ys =>
              rw [List.getD_cons_succ]
              have hb : a < b := hd (a, b) (by rw [pg_pair_cons_cons]; exact List.Mem.head _)
              have hdt : ∀ p ∈ pg_pair (b :: ys), p.1 < p.2 := by
                intro p hp
                have h2 : p ∈ pg_pair (a :: b :: ys) := by
                  rw [pg_pair_cons_cons]; exact List.Mem.tail _ hp
                exact hd p h2
              refine Nat.le_trans (Nat.le_of_lt hb) (ih j (by simpa using hi) hdt)

/-- Telescoping: consecutive-step sum over the first `d` steps. -/
theorem pg_take_ezs : ∀ (l : List Nat) (d : Nat), d < l.length →
    (∀ p ∈ pg_pair l, p.1 < p.2) →
    ((pg_ezs l).take d).sum = l.getD d 0 - l.getD 0 0 := by
  intro l
  induction l with
  | nil => intro d hd _; exact absurd hd (by simp)
  | cons a as ih =>
      intro d hd hlt
      cases as with
      | nil =>
          have h0 : d = 0 := by simpa using hd
          rw [h0, List.take_zero, List.sum_nil, List.getD_cons_zero]
          omega
      | cons b ys =>
          cases d with
          | zero =>
              rw [List.take_zero, List.sum_nil, List.getD_cons_zero]
              omega
          | succ e =>
              have hz := pg_pair_cons_cons a b ys
              have he : e < (b :: ys).length := by simpa using hd
              have hdt : ∀ p ∈ pg_pair (b :: ys), p.1 < p.2 := by
                intro p hp
                have h2 : p ∈ pg_pair (a :: b :: ys) := by
                  rw [hz]; exact List.Mem.tail _ hp
                exact hlt p h2
              have hab : a < b := hlt (a, b) (by rw [hz]; exact List.Mem.head _)
              have hsum : pg_ezs (a :: b :: ys) = (b - a) :: pg_ezs (b :: ys) := by
                unfold pg_ezs; rw [hz, List.map_cons]
              have hget : (a :: b :: ys).getD (e + 1) 0 = (b :: ys).getD e 0 :=
                List.getD_cons_succ
              have hget0 : (a :: b :: ys).getD 0 0 = a := List.getD_cons_zero
              have h1 := ih e he hdt
              rw [hsum, List.take_succ_cons, List.sum_cons, h1, hget, hget0]
              have hge : b ≤ (b :: ys).getD e 0 := pg_head_le_getD (b :: ys) e he hdt
              have hz1 : (b :: ys).getD 0 0 = b := List.getD_cons_zero
              rw [hz1]
              omega

theorem pg_sum_map_le {α : Type} (f g : α → Nat) : ∀ (l : List α),
    (∀ x ∈ l, f x ≤ g x) → (l.map f).sum ≤ (l.map g).sum := by
  intro l
  induction l with
  | nil => intro _; exact Nat.le_refl _
  | cons x xs ih =>
      intro h
      rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
      exact Nat.add_le_add (h x (List.Mem.head _)) (ih (fun y hy => h y (List.Mem.tail _ hy)))

theorem pg_sum_add_one (l : List Nat) : (l.map (fun x => x + 1)).sum = l.sum + l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons, List.sum_cons, ih, List.length_cons]
      omega

theorem pg_map_id {α : Type} (f : α → α) : ∀ (l : List α), (∀ x ∈ l, f x = x) → l.map f = l := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons x xs ih =>
      intro h
      rw [List.map_cons, h x (List.Mem.head _), ih (fun y hy => h y (List.Mem.tail _ hy))]

theorem pg_map_on {α β : Type} (f g : α → β) : ∀ (l : List α),
    (∀ x ∈ l, f x = g x) → l.map f = l.map g := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons x xs ih =>
      intro h
      rw [List.map_cons, List.map_cons, h x (List.Mem.head _),
        ih (fun y hy => h y (List.Mem.tail _ hy))]

theorem pg_pw_filter {α : Type} (R : α → α → Prop) : ∀ (l : List α), List.Pairwise R l →
    ∀ (p : α → Bool), List.Pairwise R (l.filter p) := by
  intro l
  induction l with
  | nil => intro _ _; exact List.Pairwise.nil
  | cons x xs ih =>
      intro h p
      cases h with
      | cons h1 h2 =>
          have hrec : List.Pairwise R (List.filter p xs) := ih h2 p
          refine show List.Pairwise R
              (match p x with
              | true => x :: List.filter p xs
              | false => List.filter p xs) from ?_
          cases p x with
          | true =>
              refine List.Pairwise.cons (fun y hy => h1 y ((List.mem_filter).mp hy).1) hrec
          | false => exact hrec

theorem pg_nodup_eraseDups : ∀ (l : List Nat), (l.eraseDups).Nodup := by
  have key : ∀ (n : Nat) (l : List Nat), l.length ≤ n → (l.eraseDups).Nodup := by
    intro n
    induction n with
    | zero =>
        intro l hl
        have h0 : l = [] := (List.eq_nil_iff_length_eq_zero).mpr (by omega)
        subst h0
        exact List.Pairwise.nil
    | succ n ih =>
        intro l hl
        cases l with
        | nil => exact List.Pairwise.nil
        | cons a as =>
            rw [List.eraseDups_cons]
            refine List.Pairwise.cons ?_ ?_
            · intro x hx
              have hmp : !(x == a) = true :=
                ((List.mem_filter).mp ((List.mem_eraseDups).mp hx)).2
              intro hcon
              subst hcon
              simp at hmp
            · have hlen : (as.filter fun b => !(b == a)).length ≤ n := by
                  have := List.length_filter_le (fun b => !(b == a)) as
                  simp only [List.length_cons] at hl
                  omega
              exact ih _ hlen
  intro l
  refine key l.length l (Nat.le_refl _)

theorem pg_nodup_points (xs : List Nat) : (points xs).Nodup :=
  (List.mergeSort_perm (xs.eraseDups) (fun a b => Nat.ble a b = true)).symm.nodup
    (pg_nodup_eraseDups xs)

theorem pg_pair_lt_aux : ∀ (l : List Nat), List.Pairwise (fun a b => a ≤ b) l → l.Nodup →
    ∀ p ∈ pg_pair l, p.1 < p.2 := by
  intro l
  induction l with
  | nil => intro _ _ p hp; rw [pg_pair_nil] at hp; exact absurd hp (by simp)
  | cons x xs ih =>
      intro hpw hn p hp
      cases xs with
      | nil => rw [pg_pair_singleton] at hp; exact absurd hp (by simp)
      | cons b ys =>
          have hn' : List.Pairwise (· ≠ ·) (x :: b :: ys) := hn
          have h1 := List.pairwise_cons.mp hpw
          have h2 := List.pairwise_cons.mp hn'
          have hlt : x < b :=
            Nat.lt_of_le_of_ne (h1.1 b (List.Mem.head _)) (fun h => h2.1 b (List.Mem.head _) h)
          rw [pg_pair_cons_cons, List.mem_cons] at hp
          cases hp with
          | inl h => subst h; exact hlt
          | inr h => exact ih h1.2 h2.2 p h

theorem pg_strict_points (xs : List Nat) : ∀ p ∈ pg_pair (points xs), p.1 < p.2 :=
  pg_pair_lt_aux (points xs) (pairwise_points xs) (pg_nodup_points xs)

/-- Lengths of the derived lists. -/
theorem pg_len_pair : ∀ (l : List Nat), (pg_pair l).length = l.length - 1 := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases xs with
      | nil => rfl
      | cons b ys =>
          rw [pg_pair_cons_cons, List.length_cons, List.length_cons, ih,
            List.length_cons]
          omega

theorem pg_len_D (l : List Nat) : (pg_D l).length = l.length - 1 := by
  rw [pg_D, List.length_map, pg_len_pair]

theorem pg_len_raw (fee : Nat) (l : List Nat) : (pg_raw fee l).length = l.length - 1 := by
  rw [pg_raw, List.length_map, pg_len_D]

/-- Drops commute with the derived lists. -/
theorem pg_pair_drop : ∀ (l : List Nat) (d : Nat), pg_pair (l.drop d) = (pg_pair l).drop d := by
  intro l
  induction l with
  | nil => intro d; cases d <;> rfl
  | cons x xs ih =>
      intro d
      cases d with
      | zero => rfl
      | succ e =>
          cases xs with
          | nil => cases e <;> rfl
          | cons b ys =>
              rw [List.drop_succ_cons, pg_pair_cons_cons, List.drop_succ_cons]
              exact ih e

theorem pg_D_drop (l : List Nat) (d : Nat) : pg_D (l.drop d) = (pg_D l).drop d := by
  unfold pg_D; rw [pg_pair_drop, List.map_drop]

theorem pg_raw_drop (fee : Nat) (l : List Nat) (d : Nat) :
    pg_raw fee (l.drop d) = (pg_raw fee l).drop d := by
  unfold pg_raw; rw [pg_D_drop, List.map_drop]

/-- Steps (`pg_ezs`) are raw gaps plus one. -/
theorem pg_E_map (l : List Nat) (hd : ∀ p ∈ pg_pair l, p.1 < p.2) :
    pg_ezs l = (pg_D l).map (fun x => x + 1) := by
  unfold pg_ezs pg_D
  rw [List.map_map]
  refine pg_map_on (fun p => p.2 - p.1) (fun p => p.2 - p.1 - 1 + 1) (pg_pair l) ?_
  intro p hp
  have := hd p hp
  rcases p with ⟨x, y⟩
  omega

theorem pg_raw_le_ezs (fee : Nat) (l : List Nat) (hd : ∀ p ∈ pg_pair l, p.1 < p.2) :
    (pg_raw fee l).sum + (pg_pair l).length ≤ (pg_ezs l).sum := by
  rw [pg_E_map l hd, pg_sum_add_one, pg_len_pair, ← pg_len_D]
  have h1 : (pg_raw fee l).sum ≤ (pg_D l).sum := by
    have h1' : pg_raw fee l = (pg_D l).map (fun g => min fee g) := rfl
    have h2 : ((pg_D l).map (fun g => min fee g)).sum ≤ ((pg_D l).map (fun g => g)).sum :=
      pg_sum_map_le (fun g => min fee g) (fun g => g) (pg_D l) (fun g _ => Nat.min_le_right _ _)
    have h3 : ((pg_D l).map (fun g => g)).sum = (pg_D l).sum := by
      rw [show (fun g => g) = id from rfl, List.map_id]
    rw [h1']
    exact Nat.le_trans h2 (Nat.le_of_eq h3)
  omega

theorem pg_take_map_le (fee : Nat) : ∀ (D : List Nat),
    (∀ g ∈ D, min fee g + 1 ≤ g + 1) → ∀ k, k ≤ D.length →
      ((D.map (fun g => min fee g)).take k).sum + k ≤ ((D.map (fun g => g + 1)).take k).sum := by
  intro D
  induction D with
  | nil => intro _ k hk; simpa using hk
  | cons x xs ih =>
      intro h k hk
      cases k with
      | zero => simp
      | succ e =>
          rw [List.map_cons, List.map_cons, List.take_succ_cons, List.sum_cons,
            List.take_succ_cons, List.sum_cons]
          have h1 := h x (List.Mem.head _)
          have h2 := ih (fun y hy => h y (List.Mem.tail _ hy)) e (by simpa using hk)
          omega

theorem pg_take_raw_sum_le (fee : Nat) (l : List Nat) (hd : ∀ p ∈ pg_pair l, p.1 < p.2) :
    ∀ k, k ≤ (pg_pair l).length →
      ((pg_raw fee l).take k).sum + k ≤ ((pg_ezs l).take k).sum := by
  intro k hk
  have h1 : pg_raw fee l = (pg_D l).map (fun g => min fee g) := rfl
  have h2 : pg_ezs l = (pg_D l).map (fun x => x + 1) := pg_E_map l hd
  have hlen : (pg_D l).length = (pg_pair l).length := by
    rw [pg_D, List.length_map, pg_len_pair]
  have hk' : k ≤ (pg_D l).length := by rw [hlen]; exact hk
  have h3 := pg_take_map_le fee (pg_D l) (fun g _ => Nat.succ_le_succ (Nat.min_le_right _ _)) k hk'
  rw [h1, h2]
  exact h3

theorem pg_getD_map {α β : Type} (f : α → β) (l : List α) (i : Nat) (d₀ : α) (d : β)
    (h : i < l.length) : (l.map f).getD i d = f (l.getD i d₀) := by
  induction l generalizing i with
  | nil => exact absurd h (by simp)
  | cons x xs ih =>
      cases i with
      | zero => rfl
      | succ j =>
          rw [List.map_cons, List.getD_cons_succ, List.getD_cons_succ]
          exact ih j (by simpa using h)

theorem pg_getD_raw_ge (fee : Nat) (l : List Nat) (d : Nat) (h : fee ≤ (pg_D l).getD d 0) :
    (pg_raw fee l).getD d 0 = fee := by
  rcases Nat.lt_or_ge d (pg_D l).length with hd | hd
  · rw [pg_raw, pg_getD_map (fun g => min fee g) (pg_D l) d 0 0 hd]
    exact Nat.min_eq_left h
  · have he1 : (pg_raw fee l).getD d 0 = 0 := by
        rw [pg_raw]
        refine pg_getD_oob ((pg_D l).map (fun g => min fee g)) d 0 ?_
        rw [List.length_map]; exact hd
    have he2 : (pg_D l).getD d 0 = 0 := pg_getD_oob (pg_D l) d 0 hd
    rw [he2] at h
    rw [he1]
    omega

theorem pg_getD_raw_lt (fee : Nat) (l : List Nat) (d : Nat) (h : (pg_D l).getD d 0 < fee) :
    (pg_raw fee l).getD d 0 = (pg_D l).getD d 0 := by
  rcases Nat.lt_or_ge d (pg_D l).length with hd | hd
  · rw [pg_raw, pg_getD_map (fun g => min fee g) (pg_D l) d 0 0 hd]
    exact Nat.min_eq_right (Nat.le_of_lt h)
  · have he2 : (pg_D l).getD d 0 = 0 := pg_getD_oob (pg_D l) d 0 hd
    rw [he2] at h
    have he1 : (pg_raw fee l).getD d 0 = 0 := by
        rw [pg_raw]
        refine pg_getD_oob ((pg_D l).map (fun g => min fee g)) d 0 ?_
        rw [List.length_map]; exact hd
    rw [he1, he2]

/-- First index at which the raw gap reaches the fee. -/
theorem pg_first_ge (fee : Nat) : ∀ (D : List Nat),
    (∃ g ∈ D, fee ≤ g) →
    ∃ i, i < D.length ∧ fee ≤ D.getD i 0 ∧ ∀ j, j < i → D.getD j 0 < fee := by
  intro D
  induction D with
  | nil => rintro ⟨_, hg, _⟩; exact absurd hg (by intro m; cases m)
  | cons x xs ih =>
      intro hex
      by_cases hx : fee ≤ x
      · exact ⟨0, by simpa, hx, fun j hj => absurd hj (by omega)⟩
      · obtain ⟨g, hg, hge⟩ := hex
        rcases List.mem_cons.mp hg with rfl | hgs
        · exact absurd hge hx
        · obtain ⟨i, hi, h1, h2⟩ := ih ⟨g, hgs, hge⟩
          refine ⟨i + 1, ?_, ?_, ?_⟩
          · simpa using hi
          · exact h1
          · intro j hj
            cases j with
            | zero => rw [List.getD_cons_zero]; exact Nat.lt_of_not_le hx
            | succ k => rw [List.getD_cons_succ]; exact h2 k (by omega)

theorem pg_take_succ_getD (l : List Nat) (hd : Nat) : ∀ (d : Nat), d < l.length →
    l.take (d + 1) = l.take d ++ [l.getD d hd] := by
  induction l with
  | nil => intro d h; exact absurd h (by simp)
  | cons a as ih =>
      intro d h
      cases d with
      | zero =>
          rw [List.take_succ_cons, List.take_zero, List.take_zero, List.getD_cons_zero,
            List.nil_append]
      | succ e =>
          cases as with
          | nil => exact absurd h (by simp)
          | cons b ys =>
              have he : e < (b :: ys).length := by simpa using h
              have h1 := ih e he
              have hget : (a :: b :: ys).getD (e + 1) hd = (b :: ys).getD e hd :=
                List.getD_cons_succ
              rw [show (a :: b :: ys).take (e + 1 + 1) = a :: (b :: ys).take (e + 1) from rfl,
                show (a :: b :: ys).take (e + 1) = a :: (b :: ys).take e from rfl,
                h1, hget, List.cons_append]

theorem pg_sum_take_drop (f : Nat → Nat) : ∀ (l : List Nat) (n : Nat),
    (l.map f).sum = ((l.take n).map f).sum + ((l.drop n).map f).sum := by
  intro l
  induction l with
  | nil => intro n; simp
  | cons x xs ih =>
      intro n
      cases n with
      | zero => simp
      | succ e =>
          rw [List.take_succ_cons, List.map_cons, List.sum_cons, List.drop_succ_cons,
            List.map_cons, List.sum_cons, ih e]
          omega

/-- On a prefix whose gaps all stay below the fee, taking the minimum is the identity. -/
theorem pg_take_raw_eq (fee : Nat) (D : List Nat) : ∀ (d : Nat),
    (∀ j, j < d → D.getD j 0 < fee) →
    (D.take d).map (fun g => min fee g) = D.take d := by
  intro d
  induction d with
  | zero => intro _; rfl
  | succ e ih =>
      intro h
      rcases Nat.lt_or_ge e D.length with he | he
      · have htake := pg_take_succ_getD D 0 e he
        have ht := ih (fun j hj => h j (by omega))
        rw [htake, List.map_append, ht, List.map_singleton,
          Nat.min_eq_right (Nat.le_of_lt (h e (Nat.lt_succ_self e)))]
      · have ht := ih (fun j hj => h j (by omega))
        rw [List.take_of_length_le (by omega)] at ht
        rw [List.take_of_length_le (by omega)]
        exact ht

/-! Part H: DistinctSectorFloor plumbing. -/

/-- Sector index of a page. -/
def sf_sec (shift : Nat) (x : Nat) : Nat := x / 2 ^ shift

/-- Number of distinct sectors touched by a page list. -/
def sf_secs (shift : Nat) (l : List Nat) : Nat := ((l.map (sf_sec shift)).eraseDups).length

/-- Drops shorten. -/
theorem sf_len_drop_le : ∀ (l : List Nat) (d : Nat), (l.drop d).length ≤ l.length := by
  intro l
  induction l with
  | nil =>
      intro d
      rw [List.drop_nil]
      exact Nat.zero_le _
  | cons x xs ih =>
      intro d
      cases d with
      | zero =>
          rw [List.drop_zero]
          exact Nat.le_refl _
      | succ e =>
          rw [List.length_cons, List.drop_succ_cons]
          exact Nat.le_trans (ih e) (Nat.le_add_right _ _)

/-- Monotone maps preserve sortedness. -/
theorem sf_pw_map (shift : Nat) : ∀ (l : List Nat), List.Pairwise (· ≤ ·) l →
    List.Pairwise (· ≤ ·) (l.map (sf_sec shift)) := by
  intro l
  induction l with
  | nil => intro _; exact List.Pairwise.nil
  | cons x xs ih =>
      intro hw
      have h2 := (List.pairwise_cons).mp hw
      refine List.Pairwise.cons ?_ (ih h2.2)
      intro z hz
      obtain ⟨y, hy, hzy⟩ := List.mem_map.mp hz
      rw [← hzy]
      exact Nat.div_le_div_right (h2.1 y hy)

/-- Drops preserve sortedness. -/
theorem sf_pw_drop : ∀ (l : List Nat), List.Pairwise (· ≤ ·) l → ∀ (n : Nat),
    List.Pairwise (· ≤ ·) (l.drop n) := by
  intro l
  induction l with
  | nil =>
      intro _ n
      rw [List.drop_nil]
      exact List.Pairwise.nil
  | cons x xs ih =>
      intro hw n
      cases n with
      | zero => exact hw
      | succ e => exact ih (List.pairwise_cons.mp hw).2 e

/-- In-bounds `getD` yields a member. -/
theorem sf_getD_mem : ∀ (l : List Nat) (i : Nat), i < l.length → l.getD i 0 ∈ l := by
  intro l
  induction l with
  | nil => intro i hi; exact absurd hi (by simp)
  | cons x xs ih =>
      intro i hi
      cases i with
      | zero => rw [List.getD_cons_zero]; exact List.Mem.head _
      | succ e =>
          rw [List.getD_cons_succ]
          exact List.Mem.tail _ (ih e (by simpa using hi))

/-- Sorted lists have order-preserving indices. -/
theorem sf_getD_le : ∀ (l : List Nat), List.Pairwise (· ≤ ·) l → ∀ i j, i ≤ j → j < l.length →
    l.getD i 0 ≤ l.getD j 0 := by
  intro l
  induction l with
  | nil => intro _ i j _ hj; exact absurd hj (by simp)
  | cons x xs ih =>
      intro hw i j hij hj
      cases i with
      | zero =>
          rw [List.getD_cons_zero]
          cases j with
          | zero => exact Nat.le_refl _
          | succ f =>
              rw [List.getD_cons_succ]
              exact (List.pairwise_cons.mp hw).1 _ (sf_getD_mem xs f (by simpa using hj))
      | succ e =>
          cases j with
          | zero => exfalso; omega
          | succ f =>
              rw [List.getD_cons_succ, List.getD_cons_succ]
              exact ih (List.pairwise_cons.mp hw).2 e f (by omega) (by simpa using hj)

/-- Filters commute. -/
theorem sf_filter_comm (p q : Nat → Bool) : ∀ (l : List Nat),
    (l.filter p).filter q = (l.filter q).filter p := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : p x
      · by_cases hy : q x
        · rw [List.filter_cons_of_pos hx, List.filter_cons_of_pos hy,
            List.filter_cons_of_pos hy, List.filter_cons_of_pos hx, ih]
        · rw [List.filter_cons_of_pos hx, List.filter_cons_of_neg hy,
            List.filter_cons_of_neg hy, ih]
      · by_cases hy : q x
        · rw [List.filter_cons_of_neg hx, List.filter_cons_of_pos hy,
            List.filter_cons_of_neg hx, ih]
        · rw [List.filter_cons_of_neg hx, List.filter_cons_of_neg hy, ih]

/-- Removing all copies of one value loses at most one distinct value. -/
theorem sf_ed_mini : ∀ (t : List Nat) (x : Nat),
    ((t.filter fun b => !(b == x)).eraseDups).length ≤ (t.eraseDups).length := by
  intro t
  have key : ∀ (n : Nat) (t : List Nat), t.length ≤ n → ∀ (x : Nat),
      ((t.filter fun b => !(b == x)).eraseDups).length ≤ (t.eraseDups).length := by
    intro n
    induction n with
    | zero =>
        intro t ht x
        have h0 : t = [] := (List.eq_nil_iff_length_eq_zero).mpr (by omega)
        subst h0
        rw [List.filter_nil, List.eraseDups_nil]
        exact Nat.le_refl _
    | succ n ih =>
        intro t ht x
        cases t with
        | nil =>
            rw [List.filter_nil, List.eraseDups_nil]
            exact Nat.le_refl _
        | cons y ys =>
            have hys : ys.length ≤ n := by simpa using ht
            have hr : (y :: ys).eraseDups = y :: (ys.filter fun b => !(b == y)).eraseDups :=
              List.eraseDups_cons
            rw [hr, List.length_cons]
            by_cases hyx : y = x
            · subst hyx
              rw [List.filter_cons_of_neg (p := fun b => !(b == y)) (a := y) (by simp)]
              omega
            · have hpos : (fun b => !(b == x)) y := by
                show !(y == x) = true
                simp [hyx]
              have h2 := ih (ys.filter fun b => !(b == y))
                (Nat.le_trans (List.length_filter_le (fun b => !(b == y)) ys) hys) x
              have h3 := sf_filter_comm (fun b => !(b == x)) (fun b => !(b == y)) ys
              rw [List.filter_cons_of_pos (p := fun b => !(b == x)) (a := y) hpos]
              rw [List.eraseDups_cons, List.length_cons]
              rw [← h3] at h2
              omega
  exact key t.length t (Nat.le_refl _)

/-- Distinct-value count is subadditive over appends (union bound). -/
theorem sf_secs' : ∀ (u v : List Nat),
    ((u ++ v).eraseDups).length ≤ (u.eraseDups).length + (v.eraseDups).length := by
  intro u v
  have key : ∀ (n : Nat) (u v : List Nat), u.length ≤ n →
      ((u ++ v).eraseDups).length ≤ (u.eraseDups).length + (v.eraseDups).length := by
    intro n
    induction n with
    | zero =>
        intro u v hu
        have h0 : u = [] := (List.eq_nil_iff_length_eq_zero).mpr (by omega)
        subst h0
        rw [List.nil_append, List.eraseDups_nil, List.length_nil, Nat.zero_add]
        exact Nat.le_refl _
    | succ n ih =>
        intro u v hu
        cases u with
        | nil =>
            rw [List.nil_append, List.eraseDups_nil, List.length_nil, Nat.zero_add]
            exact Nat.le_refl _
        | cons x us =>
            have hus : us.length ≤ n := by simpa using hu
            rw [List.cons_append, List.eraseDups_cons, List.eraseDups_cons,
              List.length_cons, List.length_cons]
            have hchain : ((us ++ v).filter fun b => !(b == x)).eraseDups.length
                ≤ (us.filter fun b => !(b == x)).eraseDups.length + (v.eraseDups).length := by
              rw [List.filter_append]
              refine Nat.le_trans (ih (us.filter fun b => !(b == x))
                  (v.filter fun b => !(b == x))
                  (by
                    have := List.length_filter_le (fun b => !(b == x)) us
                    omega)) ?_
              exact Nat.add_le_add_left (sf_ed_mini v x) _
            omega
  exact key u.length u v (Nat.le_refl _)

/-- Sector indices are monotone. -/
theorem sf_sec_mono (shift : Nat) : ∀ x y, x ≤ y → sf_sec shift x ≤ sf_sec shift y := by
  intro x y h
  exact Nat.div_le_div_right h

/-- Takes shrink membership. -/
theorem sf_take_mem : ∀ (n : Nat) (l : List Nat) (x : Nat), x ∈ l.take n → x ∈ l := by
  intro n
  induction n with
  | zero =>
      intro l x hx
      cases hx
  | succ e ih =>
      intro l x hx
      cases l with
      | nil => cases hx
      | cons a as =>
          rw [List.take_succ_cons] at hx
          rcases List.mem_cons.mp hx with rfl | hx2
          · exact List.Mem.head _
          · exact List.Mem.tail _ (ih as x hx2)

/-- Takes preserve sortedness. -/
theorem sf_pw_take : ∀ (n : Nat) (l : List Nat), List.Pairwise (· ≤ ·) l →
    List.Pairwise (· ≤ ·) (l.take n) := by
  intro n
  induction n with
  | zero =>
      intro l _
      show List.Pairwise (· ≤ ·) ([] : List Nat)
      exact List.Pairwise.nil
  | succ e ih =>
      intro l hw
      cases l with
      | nil =>
          show List.Pairwise (· ≤ ·) ([] : List Nat)
          exact List.Pairwise.nil
      | cons a as =>
          rw [List.take_succ_cons]
          refine List.Pairwise.cons ?_ (ih as (List.pairwise_cons.mp hw).2)
          intro b hb
          exact (List.pairwise_cons.mp hw).1 b (sf_take_mem e as b hb)

/-- Membership yields an in-bounds index. -/
theorem sf_mem_getD : ∀ (l : List Nat) (x : Nat), x ∈ l →
    ∃ i, i < l.length ∧ l.getD i 0 = x := by
  intro l
  induction l with
  | nil => intro x hx; exact absurd hx (by simp)
  | cons a as ih =>
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hy
      · exact ⟨0, by rw [List.length_cons]; omega, List.getD_cons_zero⟩
      · obtain ⟨i, hi, he⟩ := ih x hy
        refine ⟨i + 1, ?_, ?_⟩
        · rw [List.length_cons]
          exact Nat.succ_lt_succ hi
        · rw [List.getD_cons_succ]
          exact he

/-- Getting inside a prefix matches the full list. -/
theorem sf_take_getD : ∀ (l : List Nat) (j i : Nat), i < (l.take (j + 1)).length →
    (l.take (j + 1)).getD i 0 = l.getD i 0 := by
  intro l
  induction l with
  | nil =>
      intro j i hi
      exact absurd hi (by simpa)
  | cons a as ih =>
      intro j i hi
      cases j with
      | zero =>
          have ht : (a :: as).take 1 = [a] := by
            rw [List.take_succ_cons, List.take_zero]
          rw [ht] at hi ⊢
          cases i with
          | zero => rfl
          | succ f => exact absurd hi (by simpa)
      | succ e =>
          rw [List.take_succ_cons] at hi ⊢
          cases i with
          | zero => rw [List.getD_cons_zero, List.getD_cons_zero]
          | succ f =>
              rw [List.getD_cons_succ, List.getD_cons_succ]
              exact ih e f (by simpa using hi)

/-- Takes shorten. -/
theorem sf_len_take_le : ∀ (n : Nat) (l : List Nat), (l.take n).length ≤ l.length := by
  intro n
  induction n with
  | zero =>
      intro l
      rw [List.take_zero, List.length_nil]
      exact Nat.zero_le _
  | succ e ih =>
      intro l
      cases l with
      | nil =>
          rw [List.take_nil, List.length_nil]
          exact Nat.zero_le _
      | cons a as =>
          rw [List.take_succ_cons, List.length_cons, List.length_cons]
          exact Nat.succ_le_succ (ih as)

theorem sf_len_take_min : ∀ (n : Nat) (l : List Nat), (l.take n).length ≤ n := by
  intro n
  induction n with
  | zero =>
      intro l
      rw [List.take_zero, List.length_nil]
      exact Nat.zero_le _
  | succ e ih =>
      intro l
      cases l with
      | nil =>
          rw [List.take_nil, List.length_nil]
          exact Nat.zero_le _
      | cons a as =>
          rw [List.take_succ_cons, List.length_cons]
          exact Nat.succ_le_succ (ih as)

/-- Sorted lists with values in `[lo..A]` have at most `A - lo + 1` distinct entries. -/
theorem sf_bound : ∀ (M : List Nat) (A lo : Nat), List.Pairwise (· ≤ ·) M →
    (∀ x ∈ M, lo ≤ x) → (∀ x ∈ M, x ≤ A) → M.eraseDups.length ≤ A - lo + 1 := by
  intro M A lo hw hlo hA
  have key : ∀ (n : Nat) (M : List Nat), M.length ≤ n → ∀ A lo, List.Pairwise (· ≤ ·) M →
      (∀ x ∈ M, lo ≤ x) → (∀ x ∈ M, x ≤ A) → M.eraseDups.length ≤ A - lo + 1 := by
    intro n
    induction n with
    | zero =>
        intro M hM A lo _ _ _
        have h0 : M = [] := (List.eq_nil_iff_length_eq_zero).mpr (by omega)
        subst h0
        rw [List.eraseDups_nil, List.length_nil]
        omega
    | succ n ih =>
        intro M hM A lo hw hlo hA
        cases M with
        | nil =>
            rw [List.eraseDups_nil, List.length_nil]
            omega
        | cons m M' =>
            have hc := List.pairwise_cons.mp hw
            have hm : lo ≤ m := hlo m (List.Mem.head _)
            have hmA : m ≤ A := hA m (List.Mem.head _)
            have hM'A : ∀ x ∈ M', x ≤ A := fun x hx => hA x (List.Mem.tail m hx)
            rw [List.eraseDups_cons, List.length_cons]
            have hlen : M'.length ≤ n := by simpa using hM
            have hlt : m < A ∨ m = A := by omega
            rcases hlt with hlt | rfl
            · have hF : ∀ x ∈ M'.filter (fun b => !(b == m)), m + 1 ≤ x := by
                intro y hy
                obtain ⟨hy1, hy2⟩ := List.mem_filter.mp hy
                have hym : m ≤ y := hc.1 y hy1
                simp at hy2
                omega
              have h2 := ih (M'.filter (fun b => !(b == m)))
                (Nat.le_trans (List.length_filter_le (fun b => !(b == m)) M') hlen) A (m + 1)
                (pg_pw_filter (· ≤ ·) M' hc.2 (fun b => !(b == m))) hF
                (fun y hy => hM'A y (List.mem_filter.mp hy).1)
              omega
            · have hnil : M'.filter (fun b => !(b == m)) = [] :=
                (List.eq_nil_iff_forall_not_mem).mpr (fun y hy => by
                  obtain ⟨hy1, hy2⟩ := List.mem_filter.mp hy
                  have hym : m ≤ y := hc.1 y hy1
                  have hyA : y ≤ m := hM'A y hy1
                  simp at hy2
                  omega)
              rw [hnil, List.eraseDups_nil, List.length_nil]
              omega
  exact key M.length M (Nat.le_refl _) A lo hw hlo hA

/-- A sorted prefix spans at most its sector range. -/
theorem sf_span (shift : Nat) : ∀ j l, List.Pairwise (· ≤ ·) l → j < l.length →
    sf_secs shift (l.take (j + 1)) ≤
      l.getD j 0 / 2 ^ shift - l.getD 0 0 / 2 ^ shift + 1 := by
  intro j l hw hj
  show (((l.take (j + 1)).map (sf_sec shift)).eraseDups).length ≤
      sf_sec shift (l.getD j 0) - sf_sec shift (l.getD 0 0) + 1
  have hpwt : List.Pairwise (· ≤ ·) (l.take (j + 1)) := sf_pw_take (j + 1) l hw
  exact sf_bound _ (sf_sec shift (l.getD j 0)) (sf_sec shift (l.getD 0 0))
    (sf_pw_map shift (l.take (j + 1)) hpwt)
    (by
      intro x hx
      obtain ⟨y, hy, rfl⟩ := List.mem_map.mp hx
      obtain ⟨i, hi, hey⟩ := sf_mem_getD (l.take (j + 1)) y hy
      have hi' : i < l.length :=
        Nat.lt_of_lt_of_le hi (sf_len_take_le (j + 1) l)
      rw [← hey, sf_take_getD l j i hi]
      exact sf_sec_mono shift _ _ (sf_getD_le l hw 0 i (Nat.zero_le _) hi'))
    (by
      intro x hx
      obtain ⟨y, hy, rfl⟩ := List.mem_map.mp hx
      obtain ⟨i, hi, hey⟩ := sf_mem_getD (l.take (j + 1)) y hy
      have hij : i ≤ j := by
        have h1 : (l.take (j + 1)).length ≤ j + 1 := sf_len_take_min (j + 1) l
        omega
      rw [← hey, sf_take_getD l j i hi]
      exact sf_sec_mono shift _ _ (sf_getD_le l hw i j hij hj))

/-- Sector-floor bound: distinct sectors touched never exceed any cover's page count. -/
theorem sf_le (shift : Nat) : ∀ l, List.Pairwise (· ≤ ·) l →
    ∀ p ∈ covers l, sf_secs shift l ≤ (p.map (fun (a, b) => pages (2 ^ shift) a b)).sum := by
  intro l hw p hp
  have key : ∀ n l, l.length < n → List.Pairwise (· ≤ ·) l →
      ∀ p ∈ covers l, sf_secs shift l ≤ (p.map (fun (a, b) => pages (2 ^ shift) a b)).sum := by
    intro n
    induction n with
    | zero =>
        intro l hl _ _ _
        exact absurd hl (Nat.not_lt_zero _)
    | succ e ih =>
        intro l hl hw p hp
        cases l with
        | nil =>
            rw [covers_nil] at hp
            rcases List.mem_singleton.mp hp with rfl
            exact Nat.le_refl _
        | cons a as =>
            obtain ⟨d, q, hd, hq, hpq⟩ := (mem_covers_firstBlock (a := a) (as := as)).1 hp
            have hsum : (p.map (fun (a, b) => pages (2 ^ shift) a b)).sum =
                pages (2 ^ shift) a ((a :: as).getD d 0) +
                  (q.map (fun (a, b) => pages (2 ^ shift) a b)).sum := by
              rw [hpq, List.map_cons, List.sum_cons]
            have hsplit : (a :: as).take (d + 1) ++ as.drop d = a :: as := by
              rw [← @List.drop_succ_cons Nat a as d]
              exact List.take_append_drop (d + 1) (a :: as)
            have hpre : (((a :: as).take (d + 1)).map (sf_sec shift)).eraseDups.length ≤
                pages (2 ^ shift) a ((a :: as).getD d 0) := by
              have h1 := sf_span shift d (a :: as) hw
                (Nat.lt_of_succ_le (Nat.succ_le_succ hd))
              have hz : (a :: as).getD 0 0 = a := List.getD_cons_zero
              rw [hz] at h1
              unfold sf_secs at h1
              unfold pages
              exact h1
            have hlen : (as.drop d).length < e := by
              have h5 := sf_len_drop_le as d
              rw [List.length_cons] at hl
              omega
            have htail : ((as.drop d).map (sf_sec shift)).eraseDups.length ≤
                (q.map (fun (a, b) => pages (2 ^ shift) a b)).sum := by
              have h2 := ih (as.drop d) hlen
                (sf_pw_drop as (List.pairwise_cons.mp hw).2 d) q hq
              unfold sf_secs at h2
              exact h2
            unfold sf_secs
            rw [← hsplit, List.map_append, hsum]
            exact Nat.le_trans (sf_secs' _ _) (Nat.add_le_add hpre htail)
  exact key (l.length + 1) l (Nat.lt_succ_self _) hw p hp

/-- `DistinctSectorFloor` via the min-cost plan's cover. -/
theorem ds_floor (shift startup : Nat) (xs : List Nat) :
    (((points xs).map (· / 2 ^ shift)).eraseDups).length ≤
      cost (Sector.price shift startup) (choose (Sector.price shift startup) xs) := by
  have hmin : cost (Sector.price shift startup) (choose (Sector.price shift startup) xs) ∈
      ((covers (points xs)).map (cost (Sector.price shift startup))) :=
    ((List.min?_eq_some_iff).mp (choose_cost_min _ _)).1
  obtain ⟨p, hp, hpe⟩ := List.mem_map.mp hmin
  have h1 : sf_secs shift (points xs) ≤
      (p.map (fun (a, b) => pages (2 ^ shift) a b)).sum :=
    sf_le shift (points xs) (pairwise_points xs) p hp
  have h2 : (p.map (fun (a, b) => pages (2 ^ shift) a b)).sum ≤
      (p.map (fun (a, b) => Sector.price shift startup a b)).sum :=
    pg_sum_map_le (fun (a, b) => pages (2 ^ shift) a b)
      (fun (a, b) => Sector.price shift startup a b) p
      (fun ⟨a, b⟩ _ => Nat.le_add_left _ _)
  exact Nat.le_trans h1 (Nat.le_trans h2 (by rw [← hpe]; exact Nat.le_refl _))

theorem sound_DistinctSectorFloor : Sector.DistinctSectorFloor.Goal :=
  fun shift startup xs => ds_floor shift startup xs

private theorem covers_no_empty (xs : List Nat) (hxs : xs ≠ []) (ps : List Extent) (hps : ps ∈ covers xs) : ps ≠ [] := by
  induction xs generalizing ps with
  | nil => exact absurd rfl hxs
  | cons y ys ih =>
    simp only [covers, List.mem_flatMap] at hps
    obtain ⟨qs, hqs, hmem⟩ := hps
    cases qs with
    | nil =>
      simp at hmem
      subst hmem
      simp
    | cons hd tl =>
      simp at hmem
      rcases hmem with rfl | rfl <;> simp


private theorem sector_extent_ge (k fee a b : Nat) : fee + 1 ≤ Sector.price k fee a b := by
  simp only [Sector.price, pages]
  omega

private theorem cost_nonempty_ge (f : Price) (bound : Nat) (hbound : ∀ a b, bound ≤ f a b)
    (ps : List Extent) (hne : ps ≠ []) : bound ≤ cost f ps := by
  cases ps with
  | nil => exact absurd rfl hne
  | cons p ps =>
    simp only [cost, List.map, List.sum_cons]
    have := hbound p.1 p.2
    omega

theorem sound_OneSector : Sector.OneSector.Goal := by
  intro k fee xs hne hall
  let ks := points xs
  have hks_ne : ks ≠ [] := by
    intro h
    apply hne
    have hperm : (List.eraseDups xs).mergeSort.Perm (List.eraseDups xs) := List.mergeSort_perm _ _
    have hempty : List.eraseDups xs = [] := by
      have hlen := hperm.symm.length_eq
      have h' : (List.eraseDups xs).mergeSort = [] := by simpa [ks, points] using h
      rw [h'] at hlen
      exact List.eq_nil_of_length_eq_zero (by simpa using hlen)
    exact List.eq_nil_iff_forall_not_mem.mpr (fun a ha => by
      have := List.mem_eraseDups.mpr ha; simp [hempty] at this)
  -- upper bound
  have hfull_mem : [(ks.head?.getD 0, ks.getLast?.getD 0)] ∈ covers ks := mem_covers_span ks hks_ne
  have hle := choose_cost_le (Sector.price k fee) xs [(ks.head?.getD 0, ks.getLast?.getD 0)] hfull_mem
  have hcost_full : cost (Sector.price k fee) [(ks.head?.getD 0, ks.getLast?.getD 0)] = fee + 1 := by
    simp only [cost, List.map, List.sum_cons, List.sum_nil, Nat.add_zero, Sector.price]
    simp only [pages]
    have hmem_ks_xs : ∀ a, a ∈ ks → a ∈ xs := by
      intro a ha
      have : a ∈ (List.eraseDups xs).mergeSort := by simpa [points, ks] using ha
      exact List.mem_eraseDups.mp (List.mem_mergeSort.mp this)
    have hhead_xs : ks.head?.getD 0 ∈ xs := hmem_ks_xs _ (by
      cases hks : ks with
      | nil => exact absurd hks hks_ne
      | cons a as => simp [hks])
    have hlast_xs : ks.getLast?.getD 0 ∈ xs := hmem_ks_xs _ (by
      cases hks : ks with
      | nil => exact absurd hks hks_ne
      | cons a as =>
        cases as with
        | nil => simp [List.getLast?]
        | cons b bs => simp [List.getLast?, List.getLast?_cons])
    have hsec : (ks.getLast?.getD 0) / 2 ^ k = (ks.head?.getD 0) / 2 ^ k := by
      rw [hall (ks.getLast?.getD 0) hlast_xs, hall (ks.head?.getD 0) hhead_xs]
    simp [hsec]
  -- lower bound
  have hge : fee + 1 ≤ cost (Sector.price k fee) (choose (Sector.price k fee) xs) := by
    have hne_cov : covers ks ≠ [] := covers_ne_nil ks
    have hmem := choose_mem_covers (Sector.price k fee) xs
    have hne_ps : choose (Sector.price k fee) xs ≠ [] := covers_no_empty ks hks_ne _ hmem
    exact cost_nonempty_ge _ _ (fun a b => sector_extent_ge k fee a b) _ hne_ps
  have hle' : cost (Sector.price k fee) (choose (Sector.price k fee) xs) ≤ fee + 1 := by
    calc cost (Sector.price k fee) (choose (Sector.price k fee) xs)
        ≤ cost (Sector.price k fee) [(ks.head?.getD 0, ks.getLast?.getD 0)] := hle
      _ = fee + 1 := hcost_full
  omega

end TinylambOpt.ResearchProofs.StorageExecution.GatherPlans.Sector
