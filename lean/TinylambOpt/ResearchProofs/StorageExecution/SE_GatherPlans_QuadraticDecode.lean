import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved (10 claims, including the following)

- `QuadraticDecode.EqualLengthSplit` — `∀ fee a n, 0<n → (price fee a (a+2*n-1) ≤ price
  fee a (a+n-1)+price fee (a+n) (a+2*n-1) ↔ 2*n^2≤fee)`
- `QuadraticDecode.GatherOrder` — `∀ (fixedFee : Nat), gatherOrder (price fixedFee)`
- `QuadraticDecode.LatencyCap` — `∀ (fixedFee : Nat), latencyCap (price fixedFee)`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.GatherPlans.QuadraticDecode
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

theorem sound_LeastCost : QuadraticDecode.LeastCost.Goal := fun fixedFee => genericLeastCost (QuadraticDecode.price fixedFee)
theorem sound_GatherOrder : QuadraticDecode.GatherOrder.Goal := fun fixedFee => genericGatherOrder (QuadraticDecode.price fixedFee)
theorem sound_SparseOrDense : QuadraticDecode.SparseOrDense.Goal := fun fixedFee => genericSparseOrDense (QuadraticDecode.price fixedFee)
theorem sound_SeekTariff : QuadraticDecode.SeekTariff.Goal := fun fixedFee => genericSeekTariff (QuadraticDecode.price fixedFee)
theorem sound_ReadAmplification : QuadraticDecode.ReadAmplification.Goal := fun fixedFee => genericReadAmplification (QuadraticDecode.price fixedFee)
theorem sound_ShardBoundary : QuadraticDecode.ShardBoundary.Goal := fun fixedFee => genericShardBoundary (QuadraticDecode.price fixedFee)
theorem sound_LatencyCap : QuadraticDecode.LatencyCap.Goal := fun fixedFee => genericLatencyCap (QuadraticDecode.price fixedFee)
theorem sound_TwoRequests : QuadraticDecode.TwoRequests.Goal := fun fixedFee => genericTwoRequests (QuadraticDecode.price fixedFee)

theorem sound_EqualLengthSplit : QuadraticDecode.EqualLengthSplit.Goal := by
  intro fee a n _hn
  simp only [QuadraticDecode.price, span]
  have hs1 : a + 2 * n - 1 + 1 - a = 2 * n := by omega
  have hs2 : a + n - 1 + 1 - a = n := by omega
  have hs3 : a + 2 * n - 1 + 1 - (a + n) = n := by omega
  rw [hs1, hs2, hs3]
  have h : 2 * n * (2 * n) = n * n + n * n + (n * n + n * n) := by
    rw [show 2 * n = n + n from by omega, Nat.add_mul, Nat.mul_add]
  simp only [Nat.pow_two]
  rw [h]
  constructor <;> intro h <;> omega
theorem sound_MergeNeighbors : QuadraticDecode.MergeNeighbors.Goal := by
  intro f a m b ham hmb
  simp only [QuadraticDecode.price, span]
  have hsab : b + 1 - a = (m + 1 - a) + (b - m) := by omega
  have hsmb : b + 1 - (m + 1) = b - m := by omega
  rw [hsab, hsmb]
  let x := m + 1 - a
  let y := b - m
  show f + (x + y) ^ 2 ≤ f + x ^ 2 + (f + y ^ 2) ↔ 2 * x * y ≤ f
  have hsq : (x + y) ^ 2 = x ^ 2 + y ^ 2 + 2 * (x * y) := by
    simp only [Nat.pow_two, Nat.add_mul, Nat.mul_add]
    rw [Nat.mul_comm y x]
    omega
  simp only [hsq, Nat.pow_two, Nat.mul_assoc]
  constructor <;> intro h <;> omega

end TinylambOpt.ResearchProofs.StorageExecution.GatherPlans.QuadraticDecode
