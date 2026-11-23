import TinylambOpt.ResearchGoals.CongruenceCount

/-!
# Soundness of modular-congruence (skip / all / inspect) page classification

Each page carries the summary `⟨first value a, gcd of all (x - a) + (a - x)⟩`
over its rows.  For a query predicate `x % m = r % m` (`m > 0`), `classify`
derives one of three modes from `g := gcd m stride`:

* `.skip` — `a % g ≠ r % g`; since `g` divides `m` and every truncated
  difference `(x - a) + (a - x)`, every row satisfies `x % g = a % g ≠ r % g`,
  hence `x % m ≠ r % m`: nothing matches.
* `.all` — `stride % m = 0`; then `m = gcd m stride`, so `a % m = r % m` and
  `m` divides every `(x - a) + (a - x)`: all rows match and the size is added
  directly.
* `.inspect` — `a % g = r % g` but `stride % m ≠ 0`.  The arithmetic progression
  `anchor + k * stride` then contains values passing the test (Bézout: the
  linear congruence `a + k * stride ≡ r [MOD m]` is solvable because
  `g ∣ a - r` up to sign) and values failing it (`k = 0` or `k = 1` suffices).

The proof proceeds per page through these three modes, feeding an `execute`
fold characterization (`foldl_count`, `foldl_opened`) proven here.  Bézout's
identity (`gcd_bezout`) is proved from scratch via `Nat.gcd.induction`.
-/

namespace TinylambOpt.ResearchProofs.CongruenceCount

open TinylambOpt.ResearchGoals.BlockCountModel
open TinylambOpt.ResearchGoals.CongruenceCount
open List

/-! ## Small generic helpers -/

private theorem map_congr' {α β : Type} (f g : α → β) :
    ∀ (l : List α), (∀ x ∈ l, f x = g x) → l.map f = l.map g := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a l ih =>
      intro h
      rw [List.map_cons, h a List.mem_cons_self,
        ih (fun x hx => h x (List.mem_cons_of_mem _ hx)), List.map_cons]

private theorem filter_flatten {α : Type} (p : α → Bool) :
    ∀ (L : List (List α)), (L.flatten).filter p = (L.map (fun l => l.filter p)).flatten := by
  intro L
  induction L with
  | nil => rfl
  | cons l L ih =>
      rw [List.flatten_cons, List.filter_append, List.map_cons, ih, List.flatten_cons]

private theorem foldl_add_eq_add_sum (l : List Nat) :
    ∀ acc : Nat, l.foldl (fun a x => a + x) acc = acc + l.sum := by
  induction l with
  | nil => intro acc; rfl
  | cons a l ih =>
      intro acc
      rw [List.foldl_cons, ih (acc + a), List.sum_cons, Nat.add_assoc]

private theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) :
    ∀ (xs : List α), (∀ x ∈ xs, p x = false) → List.filter p xs = [] := by
  intro xs
  induction xs with
  | nil => intro _; rfl
  | cons a as ih =>
      intro h
      rw [List.filter_cons_of_neg (by simpa using h a List.mem_cons_self),
        ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

private theorem filter_eq_self_of_all_true {α : Type} (p : α → Bool) :
    ∀ (xs : List α), (∀ x ∈ xs, p x = true) → List.filter p xs = xs := by
  intro xs
  induction xs with
  | nil => intro _; rfl
  | cons a as ih =>
      intro h
      rw [List.filter_cons_of_pos (by simpa using h a List.mem_cons_self),
        ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

/-! ## Truncated absolute difference and congruence arithmetic -/

/-- The symmetric truncated difference `(x - a) + (a - x)`, i.e. `|x - a|`
    living in `Nat`; it is exactly the body of the `summary` fold. -/
private def adiff (a x : Nat) : Nat := (x - a) + (a - x)

private theorem adiff_comm (a x : Nat) : adiff a x = adiff x a := by
  unfold adiff
  rw [Nat.add_comm]

private theorem mod_eq_of_dvd_adiff (a x d : Nat) (h : d ∣ adiff a x) : x % d = a % d := by
  unfold adiff at h
  cases Nat.lt_or_ge x a with
  | inl hlt =>
      rw [Nat.sub_eq_zero_of_le (Nat.le_of_lt hlt), Nat.zero_add] at h
      obtain ⟨t, ht⟩ := h
      rw [← Nat.add_sub_of_le (Nat.le_of_lt hlt), ht, Nat.add_mul_mod_self_left]
  | inr hge =>
      rw [Nat.sub_eq_zero_of_le hge, Nat.add_zero] at h
      obtain ⟨t, ht⟩ := h
      rw [← Nat.add_sub_of_le hge, ht, Nat.add_mul_mod_self_left]

/-- The final accumulator of a gcd fold divides both the starting value and
    every folded term. -/
private theorem fold_dvd (a : Nat) :
    ∀ (xs : List Nat) (acc : Nat),
      (List.foldl (fun d x => Nat.gcd d (adiff a x)) acc xs ∣ acc) ∧
      (∀ x ∈ xs, List.foldl (fun d x => Nat.gcd d (adiff a x)) acc xs ∣ adiff a x) := by
  intro xs
  induction xs with
  | nil => intro acc; exact ⟨Nat.dvd_refl acc, fun _ hx => absurd hx List.not_mem_nil⟩
  | cons b bs ih =>
      intro acc
      obtain ⟨hF1, hF2⟩ := ih (Nat.gcd acc (adiff a b))
      have hstep : List.foldl (fun d x => Nat.gcd d (adiff a x)) acc (b :: bs)
          = List.foldl (fun d x => Nat.gcd d (adiff a x)) (Nat.gcd acc (adiff a b)) bs := by
        rw [List.foldl_cons]
      rw [hstep]
      refine ⟨Nat.dvd_trans hF1 (Nat.gcd_dvd_left acc (adiff a b)), ?_⟩
      intro x hx
      cases List.mem_cons.mp hx with
      | inl he =>
          rw [he]
          exact Nat.dvd_trans hF1 (Nat.gcd_dvd_right acc (adiff a b))
      | inr hrest => exact hF2 x hrest

private theorem summary_spec (xs : List Nat) :
    (summary xs).anchor = xs.headD 0 ∧ (summary xs).stride =
      List.foldl (fun d x => Nat.gcd d (adiff (xs.headD 0) x)) 0 xs := ⟨rfl, rfl⟩

/-- `gcd m stride` divides every `adiff anchor x`, so all rows of a page are
    congruent to the anchor modulo `gcd m stride`. -/
private theorem adiff_dvd_stride (b : Block Nat Meta) (hb : b.header = summary b.data) :
    ∀ x ∈ b.data, b.header.stride ∣ adiff b.header.anchor x := by
  rw [hb]
  have h1 : (summary b.data).anchor = b.data.headD 0 := (summary_spec b.data).1
  have h2 : (summary b.data).stride =
      List.foldl (fun d x => Nat.gcd d (adiff (b.data.headD 0) x)) 0 b.data := (summary_spec b.data).2
  rw [h1, h2]
  intro x hx
  exact (fold_dvd (b.data.headD 0) b.data 0).2 x hx

/-! ## Bézout and the linear-congruence witnesses -/

private theorem gcd_bezout (m n : Nat) :
    ∃ u v : Int, (Nat.gcd m n : Int) = (m : Int) * u + (n : Int) * v := by
  induction m, n using Nat.gcd.induction with
  | H0 n => exact ⟨0, 1, by rw [Nat.gcd_zero_left]; simp⟩
  | H1 m n _ IH =>
      obtain ⟨u, v, huv⟩ := IH
      refine ⟨v - ((n / m : Nat) : Int) * u, u, ?_⟩
      rw [Nat.gcd_rec, huv]
      have hd : (n : Int) = (m : Int) * ((n / m : Nat) : Int) + ((n % m : Nat) : Int) := by
        exact_mod_cast (Nat.div_add_mod n m).symm
      rw [hd, Int.mul_sub, Int.add_mul]
      rw [Int.mul_assoc, Int.sub_eq_add_neg, Int.add_assoc, Int.neg_add_cancel_left,
        Int.add_comm]

private theorem int_dvd_sub_of_mod_eq (a r d : Nat) (h : a % d = r % d) :
    (d : Int) ∣ (a : Int) - (r : Int) := by
  refine ⟨((a / d : Nat) : Int) - ((r / d : Nat) : Int), ?_⟩
  have h1 : (a : Int) = (d : Int) * ((a / d : Nat) : Int) + ((a % d : Nat) : Int) := by
    exact_mod_cast (Nat.div_add_mod a d).symm
  have h2 : (r : Int) = (d : Int) * ((r / d : Nat) : Int) + ((r % d : Nat) : Int) := by
    exact_mod_cast (Nat.div_add_mod r d).symm
  rw [h1, h2, h, Int.add_sub_add_right, ← Int.mul_sub]

private theorem natAbs_sub_nat (x y : Nat) : ((x : Int) - (y : Int)).natAbs = adiff x y := by
  unfold adiff
  omega

private theorem coe_dvd_sub_to_adiff (x y d : Nat) (h : (d : Int) ∣ (x : Int) - (y : Int)) :
    d ∣ adiff x y := by
  have h2 : (d : Int).natAbs ∣ ((x : Int) - (y : Int)).natAbs := Int.natAbs_dvd_natAbs.mpr h
  rw [natAbs_sub_nat] at h2
  simpa using h2

private theorem exists_step_hit (m r s a : Nat) (hm : 0 < m)
    (h : a % Nat.gcd m s = r % Nat.gcd m s) :
    ∃ k : Nat, (a + k * s) % m = r % m := by
  obtain ⟨u, v, huv⟩ := gcd_bezout m s
  obtain ⟨z, hz⟩ := int_dvd_sub_of_mod_eq a r (Nat.gcd m s) h
  rw [huv] at hz
  have h1 : (a : Int) + (-(v * z)) * (s : Int) - (r : Int) = (m : Int) * (u * z) := by
    have c1 : (a : Int) + (-(v * z)) * (s : Int) - (r : Int)
        = ((a : Int) - (r : Int)) + (-(v * z)) * (s : Int) := by
      simp only [Int.sub_eq_add_neg, Int.add_comm, Int.add_assoc, Int.add_left_comm]
    rw [c1, hz, Int.add_mul]
    have c2 : ((s : Int) * v) * z + (-(v * z)) * (s : Int) = 0 := by
      rw [Int.mul_assoc, Int.neg_mul, Int.mul_comm (v * z) (s : Int), Int.add_right_neg]
    have c3 : ((m : Int) * u) * z = (m : Int) * (u * z) := Int.mul_assoc (m) u z
    rw [c3, Int.add_assoc, c2, Int.add_zero]
  have hK : (0 : Int) ≤ -(v * z) + (m : Int) * (-(v * z)).natAbs := by
    have h2 : ((-(v * z)).natAbs : Int) ≤ (m : Int) * (-(v * z)).natAbs := by
      exact_mod_cast Nat.le_mul_of_pos_left (-(v * z)).natAbs hm
    have hb : -((-(v * z)).natAbs : Int) ≤ -(v * z) := by omega
    omega
  have h2 : (a : Int)
      + ((-(v * z) + (m : Int) * (-(v * z)).natAbs) * (s : Int)) - (r : Int)
      = (m : Int) * (u * z + (s : Int) * (-(v * z)).natAbs) := by
    have c1 : (a : Int)
        + ((-(v * z) + (m : Int) * (-(v * z)).natAbs) * (s : Int)) - (r : Int)
        = ((a : Int) + (-(v * z)) * (s : Int) - (r : Int))
          + ((m : Int) * (-(v * z)).natAbs) * (s : Int) := by
      rw [Int.add_mul]
      simp only [Int.sub_eq_add_neg, Int.add_comm, Int.add_assoc, Int.add_left_comm]
    rw [c1, h1, Int.mul_add]
    have c2 : ((m : Int) * (-(v * z)).natAbs) * (s : Int)
        = (m : Int) * ((s : Int) * (-(v * z)).natAbs) := by
      rw [Int.mul_assoc, Int.mul_comm ((-(v * z)).natAbs) (s : Int)]
    rw [c2]
  refine ⟨(-(v * z) + (m : Int) * (-(v * z)).natAbs).toNat, ?_⟩
  have h3 : ((a + (-(v * z) + (m : Int) * (-(v * z)).natAbs).toNat * s : Nat) : Int) - (r : Int)
      = (m : Int) * (u * z + (s : Int) * (-(v * z)).natAbs) := by
    rw [show ((a + (-(v * z) + (m : Int) * (-(v * z)).natAbs).toNat * s : Nat) : Int)
        = (a : Int) + ((-(v * z) + (m : Int) * (-(v * z)).natAbs).toNat : Int) * (s : Int)
      from by exact_mod_cast rfl, Int.toNat_of_nonneg hK, h2]
  have hnat : m ∣ adiff (a + (-(v * z) + (m : Int) * (-(v * z)).natAbs).toNat * s) r :=
    coe_dvd_sub_to_adiff _ _ m ⟨_, h3⟩
  rw [adiff_comm] at hnat
  exact mod_eq_of_dvd_adiff r _ m hnat

private theorem exists_step_miss (m r s a : Nat) (hm : 0 < m) (hs : s % m ≠ 0) :
    ∃ k : Nat, (a + k * s) % m ≠ r % m := by
  by_cases h : a % m = r % m
  · refine ⟨1, ?_⟩
    intro hh
    rw [Nat.one_mul] at hh
    have h1 := Nat.add_mod a s m
    rw [h1, h] at hh
    -- hh : (r % m + s % m) % m = r % m
    have h2 := Nat.div_add_mod (r % m + s % m) m
    rw [hh] at h2
    -- h2 : m * ((r % m + s % m) / m) + r % m = r % m + s % m
    have h3 := Nat.add_left_cancel
      ((Nat.add_comm (m * ((r % m + s % m) / m)) (r % m)).symm.trans h2)
    -- h3 : m * ((r % m + s % m) / m) = s % m
    by_cases hq : ((r % m + s % m) / m) = 0
    · rw [hq, Nat.mul_zero] at h3
      exact hs h3.symm
    · have hge := Nat.le_trans (Nat.le_mul_of_pos_right m (Nat.pos_of_ne_zero hq))
        (Nat.le_of_eq h3)
      exact Nat.not_lt_of_le hge (Nat.mod_lt s hm)
  · refine ⟨0, ?_⟩
    intro hh
    rw [Nat.zero_mul, Nat.add_zero] at hh
    exact h hh

private theorem test_true (m r x : Nat) : test m r x = true ↔ x % m = r % m := by
  unfold test
  exact ⟨Nat.eq_of_beq_eq_true, fun h => by rw [h]; exact Nat.beq_refl _⟩

private theorem test_false (m r x : Nat) : test m r x = false ↔ x % m ≠ r % m := by
  unfold test
  by_cases h : x % m = r % m <;> simp [h]

private theorem classify_eq_skip (m r : Nat) (s : Meta) :
    classify m r s = .skip ↔ ¬(s.anchor % Nat.gcd m s.stride = r % Nat.gcd m s.stride) := by
  unfold classify
  by_cases hc : s.anchor % Nat.gcd m s.stride = r % Nat.gcd m s.stride
  · simp [hc]
    by_cases hd : s.stride % m = 0 <;> simp [hd]
  · simp [hc]

private theorem classify_eq_all (m r : Nat) (s : Meta) :
    classify m r s = .all ↔
      (s.anchor % Nat.gcd m s.stride = r % Nat.gcd m s.stride) ∧ (s.stride % m = 0) := by
  unfold classify
  by_cases hc : s.anchor % Nat.gcd m s.stride = r % Nat.gcd m s.stride <;>
    by_cases hd : s.stride % m = 0 <;>
      simp [hc, hd]

private theorem classify_eq_inspect (m r : Nat) (s : Meta) :
    classify m r s = .inspect ↔
      (s.anchor % Nat.gcd m s.stride = r % Nat.gcd m s.stride) ∧ ¬(s.stride % m = 0) := by
  unfold classify
  by_cases hc : s.anchor % Nat.gcd m s.stride = r % Nat.gcd m s.stride <;>
    by_cases hd : s.stride % m = 0 <;>
      simp [hc, hd]

/-! ## `execute` fold characterization -/

private def step (cl : Meta → Mode) (ptest : Nat → Bool) (s : Result)
    (b : Block Nat Meta) (i : Nat) : Result :=
  match cl b.header with
  | .skip => s
  | .all => {s with count := s.count + b.size}
  | .inspect => ⟨s.count + (b.data.filter ptest).length, i :: s.opened⟩

private def incr (cl : Meta → Mode) (ptest : Nat → Bool) (b : Block Nat Meta) : Nat :=
  match cl b.header with
  | .skip => 0
  | .all => b.size
  | .inspect => (b.data.filter ptest).length

private def insIdx (cl : Meta → Mode) :
    (pages : List (Block Nat Meta)) → Nat → List Nat
  | [], _ => []
  | b :: bs, n => if cl b.header = .inspect then n :: insIdx cl bs (n + 1) else insIdx cl bs (n + 1)

private theorem execute_eq_fold (cl : Meta → Mode) (ptest : Nat → Bool)
    (pages : List (Block Nat Meta)) :
    execute cl ptest pages =
      (List.zipIdx pages 0).foldl (fun s p => step cl ptest s p.1 p.2) ({} : Result) := rfl

private theorem insIdx_cons_true (cl : Meta → Mode) (b : Block Nat Meta)
    (bs : List (Block Nat Meta)) (n : Nat) (h : cl b.header = .inspect) :
    insIdx cl (b :: bs) n = n :: insIdx cl bs (n + 1) := by
  simp only [insIdx, ite_eq_left h]

private theorem insIdx_cons_false (cl : Meta → Mode) (b : Block Nat Meta)
    (bs : List (Block Nat Meta)) (n : Nat) (h : ¬(cl b.header = .inspect)) :
    insIdx cl (b :: bs) n = insIdx cl bs (n + 1) := by
  simp only [insIdx, ite_eq_right h]

private theorem mem_insIdx (cl : Meta → Mode) :
    ∀ (pages : List (Block Nat Meta)) (n i : Nat),
      i ∈ insIdx cl pages n ↔ ∃ p ∈ List.zipIdx pages n, p.2 = i ∧ cl p.1.header = .inspect := by
  intro pages
  induction pages with
  | nil => intro n i; simp [insIdx]
  | cons b bs ih =>
      intro n i
      rw [List.zipIdx_cons]
      simp only [List.mem_cons]
      by_cases hb : cl b.header = .inspect
      · rw [insIdx_cons_true cl b bs n hb]
        simp only [List.mem_cons, ih (n + 1) i]
        constructor
        · rintro (heq | ⟨p, hp, hpi, hclp⟩)
          · exact ⟨(b, n), Or.inl rfl, heq.symm, hb⟩
          · exact ⟨p, Or.inr hp, hpi, hclp⟩
        · rintro ⟨p, hp, hpi, hclp⟩
          rcases hp with heq | hrest
          · exact Or.inl (hpi ▸ congrArg Prod.snd heq)
          · exact Or.inr ⟨p, hrest, hpi, hclp⟩
      · rw [insIdx_cons_false cl b bs n hb, ih (n + 1) i]
        constructor
        · rintro ⟨p, hp, hpi, hclp⟩
          exact ⟨p, Or.inr hp, hpi, hclp⟩
        · rintro ⟨p, hp, hpi, hclp⟩
          rcases hp with heq | hrest
          · rw [heq] at hclp
            exact absurd hclp hb
          · exact ⟨p, hrest, hpi, hclp⟩

private theorem foldl_count (cl : Meta → Mode) (ptest : Nat → Bool) :
    ∀ (pages : List (Block Nat Meta)) (n : Nat) (s : Result),
      ((List.zipIdx pages n).foldl (fun s p => step cl ptest s p.1 p.2) s).count =
        (pages.map (incr cl ptest)).foldl (fun a x => a + x) s.count := by
  intro pages
  induction pages with
  | nil => intro n s; rfl
  | cons b bs ih =>
      intro n s
      rw [List.zipIdx_cons, List.foldl_cons, ih (n + 1) (step cl ptest s b n),
        List.map_cons, List.foldl_cons]
      match h : cl b.header with
      | .skip =>
          rw [show (step cl ptest s b n).count = s.count from by unfold step; rw [h],
            show incr cl ptest b = (0 : Nat) from by unfold incr; rw [h],
            Nat.add_zero]
      | .all =>
          rw [show (step cl ptest s b n).count = s.count + b.size from by unfold step; rw [h],
            show incr cl ptest b = b.size from by unfold incr; rw [h]]
      | .inspect =>
          rw [show (step cl ptest s b n).count = s.count + (b.data.filter ptest).length
            from by unfold step; rw [h],
            show incr cl ptest b = (b.data.filter ptest).length from by unfold incr; rw [h]]

private theorem foldl_opened (cl : Meta → Mode) (ptest : Nat → Bool) :
    ∀ (pages : List (Block Nat Meta)) (n : Nat) (s : Result),
      ((List.zipIdx pages n).foldl (fun s p => step cl ptest s p.1 p.2) s).opened =
        (insIdx cl pages n).reverse ++ s.opened := by
  intro pages
  induction pages with
  | nil => intro n s; rfl
  | cons b bs ih =>
      intro n s
      rw [List.zipIdx_cons, List.foldl_cons, ih (n + 1) (step cl ptest s b n)]
      match h : cl b.header with
      | .skip =>
          have hn : ¬(cl b.header = .inspect) := by rw [h]; decide
          rw [insIdx_cons_false cl b bs n hn,
            show step cl ptest s b n = s from by unfold step; rw [h]]
      | .all =>
          have hn : ¬(cl b.header = .inspect) := by rw [h]; decide
          rw [insIdx_cons_false cl b bs n hn,
            show (step cl ptest s b n).opened = s.opened from by unfold step; rw [h]]
      | .inspect =>
          rw [insIdx_cons_true cl b bs n h, List.reverse_cons, List.append_assoc,
            show (step cl ptest s b n).opened = n :: s.opened from by unfold step; rw [h],
            show ([n] ++ s.opened) = (n :: s.opened) from rfl]

private theorem reads_execute (cl : Meta → Mode) (ptest : Nat → Bool)
    (pages : List (Block Nat Meta)) :
    reads (execute cl ptest pages) = insIdx cl pages 0 := by
  unfold reads
  rw [execute_eq_fold, foldl_opened cl ptest pages 0 ({})]
  rw [show ({} : Result).opened = ([] : List Nat) from rfl, List.append_nil,
    List.reverse_reverse]

/-! ## Page mode correctness -/

private theorem mode_skip (m r : Nat) (b : Block Nat Meta) (hb : b.header = summary b.data)
    (h : classify m r b.header = .skip) :
    List.filter (test m r) b.data = [] := by
  have hc := (classify_eq_skip m r b.header).mp h
  refine filter_eq_nil_of_all_false (test m r) b.data ?_
  intro x hx
  rw [test_false]
  intro hpass
  have hg1 : Nat.gcd m b.header.stride ∣ m := Nat.gcd_dvd_left m b.header.stride
  have h1 : x % Nat.gcd m b.header.stride = (x % m) % Nat.gcd m b.header.stride :=
    (Nat.mod_mod_of_dvd x hg1).symm
  rw [hpass, Nat.mod_mod_of_dvd r hg1] at h1
  have h2 : x % Nat.gcd m b.header.stride = b.header.anchor % Nat.gcd m b.header.stride :=
    mod_eq_of_dvd_adiff b.header.anchor x (Nat.gcd m b.header.stride)
      (Nat.dvd_trans (Nat.gcd_dvd_right m b.header.stride) (adiff_dvd_stride b hb x hx))
  exact hc (h2.symm.trans h1)

private theorem mode_all (m r : Nat) (b : Block Nat Meta) (hb : b.header = summary b.data)
    (h : classify m r b.header = .all) :
    List.filter (test m r) b.data = b.data := by
  obtain ⟨hc, hd⟩ := (classify_eq_all m r b.header).mp h
  have hmdvd : m ∣ b.header.stride := Nat.dvd_of_mod_eq_zero hd
  have hg : Nat.gcd m b.header.stride = m := Nat.gcd_eq_left hmdvd
  rw [hg] at hc
  refine filter_eq_self_of_all_true (test m r) b.data ?_
  intro x hx
  rw [test_true]
  have h2 : x % m = b.header.anchor % m :=
    mod_eq_of_dvd_adiff b.header.anchor x m (Nat.dvd_trans hmdvd (adiff_dvd_stride b hb x hx))
  exact h2.trans hc

private theorem mode_inspect (m r : Nat) (s : Meta) (hm : 0 < m)
    (h : classify m r s = .inspect) :
    (∃ k, test m r (s.anchor + k * s.stride) = true) ∧
    (∃ k, test m r (s.anchor + k * s.stride) = false) := by
  obtain ⟨hc, hs⟩ := (classify_eq_inspect m r s).mp h
  refine ⟨?_, ?_⟩
  · obtain ⟨k, hk⟩ := exists_step_hit m r s.stride s.anchor hm hc
    exact ⟨k, (test_true m r _).mpr hk⟩
  · obtain ⟨k, hk⟩ := exists_step_miss m r s.stride s.anchor hm hs
    exact ⟨k, (test_false m r _).mpr hk⟩

private theorem incr_eq (m r : Nat) (b : Block Nat Meta)
    (hb : b.header = summary b.data) (hsz : b.size = b.data.length) :
    incr (classify m r) (test m r) b = (b.data.filter (test m r)).length := by
  match h : classify m r b.header with
  | .skip =>
      simp only [incr, h, mode_skip m r b hb h, List.length_nil]
  | .all =>
      rw [show incr (classify m r) (test m r) b = b.size from by unfold incr; rw [h],
        mode_all m r b hb h]
      exact hsz
  | .inspect =>
      unfold incr; rw [h]

/-! ## The soundness theorem -/

theorem sound : TinylambOpt.ResearchGoals.CongruenceCount.Goal := by
  intro m r pages hm h
  show (execute (classify m r) (test m r) pages).count =
        ((pages.flatMap Block.data).filter (test m r)).length ∧
      ∀ b ∈ List.zipIdx pages 0,
        b.2 ∈ reads (execute (classify m r) (test m r) pages) →
          (∃ k, test m r (b.1.header.anchor + k * b.1.header.stride) = true) ∧
          (∃ k, test m r (b.1.header.anchor + k * b.1.header.stride) = false)
  refine ⟨?_, ?_⟩
  · have hcl : ∀ b ∈ pages,
        incr (classify m r) (test m r) b = (b.data.filter (test m r)).length :=
      fun b hb => incr_eq m r b (h b hb).1 (h b hb).2
    have hlen : ((pages.flatMap Block.data).filter (test m r)).length =
        (pages.map (fun b => (b.data.filter (test m r)).length)).sum := by
      rw [List.flatMap_def, filter_flatten, List.length_flatten, List.map_map, List.map_map]
      rfl
    rw [execute_eq_fold, foldl_count, map_congr' _ _ pages hcl, foldl_add_eq_add_sum,
      show ({} : Result).count = (0 : Nat) from rfl, Nat.zero_add, ← hlen]
  · intro b hbz hmem
    rw [reads_execute (classify m r) (test m r) pages] at hmem
    obtain ⟨p, hpz, hpi, hclp⟩ := (mem_insIdx (classify m r) pages 0 b.2).mp hmem
    have hb1 : b.1 = p.1 := by
      have hz1 := (List.mem_zipIdx_iff_getElem? (l := pages)).mp hbz
      have hz2 := (List.mem_zipIdx_iff_getElem? (l := pages)).mp hpz
      rw [hpi] at hz2
      exact Option.some.inj (hz1.symm.trans hz2)
    rw [← hb1] at hclp
    exact mode_inspect m r b.1.header hm hclp

end TinylambOpt.ResearchProofs.CongruenceCount
