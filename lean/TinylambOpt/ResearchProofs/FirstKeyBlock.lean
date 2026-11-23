import TinylambOpt.ResearchGoals.FirstKeyBlock

/-!
# Reference key block filtering: a pilot verification

Circuit-breaker demo for the research-proof pipeline. The Lean checker itself
is the oracle: we state the semantic spec of the block-filter optimization
directly against a real in-tree definition — `ResearchGoals.FirstKeyBlock`, a
self-contained model of the optimizer's per-block "skip fully-covered blocks"
decision — and prove, **fully checked by `lean` with no placeholders**, that the
optimized scan (which skips whole blocks whose header bitset covers every row
key, and drops duplicate keys inside surviving blocks) returns exactly the
deduplicated reference result, in the same order, and that every retained row
is a first occurrence.

The statement `Goal` is machine-generated and lives under `ResearchGoals/`;
this file contains only the proof. Run `lake build` (no extra flags, no
`maxHeartbeats`) to re-check it.

## The specification

Modeled on the first-key extraction of `plan/detail/optimize_block_filter.cpp`:
a `Block` carries a header bitset over 64-bit key-space (bit 63 is the clamp
for keys ≥ 64) and a list of rows. The scan keeps a `seen` summary (the OR of
every flag it has consumed), skips a whole block when `header` covers `seen`,
and inside an open block drops any row whose flag is already in `seen`. The
goal: the kept rows, reversed, equal the reference `reference` — for each row,
keep it iff no earlier row (over the whole flattened list) has the same key —
and membership in the output is exactly freshness.

## Proof outline

1. The skip test `header &&& ~~~summary pre = 0` holds exactly when every row
   flag of the block hits `summary pre` (`block_closed_iff`), via bit-index
   reasoning: keys live in `Fin 63` (or `none`), so the clamped flag index
   `key.elim 63 Fin.val` determines the key (`key_elim_inj`) and equals bit
   tests of `flag`/`summary` (`flag_getLsbD`, `summary_getLsbD`).
2. `reference (ys ++ [r])` is `reference ys ++ [r]` when `r`'s key is new and
   `reference ys` otherwise (`reference_append_cons`, via
   `zipIdx_snoc` + `filterMap_append`).
3. Row level (`row_inv`): after folding `stepRow` (the scan's inner step)
   over rows `xs` starting from a state matching prefix `pre`, `seen` is
   `summary (pre ++ xs)` and `kept.reverse` is `reference (pre ++ xs)`, by
   induction on `xs` generalizing `pre` and the state, using
   `reference_append_cons` and the bit/key bridge at each step.
4. Block level (`main_inv`): after folding the scan's zipIdx step over the
   suffix `bs` starting at offset `done.length`, the state satisfies the
   analogous two invariants for `done ++ bs`, and its `opened` list is
   exactly `{j | FreshAt done j}` (`FreshAt` = goal's freshness predicate),
   by induction on `bs`. Skip case: all rows of the block are already seen,
   so `summary`/`reference` are unchanged and nothing is opened
   (`block_closed_iff` + `summary_append_eq_of_and_not`-style reasoning);
   open case: `row_inv` applied to the block's rows does the work, and
   `FreshAt` at the block's own index characterizes opened-ness.
5. `scan_eq` rewrites `scan` into the zipIdx fold shape; `sound` instantiates
   `main_inv` at `done = []`, `n = 0`, `s = {}` and assembles both conjuncts
   (the second via `zipIdx_mem`).
-/
namespace TinylambOpt.ResearchProofs.FirstKeyBlock

open TinylambOpt.ResearchGoals.FirstKeyBlock

/-! ### Boolean / key-index helpers -/

private theorem bne_not_eq {x y : W} : (x != y) = !((x == y)) := rfl

private theorem wbne_true {x y : W} (h : x = y) : (x != y) = false := by
  subst h
  have hb : (x == x) = true := Nat.beq_refl _
  rw [bne_not_eq, hb]
  exact Bool.not_true

private theorem bne_false_beq {x y : W} (h : (x != y) = false) : (x == y) = true := by
  rw [bne_not_eq] at h
  rcases Bool.eq_false_or_eq_true (x == y) with hxy | hxy
  · exact hxy
  · rw [hxy] at h
    exact absurd h (by decide)

private theorem wbne_false {x y : W} (h : (x != y) = false) : x = y := by
  have hb := bne_false_beq h
  cases x with
  | ofFin a => cases y with
    | ofFin b => cases a with
      | mk av alt => cases b with
        | mk bv blt =>
            refine congrArg BitVec.ofFin (Fin.eq_of_val_eq ?_)
            exact Nat.eq_of_beq_eq_true hb

private theorem bfalse_not_true (b : Bool) (hb : b = false) : ¬(b = true) := by
  cases b with
  | true => exact absurd hb (by decide)
  | false => intro hh; exact absurd hh (by decide)

private theorem nat_beq_false {x y : Nat} (h : ¬(x = y)) : (x == y) = false := by
  rcases Bool.eq_false_or_eq_true (x == y) with hb | hb
  · exact absurd (Nat.eq_of_beq_eq_true hb) h
  · exact hb

private theorem nat_eq_or_ne (x y : Nat) : (x = y) ∨ (¬(x = y)) := by
  rcases Nat.lt_trichotomy x y with h | h | h
  · exact Or.inr (Nat.ne_of_lt h)
  · exact Or.inl h
  · exact Or.inr (Nat.ne_of_gt h)

private theorem key_elim_inj {k₁ k₂ : Option (Fin 63)} :
    k₁.elim 63 Fin.val = k₂.elim 63 Fin.val → k₁ = k₂ := by
  intro h
  cases k₁ with
  | none =>
      cases k₂ with
      | none => rfl
      | some m => exact absurd h (Nat.ne_of_gt m.isLt)
  | some a =>
      cases k₂ with
      | none => exact absurd h (Nat.ne_of_lt a.isLt)
      | some b => exact congrArg some (Fin.val_inj.mp h)

private theorem key_elim_beq {k₁ k₂ : Option (Fin 63)} :
    (k₁.elim 63 Fin.val == k₂.elim 63 Fin.val) = (k₁ == k₂) := by
  cases k₁ with
  | none =>
      cases k₂ with
      | none => rfl
      | some m =>
          show ((63 : Nat) == m.val) = ((none : Option (Fin 63)) == some m)
          rcases nat_eq_or_ne (63 : Nat) m.val with hn | hn
          · exact absurd hn (Nat.ne_of_gt m.isLt)
          · rw [nat_beq_false hn,
              show ((none : Option (Fin 63)) == some m) = false from rfl]
  | some a =>
      cases k₂ with
      | none =>
          show ((a.val : Nat) == 63) = ((some a == (none : Option (Fin 63))))
          rcases nat_eq_or_ne a.val 63 with hn | hn
          · exact absurd hn (Nat.ne_of_lt a.isLt)
          · rw [nat_beq_false hn,
              show ((some a == (none : Option (Fin 63)))) = false from rfl]
      | some b => rfl

private theorem any_elim_beq {xs : List Row} {r : Row} :
    xs.any (fun p => p.key.elim 63 Fin.val == r.key.elim 63 Fin.val)
      = xs.any (fun p => p.key == r.key) :=
  List.any_congr rfl (fun p => key_elim_beq (k₁ := p.key) (k₂ := r.key))

/-! ### Bit-level lemmas about `flag` and `summary` -/

private theorem flag_getLsbD {r : Row} {i : Nat} (hi : i < 64) :
    (flag r).getLsbD i = (r.key.elim 63 Fin.val == i) := by
  show ((1 : W) <<< r.key.elim 63 Fin.val).getLsbD i = (r.key.elim 63 Fin.val == i)
  cases r.key with
  | none =>
      show ((1 : W) <<< (63 : Nat)).getLsbD i = ((63 : Nat) == i)
      rw [BitVec.getLsbD_shiftLeft]
      rw [show ((1 : W).getLsbD (i - 63)) = (decide (0 < 64) && decide (i - 63 = 0))
        from BitVec.getLsbD_one]
      rw [decide_eq_true hi, Bool.true_and]
      rcases Nat.lt_or_ge i 63 with h | h
      · rw [decide_eq_true h, Bool.not_true, Bool.false_and,
          nat_beq_false (Nat.ne_of_gt h)]
      · rcases nat_eq_or_ne i 63 with rfl | hne
        · decide
        · exact absurd hne (by omega)
  | some m =>
      show ((1 : W) <<< (m.val : Nat)).getLsbD i = ((m.val : Nat) == i)
      rw [BitVec.getLsbD_shiftLeft]
      rw [show ((1 : W).getLsbD (i - m.val)) = (decide (0 < 64) && decide (i - m.val = 0))
        from BitVec.getLsbD_one]
      rw [decide_eq_true hi, Bool.true_and]
      rcases Nat.lt_or_ge i m.val with h | h
      · rw [decide_eq_true h, Bool.not_true, Bool.false_and,
          nat_beq_false (Nat.ne_of_gt h)]
      · rw [decide_eq_false (Nat.not_lt.mpr h), Bool.not_false, Bool.true_and]
        rcases nat_eq_or_ne i m.val with heq | hne
        · subst heq
          rw [Nat.sub_self, show (decide (0 = 0)) = true from rfl,
            show (decide (0 < 64)) = true from rfl, Bool.true_and,
            show ((m.val : Nat) == m.val) = true from Nat.beq_refl m.val]
        · have h2 : m.val < i := by omega
          rw [decide_eq_false (Nat.ne_of_gt (Nat.sub_pos_of_lt h2)), Bool.and_false,
            nat_beq_false (Nat.ne_of_lt h2)]

private theorem flagKey_lt {r : Row} : r.key.elim 63 Fin.val < 64 := by
  cases r.key with
  | none => decide
  | some k => exact Nat.lt_of_lt_of_le k.isLt (by omega)

private theorem summary_nil : summary ([] : List Row) = 0 := rfl

private theorem foldl_flag_gen : ∀ (xs : List Row) (m n : W),
    xs.foldl (fun s r => s ||| flag r) (m ||| n)
      = m ||| xs.foldl (fun s r => s ||| flag r) n := by
  intro xs
  induction xs with
  | nil => intro m n; rfl
  | cons x xs ih =>
      intro m n
      rw [List.foldl_cons, List.foldl_cons]
      rw [BitVec.or_assoc]
      exact ih m (n ||| flag x)

private theorem foldl_flag : ∀ (xs : List Row) (m : W),
    xs.foldl (fun s r => s ||| flag r) m = m ||| summary xs := by
  intro xs m
  have h := foldl_flag_gen xs m 0
  rw [show m ||| (0 : W) = m from by simp] at h
  exact h

private theorem summary_cons {x : Row} {xs : List Row} :
    summary (x :: xs) = flag x ||| summary xs := by
  show ((x :: xs).foldl (fun s r => s ||| flag r) (0 : W))
    = flag x ||| (xs.foldl (fun s r => s ||| flag r) (0 : W))
  rw [List.foldl_cons]
  rw [show ((0 : W) ||| flag x) = flag x from by simp]
  rw [foldl_flag]
  exact rfl

private theorem or_flag_eq_left {r : Row} {m : W}
    (h : m.getLsbD (r.key.elim 63 Fin.val) = true) : m ||| flag r = m := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  rw [BitVec.getLsbD_or, flag_getLsbD hi]
  rcases nat_eq_or_ne (r.key.elim 63 Fin.val) i with rfl | hne
  · rw [show ((r.key.elim 63 Fin.val == r.key.elim 63 Fin.val)) = true
      from Nat.beq_refl _, h, Bool.or_true]
  · rw [nat_beq_false hne, Bool.or_false]

private theorem summary_getLsbD {xs : List Row} {i : Nat} (hi : i < 64) :
    (summary xs).getLsbD i = xs.any (fun r => r.key.elim 63 Fin.val == i) := by
  induction xs with
  | nil => simp [summary]
  | cons x xs ih =>
      rw [summary_cons, BitVec.getLsbD_or, ih, List.any_cons,
        flag_getLsbD hi]

private theorem summary_append {xs ys : List Row} :
    summary (xs ++ ys) = summary xs ||| summary ys := by
  show ((xs ++ ys).foldl (fun s r => s ||| flag r) (0 : W)) = _
  rw [List.foldl_append, foldl_flag, foldl_flag]
  rw [show ((0 : W) ||| summary xs) = summary xs from by simp]

private theorem summary_single {r : Row} : summary [r] = flag r := by
  simp [summary]

private theorem summary_append_eq_of_any {pre : List Row} {r : Row}
    (h : pre.any (fun p => p.key == r.key)) :
    summary (pre ++ [r]) = summary pre := by
  rw [summary_append, summary_single]
  refine or_flag_eq_left (r := r) (m := summary pre) ?_
  rw [summary_getLsbD flagKey_lt, any_elim_beq]
  exact h

private theorem bool_and_false (b : Bool) : (b && false) = false := by
  cases b <;> rfl

private theorem flag_and_summary_false {r : Row} {m : W} :
    ((flag r &&& m) = 0) = (m.getLsbD (r.key.elim 63 Fin.val) = false) := by
  apply propext; constructor
  · intro h
    have h1 := congrArg (fun bv : W => bv.getLsbD (r.key.elim 63 Fin.val)) h
    rw [BitVec.getLsbD_and, flag_getLsbD flagKey_lt] at h1
    rw [show ((r.key.elim 63 Fin.val == r.key.elim 63 Fin.val)) = true
      from Nat.beq_refl _, Bool.true_and] at h1
    rw [show ((0 : W).getLsbD (r.key.elim 63 Fin.val)) = false
      from BitVec.getLsbD_zero] at h1
    exact h1
  · intro h
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    rw [BitVec.getLsbD_and, flag_getLsbD hi,
      show ((0 : W).getLsbD i) = false from BitVec.getLsbD_zero]
    rcases nat_eq_or_ne (r.key.elim 63 Fin.val) i with rfl | hne
    · rw [h, bool_and_false]
    · rw [nat_beq_false hne, Bool.false_and]

private theorem flag_and_summary_true {r : Row} {m : W} :
    ((flag r &&& ~~~m) = 0) = (m.getLsbD (r.key.elim 63 Fin.val) = true) := by
  apply propext; constructor
  · intro h
    have h1 := congrArg (fun bv : W => bv.getLsbD (r.key.elim 63 Fin.val)) h
    rw [BitVec.getLsbD_and, flag_getLsbD flagKey_lt, BitVec.getLsbD_not] at h1
    rw [show ((r.key.elim 63 Fin.val == r.key.elim 63 Fin.val)) = true
      from Nat.beq_refl _, Bool.true_and] at h1
    rw [show ((0 : W).getLsbD (r.key.elim 63 Fin.val)) = false
      from BitVec.getLsbD_zero] at h1
    rw [decide_eq_true flagKey_lt, Bool.true_and] at h1
    rcases Bool.eq_false_or_eq_true (m.getLsbD (r.key.elim 63 Fin.val)) with h2 | h2
    · exact h2
    · rw [h2, Bool.not_false] at h1
      exact absurd h1 (by simp)
  · intro h
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    rw [BitVec.getLsbD_and, flag_getLsbD hi, BitVec.getLsbD_not,
      show ((0 : W).getLsbD i) = false from BitVec.getLsbD_zero]
    rcases nat_eq_or_ne (r.key.elim 63 Fin.val) i with rfl | hne
    · rw [decide_eq_true flagKey_lt, h, Bool.not_true, Bool.true_and,
        bool_and_false]
    · rw [nat_beq_false hne, Bool.false_and]

private theorem mem_cons_forall {xs : List Row} {x : Row} {P : Row → Prop} :
    (∀ r ∈ x :: xs, P r) = (P x ∧ ∀ r ∈ xs, P r) := by
  apply propext; constructor
  · intro h
    exact ⟨h x List.mem_cons_self, fun r hr => h r (List.Mem.tail x hr)⟩
  · rintro ⟨h1, h2⟩ r hr
    rcases List.mem_cons.mp hr with rfl | hr
    · exact h1
    · exact h2 r hr

private theorem or_and_zero_iff {u v m : W} :
    (((u ||| v) &&& m) = 0) = ((u &&& m = 0) ∧ (v &&& m = 0)) := by
  apply propext; constructor
  · intro h
    have h2 : (u &&& m) ||| (v &&& m) = 0 := by
      rw [← BitVec.and_or_distrib_right]
      exact h
    exact (BitVec.or_eq_zero_iff (x := u &&& m) (y := v &&& m)).mp h2
  · rintro ⟨h1, h2⟩
    rw [BitVec.and_or_distrib_right]
    exact (BitVec.or_eq_zero_iff (x := u &&& m) (y := v &&& m)).mpr ⟨h1, h2⟩

private theorem foldl_and_eq_zero : ∀ (xs : List Row) (a m : W),
    ((xs.foldl (fun s r => s ||| flag r) a) &&& m = 0) =
      ((a &&& m = 0) ∧ ∀ r ∈ xs, (flag r &&& m) = 0) := by
  intro xs
  induction xs with
  | nil => intro a m; simp
  | cons x xs ih =>
      intro a m
      rw [List.foldl_cons]
      rw [ih, mem_cons_forall, ← and_assoc]
      congr 1
      exact or_and_zero_iff

private theorem flag_closed_row {r : Row} {pre : List Row} :
    ((flag r &&& ~~~summary pre) = 0) = (pre.any (fun p => p.key == r.key)) := by
  rw [flag_and_summary_true, summary_getLsbD flagKey_lt, any_elim_beq]

private theorem block_closed_iff {b : Block} {pre : List Row}
    (hb : b.header = summary b.rows) :
    ((b.header &&& ~~~summary pre) = 0) =
      (∀ r ∈ b.rows, pre.any (fun p => p.key == r.key)) := by
  rw [hb]
  show ((b.rows.foldl (fun s r => s ||| flag r) (0 : W)) &&& ~~~summary pre = 0)
    = (∀ r ∈ b.rows, pre.any (fun p => p.key == r.key))
  rw [foldl_and_eq_zero, show ((0 : W) &&& ~~~summary pre) = 0 from by simp,
    eq_self_iff_true, true_and]
  apply propext; constructor
  · intro h r hr
    rw [← flag_closed_row]
    exact h r hr
  · intro h r hr
    rw [flag_closed_row]
    exact h r hr

private theorem reference_summary {xs : List Row} {r : Row} (hr : r ∈ xs) :
    (summary xs).getLsbD (r.key.elim 63 Fin.val) = true := by
  rw [summary_getLsbD flagKey_lt, any_elim_beq]
  exact List.any_eq_true.mpr ⟨r, hr, by simp⟩

private theorem summary_no {xs : List Row} {r : Row} (hr : r ∈ xs) :
    (summary xs).getLsbD (r.key.elim 63 Fin.val) = false → False := by
  intro hf
  have h := reference_summary (xs := xs) (r := r) hr
  exact absurd h (by simp [hf])

/-! ### `reference`: the first-occurrence filter -/

private theorem if_negb {α : Type} {c : Bool} {t e : α} (hc : c = false) :
    (if c then t else e) = e := by
  cases c with
  | true => exact absurd hc (by simp)
  | false => rfl

private theorem zipIdx_snoc {α : Type} (r : α) :
    ∀ (xs : List α), ∀ (i : Nat), (xs ++ [r]).zipIdx i = xs.zipIdx i ++ [(r, i + xs.length)] := by
  intro xs i
  induction xs generalizing i with
  | nil => show [(r, i)] = [(r, i + 0)]; rw [Nat.add_zero]
  | cons x xs ih =>
      rw [List.cons_append, List.zipIdx_cons, List.zipIdx_cons, ih, List.length_cons]
      rw [Nat.add_comm xs.length 1, ← Nat.add_assoc]
      rw [← List.cons_append]

private theorem filterMap_congr' {α : Type} {β : Type} {f g : α → Option β}
    (l : List α) (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.filterMap_cons, List.filterMap_cons,
        ih (fun y hy => h y (List.Mem.tail x hy)), h x (List.Mem.head xs)]

private theorem zipIdx_snd_lt {α : Type} :
    ∀ (xs : List α) (n : Nat), ∀ (p : α × Nat), p ∈ xs.zipIdx n → p.2 < xs.length + n := by
  intro xs
  induction xs with
  | nil => intro n p hp; exact absurd hp (by simp)
  | cons x xs ih =>
      intro n p hp
      rw [List.zipIdx_cons] at hp
      rcases List.mem_cons.mp hp with heq | htail
      · subst heq
        rw [List.length_cons]
        omega
      · specialize ih (n + 1) p htail
        rw [List.length_cons]
        omega

private theorem zipIdx_get_mem {α : Type} :
    ∀ (start : Nat) (xs : List α) (a : α) (i : Nat),
      (a, i) ∈ xs.zipIdx start → start ≤ i ∧ xs[i - start]? = some a := by
  intro start xs
  induction xs generalizing start with
  | nil => intro a i h; exact absurd h (by simp)
  | cons y yt ih =>
      intro a i h
      rw [List.zipIdx_cons] at h
      rcases List.mem_cons.mp h with heq | htail
      · obtain ⟨rfl, rfl⟩ := Prod.mk.inj heq
        exact ⟨Nat.le_refl _, by rw [Nat.sub_self]; exact List.getElem?_cons_zero⟩
      · obtain ⟨hle, hget⟩ := ih (start + 1) a i htail
        rw [show i - start = (i - (start + 1)) + 1 from by omega]
        rw [List.getElem?_cons_succ]
        exact ⟨by omega, hget⟩

private theorem zipIdx_getElem? {α : Type} {xs : List α} {x : α} {i : Nat}
    (h : (x, i) ∈ xs.zipIdx) : xs[i]? = some x := by
  obtain ⟨hle, hget⟩ := zipIdx_get_mem (start := 0) xs x i h
  rw [Nat.sub_zero] at hget
  exact hget

private theorem zipIdx_mem {α : Type} {xs : List α} {x : α} {i : Nat} :
    (x, i) ∈ xs.zipIdx → xs[i]? = some x ∧ i < xs.length := by
  intro h
  exact ⟨zipIdx_getElem? h, by
    have := zipIdx_snd_lt xs 0 ⟨x, i⟩ h
    simpa using this⟩

private theorem reference_append_cons {ys : List Row} {r : Row} :
    reference (ys ++ [r])
      = if (ys.any (fun p => p.key == r.key)) then reference ys
        else reference ys ++ [r] := by
  show ((ys ++ [r]).zipIdx).filterMap (fun xr =>
      if ((ys ++ [r]).take xr.2).any (fun p => p.key == xr.1.key) then none
      else some xr.1) = _
  rw [(by rw [← Nat.zero_add ys.length]; exact zipIdx_snoc r ys 0 :
        (ys ++ [r]).zipIdx = ys.zipIdx ++ [(r, ys.length)])]
  rw [List.filterMap_append]
  have hpre : ∀ p ∈ (ys.zipIdx : List (Row × Nat)),
      (if ((ys ++ [r]).take p.2).any (fun q => q.key == p.1.key) then none
        else some p.1)
        = (if (ys.take p.2).any (fun q => q.key == p.1.key) then none
          else some p.1) := by
    intro p hp
    rcases zipIdx_mem hp with ⟨_, hlt⟩
    rw [List.take_append_of_le_length (Nat.le_of_lt hlt)]
  rw [filterMap_congr' _ hpre]
  rw [show (ys.zipIdx.filterMap (fun xr =>
        if (ys.take xr.2).any (fun p => p.key == xr.1.key) then none
        else some xr.1)) = reference ys from rfl]
  have hlast : ((ys ++ [r]).take ys.length).any (fun q => q.key == r.key)
      = (ys.any (fun q => q.key == r.key)) := by
    rw [List.take_append_length]
  have hlast2 : ([(r, ys.length)].filterMap (fun xr =>
        if ((ys ++ [r]).take xr.2).any (fun p => p.key == xr.1.key) then none
        else some xr.1))
      = (if ((ys ++ [r]).take ys.length).any (fun p => p.key == r.key)
        then [] else [r]) := by
    rcases Bool.eq_false_or_eq_true
        (((ys ++ [r]).take ys.length).any (fun p => p.key == r.key)) with hc | hc
    · rw [List.filterMap_cons, List.filterMap_nil]
      dsimp only
      rw [hc]
      simp
    · rw [List.filterMap_cons, List.filterMap_nil]
      dsimp only
      rw [hc]
      simp
  rw [hlast2]
  rw [hlast]
  rcases Bool.eq_false_or_eq_true (ys.any (fun p => p.key == r.key)) with hc | hc
  · rw [hc]; simp
  · rw [hc]; simp

/-! ### The scan's inner steps -/

private def stepRow (st : State) (r : Row) : State :=
  if (flag r &&& st.seen) != 0 then st
  else {st with seen := st.seen ||| flag r, kept := r :: st.kept}

private def stepBlock (n : Nat) (s : State) (b : Block) : State :=
  if (b.header &&& ~~~s.seen) = 0 then s
  else b.rows.foldl stepRow {s with opened := n :: s.opened}

private theorem stepRow_skip {st : State} {r : Row}
    (h : (flag r &&& st.seen) != 0) : stepRow st r = st := by
  show (if (flag r &&& st.seen) != 0 then st
      else {st with seen := st.seen ||| flag r, kept := r :: st.kept}) = st
  rcases Bool.eq_false_or_eq_true ((flag r &&& st.seen) != 0) with hc | hc
  · rw [hc]
    simp
  · exact absurd h (by rw [hc]; simp)

private theorem stepRow_open {st : State} {r : Row}
    (h : ¬((flag r &&& st.seen) != 0)) :
    (stepRow st r).seen = st.seen ||| flag r ∧ (stepRow st r).kept = r :: st.kept := by
  rcases Bool.eq_false_or_eq_true ((flag r &&& st.seen) != 0) with hc | hc
  · exact absurd hc h
  · show ((if (flag r &&& st.seen) != 0 then st
          else {st with seen := st.seen ||| flag r, kept := r :: st.kept}).seen
          = st.seen ||| flag r) ∧
        ((if (flag r &&& st.seen) != 0 then st
          else {st with seen := st.seen ||| flag r, kept := r :: st.kept}).kept
          = r :: st.kept)
    rw [hc]
    exact ⟨rfl, rfl⟩

private theorem stepRow_opened (t : State) (x : Row) : (stepRow t x).opened = t.opened := by
  show (if (flag x &&& t.seen) != 0 then t
      else {t with seen := t.seen ||| flag x, kept := x :: t.kept}).opened = t.opened
  rcases Bool.eq_false_or_eq_true ((flag x &&& t.seen) != 0) with hc | hc
  · rw [hc]
    simp
  · rw [hc]
    simp

private theorem foldl_stepRow_opened : ∀ (xs : List Row) (t : State),
    (xs.foldl stepRow t).opened = t.opened := by
  intro xs
  induction xs with
  | nil => intro t; rfl
  | cons x xs ih => intro t; rw [List.foldl_cons, ih, stepRow_opened]

private theorem foldl_stepRow_proj : ∀ (xs : List Row) (s₁ s₂ : State),
    s₁.seen = s₂.seen → s₁.kept = s₂.kept →
    (xs.foldl stepRow s₁).seen = (xs.foldl stepRow s₂).seen ∧
    (xs.foldl stepRow s₁).kept = (xs.foldl stepRow s₂).kept := by
  intro xs
  induction xs with
  | nil => intro s₁ s₂ h1 h2; exact ⟨h1, h2⟩
  | cons x xs ih =>
      intro s₁ s₂ h1 h2
      rw [List.foldl_cons, List.foldl_cons]
      have e : (stepRow s₁ x).seen = (stepRow s₂ x).seen ∧
          (stepRow s₁ x).kept = (stepRow s₂ x).kept := by
        rcases Bool.eq_false_or_eq_true ((flag x &&& s₁.seen) != 0) with hc | hc
        · have hc2 : ((flag x &&& s₂.seen) != 0) = true := by rw [← h1]; exact hc
          have f1 := stepRow_skip hc
          have f2 := stepRow_skip hc2
          exact ⟨by rw [f1, f2, h1], by rw [f1, f2, h2]⟩
        · have hc2 : ¬((flag x &&& s₂.seen) != 0) := by rw [← h1]; exact bfalse_not_true _ hc
          obtain ⟨p1, q1⟩ := stepRow_open (bfalse_not_true _ hc)
          obtain ⟨p2, q2⟩ := stepRow_open hc2
          exact ⟨by rw [p1, p2, h1], by rw [q1, q2, h2]⟩
      exact ih (stepRow s₁ x) (stepRow s₂ x) e.1 e.2

private theorem stepBlock_skip {n : Nat} {s : State} {b : Block}
    (h : (b.header &&& ~~~s.seen) = 0) : stepBlock n s b = s := by
  show (if (b.header &&& ~~~s.seen) = 0 then s
      else b.rows.foldl stepRow {s with opened := n :: s.opened}) = s
  rw [if_pos h]

private theorem stepBlock_open {n : Nat} {s : State} {b : Block}
    (h : ¬((b.header &&& ~~~s.seen) = 0)) :
    (stepBlock n s b).seen = (b.rows.foldl stepRow s).seen ∧
    (stepBlock n s b).kept = (b.rows.foldl stepRow s).kept ∧
    (stepBlock n s b).opened = n :: s.opened := by
  show ((if (b.header &&& ~~~s.seen) = 0 then s
          else b.rows.foldl stepRow {s with opened := n :: s.opened}).seen
        = (b.rows.foldl stepRow s).seen) ∧
      ((if (b.header &&& ~~~s.seen) = 0 then s
          else b.rows.foldl stepRow {s with opened := n :: s.opened}).kept
        = (b.rows.foldl stepRow s).kept) ∧
      ((if (b.header &&& ~~~s.seen) = 0 then s
          else b.rows.foldl stepRow {s with opened := n :: s.opened}).opened
        = n :: s.opened)
  rw [if_neg h]
  obtain ⟨hseen, hkept⟩ := foldl_stepRow_proj b.rows {s with opened := n :: s.opened} s rfl rfl
  exact ⟨hseen, hkept, by rw [foldl_stepRow_opened]⟩

private theorem stepRow_iff {pre : List Row} {s : State} {r : Row}
    (hs : s.seen = summary pre) :
    ((flag r &&& s.seen) != 0) = (pre.any (fun p => p.key == r.key)) := by
  rw [hs, Bool.eq_iff_iff]
  constructor
  · intro h
    rcases Bool.eq_false_or_eq_true (pre.any (fun p => p.key == r.key)) with ha | ha
    · exact ha
    · have heq0 : (flag r &&& summary pre) = 0 := by
        rw [flag_and_summary_false, summary_getLsbD flagKey_lt, any_elim_beq, ha]
      have hc : ((flag r &&& summary pre) != 0) = false := wbne_true heq0
      rw [hc] at h
      exact absurd h (by simp)
  · intro h
    rcases Bool.eq_false_or_eq_true ((flag r &&& summary pre) != 0) with hc | hc
    · exact hc
    · have hfalse : (pre.any (fun p => p.key == r.key)) = false := by
        have hb0 : (flag r &&& summary pre) = 0 := wbne_false hc
        rw [flag_and_summary_false, summary_getLsbD flagKey_lt, any_elim_beq] at hb0
        exact hb0
      exact absurd h (by simp [hfalse])

/-! ### Row-level invariant -/

private theorem row_inv : ∀ (xs : List Row) (pre : List Row) (s : State),
    s.seen = summary pre → s.kept.reverse = reference pre →
    (xs.foldl stepRow s).seen = summary (pre ++ xs) ∧
    (xs.foldl stepRow s).kept.reverse = reference (pre ++ xs) := by
  intro xs
  induction xs with
  | nil =>
      intro pre s h1 h2
      rw [List.append_nil]
      exact ⟨h1, h2⟩
  | cons x xs ih =>
      intro pre s h1 h2
      rw [List.foldl_cons]
      rw [show (x :: xs : List Row) = [x] ++ xs from rfl, ← List.append_assoc]
      rcases Bool.eq_false_or_eq_true (pre.any (fun p => p.key == x.key)) with hc | hc
      · have hskip : ((flag x &&& s.seen) != 0) = true := by rw [stepRow_iff h1, hc]
        have hv := stepRow_skip hskip
        refine ih (pre ++ [x]) (stepRow s x) ?_ ?_
        · rw [hv, h1, summary_append_eq_of_any hc]
        · rw [hv, h2, reference_append_cons, hc]
          simp
      · have hsk : ¬((flag x &&& s.seen) != 0) := by
          rw [stepRow_iff h1, hc]
          simp
        obtain ⟨hv, hk⟩ := stepRow_open hsk
        refine ih (pre ++ [x]) (stepRow s x) ?_ ?_
        · rw [hv, h1, summary_append, summary_single]
        · rw [hk, List.reverse_cons, h2, reference_append_cons, hc]
          simp

/-! ### Which blocks get opened -/

private def FreshAt (done : List Block) (j : Nat) : Prop :=
  ∃ b, done[j]? = some b ∧
    ∃ r ∈ b.rows, ∀ old ∈ done.take j |>.flatMap Block.rows, old.key ≠ r.key

private theorem freshAt_append {done : List Block} {b : Block} {j : Nat}
    (hj : j < done.length) : FreshAt done j ↔ FreshAt (done ++ [b]) j := by
  have hget : (done ++ [b])[j]? = done[j]? := List.getElem?_append_left hj
  have htk : (done ++ [b]).take j = done.take j :=
    List.take_append_of_le_length (Nat.le_of_lt hj)
  constructor
  · rintro ⟨x, hx, y, hy, h2⟩
    exact ⟨x, by rw [hget]; exact hx, y, hy, by rw [htk]; exact h2⟩
  · rintro ⟨x, hx, y, hy, h2⟩
    exact ⟨x, by rw [← hget]; exact hx, y, hy, by rw [← htk]; exact h2⟩

private theorem freshAt_snoc {done : List Block} {b : Block} :
    FreshAt (done ++ [b]) done.length ↔
      ∃ r ∈ b.rows, ∀ old ∈ done.flatMap Block.rows, old.key ≠ r.key := by
  constructor
  · rintro ⟨x, hx, y, hy, h2⟩
    have hget : (done ++ [b])[done.length]? = some b := by
      rw [List.getElem?_append_right (Nat.le_refl _), Nat.sub_self, List.getElem?_cons_zero]
    have hxb : x = b := by
      have h2' : some b = some x := Eq.trans hget.symm hx
      exact (Option.some.inj h2').symm
    subst hxb
    have htk : (done ++ [x]).take done.length = done := List.take_append_length
    rw [htk] at h2
    exact ⟨y, hy, h2⟩
  · rintro ⟨y, hy, h2⟩
    refine ⟨b, ?_, y, hy, ?_⟩
    · rw [List.getElem?_append_right (Nat.le_refl _), Nat.sub_self, List.getElem?_cons_zero]
    · rw [List.take_append_length]
      exact h2

/-! ### Absorption helpers for the skip case -/

private theorem exists_not_of_not_ball {α : Type} {l : List α} {P : α → Prop}
    (h : ¬ (∀ x ∈ l, P x)) : ∃ x ∈ l, ¬ P x := by
  induction l with
  | nil =>
      exact (h (by intro x hx; exact absurd hx (by simp))).elim
  | cons x xs ih =>
      by_cases hx : P x
      · have h2 : ¬ (∀ y ∈ xs, P y) := by
          intro hall
          exact h (by
            intro y hy
            rcases List.mem_cons.mp hy with rfl | hy2
            · exact hx
            · exact hall y hy2)
        obtain ⟨y, hy, hnp⟩ := ih h2
        exact ⟨y, List.Mem.tail x hy, hnp⟩
      · exact ⟨x, List.mem_cons_self, hx⟩

private theorem foldl_or_eq_left : ∀ (xs : List Row) (m : W),
    (∀ r ∈ xs, m.getLsbD (r.key.elim 63 Fin.val) = true) →
      xs.foldl (fun s r => s ||| flag r) m = m := by
  intro xs
  induction xs with
  | nil => intro m _; rfl
  | cons x xs ih =>
      intro m h
      rw [List.foldl_cons, or_flag_eq_left (h x (List.mem_cons_self))]
      exact ih m (fun r hr => h r (List.Mem.tail x hr))

private theorem summary_absorb : ∀ (xs : List Row) (pre : List Row),
    (∀ r ∈ xs, pre.any (fun p => p.key == r.key)) → summary (pre ++ xs) = summary pre := by
  intro xs
  induction xs with
  | nil => intro pre _; rw [List.append_nil]
  | cons x xs ih =>
      intro pre h
      have htail : ∀ r ∈ xs, (pre ++ [x]).any (fun p => p.key == r.key) := by
        intro r hr
        obtain ⟨a, ha, haeq⟩ := List.any_eq_true.mp (h r (List.Mem.tail x hr))
        exact List.any_eq_true.mpr ⟨a, List.mem_append_left [x] ha, haeq⟩
      rw [show (x :: xs : List Row) = [x] ++ xs from rfl, ← List.append_assoc,
        ih (pre ++ [x]) htail, summary_append_eq_of_any (h x (List.mem_cons_self))]

private theorem reference_absorb : ∀ (xs : List Row) (pre : List Row),
    (∀ r ∈ xs, pre.any (fun p => p.key == r.key)) → reference (pre ++ xs) = reference pre := by
  intro xs
  induction xs with
  | nil => intro pre _; rw [List.append_nil]
  | cons x xs ih =>
      intro pre h
      have htail : ∀ r ∈ xs, (pre ++ [x]).any (fun p => p.key == r.key) := by
        intro r hr
        obtain ⟨a, ha, haeq⟩ := List.any_eq_true.mp (h r (List.Mem.tail x hr))
        exact List.any_eq_true.mpr ⟨a, List.mem_append_left [x] ha, haeq⟩
      rw [show (x :: xs : List Row) = [x] ++ xs from rfl, ← List.append_assoc,
        ih (pre ++ [x]) htail, reference_append_cons, h x (List.mem_cons_self)]
      simp

private theorem key_beq_forward {k₁ k₂ : Option (Fin 63)} (h : (k₁ == k₂) = true) : k₁ = k₂ := by
  cases k₁ with
  | none =>
      cases k₂ with
      | none => rfl
      | some m =>
          have hc : ((none : Option (Fin 63)) == some m) = false := rfl
          rw [hc] at h
          exact absurd h (by simp)
  | some a =>
      cases k₂ with
      | none =>
          have hc : ((some a == (none : Option (Fin 63)))) = false := rfl
          rw [hc] at h
          exact absurd h (by simp)
      | some b =>
          exact congrArg some (Fin.eq_of_val_eq (Nat.eq_of_beq_eq_true h))

private theorem freshAt_oob {done : List Block} {j : Nat} (hj : done.length ≤ j) :
    ¬ FreshAt done j := by
  rintro ⟨x, hx, y, hy, hz⟩
  rw [List.getElem?_eq_none hj] at hx
  exact absurd hx (by simp)

/-! ### Main invariant -/

private theorem main_inv : ∀ (bs : List Block) (n : Nat) (pre : List Row)
    (s : State) (done : List Block),
    done.length = n →
    pre = done.flatMap Block.rows →
    s.seen = summary pre →
    s.kept.reverse = reference pre →
    (∀ j, j ∈ s.opened ↔ FreshAt done j) →
    (∀ b ∈ bs, b.header = summary b.rows) →
    ((bs.zipIdx n).foldl (fun s p => stepBlock p.2 s p.1) s).seen
      = summary (pre ++ bs.flatMap Block.rows) ∧
    ((bs.zipIdx n).foldl (fun s p => stepBlock p.2 s p.1) s).kept.reverse
      = reference (pre ++ bs.flatMap Block.rows) ∧
    (∀ j, j ∈ ((bs.zipIdx n).foldl (fun s p => stepBlock p.2 s p.1) s).opened
      ↔ FreshAt (done ++ bs) j) := by
  intro bs
  induction bs with
  | nil =>
      intro n pre s done hlen hpre h1 h2 hopen hhdr
      subst hlen
      rw [List.zipIdx_nil, List.foldl_nil, List.flatMap_nil, List.append_nil]
      refine ⟨h1, h2, ?_⟩
      intro j
      rw [hopen j, List.append_nil]
  | cons b bs ih =>
      intro n pre s done hlen hpre h1 h2 hopen hhdr
      subst hlen
      have hb := hhdr b List.mem_cons_self
      rw [List.zipIdx_cons, List.foldl_cons, List.flatMap_cons, ← List.append_assoc,
        show (b :: bs : List Block) = [b] ++ bs from rfl, ← List.append_assoc]
      by_cases hclosed : (b.header &&& ~~~s.seen) = 0
      · have hcz : (b.header &&& ~~~summary pre) = 0 := by rw [← h1]; exact hclosed
        have hball : ∀ r ∈ b.rows, pre.any (fun p => p.key == r.key) :=
          (block_closed_iff hb).mp hcz
        refine ih (done.length + 1) (pre ++ b.rows) (stepBlock done.length s b) (done ++ [b])
          ?_ ?_ ?_ ?_ ?_ ?_
        · exact List.length_append
        · rw [hpre, List.flatMap_append]
          simp
        · rw [stepBlock_skip hclosed, h1, summary_absorb b.rows pre hball]
        · rw [stepBlock_skip hclosed, h2, reference_absorb b.rows pre hball]
        · intro j
          rw [stepBlock_skip hclosed, hopen j]
          rcases Nat.lt_or_ge j done.length with hj | hj
          · exact freshAt_append hj
          · rcases Nat.eq_or_lt_of_le hj with rfl | hj2
            · refine ⟨fun h => absurd h (freshAt_oob (Nat.le_refl _)), ?_⟩
              intro hex
              obtain ⟨y, hy, hz⟩ := freshAt_snoc.mp hex
              obtain ⟨p, hp, hpeq⟩ := List.any_eq_true.mp (hball y hy)
              exact absurd (key_beq_forward hpeq) (hz p (hpre ▸ hp))
            · exact ⟨fun h => absurd h (freshAt_oob (Nat.le_of_lt hj2)),
                fun h => absurd h (freshAt_oob
                  (by rw [List.length_append]; exact Nat.succ_le_of_lt hj2))⟩
        · exact fun b' hb' => hhdr b' (List.Mem.tail b hb')
      · have hn : ¬((b.header &&& ~~~summary pre) = 0) := by rw [← h1]; exact hclosed
        obtain ⟨os, ok, oo⟩ := stepBlock_open hclosed
        obtain ⟨ri1, ri2⟩ := row_inv b.rows pre s h1 h2
        refine ih (done.length + 1) (pre ++ b.rows) (stepBlock done.length s b) (done ++ [b])
          ?_ ?_ ?_ ?_ ?_ ?_
        · exact List.length_append
        · rw [hpre, List.flatMap_append]
          simp
        · rw [os, ri1]
        · rw [ok, ri2]
        · intro j
          rw [oo, List.mem_cons]
          rcases Nat.lt_or_ge j done.length with hj | hj
          · rw [hopen j]
            constructor
            · intro hmem
              rcases hmem with rfl | hmem
              · exact absurd hj (by omega)
              · exact (freshAt_append hj).mp hmem
            · intro hex
              exact Or.inr ((freshAt_append hj).mpr hex)
          · rcases Nat.eq_or_lt_of_le hj with rfl | hj2
            · obtain ⟨r, hr, hnp⟩ := exists_not_of_not_ball
                (fun hall => hn ((block_closed_iff hb).mpr hall))
              constructor
              · intro _
                exact freshAt_snoc.mpr ⟨r, hr, fun old hold heq =>
                  hnp (List.any_eq_true.mpr ⟨old, hpre ▸ hold, by simpa using heq⟩)⟩
              · exact fun _ => Or.inl rfl
            · refine ⟨fun hmem => by
                rcases hmem with rfl | hmem
                · exact absurd hj2 (by omega)
                · exact absurd ((hopen j).mp hmem) (freshAt_oob (Nat.le_of_lt hj2)),
                fun hex => absurd hex (freshAt_oob
                  (by rw [List.length_append]; exact Nat.succ_le_of_lt hj2))⟩
        · exact fun b' hb' => hhdr b' (List.Mem.tail b hb')

private theorem scan_eq (bs : List Block) :
    scan bs = (bs.zipIdx).foldl (fun s p => stepBlock p.2 s p.1) {} := rfl

theorem sound : TinylambOpt.ResearchGoals.FirstKeyBlock.Goal := by
  intro bs hhdr
  rw [scan_eq]
  obtain ⟨hseen, hkept, hopp⟩ := main_inv bs 0 [] {} [] rfl rfl rfl rfl
    (fun j => ⟨fun hmem => absurd hmem (by simp),
      fun hex => absurd hex (freshAt_oob (Nat.zero_le j))⟩) hhdr
  refine ⟨hkept, ?_⟩
  intro p hp
  obtain ⟨hget, hlt⟩ := zipIdx_mem hp
  rw [hopp p.2]
  constructor
  · intro hnew
    obtain ⟨bl, hbl, r, hr, hfresh⟩ := hnew
    have hbl' : some p.fst = some bl := by
      rw [← hget, ← hbl, List.nil_append]
    have hsub : p.fst = bl := Option.some.inj hbl'
    subst hsub
    exact ⟨r, hr, hfresh⟩
  · intro h
    obtain ⟨r, hr, hfresh⟩ := h
    exact ⟨p.1, hget, r, hr, hfresh⟩

end TinylambOpt.ResearchProofs.FirstKeyBlock
