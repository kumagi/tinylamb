import TinylambOpt.ResearchProofs.StorageExecution.SE_BagKernels_Common

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved (10 claims, including the following)

- `DeletedOrdinals.AntiRevenue` — Compute revenue of all left rows whose key is absent
  on the right.
- `DeletedOrdinals.DeleteFootprint` — `∀ xs ds, weighted bag (xs,ds) (fun _ => 1) =
  xs.length-(ds.eraseDups.filter (· < xs.length)).length`
- `DeletedOrdinals.DistinctOutput` — Output live grouping keys once even if physical
  codes alias.
-/

set_option maxHeartbeats 0
set_option linter.unusedVariables false

open TinylambOpt.ResearchProofs.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace DeletedOrdinalsProofs

open DeletedOrdinals

theorem filter_and_le {α : Type} (p q : α → Bool) : ∀ (l : List α),
    (l.filter (fun e => p e && q e)).length ≤ (l.filter p).length := by
  intro l; induction l with
  | nil => exact Nat.zero_le _
  | cons x t ih =>
    rw [List.filter_cons, List.filter_cons]
    cases hp : p x with
    | false =>
      rw [Bool.false_and, if_neg (by decide : ¬((false : Bool) = true)),
        if_neg (by decide : ¬((false : Bool) = true))]
      exact ih
    | true =>
      cases hq : q x with
      | true =>
        rw [Bool.and_true, if_pos (rfl : (true : Bool) = true),
          if_pos (rfl : (true : Bool) = true)]
        rw [List.length_cons, List.length_cons]
        omega
      | false =>
        rw [Bool.and_false, if_neg (by decide : ¬((false : Bool) = true)),
          if_pos (rfl : (true : Bool) = true)]
        rw [List.length_cons]
        exact Nat.le_succ_of_le ih

theorem filter_true_g {α : Type} (l : List α) : l.filter (fun _ => true) = l := by
  induction l with
  | nil => rfl
  | cons a as ih => rw [List.filter_cons, ih]; simp

theorem decide_congr {p q : Prop} [Decidable p] [Decidable q] (h : p ↔ q) :
    decide p = decide q := by
  cases Classical.em p with
  | inl hp =>
    rw [decide_eq_true hp, decide_eq_true (h.mp hp)]
  | inr hnp =>
    rw [decide_eq_false hnp, decide_eq_false (fun hq => hnp (h.mpr hq))]

theorem partition_length {α : Type} (p g : α → Bool) : ∀ (l : List α),
    (l.filter p).length
      = (l.filter (fun e => p e && g e)).length + (l.filter (fun e => p e && !g e)).length := by
  intro l; induction l with
  | nil => rfl
  | cons x t ih =>
    rw [List.filter_cons, List.filter_cons, List.filter_cons]
    cases hp : p x with
    | false =>
      rw [Bool.false_and, Bool.false_and,
        if_neg (by decide : ¬((false : Bool) = true)),
        if_neg (by decide : ¬((false : Bool) = true)),
        if_neg (by decide : ¬((false : Bool) = true))]
      exact ih
    | true =>
      rw [Bool.true_and, Bool.true_and, if_pos (rfl : (true : Bool) = true)]
      cases hg : g x with
      | true =>
        rw [Bool.not_true, if_neg (by decide : ¬((false : Bool) = true)),
          if_pos (rfl : (true : Bool) = true), List.length_cons, ih, List.length_cons]
        omega
      | false =>
        rw [Bool.not_false, if_neg (by decide : ¬((false : Bool) = true)),
          if_pos (rfl : (true : Bool) = true), List.length_cons, ih, List.length_cons]
        omega

theorem count_map_filter {α : Type} (k : Nat) (f : α → Nat) (g : α → Bool) :
    ∀ (l : List α), List.count k ((l.filter g).map f)
      = (List.filter (fun e => (f e == k) && g e) l).length := by
  intro l; induction l with
  | nil => rfl
  | cons x t ih =>
    rw [List.filter_cons, List.filter_cons]
    cases hg : g x with
    | true =>
      rw [if_pos (rfl : (true : Bool) = true), Bool.and_true, List.map_cons, List.count_cons]
      cases hk : f x == k with
      | true =>
        rw [if_pos (rfl : (true : Bool) = true), if_pos (rfl : (true : Bool) = true),
          ih, List.length_cons]
      | false =>
        rw [if_neg (by decide : ¬((false : Bool) = true)),
          if_neg (by decide : ¬((false : Bool) = true)), ih]
        omega
    | false =>
      rw [if_neg (by decide : ¬((false : Bool) = true)), Bool.and_false]
      rw [if_neg (by decide : ¬((false : Bool) = true))]
      exact ih

theorem filter_zipIdx_fst_length (k s : Nat) : ∀ xs : List Nat,
    ((xs.zipIdx s).filter (fun e => e.1 == k)).length = (xs.filter (fun x => x == k)).length := by
  intro xs; induction xs generalizing s with
  | nil => simp
  | cons x t ih =>
    rw [List.zipIdx_cons, List.filter_cons, List.filter_cons]
    have hp : (((x, s).1 == k) : Bool) = (x == k) := rfl
    rw [hp]
    cases hx : (x == k) with
    | true =>
      rw [if_pos (rfl : (true : Bool) = true), if_pos (rfl : (true : Bool) = true),
        List.length_cons, ih, List.length_cons]
    | false =>
      rw [if_neg (by decide : ¬((false : Bool) = true)),
        if_neg (by decide : ¬((false : Bool) = true)), ih]

theorem posge (s : Nat) :
    ∀ (xs : List Nat) (y : Nat), y ∈ (xs.zipIdx s).map Prod.snd → s ≤ y := by
  intro xs; induction xs generalizing s with
  | nil => intro y h; rw [List.zipIdx_nil, List.map_nil] at h; cases h
  | cons x t ih =>
    intro y h
    rw [List.zipIdx_cons, List.map_cons, List.mem_cons] at h
    rcases h with rfl | hy
    · exact Nat.le_refl _
    · have := ih (s + 1) y hy; omega

theorem nodup_positions (k s : Nat) :
    ∀ xs : List Nat, (((xs.zipIdx s).filter (fun e => e.1 == k)).map Prod.snd).Nodup := by
  intro xs; induction xs generalizing s with
  | nil => simp
  | cons x t ih =>
    rw [List.zipIdx_cons, List.filter_cons]
    have hp : (((x, s).1 == k) : Bool) = (x == k) := rfl
    rw [hp]
    cases hx : (x == k) with
    | true =>
      rw [if_pos (rfl : (true : Bool) = true), List.map_cons]
      refine (List.nodup_cons (a := (x, s).snd)).mpr ⟨?_, ih (s + 1)⟩
      intro hin
      obtain ⟨e, he, hfe⟩ := List.mem_map.1 hin
      have hmem : e.snd ∈ (t.zipIdx (s + 1)).map Prod.snd :=
        List.mem_map_of_mem (List.mem_filter.1 he).1
      have hle := posge (s + 1) t e.2 hmem
      rw [show ((x, s).snd : Nat) = s from rfl] at hfe
      omega
    | false =>
      rw [if_neg (by decide : ¬((false : Bool) = true))]
      exact ih (s + 1)

theorem mem_positions :
    ∀ (s k : Nat) (xs : List Nat) (i : Nat),
      i ∈ ((xs.zipIdx s).filter (fun e => e.1 == k)).map Prod.snd
        ↔ ∃ j : Nat, i = s + j ∧ xs[j]? = some k := by
  intro s k xs; induction xs generalizing s with
  | nil =>
      intro i
      rw [List.zipIdx_nil, List.filter_nil, List.map_nil]
      constructor
      · intro h; cases h
      · intro ⟨j, hj1, hj2⟩
        rw [show (([] : List Nat)[j]? : Option Nat) = none from rfl] at hj2
        exact absurd hj2 (by simp)
  | cons x t ih =>
    intro i
    rw [List.zipIdx_cons, List.filter_cons]
    have hp : (((x, s).1 == k) : Bool) = (x == k) := rfl
    rw [hp]
    cases hx : (x == k) with
    | true =>
      rw [if_pos (rfl : (true : Bool) = true), List.map_cons, List.mem_cons, ih (s + 1)]
      rw [show ((x, s).snd : Nat) = s from rfl]
      have hx2 : x = k := Nat.eq_of_beq_eq_true hx
      constructor
      · intro h
        rcases h with rfl | ⟨j, hj1, hj2⟩
        · exact ⟨0, by omega, by
            rw [show ((x :: t)[0]? : Option Nat) = some x from rfl, hx2]⟩
        · exact ⟨j + 1, by omega, by
            rw [show ((x :: t)[j + 1]? : Option Nat) = t[j]? from rfl]; exact hj2⟩
      · intro ⟨j, hj1, hj2⟩
        rcases j with _|m
        · left
          omega
        · right
          rw [show ((x :: t)[m + 1]? : Option Nat) = t[m]? from rfl] at hj2
          exact ⟨m, by omega, hj2⟩
    | false =>
      rw [if_neg (by decide : ¬((false : Bool) = true)), ih (s + 1)]
      constructor
      · intro ⟨j, hj1, hj2⟩
        exact ⟨j + 1, by omega, by
          rw [show ((x :: t)[j + 1]? : Option Nat) = t[j]? from rfl]; exact hj2⟩
      · intro ⟨j, hj1, hj2⟩
        rcases j with _|m
        · rw [show ((x :: t)[0]? : Option Nat) = some x from rfl] at hj2
          have hc : x = k := Option.some.inj hj2
          exact absurd hc (not_eq_of_beq_eq_false hx)
        · exact ⟨m, by omega, by
            rw [show ((x :: t)[m + 1]? : Option Nat) = t[m]? from rfl] at hj2; exact hj2⟩

theorem countP_split (a : Nat) (p : Nat → Bool) :
    ∀ (L : List Nat), L.Nodup →
      List.countP (fun x => x == a || p x) L
        = List.countP (fun x => p x && !(x == a)) L + if L.contains a then 1 else 0 := by
  intro L; induction L with
  | nil => simp [List.countP_nil, List.contains_nil]
  | cons y t ih =>
    intro hN
    obtain ⟨hy, htN⟩ := (List.nodup_cons (a := y) (l := t)).mp hN
    rw [List.countP_cons, List.countP_cons, List.contains_cons, ih htN]
    cases hb : y == a with
    | true =>
      rw [Bool.true_or, Bool.not_true, Bool.and_false, beq_comm a y, hb, Bool.true_or,
        if_pos (rfl : (true : Bool) = true), if_neg (by decide : ¬((false : Bool) = true))]
      have hc0 : t.contains a = false := by
        rw [contains_decide]
        apply decide_eq_false
        intro ha
        apply hy
        have : y = a := Nat.eq_of_beq_eq_true hb
        rw [this]
        exact ha
      rw [hc0, if_neg (by decide : ¬((false : Bool) = true))]
    | false =>
      rw [Bool.false_or, Bool.not_false, Bool.and_true, beq_comm a y, hb, Bool.false_or]
      omega

theorem countP_contains_f {α : Type} (f : α → Nat) :
    ∀ (u : List α), (u.map f).Nodup →
      ∀ (ds : List Nat),
        List.countP (fun x => ds.contains (f x)) u
          = List.countP (fun i => (u.map f).contains i) ds.eraseDups := by
  intro u; induction u with
  | nil =>
      intro _ ds
      rw [List.map_nil,
        show (fun i : Nat => ([] : List Nat).contains i) = (fun _ => false) from by
          funext i; rw [List.contains_nil], List.countP_false, List.countP_nil]
      exact rfl
  | cons y t ih =>
      intro hu ds
      rw [List.map_cons] at hu
      obtain ⟨hy, htN⟩ := (List.nodup_cons (a := f y) (l := t.map f)).mp hu
      rw [List.countP_cons, ih htN ds, List.map_cons]
      rw [show (fun i : Nat => (f y :: List.map f t).contains i)
          = (fun i : Nat => (i == f y || (List.map f t).contains i)) from by
        funext i; rw [List.contains_cons]]
      rw [countP_split (f y) (fun i => (List.map f t).contains i) ds.eraseDups
        (nodup_eraseDups ds)]
      have hcj : (fun i : Nat => (t.map f).contains i && !(i == f y))
          = (fun i : Nat => (t.map f).contains i) := by
        funext i
        cases h : (t.map f).contains i with
        | false => rw [Bool.false_and]
        | true =>
          cases h2 : (i == f y) with
          | false => rw [Bool.not_false, Bool.and_true]
          | true =>
            have hi : i ∈ t.map f :=
              of_decide_eq_true (by rw [← contains_decide]; exact h)
            exact absurd hi (fun hh => hy (by rw [← Nat.eq_of_beq_eq_true h2]; exact hh))
      rw [hcj]
      have hce : (ds.eraseDups).contains (f y) = ds.contains (f y) := by
        rw [contains_decide, contains_decide]
        exact decide_congr List.mem_eraseDups
      rw [hce]

theorem take_of_len_le : ∀ (xs : List Nat) (n : Nat), xs.length ≤ n → xs.take n = xs := by
  intro xs; induction xs with
  | nil => intro n _; cases n <;> rfl
  | cons x t ih =>
      intro n h
      rw [List.length_cons] at h
      cases n with
      | zero => exact absurd h (by omega)
      | succ m =>
        rw [show (x :: t).take (m + 1) = x :: t.take m from rfl, ih m (by omega)]

theorem drop_of_len_le : ∀ (xs : List Nat) (n : Nat), xs.length ≤ n → xs.drop n = [] := by
  intro xs; induction xs with
  | nil => intro n _; cases n <;> rfl
  | cons x t ih =>
      intro n h
      rw [List.length_cons] at h
      cases n with
      | zero => exact absurd h (by omega)
      | succ m =>
        rw [show (x :: t).drop (m + 1) = t.drop m from rfl, ih m (by omega)]

theorem hsplit (xs : List Nat) (t : Nat) :
    xs.zipIdx = (xs.take t).zipIdx ++ (xs.drop t).zipIdx t := by
  by_cases hlt : t ≤ xs.length
  · have h1 : xs.zipIdx = (xs.take t ++ xs.drop t).zipIdx := by
      rw [List.take_append_drop]
    rw [h1, List.zipIdx_append, Nat.zero_add, List.length_take, Nat.min_eq_left hlt]
  · rw [take_of_len_le xs t (by omega), drop_of_len_le xs t (by omega), List.zipIdx_nil,
      List.append_nil]

theorem filter_map_shift_length (s : Nat) (p : Nat × Nat → Bool) :
    ∀ (l : List (Nat × Nat)),
      ((l.map (fun e => (e.1, e.2 + s))).filter p).length
        = (l.filter (fun e => p (e.1, e.2 + s))).length := by
  intro l; induction l with
  | nil => rfl
  | cons x t ih =>
    rw [List.map_cons, List.filter_cons, List.filter_cons]
    cases hp : p (x.1, x.2 + s) with
    | true =>
      rw [if_pos (rfl : (true : Bool) = true), if_pos (rfl : (true : Bool) = true),
        List.length_cons, ih, List.length_cons]
    | false =>
      rw [if_neg (by decide : ¬((false : Bool) = true)),
        if_neg (by decide : ¬((false : Bool) = true)), ih]

theorem dead_countP (xs ds : List Nat) (k : Nat) :
    (xs.zipIdx.filter (fun e => (e.1 == k) && ds.contains e.2)).length
      = List.countP (fun i => xs[i]? == some k) ds.eraseDups := by
  have hbeq : (fun i : Nat => xs[i]? == some k)
      = (fun i : Nat => decide (xs[i]? = some k)) := by
    funext i
    rw [Bool.beq_eq_decide_eq]
  have hcs : (fun e : Nat × Nat => ds.contains e.2)
      = ((fun x : Nat => ds.contains x) ∘ Prod.snd) := rfl
  have hid : (fun x : Nat => ds.contains x) = (fun x : Nat => ds.contains (id x)) := rfl
  have hNod : ((((xs.zipIdx).filter (fun e => e.1 == k)).map Prod.snd).map id).Nodup := by
    rw [List.map_id]
    exact nodup_positions k 0 xs
  have hkey2 : ∀ i : Nat,
      (((xs.zipIdx).filter (fun e => e.1 == k)).map Prod.snd).contains i
        = decide (xs[i]? = some k) := by
    intro i
    rw [contains_decide]
    exact decide_congr
      (show (i ∈ ((xs.zipIdx).filter (fun e => e.1 == k)).map Prod.snd) ↔ (xs[i]? = some k)
        from by
          constructor
          · intro hin
            obtain ⟨j, hj1, hj2⟩ := (mem_positions 0 k xs i).1 hin
            rw [Nat.zero_add] at hj1
            rw [hj1]
            exact hj2
          · intro hh
            exact (mem_positions 0 k xs i).2 ⟨i, by omega, hh⟩)
  rw [← List.countP_eq_length_filter]
  rw [show (fun e : Nat × Nat => (e.1 == k) && ds.contains e.2)
      = (fun e : Nat × Nat => ds.contains e.2 && (e.1 == k)) from by
        funext e; rw [Bool.and_comm]]
  rw [← List.countP_filter, hcs, ← List.countP_map, hid,
    countP_contains_f id (((xs.zipIdx).filter (fun e => e.1 == k)).map Prod.snd) hNod ds,
    List.map_id]
  rw [show (fun i : Nat => (((xs.zipIdx).filter (fun e => e.1 == k)).map Prod.snd).contains i)
      = (fun i : Nat => decide (xs[i]? = some k)) from by funext i; exact hkey2 i, ← hbeq]

theorem mass_eq_DO (xs ds : List Nat) (k : Nat) :
    (bag.expand (xs, ds)).count k = bag.mass (xs, ds) k := by
  have hexp : bag.expand (xs, ds)
      = (xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst := rfl
  have hmass : bag.mass (xs, ds) k
      = List.count k xs - List.countP (fun i => xs[i]? == some k) ds.eraseDups := rfl
  rw [hexp, count_map_filter, hmass]
  have htot : (xs.zipIdx.filter (fun e => e.1 == k)).length = List.count k xs := by
    rw [filter_zipIdx_fst_length, count_eq_length_filter]
    rw [show (fun x : Nat => k == x) = (fun x : Nat => x == k) from by
      funext x; rw [beq_comm]]
  have hpart := partition_length (fun e : Nat × Nat => e.1 == k)
    (fun e : Nat × Nat => ds.contains e.2) xs.zipIdx
  rw [← htot, ← dead_countP xs ds k, hpart]
  omega

theorem cover_DO :
    ∀ (p : List Nat × List Nat) (k : Nat), k ∈ bag.expand p → k ∈ bag.keys p := by
  intro p k hk
  obtain ⟨xs, ds⟩ := p
  rw [show bag.keys (xs, ds) = xs from rfl]
  rw [show bag.expand (xs, ds)
      = ((xs.zipIdx).filter (fun e => !ds.contains e.2)).map Prod.fst from rfl] at hk
  obtain ⟨e, he, hfe⟩ := List.mem_map.mp hk
  have h1 : e ∈ xs.zipIdx := (List.mem_filter.mp he).1
  have h2 : e.1 ∈ (xs.zipIdx).map Prod.fst := List.mem_map_of_mem h1
  rw [List.zipIdx_map_fst] at h2
  cases hfe
  exact h2

theorem wf : WellFormed bag :=
  ⟨fun p k => by obtain ⟨xs, ds⟩ := p; exact mass_eq_DO xs ds k, cover_DO⟩

theorem sound_DistinctOutput : DistinctOutput.Goal := wf_distinctOutput bag wf
theorem sound_FilteredMoment : FilteredMoment.Goal := wf_filteredMoment bag wf
theorem sound_OrderStatistic : OrderStatistic.Goal := wf_orderStatistic bag wf
theorem sound_JoinFanout : JoinFanout.Goal := wf_joinFanout bag wf
theorem sound_AntiRevenue : AntiRevenue.Goal := wf_antiRevenue bag wf
theorem sound_SelfPairs : SelfPairs.Goal := wf_selfPairs bag wf
theorem sound_IntersectAll : IntersectAll.Goal := wf_intersectAll bag wf
theorem sound_OuterWeight : OuterWeight.Goal := wf_outerWeight bag wf

theorem hmf (k : Nat) : ∀ (u l : List Nat), bag.mass (u, l) k
    = (u.zipIdx.filter (fun e => e.1 == k)).length
      - (u.zipIdx.filter (fun e => (e.1 == k) && l.contains e.2)).length := by
  intro u l
  have hp := partition_length (fun e : Nat × Nat => e.1 == k)
    (fun e : Nat × Nat => l.contains e.2) u.zipIdx
  rw [← mass_eq_DO u l k, show bag.expand (u, l)
      = ((u.zipIdx.filter (fun e => !l.contains e.2)).map Prod.fst) from rfl,
    count_map_filter, hp]
  omega

theorem sound_PrefixOfLive : PrefixOfLive.Goal := by
  intro xs ds t k
  rw [hmf k (xs.take t) ds, hmf k (xs.drop t) ((ds.filter (fun x => x ≥ t)).map (fun x => x - t)),
    hmf k xs ds]
  have hT : (xs.zipIdx.filter (fun e => e.1 == k)).length
      = ((xs.take t).zipIdx.filter (fun e => e.1 == k)).length
        + (((xs.drop t).zipIdx t).filter (fun e => e.1 == k)).length := by
    rw [hsplit xs t, List.filter_append, List.length_append]
  have hT2 : (((xs.drop t).zipIdx t).filter (fun e => e.1 == k)).length
      = (((xs.drop t).zipIdx (0)).filter (fun e => e.1 == k)).length := by
    rw [show (xs.drop t).zipIdx t = ((xs.drop t).zipIdx (0 + t)) from by rw [Nat.zero_add],
      zipIdx_add (xs.drop t) 0 t, filter_map_shift_length t (fun e => e.1 == k),
      show (fun e : Nat × Nat => ((e.1, e.2 + t).1 == k)) = (fun e : Nat × Nat => e.1 == k)
        from by funext e; rfl]
  have pl2 : ∀ j : Nat,
      ((ds.filter (fun x => x ≥ t)).map (fun x => x - t)).contains j = ds.contains (j + t) := by
    intro j
    rw [contains_decide, contains_decide]
    exact decide_congr
      (show (j ∈ (ds.filter (fun x => x ≥ t)).map (fun x => x - t)) ↔ (j + t ∈ ds) from by
        rw [List.mem_map]
        constructor
        · intro ⟨a, ha, ha3⟩
          rw [List.mem_filter] at ha
          have hat : t ≤ a := of_decide_eq_true ha.2
          have ha3' : j = a - t := ha3.symm
          have : j + t = a := by omega
          rw [this]
          exact ha.1
        · intro h
          refine ⟨j + t, ?_, by omega⟩
          rw [List.mem_filter]
          exact ⟨h, decide_eq_true (by omega : (j + t : Nat) ≥ t)⟩)
  have hD2 : (((xs.drop t).zipIdx t).filter (fun e => (e.1 == k) && ds.contains e.2)).length
      = (((xs.drop t).zipIdx (0)).filter
          (fun e => (e.1 == k)
            && ((ds.filter (fun x => x ≥ t)).map (fun x => x - t)).contains e.2)).length := by
    rw [show (xs.drop t).zipIdx t = ((xs.drop t).zipIdx (0 + t)) from by rw [Nat.zero_add],
      zipIdx_add (xs.drop t) 0 t, filter_map_shift_length t
        (fun e => (e.1 == k) && ds.contains e.2),
      show (fun e : Nat × Nat => ((e.1, e.2 + t).1 == k) && ds.contains ((e.1, e.2 + t).2))
        = (fun e : Nat × Nat => (e.1 == k)
            && ((ds.filter (fun x => x ≥ t)).map (fun x => x - t)).contains e.2) from by
        funext e; rw [← pl2 (e.2)]]
  have hD : (xs.zipIdx.filter (fun e => (e.1 == k) && ds.contains e.2)).length
      = ((xs.take t).zipIdx.filter (fun e => (e.1 == k) && ds.contains e.2)).length
        + (((xs.drop t).zipIdx (0)).filter
            (fun e => (e.1 == k)
              && ((ds.filter (fun x => x ≥ t)).map (fun x => x - t)).contains e.2)).length := by
    rw [hsplit xs t, List.filter_append, List.length_append, hD2]
  rw [hT, hD, hT2]
  have hle1 := filter_and_le (fun e : Nat × Nat => e.1 == k)
    (fun e : Nat × Nat => ds.contains e.2) ((xs.take t).zipIdx)
  have hle2 := filter_and_le (fun e : Nat × Nat => e.1 == k)
    (fun e : Nat × Nat => ((ds.filter (fun x => x ≥ t)).map (fun x => x - t)).contains e.2)
    ((xs.drop t).zipIdx (0))
  omega

theorem sound_DeleteFootprint : DeleteFootprint.Goal := by
  intro xs ds
  rw [weighted_eq bag wf (xs, ds) (fun _ => 1), sum_map_const, Nat.mul_one]
  rw [show bag.expand (xs, ds) = (xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst
    from rfl, List.length_map]
  have hpart := partition_length (fun _ : Nat × Nat => true)
    (fun e : Nat × Nat => ds.contains e.2) xs.zipIdx
  have hcj1 : (xs.zipIdx.filter (fun e => (true : Bool) && ds.contains e.2)).length
      = (xs.zipIdx.filter (fun e => ds.contains e.2)).length := by
    rw [show (fun e : Nat × Nat => (true : Bool) && ds.contains e.2)
      = (fun e : Nat × Nat => ds.contains e.2) from by funext e; rw [Bool.true_and]]
  have hcj2 :
      (xs.zipIdx.filter (fun e => (true : Bool) && !ds.contains e.2)).length
      = (xs.zipIdx.filter (fun e => !ds.contains e.2)).length := by
    rw [show (fun e : Nat × Nat => (true : Bool) && !ds.contains e.2)
      = (fun e : Nat × Nat => !ds.contains e.2) from by funext e; rw [Bool.true_and]]
  have htot : (xs.zipIdx.filter (fun _ : Nat × Nat => true)).length = xs.length := by
    rw [filter_true_g, List.length_zipIdx]
  have hdead2 : (xs.zipIdx.filter (fun e => ds.contains e.2)).length
      = (ds.eraseDups.filter (fun i => i < xs.length)).length := by
    have hcs : (fun e : Nat × Nat => ds.contains e.2)
        = ((fun x : Nat => ds.contains x) ∘ Prod.snd) := rfl
    have hid : (fun x : Nat => ds.contains x) = (fun x : Nat => ds.contains (id x)) := rfl
    have hNod : ((List.range' 0 xs.length).map id).Nodup := by
      rw [List.map_id]
      exact List.nodup_range' (s := 0) (n := xs.length)
    have hkey : ∀ i : Nat, (List.range' 0 xs.length).contains i = decide (i < xs.length) := by
      intro i
      rw [contains_decide]
      exact decide_congr (show (i ∈ List.range' 0 xs.length) ↔ (i < xs.length) from by
        rw [List.mem_range']
        constructor
        · intro ⟨j, hj, heq⟩
          omega
        · intro h
          exact ⟨i, h, by omega⟩)
    rw [← List.countP_eq_length_filter, hcs, ← List.countP_map, List.zipIdx_map_snd 0 xs, hid,
      countP_contains_f id (List.range' 0 xs.length) hNod ds, List.map_id]
    rw [show (fun i : Nat => (List.range' 0 xs.length).contains i)
        = (fun i : Nat => decide (i < xs.length)) from by funext i; exact hkey i]
    rw [List.countP_eq_length_filter]
  rw [hcj1, hcj2] at hpart
  rw [htot, hdead2] at hpart
  omega

end DeletedOrdinalsProofs
