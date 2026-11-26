import TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts

/-!
These models connect SQL operators to their reference rows, order, or aggregate results.

Proved (8 claims, including the following)

- `AverageFence` — Q22: prune an unfinished AVG group from an ascending top-k once its
  best possible completion is strictly above the cutoff. Existing group must have a
  non-NULL measure; future values have a lower bound and a bound on non-NULL count.
- `ExclusionErase` — Q87: destructively remove each matched store key when an exclusion
  arrives, with no storage for unmatched catalog/web values. NULL components are
  encoded in the projected key before this kernel.
- `NetPairSet` — `∀ (channels : List (List (Nat × Net))) g, let (_,qty,amt) :=
  distinctNet channels.flatten g let vs := (channels.flatten.eraseDups.filter (·.1 ==
  g)).map Prod.snd (qty,amt) = (summarize (vs.map Prod.fst),summarize (vs.map
  Prod.snd))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.AnalyticalOperators.RollupsAndChannelSetCounts

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts

private theorem lower_bound_sum (floor : Int) : ∀ (l : List Int),
    (∀ x ∈ l, floor ≤ x) → floor * (l.length : Int) ≤ l.sum := by
  intro l
  induction l with
  | nil => intro _; simp
  | cons x xs ih =>
      intro h
      have hx : floor ≤ x := h x (by simp)
      have hxs : ∀ y ∈ xs, floor ≤ y := fun y hy => h y (by simp [hy])
      have hi := ih hxs
      rw [List.length_cons, List.sum_cons]
      push_cast
      rw [Int.mul_add, Int.mul_one]
      omega

theorem AverageFence_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts.AverageFence.Goal := by
  intro seen tail floor numerator denom capacity hden hseen hcap hfloor hA hB
  have hsum := lower_bound_sum floor tail hfloor
  rw [List.length_append, List.sum_append]
  push_cast at hA hB ⊢
  let A : Int := (denom : Int) * seen.sum - numerator * (seen.length : Int)
  let D : Int := (denom : Int) * floor - numerator
  have hApos : 0 < A := by dsimp [A]; omega
  have hBpos : 0 < A + (capacity : Int) * D := by
    dsimp [A, D]
    rw [Int.mul_add] at hB
    rw [Int.mul_add, ← Int.mul_assoc] at hB
    rw [Int.mul_sub]
    rw [Int.mul_comm (capacity : Int) ((denom : Int) * floor)]
    rw [Int.mul_comm (capacity : Int) numerator]
    omega
  have htcap : (tail.length : Int) ≤ (capacity : Int) := by omega
  have hinterp : 0 < A + (tail.length : Int) * D := by
    by_cases hD : 0 ≤ D
    · have hnonneg : 0 ≤ (tail.length : Int) * D :=
        Int.mul_nonneg (by omega) hD
      omega
    · have hDneg : D < 0 := by omega
      have hmul : (capacity : Int) * D ≤ (tail.length : Int) * D :=
        Int.mul_le_mul_of_nonpos_right htcap (by omega : D ≤ 0)
      omega
  have htarget : numerator * ((seen.length : Int) + (tail.length : Int)) <
      (denom : Int) * (seen.sum + floor * (tail.length : Int)) := by
    dsimp [A, D] at hinterp
    rw [Int.mul_add]
    rw [Int.mul_add]
    rw [← Int.mul_assoc]
    rw [Int.mul_sub] at hinterp
    rw [Int.mul_comm (tail.length : Int) ((denom : Int) * floor)] at hinterp
    rw [Int.mul_comm (tail.length : Int) numerator] at hinterp
    omega
  have hscale : (denom : Int) * (floor * (tail.length : Int)) ≤
      (denom : Int) * tail.sum := by
    exact Int.mul_le_mul_of_nonneg_left hsum (by omega : 0 ≤ (denom : Int))
  repeat rw [Int.mul_add] at htarget ⊢
  omega

/-- Counting `Option` payloads when one row is replicated once per filler element. -/
private theorem counts_const_rows {α : Type} (p : Option Nat) (filler : List α) :
    ((filler.map fun _ => p).countP Option.isNone,
      (filler.map fun _ => p).countP Option.isSome,
      (filler.map fun _ => p).length) =
      (if p.isSome then 0 else filler.length,
        if p.isSome then filler.length else 0, filler.length) := by
  cases p with
  | none => simp
  | some n =>
      induction filler with
      | nil => rfl
      | cons x xs ih => simp at ih ⊢

private theorem counts_flatMap_const {α : Type} (ps : List (Option Nat)) (filler : List α) :
    ((ps.flatMap fun p => filler.map fun _ => p).countP Option.isNone,
      (ps.flatMap fun p => filler.map fun _ => p).countP Option.isSome,
      (ps.flatMap fun p => filler.map fun _ => p).length) =
      (ps.countP Option.isNone * filler.length,
        ps.countP Option.isSome * filler.length, ps.length * filler.length) := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      rw [List.flatMap_cons, List.countP_append, List.countP_append, List.length_append]
      have hc := counts_const_rows p filler
      have hcN : (filler.map fun _ => p).countP Option.isNone =
          (if p.isSome then 0 else filler.length) := by
        exact congrArg Prod.fst hc
      have hcS : (filler.map fun _ => p).countP Option.isSome =
          (if p.isSome then filler.length else 0) := by
        exact congrArg (fun x => x.2.1) hc
      have hcL : (filler.map fun _ => p).length = filler.length := by
        exact congrArg (fun x => x.2.2) hc
      have ihN : (ps.flatMap fun p => filler.map fun _ => p).countP Option.isNone =
          ps.countP Option.isNone * filler.length := by
        simpa using congrArg Prod.fst ih
      have ihS : (ps.flatMap fun p => filler.map fun _ => p).countP Option.isSome =
          ps.countP Option.isSome * filler.length := by
        simpa using congrArg (fun x => x.2.1) ih
      have ihL : (ps.flatMap fun p => filler.map fun _ => p).length = ps.length * filler.length := by
        simpa using congrArg (fun x => x.2.2) ih
      rw [hcN, hcS, hcL, ihN, ihS, ihL]
      cases p <;> simp [Nat.add_mul] <;> constructor <;> omega

theorem PromoReturnCounts_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts.PromoReturnCounts.Goal := by
  intro promos returns
  dsimp only
  let filler := if returns.isEmpty then [none] else returns.map some
  have hflen : filler.length = max 1 returns.length := by
    dsimp [filler]
    by_cases h : returns.isEmpty
    · rw [if_pos h]
      have hz : returns.length = 0 := List.isEmpty_iff_length_eq_zero.mp h
      simp [hz]
    · rw [if_neg h]
      have hnz : returns.length ≠ 0 := by
        intro he
        apply h
        exact List.isEmpty_iff_length_eq_zero.mpr he
      simp only [List.length_map]
      omega
  let base := if promos.isEmpty then [none] else promos
  have hb_none : base.countP Option.isNone =
      if promos.isEmpty then 1 else promos.countP Option.isNone := by
    dsimp [base]
    split <;> simp_all
  have hb_some : base.countP Option.isSome = promos.countP Option.isSome := by
    dsimp [base]
    by_cases h : promos.isEmpty
    · rw [if_pos h]
      have hz : promos.length = 0 := List.isEmpty_iff_length_eq_zero.mp h
      have hnil : promos = [] := List.eq_nil_of_length_eq_zero hz
      simp [hnil]
    · rw [if_neg h]
  have hb_len : base.length = max 1 promos.length := by
    dsimp [base]
    by_cases h : promos.isEmpty
    · rw [if_pos h]
      have hz : promos.length = 0 := List.isEmpty_iff_length_eq_zero.mp h
      simp [hz]
    · rw [if_neg h]
      have hnz : promos.length ≠ 0 := by
        intro he
        apply h
        exact List.isEmpty_iff_length_eq_zero.mpr he
      omega
  change ((base.flatMap fun p => filler.map fun _ => p).countP Option.isNone,
    (base.flatMap fun p => filler.map fun _ => p).countP Option.isSome,
    (base.flatMap fun p => filler.map fun _ => p).length) = _
  rw [counts_flatMap_const, hb_none, hb_some, hb_len, hflen]

private theorem nodup_eraseDups_gen {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup := by
  intro l
  have key : ∀ (n : Nat) (xs : List α), xs.length = n → xs.eraseDups.Nodup := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs hxn
        cases xs with
        | nil => exact List.Pairwise.nil
        | cons a as =>
            rw [List.eraseDups_cons]
            refine List.Pairwise.cons (fun b hb => ?_)
              (ih (as.filter fun b => !b == a).length ?_ _ rfl)
            · have hb' := (List.mem_filter.mp (List.mem_eraseDups.mp hb)).2
              have hba : b ≠ a := by simpa using hb'
              exact fun heq => hba heq.symm
            · rw [List.length_cons] at hxn
              have hle := List.length_filter_le (fun b => !b == a) as
              omega
  exact key l.length l rfl

private theorem nodup_filter_nat (p : Nat → Bool) (l : List Nat) : l.Nodup → (l.filter p).Nodup := by
  intro h
  induction l with
  | nil => exact List.Pairwise.nil
  | cons x xs ih =>
      have hx : x ∉ xs := (List.nodup_cons.mp h).1
      have hxs : xs.Nodup := (List.nodup_cons.mp h).2
      have ih' := ih hxs
      by_cases hp : p x
      · rw [List.filter_cons_of_pos hp]
        exact List.nodup_cons.mpr ⟨fun hm => hx ((List.mem_filter.mp hm).1), ih'⟩
      · rw [List.filter_cons_of_neg (by simpa using hp)]
        exact ih'

private theorem contains_iff_mem_nat (l : List Nat) (a : Nat) :
    l.contains a = true ↔ a ∈ l := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem probe_fold_acc (S b c : List Nat) (x y : Nat) :
    S.foldl (fun (found, calls) k =>
      if k ∈ b then (found + (decide (k ∈ c)).toNat, calls + 1) else (found, calls)) (x, y) =
      (x + (S.filter fun k => k ∈ b && k ∈ c).length,
        y + (S.filter fun k => k ∈ b).length) := by
  induction S generalizing x y with
  | nil => simp
  | cons k S ih =>
      by_cases hb : k ∈ b
      · by_cases hc : k ∈ c
        · simp [hb, hc, ih (x := x + 1) (y := y + 1), Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
        · simp [hb, hc, ih (x := x) (y := y + 1), Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · simp [hb, ih]

private theorem probe_fold (S b c : List Nat) :
    S.foldl (fun (found, calls) k =>
      if k ∈ b then (found + (decide (k ∈ c)).toNat, calls + 1) else (found, calls)) (0, 0) =
      ((S.filter fun k => k ∈ b && k ∈ c).length,
        (S.filter fun k => k ∈ b).length) := by
  simpa using probe_fold_acc S b c 0 0

theorem ShrinkingIntersect_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts.ShrinkingIntersect.Goal := by
  intro a b c
  have hprobe :
      a.eraseDups.foldl (fun (found, calls) k =>
        if b.contains k then (found + (c.contains k).toNat, calls + 1) else (found, calls)) (0, 0) =
        ((a.eraseDups.filter fun k => b.contains k && c.contains k).length,
          (a.eraseDups.filter fun k => b.contains k).length) := by
    simpa using probe_fold a.eraseDups b c
  unfold intersectProbe
  rw [hprobe]
  let L := a.eraseDups.filter (fun k => b.contains k && c.contains k)
  let R := (c.filter (fun k => a.contains k && b.contains k)).eraseDups
  have hnodL : L.Nodup := by
    dsimp [L]
    exact nodup_filter_nat _ _ (nodup_eraseDups_gen a)
  have hnodR : R.Nodup := by
    dsimp [R]
    exact nodup_eraseDups_gen _
  have hmemL : ∀ k, k ∈ L ↔ k ∈ a ∧ k ∈ b ∧ k ∈ c := by
    intro k
    dsimp [L]
    rw [List.mem_filter, List.mem_eraseDups]
    simp only [Bool.and_eq_true]
    rw [contains_iff_mem_nat, contains_iff_mem_nat]
  have hmemR : ∀ k, k ∈ R ↔ k ∈ a ∧ k ∈ b ∧ k ∈ c := by
    intro k
    dsimp [R]
    rw [List.mem_eraseDups, List.mem_filter]
    simp only [Bool.and_eq_true]
    rw [contains_iff_mem_nat, contains_iff_mem_nat]
    constructor
    · intro h
      exact ⟨h.2.1, h.2.2, h.1⟩
    · intro h
      exact ⟨h.2.2, h.1, h.2.1⟩
  have hperm : List.Perm L R := by
    apply List.perm_iff_count.mpr
    intro k
    have hcL := (List.nodup_iff_count_eq_ite.mp hnodL) k
    have hcR := (List.nodup_iff_count_eq_ite.mp hnodR) k
    rw [hcL, hcR]
    by_cases h : k ∈ a ∧ k ∈ b ∧ k ∈ c <;> simp [h, hmemL k, hmemR k]
  change (L.length, (a.eraseDups.filter fun k => b.contains k).length) = (R.length, _)
  rw [hperm.length_eq]

private theorem plus_assoc_t (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  cases a; cases b; cases c
  simp [plus]
  omega

private theorem plus_zero_left_t (a : Totals) : plus zero a = a := by
  cases a
  simp [plus, zero]

private theorem plus_comm_t (a b : Totals) : plus a b = plus b a := by
  cases a; cases b
  simp [plus]
  omega

private theorem plus_zero_right_t (a : Totals) : plus a zero = a := by
  cases a
  simp [plus, zero]

private theorem summarize_append_t (xs ys : List (Option Int)) :
    summarize (xs ++ ys) = plus (summarize xs) (summarize ys) := by
  unfold summarize
  rw [List.foldl_append]
  have key : ∀ (xs : List (Option Int)) (acc a : Totals),
      xs.foldl (fun s x => plus s (weighted 1 x)) (plus a acc) =
        plus a (xs.foldl (fun s x => plus s (weighted 1 x)) acc) := by
    intro xs
    induction xs with
    | nil => intro acc a; rfl
    | cons x xs ih =>
        intro acc a
        simp only [List.foldl_cons]
        rw [show plus (plus a acc) (weighted 1 x) = plus a (plus acc (weighted 1 x)) by
          exact plus_assoc_t a acc (weighted 1 x)]
        exact ih (plus acc (weighted 1 x)) a
  have h := key ys zero (xs.foldl (fun s x => plus s (weighted 1 x)) zero)
  simpa [plus_zero_right_t] using h

private theorem summarize_singleton_t (v : Option Int) : summarize [v] = weighted 1 v := by
  simp [summarize, plus, zero, weighted]

private theorem summarize_cons_t (v : Option Int) (vs : List (Option Int)) :
    summarize (v :: vs) = plus (weighted 1 v) (summarize vs) := by
  rw [show v :: vs = [v] ++ vs by rfl, summarize_append_t, summarize_singleton_t]

private theorem summarize_perm {l1 l2 : List (Option Int)} (p : l1.Perm l2) :
    summarize l1 = summarize l2 := by
  induction p with
  | nil => rfl
  | cons x p ih =>
      rw [summarize_cons_t, summarize_cons_t, ih]
  | swap x y l =>
      rw [summarize_cons_t, summarize_cons_t, summarize_cons_t, summarize_cons_t]
      rw [← plus_assoc_t, plus_comm_t (weighted 1 y) (weighted 1 x), plus_assoc_t]
  | trans p1 p2 ih1 ih2 => exact ih1.trans ih2

private theorem distinctNet_fold_acc (rows : List (Nat × Net)) (g : Nat)
    (s0 : Std.HashSet Net) (q0 a0 : Totals) :
  ∃ (kept : List Net) (sf : Std.HashSet Net) (qf af : Totals),
    rows.foldl (fun (acc : Std.HashSet Net × Totals × Totals) (r : Nat × Net) =>
      if r.1 != g || acc.1.contains r.2 then acc
      else (acc.1.insert r.2, plus acc.2.1 (weighted 1 r.2.1), plus acc.2.2 (weighted 1 r.2.2)))
      (s0, q0, a0) = (sf, qf, af) ∧
    kept.Nodup ∧
    (∀ v ∈ kept, v ∉ s0) ∧
    (∀ v, sf.contains v = true ↔ v ∈ kept ∨ s0.contains v = true) ∧
    (∀ v, v ∈ kept ↔ (∃ r ∈ rows, (r.1 == g) = true ∧ r.2 = v) ∧ v ∉ s0) ∧
    qf = plus q0 (summarize (kept.map Prod.fst)) ∧
    af = plus a0 (summarize (kept.map Prod.snd)) := by
  induction rows generalizing s0 q0 a0 with
  | nil =>
      refine ⟨[], s0, q0, a0, rfl, List.Pairwise.nil, ?_, ?_, ?_, ?_, ?_⟩
      · intro v hv; simp at hv
      · intro v; simp
      · intro v; simp
      · simp [summarize, plus, zero]
      · simp [summarize, plus, zero]
  | cons r rows ih =>
      rcases r with ⟨gg, vv⟩
      by_cases hgg : gg == g
      · have hgg_eq : gg = g := beq_iff_eq.mp hgg
        by_cases hseen : s0.contains vv = true
        · obtain ⟨kept, sf, qf, af, hfold, hnod, hdis, hmemset, hmemkept, hq, ha⟩ :=
            ih s0 q0 a0
          refine ⟨kept, sf, qf, af, ?_, hnod, hdis, hmemset, ?_, hq, ha⟩
          · have hseen_mem : vv ∈ s0 := Std.HashSet.contains_iff_mem.mp hseen
            have hfold' : List.foldl (fun acc r =>
                if ¬r.1 = g ∨ r.2 ∈ acc.1 then acc
                else (acc.1.insert r.2, plus acc.2.1 (weighted 1 r.2.1), plus acc.2.2 (weighted 1 r.2.2)))
                (s0, q0, a0) rows = (sf, qf, af) := by
              simpa using hfold
            simp [hgg_eq, hseen_mem, hfold']
          · intro v
            constructor
            · intro hv
              have hvk := (hmemkept v).mp hv
              refine ⟨?_, hvk.2⟩
              rcases hvk.1 with ⟨r, hr, hrg, hrv⟩
              exact ⟨r, by simp [hr], hrg, hrv⟩
            · intro h
              apply (hmemkept v).mpr
              rcases h with ⟨⟨r, hr, hrg, hrv⟩, hnots⟩
              rcases List.mem_cons.mp hr with heq | hrest
              · rcases heq with rfl
                subst v
                exact absurd hseen hnots
              · exact ⟨⟨r, hrest, hrg, hrv⟩, hnots⟩
        · have hseen_false : s0.contains vv = false := by
            cases h : s0.contains vv <;> simp_all
          have hvv_nots : vv ∉ s0 := by
            intro hm
            have := (Std.HashSet.contains_iff_mem).mpr hm
            rw [hseen_false] at this
            contradiction
          obtain ⟨kept, sf, qf, af, hfold, hnod, hdis, hmemset, hmemkept, hq, ha⟩ :=
            ih (s0.insert vv) (plus q0 (weighted 1 vv.1)) (plus a0 (weighted 1 vv.2))
          have hvv_not_kept : vv ∉ kept := by
            intro hvk
            exact absurd (Std.HashSet.mem_insert.mpr (Or.inl (beq_iff_eq.mpr rfl))) (hdis vv hvk)
          have hnod' : (vv :: kept).Nodup := List.nodup_cons.mpr ⟨hvv_not_kept, hnod⟩
          have hdis' : ∀ v ∈ vv :: kept, v ∉ s0 := by
            intro v hv
            rcases List.mem_cons.mp hv with rfl | hvk
            · exact hvv_nots
            · intro hvs
              exact absurd (Std.HashSet.mem_insert.mpr (Or.inr hvs)) (hdis v hvk)
          have hmemset' : ∀ v, sf.contains v = true ↔ v ∈ vv :: kept ∨ s0.contains v = true := by
            intro v
            rw [hmemset]
            repeat rw [Std.HashSet.contains_iff_mem]
            rw [Std.HashSet.mem_insert, List.mem_cons]
            constructor
            · intro h
              rcases h with hk | h
              · exact Or.inl (Or.inr hk)
              · rcases h with hbe | hs
                · exact Or.inl (Or.inl (beq_iff_eq.mp hbe).symm)
                · exact Or.inr hs
            · intro h
              rcases h with h | hs
              · rcases h with heq | hk
                · exact Or.inr (Or.inl (beq_iff_eq.mpr heq.symm))
                · exact Or.inl hk
              · exact Or.inr (Or.inr hs)
          have hmemkept' : ∀ v, v ∈ vv :: kept ↔
              (∃ r ∈ (gg, vv) :: rows, (r.1 == g) = true ∧ r.2 = v) ∧ v ∉ s0 := by
            intro v
            rw [List.mem_cons]
            constructor
            · intro h
              rcases h with hveq | hvk
              · subst v
                refine ⟨⟨(gg, vv), by simp, hgg, rfl⟩, hvv_nots⟩
              · have hk := (hmemkept v).mp hvk
                rcases hk with ⟨⟨r, hr, hrg, hrv⟩, hnots⟩
                refine ⟨⟨r, by simp [hr], hrg, hrv⟩, ?_⟩
                intro hvs
                exact absurd (Std.HashSet.mem_insert.mpr (Or.inr hvs)) (hdis v hvk)
            · intro h
              rcases h with ⟨⟨r, hr, hrg, hrv⟩, hnots⟩
              rcases List.mem_cons.mp hr with heq | hrest
              · rcases heq with rfl
                left
                exact hrv.symm
              · by_cases hveq : v = vv
                · subst v
                  left
                  exact hveq
                · right
                  have hnot_insert : v ∉ s0.insert vv := by
                    intro hm
                    rcases (Std.HashSet.mem_insert.mp hm) with hvveq | hvs
                    · exact hveq (beq_iff_eq.mp hvveq).symm
                    · exact hnots hvs
                  exact (hmemkept v).mpr ⟨⟨r, hrest, hrg, hrv⟩, hnot_insert⟩
          refine ⟨vv :: kept, sf, qf, af, ?_, hnod', hdis', hmemset', hmemkept', ?_, ?_⟩
          · have hfold' : List.foldl (fun acc r =>
                if ¬r.1 = g ∨ r.2 ∈ acc.1 then acc
                else (acc.1.insert r.2, plus acc.2.1 (weighted 1 r.2.1), plus acc.2.2 (weighted 1 r.2.2)))
                (s0.insert vv, plus q0 (weighted 1 vv.1), plus a0 (weighted 1 vv.2)) rows = (sf, qf, af) := by
              simpa using hfold
            simp [hgg_eq, hvv_nots, hfold']
          · rw [hq, plus_assoc_t]
            congr 1
            exact (summarize_cons_t vv.1 (kept.map Prod.fst)).symm
          · rw [ha, plus_assoc_t]
            congr 1
            exact (summarize_cons_t vv.2 (kept.map Prod.snd)).symm
      · have hgg_false : (gg == g) = false := by
          cases h : gg == g <;> simp_all
        obtain ⟨kept, sf, qf, af, hfold, hnod, hdis, hmemset, hmemkept, hq, ha⟩ :=
          ih s0 q0 a0
        refine ⟨kept, sf, qf, af, ?_, hnod, hdis, hmemset, ?_, hq, ha⟩
        · have hgg_ne : gg ≠ g := by
            intro he
            have ht : (gg == g) = true := beq_iff_eq.mpr he
            rw [hgg_false] at ht
            contradiction
          have hfold' : List.foldl (fun acc r =>
              if ¬r.1 = g ∨ r.2 ∈ acc.1 then acc
              else (acc.1.insert r.2, plus acc.2.1 (weighted 1 r.2.1), plus acc.2.2 (weighted 1 r.2.2)))
              (s0, q0, a0) rows = (sf, qf, af) := by
            simpa using hfold
          simp [hgg_ne, hfold']
        · intro v
          constructor
          · intro hv
            have hvk := (hmemkept v).mp hv
            refine ⟨?_, hvk.2⟩
            rcases hvk.1 with ⟨r, hr, hrg, hrv⟩
            exact ⟨r, by simp [hr], hrg, hrv⟩
          · intro h
            apply (hmemkept v).mpr
            rcases h with ⟨⟨r, hr, hrg, hrv⟩, hnots⟩
            rcases List.mem_cons.mp hr with heq | hrest
            · rcases heq with rfl
              exact absurd hrg (by simp [hgg_false])
            · exact ⟨⟨r, hrest, hrg, hrv⟩, hnots⟩

private theorem nodup_filter_gen {α : Type} [BEq α] [LawfulBEq α]
    (p : α → Bool) (l : List α) : l.Nodup → (l.filter p).Nodup := by
  intro h
  induction l with
  | nil => exact List.Pairwise.nil
  | cons x xs ih =>
      have hx : x ∉ xs := (List.nodup_cons.mp h).1
      have hxs : xs.Nodup := (List.nodup_cons.mp h).2
      have ih' := ih hxs
      by_cases hp : p x
      · rw [List.filter_cons_of_pos hp]
        exact List.nodup_cons.mpr ⟨fun hm => hx ((List.mem_filter.mp hm).1), ih'⟩
      · rw [List.filter_cons_of_neg (by simpa using hp)]
        exact ih'

private theorem nodup_map_snd_of_fst_eq_g (l : List (Nat × Net)) (g : Nat)
    (h : ∀ r ∈ l, (r.1 == g) = true) (hn : l.Nodup) : (l.map Prod.snd).Nodup := by
  induction l with
  | nil => exact List.Pairwise.nil
  | cons r rs ih =>
      have hr : (r.1 == g) = true := h r (by simp)
      have hrs : ∀ x ∈ rs, (x.1 == g) = true := fun x hx => h x (by simp [hx])
      have hnrs : rs.Nodup := (List.nodup_cons.mp hn).2
      rw [List.map_cons]
      refine List.nodup_cons.mpr ⟨?_, ih hrs hnrs⟩
      intro hmem
      obtain ⟨r2, hr2mem, hr2eq⟩ := List.mem_map.mp hmem
      have hr2g : (r2.1 == g) = true := hrs r2 hr2mem
      have heqf : r.1 = r2.1 := by
        rw [beq_iff_eq.mp hr, beq_iff_eq.mp hr2g]
      have hrreq : r = r2 := Prod.ext heqf hr2eq.symm
      exact (List.nodup_cons.mp hn).1 (by rw [hrreq]; exact hr2mem)

theorem NetPairSet_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts.NetPairSet.Goal := by
  intro channels g
  dsimp only
  let rows := channels.flatten
  obtain ⟨kept, sf, qty, amt, hfold, hnod, hdis, hmemset, hmemkept, hq, ha⟩ :=
    distinctNet_fold_acc rows g (∅ : Std.HashSet Net) zero zero
  let vs := (rows.eraseDups.filter (fun r => r.1 == g)).map Prod.snd
  have hmemkeep : ∀ v, v ∈ kept ↔ ∃ r ∈ rows, (r.1 == g) = true ∧ r.2 = v := by
    intro v
    rw [hmemkept v]
    simp [Std.HashSet.not_mem_empty]
  have hmemvs : ∀ v, v ∈ vs ↔ ∃ r ∈ rows, (r.1 == g) = true ∧ r.2 = v := by
    intro v
    dsimp [vs]
    simp only [List.mem_map, List.mem_filter, List.mem_eraseDups]
    constructor
    · rintro ⟨r, ⟨hrrows, hrg⟩, hrv⟩
      exact ⟨r, hrrows, hrg, hrv⟩
    · rintro ⟨r, hrrows, hrg, hrv⟩
      exact ⟨r, ⟨hrrows, hrg⟩, hrv⟩
  have hnodvs : vs.Nodup := by
    dsimp [vs]
    apply nodup_map_snd_of_fst_eq_g
    · intro r hr
      exact (List.mem_filter.mp hr).2
    · exact nodup_filter_gen _ _ (nodup_eraseDups_gen _)
  have hperm : List.Perm kept vs := by
    apply List.perm_iff_count.mpr
    intro v
    have hcK := (List.nodup_iff_count_eq_ite.mp hnod) v
    have hcV := (List.nodup_iff_count_eq_ite.mp hnodvs) v
    rw [hcK, hcV]
    by_cases h : ∃ r ∈ rows, (r.1 == g) = true ∧ r.2 = v
    · simp [h, hmemkeep v, hmemvs v]
    · simp [h, hmemkeep v, hmemvs v]
  have hf : distinctNet rows g = (sf, qty, amt) := by
    simpa [distinctNet] using hfold
  rw [show distinctNet channels.flatten g = (sf, qty, amt) by simpa [rows] using hf]
  change (qty, amt) = (summarize (vs.map Prod.fst), summarize (vs.map Prod.snd))
  have hq' : qty = summarize (vs.map Prod.fst) := by
    rw [hq, plus_zero_left_t]
    exact summarize_perm (hperm.map Prod.fst)
  have ha' : amt = summarize (vs.map Prod.snd) := by
    rw [ha, plus_zero_left_t]
    exact summarize_perm (hperm.map Prod.snd)
  exact Prod.ext hq' ha'

private theorem lcp_le_left (a b : Path) : lcp a b ≤ a.length := by
  induction a generalizing b with
  | nil => simp [lcp]
  | cons x xs ih =>
      cases b with
      | nil => simp [lcp]
      | cons y ys =>
          by_cases h : x == y
          · simp [lcp, h]
            have hi := ih ys
            omega
          · simp [lcp, h]

private theorem lcp_le_right (a b : Path) : lcp a b ≤ b.length := by
  induction a generalizing b with
  | nil =>
      cases b with
      | nil => simp [lcp]
      | cons y ys => simp [lcp]
  | cons x xs ih =>
      cases b with
      | nil => simp [lcp]
      | cons y ys =>
          by_cases h : x == y
          · simp [lcp, h]
            have hi := ih ys
            omega
          · simp [lcp, h]

private theorem take_eq_of_le_lcp (a b : Path) {d : Nat} (h : d ≤ lcp a b) :
    a.take d = b.take d := by
  induction a generalizing b d with
  | nil =>
      cases b with
      | nil => simp
      | cons y ys =>
          simp [lcp] at h
          subst d
          simp
  | cons x xs ih =>
      cases b with
      | nil =>
          simp [lcp] at h
          subst d
          simp
      | cons y ys =>
          by_cases hxy : x == y
          · have hxeq : x = y := beq_iff_eq.mp hxy
            subst y
            simp [lcp, hxy] at h
            cases d with
            | zero => simp
            | succ d' =>
                simp only [List.take]
                congr 1
                apply ih ys
                omega
          · simp [lcp, hxy] at h
            subst d
            simp

private theorem take_ne_of_lcp_lt (a b : Path) {d : Nat}
    (h : lcp a b < d) (hle : d ≤ a.length) (hlen : a.length = b.length) :
    a.take d ≠ b.take d := by
  induction a generalizing b d with
  | nil =>
      simp at hle
      omega
  | cons x xs ih =>
      cases b with
      | nil =>
          simp at hlen
      | cons y ys =>
          by_cases hxy : x == y
          · have hxeq : x = y := beq_iff_eq.mp hxy
            subst y
            simp [lcp, hxy] at h
            cases d with
            | zero => omega
            | succ d' =>
                have hd' : lcp xs ys < d' := by omega
                have hle' : d' ≤ xs.length := by simp at hle; omega
                have hneq := ih ys hd' hle' (by simpa using hlen)
                intro heq
                apply hneq
                simpa using heq
          · have hxne : x ≠ y := by
              intro he
              apply hxy
              exact beq_iff_eq.mpr he
            have hL0 : lcp (x :: xs) (y :: ys) = 0 := by simp [lcp, hxy]
            rw [hL0] at h
            cases d with
            | zero => omega
            | succ d' =>
                intro heq
                simp at heq
                exact hxne heq.1

private theorem filter_range_empty_of_gt (L n : Nat) (h : ∀ d, d < n → d ≤ L) :
    (List.range n).filter (fun d => L < d) = [] := by
  apply List.eq_nil_iff_forall_not_mem.mpr
  intro d hd
  rw [List.mem_filter] at hd
  have hr := List.mem_range.mp hd.1
  have hle := h d hr
  have hlt : L < d := by simpa using hd.2
  omega

private theorem filter_range_gt_add (L k : Nat) :
    (List.range (L + k + 1)).filter (fun d => L < d) =
      (List.range k).map (fun i => L + i + 1) := by
  induction k with
  | zero =>
      simpa using filter_range_empty_of_gt L (L + 1) (fun d hd => by omega)
  | succ k ih =>
      rw [show L + Nat.succ k + 1 = (L + k + 1) + 1 by omega]
      rw [List.range_succ, List.filter_append, ih]
      have hpos : L < L + k + 1 := by omega
      simp [hpos]
      rw [List.range_succ, List.map_append]
      congr 1

theorem SuffixFlush_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts.SuffixFlush.Goal := by
  intro a b hlen
  let L := lcp a b
  have hLle : L ≤ a.length := by dsimp [L]; exact lcp_le_left a b
  have hpred : ∀ d ∈ List.range (a.length + 1),
      (a.take d != b.take d) = decide (L < d) := by
    intro d hd
    have hdle : d ≤ a.length := by
      have := List.mem_range.mp hd
      omega
    by_cases hle : d ≤ L
    · have heq := take_eq_of_le_lcp a b hle
      have hbeq : (a.take d == b.take d) = true := beq_iff_eq.mpr heq
      have hnlt : ¬ L < d := by omega
      simp [hbeq, hnlt]
      exact heq
    · have hlt : L < d := by omega
      have hne := take_ne_of_lcp_lt a b hlt hdle hlen
      have hbeq : (a.take d == b.take d) = false := by
        cases h : (a.take d == b.take d) with
        | true => exact False.elim (hne (beq_iff_eq.mp h))
        | false => rfl
      simp [hbeq, hlt]
      exact hne
  rw [List.filter_congr hpred]
  rw [show a.length + 1 = L + (a.length - L) + 1 by omega]
  exact filter_range_gt_add L (a.length - L)

private theorem countP_range_all {p : Nat → Bool} (n : Nat)
    (h : ∀ i, i < n → p i = true) : (List.range n).countP p = n := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      simp [h n (by omega), ih (fun i hi => h i (by omega))]

private theorem countP_range_none {p : Nat → Bool} (n : Nat)
    (h : ∀ i, i < n → p i = false) : (List.range n).countP p = 0 := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      simp [h n (by omega), ih (fun i hi => h i (by omega))]

private theorem sorted_getD_le (l : List Int) (hs : l.Pairwise (fun a b => a ≤ b)) :
    ∀ i j, i ≤ j → j < l.length → l.getD i 0 ≤ l.getD j 0 := by
  induction l with
  | nil => intro i j hij hj; simp at hj
  | cons x xs ih =>
      intro i j hij hj
      simp at hj
      cases hs with
      | cons hfirst htail =>
          cases i with
          | zero =>
              by_cases hj0 : j = 0
              · subst j
                simp [List.getD]
              · have hjpos : 0 < j := Nat.pos_of_ne_zero hj0
                have hidx : j - 1 < xs.length := by omega
                have hjval : (x :: xs).getD j 0 = xs.getD (j - 1) 0 := by
                  rw [show j = (j - 1) + 1 by omega]
                  simp [List.getD, List.getElem?_cons_succ]
                rw [hjval]
                simpa [List.getD] using hfirst (xs.getD (j - 1) 0) (by
                  rw [show xs.getD (j - 1) 0 = xs.get ⟨j - 1, hidx⟩ by simp [List.getD, hidx]]
                  exact List.get_mem xs ⟨j - 1, hidx⟩)
          | succ i' =>
              have hi'j : i' ≤ j - 1 := by omega
              have hjl : j - 1 < xs.length := by omega
              have hres := ih htail i' (j - 1) hi'j hjl
              have hi_succ : (x :: xs).getD (i' + 1) 0 = xs.getD i' 0 := by
                simp [List.getD, List.getElem?_cons_succ]
              have hj_succ : (x :: xs).getD j 0 = xs.getD (j - 1) 0 := by
                have hjpos : 0 < j := by omega
                rw [show j = (j - 1) + 1 by omega]
                simp [List.getD, List.getElem?_cons_succ]
              rw [hi_succ, hj_succ]
              exact hres

private def lowerCnt (a : Array Int) (q : Int) (lo hi : Nat) : Nat :=
  (List.range (hi - lo)).countP (fun i => a.getD (lo + i) 0 < q)

private theorem lowerRank_eq_cnt (a : Array Int) (q : Int)
    (hsort : ∀ i j, i ≤ j → j < a.size → a.getD i 0 ≤ a.getD j 0) :
    ∀ lo hi, lo ≤ hi → hi ≤ a.size →
      lowerRank a q lo hi = lo + lowerCnt a q lo hi := by
  intro lo hi hlohi hhile
  induction h : hi - lo using Nat.strongRecOn generalizing lo hi with
  | ind n ih =>
      by_cases hlt : lo < hi
      · rw [lowerRank]
        simp [hlt]
        let mid := (lo + hi) / 2
        change (if a[mid]?.any (fun x : Int => x < q) then lowerRank a q (mid + 1) hi
          else lowerRank a q lo mid) = lo + lowerCnt a q lo hi
        have hmlo : lo ≤ mid := by omega
        have hmhi : mid < hi := by omega
        have hmsize : mid < a.size := by omega
        by_cases hav : a[mid]?.any (fun x : Int => x < q)
        · have hav' : a.getD mid 0 < q := by
            simpa [hmsize, Array.getD] using hav
          have hstep := ih (hi - (mid + 1)) (by omega) (mid + 1) hi (by omega) (by omega) (by rfl)
          rw [hstep]
          have hsplit : lowerCnt a q lo hi = (mid + 1 - lo) + lowerCnt a q (mid + 1) hi := by
            unfold lowerCnt
            have hidx : hi - lo = (mid + 1 - lo) + (hi - (mid + 1)) := by omega
            rw [hidx, List.range_add, List.countP_append]
            have hfirst : (List.range (mid + 1 - lo)).countP (fun i => a.getD (lo + i) 0 < q) = mid + 1 - lo := by
              apply countP_range_all
              intro i hir
              have hleidx : lo + i ≤ mid := by omega
              have hle := hsort (lo + i) mid hleidx hmsize
              have hlt : a.getD (lo + i) 0 < q := by omega
              simpa using hlt
            rw [hfirst]
            congr 1
            rw [List.countP_map]
            apply List.countP_congr
            intro i hi
            change (decide (a.getD (lo + ((mid + 1 - lo) + i)) 0 < q) = true ↔
              decide (a.getD ((mid + 1) + i) 0 < q) = true)
            have hidx : lo + ((mid + 1 - lo) + i) = (mid + 1) + i := by omega
            rw [hidx]
          rw [hsplit]
          simp [hav]
          have hsub : lo + (mid + 1 - lo) = mid + 1 := Nat.add_sub_of_le (by omega : lo ≤ mid + 1)
          omega
        · have hav' : ¬ a.getD mid 0 < q := by
            intro hlt
            apply hav
            simpa [hmsize, Array.getD] using hlt
          have hstep := ih (mid - lo) (by omega) lo mid hmlo (by omega) (by rfl)
          rw [hstep]
          have hsplit : lowerCnt a q lo hi = lowerCnt a q lo mid := by
            unfold lowerCnt
            have hidx : hi - lo = (mid - lo) + (hi - mid) := by omega
            rw [hidx, List.range_add, List.countP_append, List.countP_map]
            have hcmp : List.countP
                ((fun i => decide (a.getD (lo + i) 0 < q)) ∘ fun x => mid - lo + x)
                (List.range (hi - mid)) =
                List.countP (fun i => decide (a.getD (mid + i) 0 < q))
                  (List.range (hi - mid)) := by
              apply List.countP_congr
              intro i hir
              change (decide (a.getD (lo + ((mid - lo) + i)) 0 < q) = true ↔
                decide (a.getD (mid + i) 0 < q) = true)
              have hidx2 : lo + ((mid - lo) + i) = mid + i := by omega
              rw [hidx2]
            rw [hcmp]
            have hsecond : (List.range (hi - mid)).countP (fun i => a.getD (mid + i) 0 < q) = 0 := by
              apply countP_range_none
              intro i hir
              have hle := hsort mid (mid + i) (by omega) (by omega)
              have hnle : q ≤ a.getD (mid + i) 0 := by omega
              simpa [Array.getD] using hnle
            rw [hsecond, Nat.add_zero]
          rw [hsplit]
          simp [hav]
      · have hlohi' : lo = hi := by omega
        subst hi
        rw [lowerRank]
        simp [lowerCnt]

private theorem array_getD_toArray (l : List Int) (i : Nat) :
    l.toArray.getD i 0 = l.getD i 0 := by
  by_cases h : i < l.length
  · have hla : l.toArray[i] = l[i] := List.getElem_toArray (by simpa using h)
    simp [Array.getD, List.getD, h, hla]
  · simp [Array.getD, List.getD, h]

private theorem lowerCnt_eq_filter_count (l : List Int) (q : Int) :
    lowerCnt l.toArray q 0 l.toArray.size = (l.filter fun x => x < q).length := by
  unfold lowerCnt
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [show (x :: xs).toArray.size = xs.toArray.size + 1 by simp]
      simp only [Nat.sub_zero, Nat.zero_add]
      rw [show List.range (xs.toArray.size + 1) =
          [0] ++ (List.range xs.toArray.size).map (fun i => i + 1) by
        rw [show xs.toArray.size + 1 = 1 + xs.toArray.size by omega, List.range_add]
        simp [Nat.add_comm]]
      rw [List.countP_append, List.countP_map]
      have hcmp : List.countP
          ((fun i => decide ((x :: xs).toArray.getD i 0 < q)) ∘ fun i => i + 1)
          (List.range xs.toArray.size) =
          List.countP (fun i => decide (xs.toArray.getD i 0 < q))
            (List.range xs.toArray.size) := by
        apply List.countP_congr
        intro i hi
        change (decide ((x :: xs).toArray.getD (i + 1) 0 < q) = true ↔
          decide (xs.toArray.getD i 0 < q) = true)
        simp [Array.getD]
      rw [hcmp]
      simp only [Nat.zero_add, Nat.sub_zero] at ih
      rw [ih]
      by_cases hx : x < q <;> simp [hx, Nat.add_comm]

private theorem count_filterMap_lt (inv : List (Option Int)) (q : Int) :
    ((inv.filterMap id).filter fun x => x < q).length =
      inv.countP (fun u => u.any fun x => x < q) := by
  induction inv with
  | nil => rfl
  | cons u us ih =>
      cases u with
      | none => simpa using ih
      | some x =>
          by_cases hx : x < q <;> simp [hx, ih]

theorem ThresholdFanout_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts.ThresholdFanout.Goal := by
  intro inv sales
  dsimp only
  let l := (inv.filterMap id).mergeSort (fun x y : Int => x ≤ y)
  let a := l.toArray
  have hpair : l.Pairwise (fun x y : Int => x ≤ y) := by
    dsimp [l]
    simpa using List.pairwise_mergeSort (le := fun x y : Int => decide (x ≤ y)) (trans := by
      intro x y z hxy hyz
      have hxy' : x ≤ y := by simpa using hxy
      have hyz' : y ≤ z := by simpa using hyz
      simp
      omega) (total := by
      intro x y
      by_cases h : x ≤ y
      · simp [h]
      · have hyx : y ≤ x := by omega
        simp [h, hyx]) (inv.filterMap id)
  have hsort : ∀ i j, i ≤ j → j < a.size → a.getD i 0 ≤ a.getD j 0 := by
    intro i j hij hj
    have hji : j < l.length := by simpa [a] using hj
    have hli := sorted_getD_le l hpair i j hij hji
    have hai : a.getD i 0 = l.getD i 0 := by dsimp [a]; exact array_getD_toArray l i
    have haj : a.getD j 0 = l.getD j 0 := by dsimp [a]; exact array_getD_toArray l j
    rw [hai, haj]
    exact hli
  have hlow : ∀ q : Int, lowerRank a q 0 a.size = (l.filter fun x => x < q).length := by
    intro q
    have hr := lowerRank_eq_cnt a q hsort 0 a.size (by omega) (by omega)
    rw [hr]
    simp only [Nat.zero_add]
    dsimp [a]
    exact lowerCnt_eq_filter_count l q
  induction sales with
  | nil => rfl
  | cons s ss ih =>
      rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons, ih]
      congr 1
      rcases s with ⟨v, w⟩
      cases v with
      | none => simp
      | some q =>
          have hfl : (l.filter fun x : Int => x < q).length =
              ((inv.filterMap id).filter fun x : Int => x < q).length := by
            dsimp [l]
            have hp := (List.mergeSort_perm (inv.filterMap id) (fun x y : Int => x ≤ y)).filter
              (fun x : Int => x < q)
            exact hp.length_eq
          change w * lowerRank a q 0 a.size = w * inv.countP (fun u => u.any fun x => x < q)
          rw [hlow q, hfl, count_filterMap_lt]

private theorem fold_insert_mem_acc {α : Type} [BEq α] [Hashable α] [EquivBEq α]
    [LawfulHashable α] [LawfulBEq α]
    (xs : List α) (s : Std.HashSet α) (a : α) :
    a ∈ xs.foldl (fun (s : Std.HashSet α) k => s.insert k) s ↔ a ∈ s ∨ a ∈ xs := by
  induction xs generalizing s with
  | nil => simp
  | cons k ks ih =>
      rw [List.foldl_cons, ih, Std.HashSet.mem_insert]
      constructor
      · intro h
        rcases h with hleft | hk
        · rcases hleft with hkeq | has
          · have hka : k = a := beq_iff_eq.mp hkeq
            subst a
            exact Or.inr (by simp)
          · exact Or.inl has
        · exact Or.inr (by simp [hk])
      · intro h
        rcases h with has | hmem
        · exact Or.inl (Or.inr has)
        · rw [List.mem_cons] at hmem
          rcases hmem with hka | hk
          · subst a
            exact Or.inl (Or.inl (beq_iff_eq.mpr rfl))
          · exact Or.inr hk

private theorem fold_insert_mem {α : Type} [BEq α] [Hashable α] [EquivBEq α]
    [LawfulHashable α] [LawfulBEq α]
    (xs : List α) (a : α) : a ∈ xs.foldl (fun (s : Std.HashSet α) k => s.insert k) ∅ ↔ a ∈ xs := by
  simpa using fold_insert_mem_acc xs (∅ : Std.HashSet α) a

private theorem filter_not_mem_insert {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α] [LawfulBEq α]
    (xs : List α) (s : Std.HashSet α) (a : α) :
    xs.filter (fun k => k ∉ s.insert a) = (xs.filter fun k => k ∉ s).filter (fun k => !k == a) := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro x hx
  by_cases hxs : x ∈ s
  · have hins : x ∈ s.insert a := Std.HashSet.mem_insert.mpr (Or.inr hxs)
    simp [hxs, hins]
  · by_cases hxa : x == a
    · have hax : (a == x) = true := beq_iff_eq.mpr (beq_iff_eq.mp hxa).symm
      have hins : x ∈ s.insert a := Std.HashSet.mem_insert.mpr (Or.inl hax)
      simp [hxs, hxa, hins]
    · have hax : (a == x) = false := by
        cases h : a == x with
        | true => exact False.elim (hxa (beq_iff_eq.mpr (beq_iff_eq.mp h).symm))
        | false => rfl
      have hins : x ∉ s.insert a := by
        intro hm
        rcases Std.HashSet.mem_insert.mp hm with h1 | h2
        · rw [hax] at h1
          contradiction
        · exact hxs h2
      simp [hxs, hxa, hax, hins]

private theorem fold_insert_size_acc {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α] [LawfulBEq α]
    (xs : List α) (s : Std.HashSet α) :
    (xs.foldl (fun (s : Std.HashSet α) k => s.insert k) s).size =
      s.size + (xs.filter fun k => k ∉ s).eraseDups.length := by
  induction xs generalizing s with
  | nil => simp
  | cons a as ih =>
      rw [List.foldl_cons]
      have ht := ih (s.insert a)
      rw [ht, Std.HashSet.size_insert]
      by_cases ha : a ∈ s
      · rw [if_pos ha]
        have hf : (a :: as).filter (fun k => k ∉ s) = as.filter (fun k => k ∉ s) := by simp [ha]
        rw [hf]
        have hf2 : as.filter (fun k => k ∉ s.insert a) = as.filter (fun k => k ∉ s) := by
          apply List.filter_congr
          intro x hx
          have hiff : x ∈ s.insert a ↔ x ∈ s := by
            constructor
            · intro hm
              rcases Std.HashSet.mem_insert.mp hm with hxa | hxs
              · have hxa' : a = x := beq_iff_eq.mp hxa
                subst x
                exact ha
              · exact hxs
            · intro hxs
              exact Std.HashSet.mem_insert.mpr (Or.inr hxs)
          simp [hiff]
        rw [hf2]
      · rw [if_neg ha]
        have hf : (a :: as).filter (fun k => k ∉ s) = a :: as.filter (fun k => k ∉ s) := by simp [ha]
        rw [hf]
        simp only [List.eraseDups_cons, List.length_cons]
        rw [filter_not_mem_insert]
        omega

private theorem fold_erase_size_acc {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α] [LawfulBEq α]
    (xs : List α) (s : Std.HashSet α) :
    (xs.foldl (fun (s : Std.HashSet α) k => s.erase k) s).size =
      s.size - (xs.filter fun k => k ∈ s).eraseDups.length := by
  induction xs generalizing s with
  | nil => simp
  | cons a as ih =>
      rw [List.foldl_cons]
      have ht := ih (s.erase a)
      rw [ht, Std.HashSet.size_erase]
      by_cases ha : a ∈ s
      · rw [if_pos ha]
        have hf : as.filter (fun k => k ∈ s.erase a) =
            (as.filter fun k => k ∈ s).filter (fun k => !k == a) := by
          rw [List.filter_filter]
          apply List.filter_congr
          intro x hx
          by_cases hxs : x ∈ s
          · by_cases hxa : x == a
            · have hax : (a == x) = true := beq_iff_eq.mpr (beq_iff_eq.mp hxa).symm
              have hnotin : x ∉ s.erase a := by
                intro hm
                have hres := (Std.HashSet.mem_erase.mp hm).1
                rw [hax] at hres
                contradiction
              simp [hxs, hxa, hnotin]
            · have hax : (a == x) = false := by
                cases h : a == x with
                | true => exact False.elim (hxa (beq_iff_eq.mpr (beq_iff_eq.mp h).symm))
                | false => rfl
              simp [hxs, hxa, hax]
          · have hnotin : x ∉ s.erase a := by
              intro hm
              exact hxs (Std.HashSet.mem_of_mem_erase hm)
            simp [hxs, hnotin]
        have hfhead : (a :: as).filter (fun k => k ∈ s) = a :: as.filter (fun k => k ∈ s) := by simp [ha]
        rw [hfhead]
        simp only [List.eraseDups_cons, List.length_cons]
        rw [hf]
        have hpos : 0 < s.size := by
          have hne : s.size ≠ 0 := by
            intro hs0
            have hiemp : s.isEmpty = true := by simpa [Std.HashSet.isEmpty_eq_size_eq_zero, hs0]
            exact (Std.HashSet.not_mem_of_isEmpty hiemp) ha
          omega
        omega
      · rw [if_neg ha]
        have hfhead : (a :: as).filter (fun k => k ∈ s) = as.filter (fun k => k ∈ s) := by simp [ha]
        rw [hfhead]
        have hf2 : as.filter (fun k => k ∈ s.erase a) = as.filter (fun k => k ∈ s) := by
          apply List.filter_congr
          intro x hx
          by_cases hxs : x ∈ s
          · have hax : (a == x) = false := by
              cases h : a == x with
              | true =>
                  have hax' : a = x := beq_iff_eq.mp h
                  subst x
                  exact False.elim (ha hxs)
              | false => rfl
            simp [Std.HashSet.mem_erase, hxs, hax]
          · have hnotin : x ∉ s.erase a := by
              intro hm
              exact hxs (Std.HashSet.mem_of_mem_erase hm)
            simp [hxs, hnotin]
        rw [hf2]

private theorem fold_erase_le {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α] [LawfulBEq α]
    (xs : List α) (s : Std.HashSet α) :
    (xs.foldl (fun (s : Std.HashSet α) k => s.erase k) s).size ≤ s.size := by
  induction xs generalizing s with
  | nil => exact Nat.le_refl _
  | cons a as ih =>
      rw [List.foldl_cons]
      exact Nat.le_trans (ih (s.erase a)) Std.HashSet.size_erase_le

private theorem perm_of_nodup_mem {α : Type} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hm : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  apply List.perm_iff_count.mpr
  intro x
  have c₁ := (List.nodup_iff_count_eq_ite.mp h₁) x
  have c₂ := (List.nodup_iff_count_eq_ite.mp h₂) x
  rw [c₁, c₂]
  by_cases h : x ∈ l₁
  · have h2 : x ∈ l₂ := (hm x).mp h
    simp [h, h2]
  · have h2 : x ∉ l₂ := fun hx => h ((hm x).mpr hx)
    simp [h, h2]

private theorem filter_complement_length_of_nodup {α : Type} (l : List α) (p : α → Bool)
    (hn : l.Nodup) : (l.filter p).length + (l.filter fun x => !p x).length = l.length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hn' : as.Nodup := (List.nodup_cons.mp hn).2
      have hi := ih hn'
      by_cases hp : p a <;> simp [hp, hi] <;> omega

theorem ExclusionErase_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts.ExclusionErase.Goal := by
  intro store catalog web
  dsimp only
  let active := store.foldl (fun (s : Std.HashSet _) k => s.insert k) {}
  let rest := (catalog ++ web).foldl (fun (s : Std.HashSet _) k => s.erase k) active
  have hactive : active.size = store.eraseDups.length := by
    dsimp [active]
    have h := fold_insert_size_acc store (∅ : Std.HashSet (Option Nat × Option Nat))
    simp [Std.HashSet.not_mem_empty] at h
    have hf : store.filter (fun _ => true) = store := by simp
    rw [hf] at h
    exact h
  have hmemactive : ∀ k, k ∈ active ↔ k ∈ store := by
    intro k
    dsimp [active]
    exact fold_insert_mem store k
  have hrest_le : rest.size ≤ active.size := by
    dsimp [rest]
    exact fold_erase_le (catalog ++ web) active
  refine ⟨?_, hrest_le⟩
  have hrest_size : rest.size = active.size -
      ((catalog ++ web).filter fun k => k ∈ active).eraseDups.length := by
    dsimp [rest]
    exact fold_erase_size_acc (catalog ++ web) active
  rw [hrest_size, hactive]
  let removed := ((catalog ++ web).filter fun k => k ∈ active).eraseDups
  let kept := (store.eraseDups.filter fun k => !catalog.contains k && !web.contains k)
  have hremoved_mem : ∀ k, k ∈ removed ↔ (k ∈ store ∧ (k ∈ catalog ∨ k ∈ web)) := by
    intro k
    dsimp [removed]
    rw [List.mem_eraseDups, List.mem_filter, List.mem_append]
    constructor
    · intro h
      exact ⟨(hmemactive k).mp h.2, h.1⟩
    · intro h
      refine ⟨h.2, (hmemactive k).mpr h.1⟩
  let p : (Option Nat × Option Nat) → Bool := fun k => catalog.contains k || web.contains k
  have hremoved_eq : removed.length = (store.eraseDups.filter p).length := by
    have hnRemoved : removed.Nodup := by
      dsimp [removed]
      exact nodup_eraseDups_gen _
    have hp := perm_of_nodup_mem hnRemoved
      (nodup_filter_gen p store.eraseDups (nodup_eraseDups_gen store)) (by
        intro k
        rw [hremoved_mem]
        dsimp [p]
        rw [List.mem_filter, List.mem_eraseDups]
        by_cases hc : catalog.contains k <;> by_cases hw : web.contains k <;> simp [hc, hw])
    exact hp.length_eq
  have hkept_eq : kept.length = (store.eraseDups.filter fun k => !p k).length := by
    apply congrArg List.length
    apply List.filter_congr
    intro k hk
    dsimp [kept, p]
    by_cases hc : catalog.contains k <;> by_cases hw : web.contains k <;> simp [hc, hw]
  have hpart : removed.length + kept.length = store.eraseDups.length := by
    rw [hremoved_eq, hkept_eq]
    exact filter_complement_length_of_nodup store.eraseDups p (nodup_eraseDups_gen store)
  change store.eraseDups.length - removed.length = kept.length
  omega

private theorem optnat_compare_eq {a b : Option Nat} (h : compare a b = .eq) : a = b := by
  cases a <;> cases b <;> simp at h ⊢ <;> omega

private theorem path_compare_self_le (a : Path) : compare a a != .gt := by
  induction a with
  | nil => rfl
  | cons x xs ih =>
      cases x <;> simp [ih]

private theorem path_compare_eq_of_eq : ∀ a b : Path, compare a b = .eq → a = b
  | [], [], _ => rfl
  | [], _ :: _, h => by simp at h
  | _ :: _, [], h => by simp at h
  | none :: xs, none :: ys, h => by
      simp [compare, List.compareLex] at h
      exact congrArg (fun l => (none : Option Nat) :: l) (path_compare_eq_of_eq xs ys h)
  | none :: xs, some n :: ys, h => by simp [compare, List.compareLex] at h
  | some m :: xs, none :: ys, h => by simp [compare, List.compareLex] at h
  | some m :: xs, some n :: ys, h => by
      cases hcmp : compareOfLessAndEq m n with
      | lt => simp [compare, List.compareLex, hcmp] at h
      | eq =>
          have hcmp' : compare m n = .eq := by
            change compareOfLessAndEq m n = .eq
            exact hcmp
          have hmn : m = n := Nat.compare_eq_eq.mp hcmp'
          subst n
          simp [compare, List.compareLex, hcmp] at h
          have htail : compare xs ys = .eq := by
            change List.compareLex (fun a b : Option Nat => compare a b) xs ys = .eq at h
            exact h
          exact congrArg (fun l => (some m : Option Nat) :: l) (path_compare_eq_of_eq xs ys htail)
      | gt => simp [compare, List.compareLex, hcmp] at h

private theorem compareLex_of_compare {a b : Path} (o : Ordering) (h : compare a b = o) :
    List.compareLex (fun x y : Option Nat => compare x y) a b = o := by
  simpa [List.compare_eq_compareLex] using h

private theorem path_compare_total : ∀ a b : Path,
    compare a b = .gt ∨ compare b a = .gt ∨ compare a b = .eq := by
  intro a
  induction a with
  | nil =>
      intro b
      cases b with
      | nil => simp
      | cons y ys => simp
  | cons x xs ih =>
      intro b
      cases b with
      | nil => simp
      | cons y ys =>
          cases x with
          | none =>
              cases y with
              | none =>
                  simp [compare, List.compareLex]
                  rcases ih ys with h | h | h
                  · exact Or.inl (compareLex_of_compare .gt h)
                  · exact Or.inr (Or.inl (compareLex_of_compare .gt h))
                  · exact Or.inr (Or.inr (compareLex_of_compare .eq h))
              | some n => simp [compare, List.compareLex]
          | some m =>
              cases y with
              | none => simp [compare, List.compareLex]
              | some n =>
                  cases hcmp : compareOfLessAndEq m n with
                  | lt =>
                      have hlt : m < n := by
                        rw [← Nat.compare_eq_lt]
                        exact hcmp
                      have hcmp2 : compareOfLessAndEq n m = .gt := by
                        change compare n m = .gt
                        exact Nat.compare_eq_gt.mpr hlt
                      simp [compare, List.compareLex, hcmp, hcmp2]
                  | eq =>
                      have hcmp' : compare m n = .eq := by
                        change compareOfLessAndEq m n = .eq
                        exact hcmp
                      have hmn : m = n := Nat.compare_eq_eq.mp hcmp'
                      subst n
                      simp [compare, List.compareLex, hcmp]
                      rcases ih ys with h | h | h
                      · exact Or.inl (compareLex_of_compare .gt h)
                      · exact Or.inr (Or.inl (compareLex_of_compare .gt h))
                      · exact Or.inr (Or.inr (compareLex_of_compare .eq h))
                  | gt =>
                      have hgt : n < m := by
                        rw [← Nat.compare_eq_gt]
                        exact hcmp
                      have hcmp2 : compareOfLessAndEq n m = .lt := by
                        change compare n m = .lt
                        exact Nat.compare_eq_lt.mpr hgt
                      simp [compare, List.compareLex, hcmp, hcmp2]

private theorem path_compare_eq_of_both {a b : Path}
    (hab : ¬ compare a b = .gt) (hba : ¬ compare b a = .gt) : a = b := by
  rcases path_compare_total a b with hgt | hgt | heq
  · exact False.elim (hab hgt)
  · exact False.elim (hba hgt)
  · exact path_compare_eq_of_eq a b heq

private theorem eraseDups_append_of_mem {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (a : α) (ha : a ∈ l) : (l ++ [a]).eraseDups = l.eraseDups := by
  rw [List.eraseDups_append]
  have hnil : [a].removeAll l = [] := by
    simp [List.removeAll, ha]
  rw [hnil]
  simp

private theorem eraseDups_append_of_not_mem {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (a : α) (ha : a ∉ l) : (l ++ [a]).eraseDups = l.eraseDups ++ [a] := by
  rw [List.eraseDups_append]
  have hsingle : [a].removeAll l = [a] := by
    simp [List.removeAll, ha]
  rw [hsingle]
  rfl

private def keyOf (depth : Nat) (r : Fact) : Path := r.1.take depth

private def valsOf (depth : Nat) (rs : List Fact) (k : Path) : List (Option Int) :=
  (rs.filter fun r => keyOf depth r == k).map Prod.snd

private theorem valsOf_append_same (depth : Nat) (rs : List Fact) (r : Fact)
    (h : keyOf depth r == (keyOf depth r)) :
    valsOf depth (rs ++ [r]) (keyOf depth r) = valsOf depth rs (keyOf depth r) ++ [r.2] := by
  dsimp [valsOf]
  rw [List.filter_append, List.map_append]
  simp

private theorem valsOf_append_ne (depth : Nat) (rs : List Fact) (r : Fact) (k : Path)
    (h : keyOf depth r != k) : valsOf depth (rs ++ [r]) k = valsOf depth rs k := by
  have hne : keyOf depth r ≠ k := by
    intro heq
    subst k
    simp at h
  dsimp [valsOf]
  rw [List.filter_append, List.map_append]
  have hnil : ([r].filter fun x => keyOf depth x == k) = [] := by
    simp [hne]
  rw [hnil]
  simp

private theorem pairwise_before (depth : Nat) {pre rest : List Fact} {r : Fact}
    (h : (pre ++ r :: rest).Pairwise
      (fun a b => compare (keyOf depth a) (keyOf depth b) != .gt))
    (r2 : Fact) (hr2 : r2 ∈ pre) :
    compare (keyOf depth r2) (keyOf depth r) != .gt := by
  induction pre generalizing rest with
  | nil => simp at hr2
  | cons x xs ih =>
      cases h with
      | cons hx htail =>
          rw [List.mem_cons] at hr2
          rcases hr2 with rfl | hmem
          · simpa using hx r (by simp)
          · exact ih htail hmem

private theorem valsOf_eq_nil_of_not_mem (depth : Nat) (rs : List Fact) (k : Path)
    (h : k ∉ rs.map (keyOf depth)) : valsOf depth rs k = [] := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      have hne : keyOf depth r ≠ k := by
        intro heq
        apply h
        rw [List.map_cons]
        exact List.mem_cons.mpr (Or.inl heq.symm)
      have hbeq : (keyOf depth r == k) = false := by
        cases hb : keyOf depth r == k with
        | true => exact False.elim (hne (beq_iff_eq.mp hb))
        | false => rfl
      have htail : k ∉ rs.map (keyOf depth) := by
        intro hm
        apply h
        rw [List.map_cons]
        exact List.mem_cons.mpr (Or.inr hm)
      have hall : ∀ f ∈ rs, ¬ keyOf depth f = k := by
        intro f hf heq
        apply htail
        rw [List.mem_map]
        exact ⟨f, hf, heq⟩
      have hfilter : rs.filter (fun x => keyOf depth x == k) = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro x hx
        have hxk : keyOf depth x ≠ k := hall x (List.mem_filter.mp hx).1
        have hb' : (keyOf depth x == k) = false := by
          cases hb : keyOf depth x == k with
          | true => exact False.elim (hxk (beq_iff_eq.mp hb))
          | false => rfl
        have hbk := (List.mem_filter.mp hx).2
        rw [hb'] at hbk
        contradiction
      dsimp [valsOf]
      rw [List.filter_cons_of_neg (by simpa using hbeq), hfilter]
      simp [ih htail]

private def rollupStep (depth : Nat) (acc : List (Path × Totals)) (r : Fact) : List (Path × Totals) :=
  let k := keyOf depth r
  match acc with
  | [] => [(k, weighted 1 r.2)]
  | (last, a) :: done =>
      if last == k then (k, plus a (weighted 1 r.2)) :: done
      else (k, weighted 1 r.2) :: acc

private def RollInv (depth : Nat) (pre : List Fact) (acc : List (Path × Totals)) : Prop :=
  (acc.map Prod.fst).Nodup ∧
  acc.map Prod.fst = (pre.map (keyOf depth)).eraseDups.reverse ∧
  (∀ k t, (k, t) ∈ acc → t = summarize (valsOf depth pre k)) ∧
  (acc ≠ [] → ∃ kmax, ∃ r ∈ pre,
      (∃ t done_, acc = (kmax, t) :: done_) ∧
      keyOf depth r = kmax ∧
      ∀ r2 ∈ pre, compare (keyOf depth r2) kmax != .gt)

private theorem rollup_step_inv (depth : Nat) {pre rest : List Fact} {r : Fact}
    {acc : List (Path × Totals)}
    (hsorted : (pre ++ r :: rest).Pairwise
      (fun a b => compare (keyOf depth a) (keyOf depth b) != .gt))
    (hinv : RollInv depth pre acc) :
    RollInv depth (pre ++ [r]) (rollupStep depth acc r) := by
  rcases hinv with ⟨hnod, hkeys, htot, hmax⟩
  cases acc with
  | nil =>
      have hpre : pre = [] := by
        cases pre with
        | nil => rfl
        | cons p ps =>
            exfalso
            have hne : ((p :: ps).map (keyOf depth)).eraseDups.reverse ≠ [] := by
              simp [List.eraseDups_cons]
            apply hne
            simpa using hkeys.symm
      subst pre
      simp [RollInv, rollupStep, keyOf, valsOf, summarize, plus, zero, weighted,
        List.eraseDups_cons, path_compare_self_le]
  | cons entry done =>
      rcases entry with ⟨last, a⟩
      let k := keyOf depth r
      have hlastkeys : last ∈ (pre.map (keyOf depth)) := by
        have hm : last ∈ (((last, a) :: done).map Prod.fst) := by simp
        rw [hkeys] at hm
        have hm' : last ∈ (pre.map (keyOf depth)).eraseDups := by simpa using hm
        exact List.mem_eraseDups.mp hm'
      have hnodkeys := hnod
      have hdone_nod : (done.map Prod.fst).Nodup := (List.nodup_cons.mp hnodkeys).2
      have hdone_ne : last ∉ done.map Prod.fst := (List.nodup_cons.mp hnodkeys).1
      by_cases hbeq : last == k
      · have hlast_eq_k : last = k := beq_iff_eq.mp hbeq
        subst last
        have hkeys' : ((pre ++ [r]).map (keyOf depth)).eraseDups =
            (pre.map (keyOf depth)).eraseDups := by
          rw [List.map_append]
          apply eraseDups_append_of_mem
          simpa [k] using hlastkeys
        have htot' : ∀ kk tt, (kk, tt) ∈ ((k, plus a (weighted 1 r.2)) :: done) →
            tt = summarize (valsOf depth (pre ++ [r]) kk) := by
          intro kk tt htt
          rw [List.mem_cons] at htt
          rcases htt with heq | hdone
          · cases heq
            have hpre_a := htot k a (List.mem_cons.mpr (Or.inl rfl))
            rw [hpre_a]
            have hv := valsOf_append_same depth pre r (beq_iff_eq.mpr rfl)
            rw [hv, summarize_append_t, summarize_singleton_t]
          · have hk_ne : kk ≠ k := by
              intro hk
              apply hdone_ne
              have hkkmem : kk ∈ done.map Prod.fst := by
                rw [List.mem_map]
                exact ⟨(kk, tt), hdone, rfl⟩
              rw [← hk]
              exact hkkmem
            have hpre_t := htot kk tt (List.mem_cons.mpr (Or.inr hdone))
            rw [hpre_t]
            have hbeq_ne : keyOf depth r != kk := by
              cases hb : keyOf depth r == kk with
              | true => exact False.elim (hk_ne (by simpa [k] using (beq_iff_eq.mp hb).symm))
              | false =>
                  change !(keyOf depth r == kk) = true
                  simp [hb]
            rw [← valsOf_append_ne depth pre r kk hbeq_ne]
        have hmax' : ((k, plus a (weighted 1 r.2)) :: done) ≠ [] → ∃ kmax, ∃ r' ∈ pre ++ [r],
            (∃ t done_, (k, plus a (weighted 1 r.2)) :: done = (kmax, t) :: done_) ∧
            keyOf depth (r') = kmax ∧
            ∀ r2 ∈ pre ++ [r], compare (keyOf depth r2) kmax != .gt := by
          intro _
          obtain ⟨kmax, rH, hrHmem, haccH, hkeyH, hleH⟩ := hmax (by simp)
          rcases haccH with ⟨t, done_, haccH⟩
          cases haccH
          refine ⟨k, rH, ?_, ?_, ?_, ?_⟩
          · exact List.mem_append.mpr (Or.inl hrHmem)
          · exact ⟨plus a (weighted 1 r.2), done, rfl⟩
          · simpa using hkeyH
          · intro r2 hr2
            rw [List.mem_append] at hr2
            rcases hr2 with hr2pre | hr2r
            · simpa [hkeyH] using hleH r2 hr2pre
            · simp at hr2r
              subst r2
              exact path_compare_self_le k
        dsimp [rollupStep]
        simp [k]
        refine ⟨?_, ?_, htot', hmax'⟩
        · simpa [k] using hnodkeys
        · rw [List.map_append]
          simp only [List.map_cons, List.map_nil]
          have hE := eraseDups_append_of_mem (pre.map (keyOf depth)) k hlastkeys
          rw [show (pre.map (keyOf depth) ++ [keyOf depth r]).eraseDups =
            (pre.map (keyOf depth)).eraseDups by simpa [k] using hE]
          simpa [hkeys]
      · have hbeq_false : (last == k) = false := by
          cases h : last == k with
          | true => exact False.elim (hbeq h)
          | false => rfl
        have hmax_old := hmax (by simp)
        obtain ⟨kmax, rH, hrHmem, haccH, hkeyH, hleH⟩ := hmax_old
        rcases haccH with ⟨t, done_, haccH⟩
        cases haccH
        have hlast_eq_max : last = keyOf depth rH := hkeyH.symm
        have hk_not_pre : k ∉ pre.map (keyOf depth) := by
          intro hkpre
          obtain ⟨r2, hr2pre, hr2key⟩ := List.mem_map.mp hkpre
          have hle1 : compare k last != .gt := by
            have := hleH r2 hr2pre
            simpa [hr2key] using this
          have hle2 : compare last k != .gt := by
            obtain ⟨rL, hrLpre, hrLkey⟩ := List.mem_map.mp hlastkeys
            have := pairwise_before depth hsorted rL hrLpre
            simpa [k, hrLkey] using this
          have heqkl : k = last := path_compare_eq_of_both (by simpa using hle1) (by simpa using hle2)
          exact hbeq (beq_iff_eq.mpr heqkl.symm)
        have hnod' : (((k, weighted 1 r.2) :: (last, a) :: done).map Prod.fst).Nodup := by
          rw [List.map_cons]
          refine List.nodup_cons.mpr ⟨?_, hnodkeys⟩
          intro hm
          rcases List.mem_cons.mp hm with hklast | hkdone
          · exact False.elim (hbeq (beq_iff_eq.mpr hklast.symm))
          · apply hk_not_pre
            have hold : k ∈ (((last, a) :: done).map Prod.fst) := List.mem_cons.mpr (Or.inr hkdone)
            have hmRev : k ∈ (pre.map (keyOf depth)).eraseDups.reverse := by simpa [hkeys] using hold
            exact List.mem_eraseDups.mp (by simpa using hmRev)
        have hkeys' : ((pre ++ [r]).map (keyOf depth)).eraseDups.reverse =
            k :: ((last, a) :: done).map Prod.fst := by
          rw [List.map_append]
          simp only [List.map_cons, List.map_nil]
          rw [eraseDups_append_of_not_mem (pre.map (keyOf depth)) k hk_not_pre]
          simp only [List.reverse_append, List.reverse_cons, List.reverse_nil, List.nil_append]
          rw [← hkeys]
          rfl
        have htot' : ∀ kk tt, (kk, tt) ∈ ((k, weighted 1 r.2) :: (last, a) :: done) →
            tt = summarize (valsOf depth (pre ++ [r]) kk) := by
          intro kk tt htt
          rw [List.mem_cons] at htt
          rcases htt with heq | hrest
          · cases heq
            have hv0 := valsOf_eq_nil_of_not_mem depth pre k hk_not_pre
            have hv := valsOf_append_same depth pre r (beq_iff_eq.mpr rfl)
            rw [hv, hv0]
            simp [summarize, plus, zero, weighted]
          · have hkpre : kk ∈ pre.map (keyOf depth) := by
              have hold : kk ∈ (((last, a) :: done).map Prod.fst) := by
                rw [List.mem_map]
                exact ⟨(kk, tt), hrest, rfl⟩
              have hmRev : kk ∈ (pre.map (keyOf depth)).eraseDups.reverse := by simpa [hkeys] using hold
              exact List.mem_eraseDups.mp (by simpa using hmRev)
            have hk_ne : kk ≠ k := by
              intro heq
              apply hk_not_pre
              simpa [heq] using hkpre
            have hbeq_ne : keyOf depth r != kk := by
              cases hb : keyOf depth r == kk with
              | true => exact False.elim (hk_ne (by simpa [k] using (beq_iff_eq.mp hb).symm))
              | false =>
                  change !(keyOf depth r == kk) = true
                  simp [hb]
            have hpre_t := htot kk tt hrest
            rw [hpre_t, valsOf_append_ne depth pre r kk hbeq_ne]
        have hmax' : ((k, weighted 1 r.2) :: (last, a) :: done) ≠ [] → ∃ kmax, ∃ r' ∈ pre ++ [r],
            (∃ t done_, (k, weighted 1 r.2) :: (last, a) :: done = (kmax, t) :: done_) ∧
            keyOf depth (r') = kmax ∧
            ∀ r2 ∈ pre ++ [r], compare (keyOf depth r2) kmax != .gt := by
          intro _
          refine ⟨k, r, ?_, ?_, rfl, ?_⟩
          · exact List.mem_append.mpr (Or.inr (by simp))
          · exact ⟨weighted 1 r.2, (last, a) :: done, rfl⟩
          · intro r2 hr2
            rw [List.mem_append] at hr2
            rcases hr2 with hr2pre | hr2r
            · have := pairwise_before depth hsorted r2 hr2pre
              simpa [k] using this
            · simp at hr2r
              subst r2
              exact path_compare_self_le k
        dsimp [rollupStep]
        simp [k, hbeq_false]
        refine ⟨hnod', hkeys'.symm, htot', hmax'⟩

private theorem rollup_fold_inv (depth : Nat) :
    ∀ (pre rest : List Fact) (acc : List (Path × Totals)),
      RollInv depth pre acc →
      (pre ++ rest).Pairwise
        (fun a b => compare (keyOf depth a) (keyOf depth b) != .gt) →
      RollInv depth (pre ++ rest) (rest.foldl (rollupStep depth) acc) := by
  intro pre rest acc
  induction rest generalizing pre acc with
  | nil =>
      intro hinv hsorted
      simpa using hinv
  | cons r rs ih =>
      intro hinv hsorted
      have hstep := rollup_step_inv depth hsorted hinv
      have happ : (pre ++ [r]) ++ rs = pre ++ r :: rs := by simp
      rw [← happ] at hsorted
      have hres := ih (pre ++ [r]) (rollupStep depth acc r) hstep hsorted
      simpa [List.foldl_cons, happ] using hres

private theorem nodup_reverse_of_nodup {α : Type} : ∀ l : List α, l.Nodup → l.reverse.Nodup := by
  intro l h
  induction l with
  | nil => exact List.Pairwise.nil
  | cons a as ih =>
      have htail : as.Nodup := (List.nodup_cons.mp h).2
      have hnot : a ∉ as := (List.nodup_cons.mp h).1
      simp only [List.reverse_cons]
      rw [List.nodup_append]
      refine ⟨ih htail, by simp, ?_⟩
      intro x hx y hy
      simp at hy
      subst y
      intro heq
      apply hnot
      have hxas : x ∈ as := by simpa using hx
      simpa [heq] using hxas

private theorem lookup_of_mem_nodup {α β : Type} [BEq α] [LawfulBEq α]
    (l : List (α × β)) (k : α) (f : α → β)
    (hnod : (l.map Prod.fst).Nodup)
    (hmem : ∃ v', (k, v') ∈ l)
    (htot : ∀ kk tt, (kk, tt) ∈ l → tt = f kk) :
    l.lookup k = some (f k) := by
  induction l with
  | nil => simp at hmem
  | cons p l ih =>
      rcases p with ⟨kk, vv⟩
      by_cases hbeq : kk == k
      · have hkkeq : kk = k := beq_iff_eq.mp hbeq
        subst kk
        have hfirst : (k, vv) ∈ (k, vv) :: l := by simp
        have hv := htot k vv hfirst
        rw [hv]
        simp
      · have hne : kk ≠ k := by
          intro heq
          apply hbeq
          exact beq_iff_eq.mpr heq
        have hmem_tail : ∃ v', (k, v') ∈ l := by
          rcases hmem with ⟨v', hm⟩
          rw [List.mem_cons] at hm
          rcases hm with heq | hm
          · rcases heq with heq
            exact False.elim (hne (congrArg Prod.fst heq).symm)
          · exact ⟨v', hm⟩
        have hnod_tail : (l.map Prod.fst).Nodup := (List.nodup_cons.mp hnod).2
        have htot_tail : ∀ kk tt, (kk, tt) ∈ l → tt = f kk := by
          intro a b hab
          exact htot a b (List.mem_cons.mpr (Or.inr hab))
        have hbeq' : (k == kk) = false := by
          cases hb : k == kk with
          | true => exact False.elim (hne (beq_iff_eq.mp hb).symm)
          | false => rfl
        rw [List.lookup_cons]
        rw [hbeq']
        exact ih hnod_tail hmem_tail htot_tail

private theorem prefixRuns_fold_eq_acc (depth : Nat) (rs : List Fact)
    (acc : List (Path × Totals)) :
    rs.foldl (fun (acc : List (Path × Totals)) (r : Fact) =>
      match acc with
      | [] => [(r.1.take depth, weighted 1 r.2)]
      | (last, a) :: done_ =>
          if last = r.1.take depth then
            (r.1.take depth, plus a (weighted 1 r.2)) :: done_
          else (r.1.take depth, weighted 1 r.2) :: acc) acc =
    rs.foldl (rollupStep depth) acc := by
  induction rs generalizing acc with
  | nil => rfl
  | cons r rs ih =>
      simp only [List.foldl_cons]
      dsimp [rollupStep]
      simpa [keyOf] using ih (acc :=
        match acc with
        | [] => [(r.1.take depth, weighted 1 r.2)]
        | (last, a) :: done_ =>
            if last = r.1.take depth then
              (r.1.take depth, plus a (weighted 1 r.2)) :: done_
            else (r.1.take depth, weighted 1 r.2) :: acc)

private theorem prefixRuns_fold_eq (depth : Nat) (rs : List Fact) :
    rs.foldl (fun (acc : List (Path × Totals)) (r : Fact) =>
      match acc with
      | [] => [(r.1.take depth, weighted 1 r.2)]
      | (last, a) :: done_ =>
          if last = r.1.take depth then
            (r.1.take depth, plus a (weighted 1 r.2)) :: done_
          else (r.1.take depth, weighted 1 r.2) :: acc) [] =
    rs.foldl (rollupStep depth) [] :=
  prefixRuns_fold_eq_acc depth rs []

theorem OrderedRollup_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.RollupsAndChannelSetCounts.OrderedRollup.Goal := by
  intro rs depth k hsorted
  dsimp only
  by_cases hspecial : rs.isEmpty && depth == 0
  · have hc := hspecial
    simp at hc
    have hnil : rs = [] := by simpa using hc.1
    subst rs
    have hd0 : depth = 0 := by simpa using hc.2
    subst depth
    by_cases hk : k.isEmpty
    · have hk0 : k = [] := List.eq_nil_of_length_eq_zero (List.isEmpty_iff_length_eq_zero.mp hk)
      subst k
      simp [prefixRuns, summarize, zero]
    · have hkne : k ≠ [] := by
        intro hk0
        apply hk
        rw [hk0]
        rfl
      simp [prefixRuns, hk, hkne]
  let out := prefixRuns depth rs
  let vs := (rs.filter fun r => r.1.take depth == k).map Prod.snd
  have hinv : RollInv depth [] [] := by
    refine ⟨List.Pairwise.nil, rfl, ?_, ?_⟩
    · intro kk tt h
      simp at h
    · intro h
      simp at h
  have hfold := rollup_fold_inv depth [] rs [] hinv (by simpa [keyOf] using hsorted)
  let acc := rs.foldl (rollupStep depth) []
  have hfold' : RollInv depth rs acc := by
    dsimp [acc]
    simpa using hfold
  rcases hfold' with ⟨hnod', hkeys', htot', hmax'⟩
  have hout : out = acc.reverse := by
    dsimp [out, acc, prefixRuns]
    simp [hspecial]
    exact prefixRuns_fold_eq depth rs
  have hnodout : (out.map Prod.fst).Nodup := by
    rw [hout, List.map_reverse]
    exact nodup_reverse_of_nodup _ hnod'
  have htotout : ∀ kk tt, (kk, tt) ∈ out → tt = summarize (valsOf depth rs kk) := by
    intro kk tt htt
    rw [hout] at htt
    have httacc : (kk, tt) ∈ acc := by simpa using htt
    exact htot' kk tt httacc
  have hkeyst : ∀ kk, kk ∈ out.map Prod.fst ↔ kk ∈ (rs.map (keyOf depth)).eraseDups := by
    intro kk
    rw [hout, List.map_reverse, hkeys']
    simp
  have hvs : vs = valsOf depth rs k := by rfl
  constructor
  · by_cases hkK : k ∈ (rs.map (keyOf depth)).eraseDups
    · have hmemout : ∃ v', (k, v') ∈ out := by
        have hkmem : k ∈ out.map Prod.fst := (hkeyst k).mpr hkK
        rcases List.mem_map.mp hkmem with ⟨p, hp, hpk⟩
        rcases p with ⟨kk, vv⟩
        simp at hpk
        subst kk
        exact ⟨vv, hp⟩
      have hl := lookup_of_mem_nodup out k (fun kk => summarize (valsOf depth rs kk)) hnodout hmemout htotout
      rw [hl]
      have hvne : !vs.isEmpty = true := by
        have hkmap : k ∈ rs.map (keyOf depth) := List.mem_eraseDups.mp hkK
        obtain ⟨r0, hr0, hr0k⟩ := List.mem_map.mp hkmap
        have hvalsne : valsOf depth rs k ≠ [] := by
          intro hn
          have hmemval : r0.2 ∈ valsOf depth rs k := by
            dsimp [valsOf]
            rw [List.mem_map]
            refine ⟨r0, ?_, rfl⟩
            rw [List.mem_filter]
            exact ⟨hr0, beq_iff_eq.mpr hr0k⟩
          rw [hn] at hmemval
          simpa using hmemval
        have hvsne : vs ≠ [] := by rw [hvs]; exact hvalsne
        have hvslen : vs.length ≠ 0 := by
          intro hz
          apply hvsne
          exact List.eq_nil_of_length_eq_zero hz
        cases hE : vs.isEmpty with
        | true =>
            exact False.elim (hvslen (List.isEmpty_iff_length_eq_zero.mp hE))
        | false => rfl
      have hkmap : k ∈ rs.map (keyOf depth) := List.mem_eraseDups.mp hkK
      obtain ⟨r0, hr0, hr0k⟩ := List.mem_map.mp hkmap
      have hcond : (∃ x, (∃ x_1, (x, x_1) ∈ rs) ∧ List.take depth x = k) :=
        ⟨r0.1, ⟨r0.2, hr0⟩, hr0k⟩
      simp [hvs, hvne, hcond]
      rfl
    · have hl : out.lookup k = none := by
        rw [List.lookup_eq_none_iff]
        intro p hp
        have hpk : p.1 ∈ out.map Prod.fst := by
          rw [List.mem_map]
          exact ⟨p, hp, rfl⟩
        have hpK := (hkeyst p.1).mp hpk
        have hne : k ≠ p.1 := by
          intro heq
          apply hkK
          simpa [heq] using hpK
        change !(k == p.1) = true
        cases hb : k == p.1 with
        | true => exact False.elim (hne (beq_iff_eq.mp hb))
        | false => simp [hb]
      rw [hl]
      have hv : vs = [] := by
        rw [hvs]
        apply valsOf_eq_nil_of_not_mem
        intro hm
        apply hkK
        rw [List.mem_eraseDups]
        exact hm
      have hnorow : ∀ (a : Path) (b : Option Int), (a, b) ∈ rs → ¬ List.take depth a = k := by
        intro a b hab heq
        have hbmem : b ∈ vs := by
          rw [hvs]
          dsimp [valsOf]
          rw [List.mem_map]
          refine ⟨(a, b), ?_, rfl⟩
          rw [List.mem_filter]
          exact ⟨hab, beq_iff_eq.mpr heq⟩
        rw [hv] at hbmem
        simpa using hbmem
      have hkempty : depth = 0 → ¬ k = [] := by
        intro hd0 hk0
        have hrne : rs ≠ [] := by
          intro hnil
          apply hspecial
          simp [hnil, hd0]
        obtain ⟨r, tail, htail⟩ := List.exists_cons_of_ne_nil hrne
        have hrmem : r ∈ rs := by rw [htail]; simp
        exact hnorow r.1 r.2 hrmem (by simp [hd0, hk0])
      simp [hv, hkK]
      exact ⟨hnorow, hkempty⟩
  · change (out.map Prod.fst).Nodup
    rw [hout, List.map_reverse]
    exact nodup_reverse_of_nodup _ hnod'

end TinylambOpt.ResearchProofs.AnalyticalOperators.RollupsAndChannelSetCounts

-- C++ references to this model or its proof.
-- executor/detail/execution_strategies.hpp
-- executor/relational_quotient_set_counts.cpp
-- executor/relational_regression_test_part8.cpp
-- executor/relational_return_counts.cpp
