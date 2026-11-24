import TinylambOpt.ResearchGoals.CloudBI.NestedBI
import TinylambOpt.ResearchProofs.CloudBI.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.CloudBI.NestedBI

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.CloudBI.NestedBI
open TinylambOpt.ResearchProofs.CloudBI.Helpers

/-! ### Shared helpers -/

private theorem scale_eq_scaleT (a : Totals) (n : Nat) : scale a n = scaleT a n := rfl

private theorem natbeq_swap (a b : Nat) : (a == b) = (b == a) := by
  cases h : a == b <;> cases h' : b == a <;> simp_all (config := { decide := true })

private theorem natbeq_true {a b : Nat} (h : (a == b) = true) : a = b := by simpa using h

private theorem natbeq_false_of_ne {a b : Nat} (h : a ≠ b) : (a == b) = false := by
  cases h' : a == b <;> simp_all (config := { decide := true })

private theorem foldl_plus_acc (xs : List (Option Int)) (z : Totals) :
    List.foldl (fun a v => plus a (weighted 1 v)) z xs = plus z (summarize xs) :=
  foldl_weighted xs z

private theorem foldl_map_plus {α : Type} (xs : List α) (g : α → Totals) :
    ∀ (z : Totals), List.foldl (fun a b => plus a b) z (List.map g xs) =
      List.foldl (fun a x => plus a (g x)) z xs := by
  induction xs with
  | nil => intro z; rfl
  | cons x xs ih =>
      intro z
      rw [List.map_cons, List.foldl_cons, List.foldl_cons, ih (plus z (g x))]

private theorem foldl_TSum {α : Type} (xs : List α) (g : α → Totals) :
    xs.foldl (fun a x => plus a (g x)) zero = TSum (xs.map g) := by
  rw [← foldl_map_plus _ _ zero, foldl_plus, plus_zero']

private theorem TSum_cons (x : Totals) (xs : List Totals) : TSum (x :: xs) = plus x (TSum xs) := rfl

private theorem TSum_replicate (a : Totals) (n : Nat) : TSum (List.replicate n a) = scaleT a n := by
  induction n with
  | zero => rw [List.replicate_zero, TSum_nil, scale_zero]
  | succ n ih =>
      rw [List.replicate_succ, TSum_cons, ih]
      refine totals_ext ?_ ?_ ?_
      · show a.cnt + a.cnt * n = a.cnt * (n + 1)
        rw [Nat.mul_succ, Nat.add_comm]
      · show a.nn + a.nn * n = a.nn * (n + 1)
        rw [Nat.mul_succ, Nat.add_comm]
      · show a.sum + a.sum * (↑n : Int) = a.sum * ((n + 1 : Nat) : Int)
        rw [show ((n + 1 : Nat) : Int) = (↑n : Int) + 1 from rfl,
          Int.mul_add, Int.mul_one, Int.add_comm]

private theorem TSum_eq_zero {xs : List Totals} (h : ∀ x ∈ xs, x = zero) : TSum xs = zero := by
  induction xs with
  | nil => exact TSum_nil
  | cons x xs ih =>
      rw [TSum_cons, h x (List.mem_cons_self)]
      rw [ih (fun y hy => h y (List.mem_cons_of_mem x hy))]
      exact plus_zero zero

private theorem scaleT_zero (n : Nat) : scaleT zero n = zero := by
  cases n with
  | zero => rfl
  | succ n =>
      refine totals_ext (Nat.zero_mul _) (Nat.zero_mul _) (Int.zero_mul _)

private theorem TSum_map_TSum {α : Type} (xs : List α) (f : α → List Totals) :
    TSum (xs.map (fun i => TSum (f i))) = TSum ((xs.map f).flatten) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rw [List.map_cons, TSum_cons, ih, ← TSum_append, List.map_cons, List.flatten_cons]

private theorem flatMap_eq_map_flatten {α β : Type} (xs : List α) (f : α → List β) :
    xs.flatMap f = (xs.map f).flatten := by
  induction xs with
  | nil => rfl
  | cons x xs ih => exact congrArg (List.append (f x)) ih

private theorem perm_filter_map_split {α β : Type} (l : List α) (p : α → Bool) (f : α → β) :
    List.Perm ((l.filter p).map f ++ (l.filter (fun x => !p x)).map f) (l.map f) := by
  rw [← List.map_append]
  exact List.Perm.map f (filter_not_partition p l)

private theorem map_eq_replicate_of_all {α β : Type} (f : α → β) :
    ∀ (ys : List α) (v : β), (∀ y ∈ ys, f y = v) → ys.map f = List.replicate ys.length v := by
  intro ys
  induction ys with
  | nil => intro v _; rfl
  | cons y ys ih =>
      intro v h
      rw [List.map_cons, List.length_cons, List.replicate_succ, h y (List.mem_cons_self),
        ih v (fun z hz => h z (List.mem_cons_of_mem y hz))]

private theorem map_flatten_distrib {α β : Type} (xs : List (List α)) (g : α → β) :
    (xs.map (fun l => l.map g)).flatten = xs.flatten.map g := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rw [List.map_cons, List.flatten_cons, ih, ← List.map_append, List.flatten_cons]

private theorem map_map_p {α β γ : Type} (xs : List α) (f : α → List β) (g : β → γ) :
    xs.map (fun i => (f i).map g) = (xs.map f).map (fun l => l.map g) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => rw [List.map_cons, List.map_cons, List.map_cons, ih]

private theorem filter_all_true {α : Type} (p : α → Bool) (l : List α)
    (h : ∀ x ∈ l, p x = true) : l.filter p = l := by
  induction l with
  | nil => rfl
  | cons x l ih =>
      rw [List.filter_cons_of_pos (p := p) (h x (List.mem_cons_self)),
        ih (fun y hy => h y (List.mem_cons_of_mem x hy))]

private theorem map_congr_mem {α β : Type} (l : List α) (f g : α → β)
    (h : ∀ x ∈ l, f x = g x) : l.map f = l.map g := by
  induction l with
  | nil => rfl
  | cons x l ih =>
      rw [List.map_cons, List.map_cons, h x (List.mem_cons_self),
        ih (fun y hy => h y (List.mem_cons_of_mem x hy))]

private theorem map_congr_iff {α β : Type} (f g : α → β) (l : List α) :
    (l.map f = l.map g) ↔ ∀ x ∈ l, f x = g x := by
  constructor
  · intro h
    induction l with
    | nil => intro x hx; exact absurd hx List.not_mem_nil
    | cons y l ih =>
        rw [List.map_cons, List.map_cons] at h
        obtain ⟨h1, h2⟩ := List.cons.inj h
        intro x hx
        cases hx with
        | head => exact h1
        | tail _ hm => exact ih h2 x hm
  · intro h
    induction l with
    | nil => rfl
    | cons y ys ih =>
        rw [List.map_cons, List.map_cons, h y (List.mem_cons_self),
          ih (fun z hz => h z (List.mem_cons_of_mem y hz))]

private theorem run_eq_flatMap (source : Nat → List (Option Int)) :
    ∀ (plan : Branches), run source plan = (leaves plan).flatMap source := by
  intro plan
  induction plan with
  | leaf k =>
      rw [show run source (Branches.leaf k) = source k from rfl,
        show (leaves (Branches.leaf k)).flatMap source = source k ++ [] from rfl,
        List.append_nil]
  | unionAll l r ihl ihr =>
      show run source l ++ run source r = (leaves l ++ leaves r).flatMap source
      rw [List.flatMap_append, ihl, ihr]

private theorem summarize_flatMap (xs : List α) (g : α → List (Option Int)) (z : Totals) :
    (xs.flatMap g).foldl (fun a v => plus a (weighted 1 v)) z =
      xs.foldl (fun a x => plus a ((g x).foldl (fun a v => plus a (weighted 1 v)) zero)) z := by
  induction xs generalizing z with
  | nil => rfl
  | cons x xs ih =>
      rw [List.flatMap_cons, List.foldl_append,
        show (g x).foldl (fun a v => plus a (weighted 1 v)) z =
            plus z (summarize (g x)) from foldl_weighted (g x) z,
        ih (plus z (summarize (g x))), List.foldl_cons,
        show (g x).foldl (fun a v => plus a (weighted 1 v)) zero =
            plus zero (summarize (g x)) from foldl_weighted (g x) zero,
        plus_zero']

/-! ### BranchHistogram -/

/-- Branch histogram reweighting equals direct evaluation of the nested plan. -/
theorem sound_BranchHistogram : BranchHistogram.Goal := by
  intro plan source
  change ((histogram (leaves plan)).toList).foldl
      (fun a (k, n) => plus a (scaleT (summarize (source k)) n)) zero = summarize (run source plan)
  have h := histogram_scale (leaves plan) (fun k => summarize (source k))
  refine h.trans ?_
  rw [run_eq_flatMap source plan]
  exact ((show (leaves plan).foldl (fun a k => plus a (summarize (source k))) zero =
        (leaves plan).foldl
          (fun a k => plus a ((source k).foldl (fun a v => plus a (weighted 1 v)) zero)) zero
      from rfl).trans (summarize_flatMap (leaves plan) source zero).symm)

/-! ### JoinedLeafWeights -/

private theorem natbeq_self (n : Nat) : (n == n) = true := (beq_iff_eq (α := Nat)).mpr rfl

private theorem foldl_id {α β : Type} (l : List α) (z : β) : l.foldl (fun a _ => a) z = z := by
  induction l generalizing z with
  | nil => rfl
  | cons x rest ih => rw [List.foldl_cons, ih]

private theorem foldl_acc2 {α : Type} (f : α → Totals) :
    ∀ (l : List α) (p q : Totals),
      l.foldl (fun a x => plus a (f x)) (plus p q)
        = plus (l.foldl (fun a x => plus a (f x)) p) q := by
  intro l
  induction l with
  | nil => intro p q; rfl
  | cons x rest ih =>
      intro p q
      rw [List.foldl_cons, List.foldl_cons]
      rw [show plus (plus p q) (f x) = plus (plus p (f x)) q from by
        rw [plus_assoc, plus_comm q (f x), ← plus_assoc], ih]

private theorem foldl_fuse {α : Type} (l : List α) (f g : α → Totals) :
    ∀ (z : Totals),
      l.foldl (fun a x => plus a (plus (f x) (g x))) z
        = l.foldl (fun a x => plus a (g x)) (l.foldl (fun a x => plus a (f x)) z) := by
  intro z
  induction l generalizing z with
  | nil => rfl
  | cons x rest ih =>
      rw [List.foldl_cons, ih, foldl_acc2 (f := f), ← plus_assoc, List.foldl_cons,
        List.foldl_cons, foldl_acc2 (f := f)]

/-- Folding a `Nodup` list where only `k0` (if present) contributes. -/
private theorem foldl_single {α : Type} [DecidableEq α] :
    ∀ (l : List α), l.Nodup → ∀ (k0 : α) (d : α → Totals) (z : Totals),
      (∀ k ∈ l, ¬(k == k0) → d k = zero) →
      l.foldl (fun a k => plus a (d k)) z = if k0 ∈ l then plus z (d k0) else z := by
  intro l
  induction l with
  | nil => intro _ k0 d z _; simp
  | cons x xs ih =>
      intro hnd k0 d z hd
      obtain ⟨hnx, hns⟩ := List.nodup_cons.mp hnd
      rw [List.foldl_cons]
      by_cases hx : (x == k0)
      · obtain rfl : k0 = x := ((beq_iff_eq (α := α) (a := x) (b := k0)).mp hx).symm
        have hz := ih hns k0 d (plus z (d k0))
          (fun k hk hne => hd k (List.mem_cons_of_mem k0 hk) hne)
        rw [hz, ite_eq_right hnx, ite_eq_left List.mem_cons_self]
      · rw [show (plus z (d x)) = z from by
          rw [hd x List.mem_cons_self hx, plus_zero]]
        rw [ih hns k0 d z (fun k hk hne => hd k (List.mem_cons_of_mem x hk) hne)]
        by_cases hmem : k0 ∈ xs
        · rw [ite_eq_left hmem, ite_eq_left (List.mem_cons_of_mem x hmem)]
        · rw [ite_eq_right hmem, ite_eq_right (fun m =>
              hmem ((List.mem_cons.mp m).resolve_left
                (fun e => hx ((beq_iff_eq (α := α)).mpr e.symm))))]

private def AJ (facts : List (Nat × Option Int)) (k : Nat) : Totals :=
  summarize ((facts.filter (·.1 == k)).map Prod.snd)

private def WJ (facts : List (Nat × Option Int)) (dim : List (Nat × Int)) (k : Nat) : Totals :=
  let ws := (dim.filter (·.1 == k)).map Prod.snd
  ⟨(AJ facts k).cnt * ws.length, (AJ facts k).nn * ws.length, (AJ facts k).sum * ws.sum⟩

private theorem AJ_nil (facts : List (Nat × Option Int)) (k : Nat)
    (h : k ∉ facts.map Prod.fst) : AJ facts k = zero := by
  unfold AJ
  have hf : facts.filter (·.1 == k) = [] := by
    refine (List.filter_eq_nil_iff).2 (fun p hp hm => ?_)
    obtain rfl : p.fst = k := (beq_iff_eq (α := Nat)).mp hm
    exact h (List.mem_map_of_mem (f := Prod.fst) hp)
  rw [hf]
  rfl

private theorem WJ_nil (facts : List (Nat × Option Int)) (dim : List (Nat × Int)) (k : Nat)
    (h : k ∉ facts.map Prod.fst) : WJ facts dim k = zero := by
  unfold WJ
  rw [AJ_nil facts k h]
  refine totals_ext ?_ ?_ ?_ <;> simp [zero]

private theorem blk_none (ws : List Int) :
    summarize (ws.map (fun w => (none : Option Int).map (fun x => x * w))) =
      ⟨ws.length, 0, 0⟩ := by
  induction ws with
  | nil => rfl
  | cons w rest ih =>
      rw [List.map_cons, summarize_cons, ih]
      rw [show Option.map (fun x => x * w) (none : Option Int) = (none : Option Int) from rfl]
      exact totals_ext rfl rfl rfl

private theorem blk_some (x : Int) (ws : List Int) :
    summarize (ws.map (fun w => (some x : Option Int).map (fun a => a * w))) =
      ⟨ws.length, ws.length, x * ws.sum⟩ := by
  induction ws with
  | nil =>
      refine totals_ext rfl rfl ?_
      rw [List.sum_nil, Int.mul_zero]
      rfl
  | cons w rest ih =>
      rw [List.map_cons, summarize_cons, ih]
      rw [show Option.map (fun a => a * w) (some x : Option Int) = some (x * w) from rfl]
      refine totals_ext rfl rfl ?_
      rw [plus_sum, show (weighted 1 (some (x * w) : Option Int)).sum = (1 : Int) * (x * w)
        from rfl, Int.one_mul, List.sum_cons, Int.mul_add]
      dsimp only
      omega

private theorem blkK (v : Option Int) (ws : List Int) :
    summarize (ws.map (fun w => v.map (fun x => x * w))) =
      ⟨ws.length, if v.isSome then ws.length else 0, v.getD 0 * ws.sum⟩ := by
  cases v with
  | none =>
      rw [blk_none]
      refine totals_ext rfl rfl (Int.zero_mul _).symm
  | some x =>
      rw [blk_some x]
      refine totals_ext rfl rfl ?_
      exact (congrArg (fun z : Int => z * ws.sum) rfl)

private theorem flatPair_map2 (l : List (Nat × Option Int)) (dk : List (Nat × Int)) :
    l.flatMap (fun p => dk.map (fun q => p.snd.map (fun x => x * q.snd)))
      = (l.map Prod.snd).flatMap (fun v => (dk.map Prod.snd).map (fun w => v.map (fun x => x * w))) := by
  induction l with
  | nil => rfl
  | cons a rest ih =>
      rw [List.flatMap_cons, List.map_cons, List.flatMap_cons, ih]
      rw [List.map_map]
      rfl

private theorem cart (rs : List (Option Int)) (ws : List Int) :
    summarize (rs.flatMap (fun v => ws.map (fun w => v.map (fun x => x * w)))) =
      ⟨(summarize rs).cnt * ws.length, (summarize rs).nn * ws.length,
        (summarize rs).sum * ws.sum⟩ := by
  induction rs with
  | nil =>
      rw [List.flatMap_nil, summarize_nil]
      refine totals_ext ?_ ?_ ?_ <;> simp only [zero, Nat.zero_mul, Int.zero_mul]
  | cons v rest ih =>
      rw [List.flatMap_cons, summarize_append, ih]
      cases v with
      | none =>
          rw [blk_none]
          refine totals_ext ?_ ?_ ?_
          · rw [plus_cnt]; dsimp only
            rw [summarize_cnt, summarize_cnt, List.length_cons, Nat.add_mul, Nat.one_mul]
            omega
          · rw [plus_nn]; dsimp only
            rw [summarize_nn, summarize_nn,
              show ((none :: rest).filterMap id) = rest.filterMap id from rfl,
              Nat.zero_add]
          · rw [plus_sum]; dsimp only
            simp only [summarize_sum, show ((none :: rest).map (fun v => v.getD 0))
                = ((0 : Int) :: rest.map (fun v => v.getD 0)) from rfl,
              List.sum_cons, Int.zero_add]
      | some x =>
          rw [blk_some x]
          refine totals_ext ?_ ?_ ?_
          · rw [plus_cnt]; dsimp only
            rw [summarize_cnt, summarize_cnt, List.length_cons, Nat.add_mul, Nat.one_mul]
            omega
          · rw [plus_nn]; dsimp only
            rw [summarize_nn, summarize_nn,
              show ((some x :: rest).filterMap id) = x :: rest.filterMap id from rfl,
              List.length_cons, Nat.add_mul, Nat.one_mul]
            omega
          · rw [plus_sum]; dsimp only
            simp only [summarize_sum, show ((some x :: rest).map (fun v => v.getD 0))
                = (x :: rest.map (fun v => v.getD 0)) from rfl,
              List.sum_cons, Int.add_mul]

private theorem flatPair_single (l : List (Nat × Option Int)) :
    l.flatMap (fun p => [p.snd]) = l.map Prod.snd := by
  induction l with
  | nil => rfl
  | cons p rest ih => rw [List.flatMap_cons, List.map_cons, ih]; rfl

private theorem blockK (facts : List (Nat × Option Int)) (dim : List (Nat × Int))
    (k : Nat) (b : Bool) :
    summarize ((facts.filter (·.1 == k)).flatMap fun p =>
        if b then (dim.filter (·.1 == k)).map (fun q => p.snd.map (fun x => x * q.snd))
          else [p.snd]) =
      if b then WJ facts dim k else AJ facts k := by
  cases b with
  | false =>
      show summarize ((facts.filter (·.1 == k)).flatMap fun p => [p.snd]) = AJ facts k
      rw [flatPair_single]
      rfl
  | true =>
      show summarize ((facts.filter (·.1 == k)).flatMap fun p =>
          (dim.filter (·.1 == k)).map (fun q => p.snd.map (fun x => x * q.snd))) = WJ facts dim k
      rw [flatPair_map2, cart]
      unfold WJ AJ
      rfl

private theorem cntF (k0 k : Nat) (b0 : Bool) (req : List (Nat × Bool)) :
    ((k0, b0) :: req).count (k, false) =
      req.count (k, false) + if b0 then 0 else if k == k0 then 1 else 0 := by
  rw [List.count_cons]
  cases b0 <;> by_cases hk : k = k0 <;> simp [hk] <;> omega

private theorem cntT (k0 k : Nat) (b0 : Bool) (req : List (Nat × Bool)) :
    ((k0, b0) :: req).count (k, true) =
      req.count (k, true) + if b0 then if k == k0 then 1 else 0 else 0 := by
  rw [List.count_cons]
  cases b0 <;> by_cases hk : k = k0 <;> simp [hk] <;> omega

private theorem beq_not_symm {α : Type} [BEq α] [LawfulBEq α] {a b : α}
    (h : ¬((a == b) = true)) : ¬((b == a) = true) :=
  fun h2 => h ((beq_iff_eq (α := α)).mpr (Eq.symm ((beq_iff_eq (α := α)).mp h2)))

private theorem scaleT_plus1 (a : Totals) (n : Nat) :
    scaleT a (n + 1) = plus (scaleT a n) a := by
  rw [scale_add, scale_one]

private theorem bodyDelta (facts : List (Nat × Option Int)) (dim : List (Nat × Int))
    (k0 : Nat) (b0 : Bool) (k : Nat) (req : List (Nat × Bool)) :
    plus (scaleT (AJ facts k) (((k0, b0) :: req).count (k, false)))
      (scaleT (WJ facts dim k) (((k0, b0) :: req).count (k, true)))
      = plus (plus (scaleT (AJ facts k) (req.count (k, false)))
          (scaleT (WJ facts dim k) (req.count (k, true))))
        (if k0 == k then if b0 then WJ facts dim k else AJ facts k else zero) := by
  rw [cntF, cntT]
  by_cases hk : k = k0
  · obtain rfl : k = k0 := hk
    cases b0 <;> simp [scaleT_plus1, plus_comm, plus_assoc, plus_left_comm]
  · have hk2 : ¬(k0 = k) := fun e => hk e.symm
    cases b0 <;> simp [hk, hk2, beq_iff_eq, plus_zero]

private theorem foldl_congr_body {α : Type} (l : List α) (f g : Totals → α → Totals)
    (z : Totals) (h : ∀ acc x, f acc x = g acc x) : l.foldl f z = l.foldl g z := by
  have : f = g := funext fun acc => funext fun x => h acc x
  rw [this]

private theorem bodyDelta2 (facts : List (Nat × Option Int)) (dim : List (Nat × Int))
    (k0 : Nat) (b0 : Bool) (req : List (Nat × Bool)) (total : Totals) (k : Nat) :
    plus total
        (plus (scaleT (AJ facts k) (((k0, b0) :: req).count (k, false)))
          (scaleT (WJ facts dim k) (((k0, b0) :: req).count (k, true))))
      = plus total
          (plus (plus (scaleT (AJ facts k) (req.count (k, false)))
              (scaleT (WJ facts dim k) (req.count (k, true))))
            (if k0 == k then if b0 then WJ facts dim k else AJ facts k else zero)) := by
  rw [bodyDelta facts dim k0 b0 k req]

private theorem jmain (facts : List (Nat × Option Int)) (dim : List (Nat × Int)) :
    ∀ (requests : List (Nat × Bool)),
      (facts.map Prod.fst).eraseDups.foldl
        (fun total k =>
          plus total (plus (scaleT (AJ facts k) (requests.count (k, false)))
            (scaleT (WJ facts dim k) (requests.count (k, true))))) zero
        = summarize (requests.flatMap fun p =>
            (facts.filter (·.1 == p.fst)).flatMap fun q =>
              if p.snd then
                (dim.filter (·.1 == p.fst)).map fun r => q.snd.map fun x => x * r.snd
              else [q.snd]) := by
  intro requests
  induction requests with
  | nil =>
      rw [List.flatMap_nil, summarize_nil]
      refine (congrArg (fun s =>
        (facts.map Prod.fst).eraseDups.foldl s zero) ?_).trans (foldl_id _ _)
      funext total k
      rw [List.count_nil, List.count_nil, scale_zero, scale_zero, plus_zero, plus_zero]
  | cons c req ih =>
      obtain ⟨k0, b0⟩ := c
      rw [List.flatMap_cons, summarize_append]
      rw [blockK facts dim k0 b0, ← ih]
      refine ((foldl_congr_body (facts.map Prod.fst).eraseDups _ _ _ fun acc k =>
        bodyDelta2 facts dim k0 b0 req acc k).trans ?rest)
      rw [foldl_fuse,
        foldl_single (facts.map Prod.fst).eraseDups (nodup_eraseDups _) k0
          (fun k => if k0 == k then if b0 then WJ facts dim k else AJ facts k else zero)
          ((facts.map Prod.fst).eraseDups.foldl
            (fun total k =>
              plus total (plus (scaleT (AJ facts k) (req.count (k, false)))
                (scaleT (WJ facts dim k) (req.count (k, true))))) zero)
          (fun j _ hj => ite_eq_right (beq_not_symm hj))]
      by_cases hmem : k0 ∈ (facts.map Prod.fst).eraseDups
      · rw [ite_eq_left hmem, ite_eq_left (natbeq_self k0)]
        exact plus_comm _ _
      · rw [ite_eq_right hmem]
        have hnm : k0 ∉ facts.map Prod.fst := by
          intro h; exact hmem (List.mem_eraseDups.2 h)
        rw [AJ_nil facts k0 hnm, WJ_nil facts dim k0 hnm]
        cases b0
        · exact (plus_zero' _).symm
        · exact (plus_zero' _).symm

theorem sound_JoinedLeafWeights : JoinedLeafWeights.Goal := by
  intro facts dim requests
  dsimp only
  simp only [histogram_getD, scale_eq_scaleT]
  exact jmain facts dim requests

/-! ### FlattenSum -/

private theorem finish_eq' (xs : List (Option Int)) :
    finish (summarize xs) =
      if (xs.filterMap id).length == (0 : Nat) then none else some (xs.map (fun v => v.getD 0)).sum := by
  unfold finish
  rw [summarize_nn, summarize_sum]

private theorem lenbeq0 {l : List α} :
    ((l.length == (0 : Nat)) = true) ↔ l.length = 0 := by
  cases h : l.length with
  | zero => exact ⟨fun _ => rfl, fun _ => rfl⟩
  | succ m =>
      exact ⟨fun hz => absurd hz Bool.false_ne_true, fun hf => absurd hf (Nat.succ_ne_zero m)⟩

private theorem bool_not_true {b : Bool} (h : ¬(b = true)) : b = false := by
  cases b with
  | true => exact absurd rfl h
  | false => rfl

private theorem finish_congr {xs ys : List (Option Int)}
    (he : (xs.filterMap id = []) ↔ (ys.filterMap id = []))
    (hs : (xs.map (fun v => v.getD 0)).sum = (ys.map (fun v => v.getD 0)).sum) :
    finish (summarize xs) = finish (summarize ys) := by
  rw [finish_eq', finish_eq']
  by_cases hx : ((xs.filterMap id).length == (0 : Nat)) = true
  · have hy : ((ys.filterMap id).length == (0 : Nat)) = true :=
      lenbeq0.2 (congrArg List.length
        (he.1 (List.eq_nil_of_length_eq_zero (lenbeq0.1 hx))))
    rw [ite_eq_left hx, ite_eq_left hy]
  · have hy : ¬((ys.filterMap id).length == (0 : Nat)) = true := fun hh =>
      hx (lenbeq0.2 (congrArg List.length
        (he.2 (List.eq_nil_of_length_eq_zero (lenbeq0.1 hh)))))
    rw [ite_eq_right hx, ite_eq_right hy, hs]

private theorem sum_getD_eq_zero (l : List (Option Int)) (h : ∀ v ∈ l, v = none) :
    (l.map (fun v => v.getD 0)).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.map_cons, List.sum_cons, h a (by simp),
        ih (fun v hv => h v (List.mem_cons_of_mem a hv))]
      exact rfl

private theorem F_none_iff (g : List (Option Int)) :
    finish (summarize g) = none ↔ g.filterMap id = [] := by
  rw [finish_eq']
  by_cases hz : ((List.filterMap id g).length == (0 : Nat)) = true
  · rw [ite_eq_left hz]
    exact ⟨fun _ => List.eq_nil_of_length_eq_zero (lenbeq0.1 hz), fun _ => rfl⟩
  · rw [ite_eq_right hz]
    refine ⟨fun hh => absurd hh (by simp), ?_⟩
    intro hnil
    exact absurd (lenbeq0.2 (congrArg List.length hnil)) hz

private theorem map_fin_sum (gs : List (List (Option Int))) :
    ((gs.map fun g => finish (summarize g)).map fun v => v.getD 0).sum =
      (gs.flatten.map fun v => v.getD 0).sum := by
  induction gs with
  | nil => rfl
  | cons g gs ih =>
      rw [List.map_cons, List.flatten_cons, List.map_append, List.sum_append, List.map_cons,
        List.sum_cons, ih, finish_eq']
      by_cases hz : ((List.filterMap id g).length == (0 : Nat)) = true
      · rw [ite_eq_left hz, show (none : Option Int).getD 0 = 0 from rfl,
          sum_getD_eq_zero g
            (List.filterMap_eq_nil_iff.mp
              (List.eq_nil_of_length_eq_zero (lenbeq0.1 hz)))]
      · rw [ite_eq_right hz]
        exact rfl

private theorem fmF_nil (gs : List (List (Option Int))) :
    ((gs.map fun g => finish (summarize g)).filterMap id = []) ↔
      (gs.flatten.filterMap id = []) := by
  rw [List.filterMap_eq_nil_iff, List.filterMap_eq_nil_iff]
  refine ⟨fun H x hx => ?_, fun H x hx => ?_⟩
  · obtain ⟨g, hg, hxg⟩ := List.mem_flatten.1 hx
    have hv : finish (summarize g) = none :=
      H (finish (summarize g)) (List.mem_map.2 ⟨g, hg, rfl⟩)
    have hgm : g.filterMap id = [] := (F_none_iff g).mp hv
    exact (List.filterMap_eq_nil_iff.mp hgm) x hxg
  · obtain ⟨g, hg, hxg⟩ := List.mem_map.1 hx
    rw [← hxg]
    exact (F_none_iff g).mpr (List.filterMap_eq_nil_iff.mpr
      fun v hv => H v (List.mem_flatten.2 ⟨g, hg, hv⟩))

private theorem F_group (g : List (List (Option Int))) :
    finish (summarize (g.map fun leaf => finish (summarize leaf))) = finish (summarize g.flatten) :=
  finish_congr (fmF_nil g) (map_fin_sum g)

private theorem flatten_map_flatten (gs : List (List (List α))) :
    (gs.map List.flatten).flatten = gs.flatten.flatten := by
  induction gs with
  | nil => rfl
  | cons g gs ih =>
      rw [List.map_cons, List.flatten_cons, List.flatten_cons, List.flatten_append, ih]

/-- Nested scalar-SUM flattening preserves the final SUM. -/
private theorem map_finish_flatten (groups : List (List (List (Option Int)))) :
    groups.map (fun group => finish (summarize group.flatten)) =
      (groups.map List.flatten).map (fun leaf => finish (summarize leaf)) := by
  induction groups with
  | nil => rfl
  | cons g gs ih =>
      rw [List.map_cons, List.map_cons, ih, List.map_cons]

theorem sound_FlattenSum : FlattenSum.Goal := by
  intro groups
  have hA : groups.map (fun group =>
      finish (summarize (group.map fun leaf => finish (summarize leaf)))) =
      groups.map (fun group => finish (summarize group.flatten)) :=
    congrArg (fun f => List.map f groups) (funext fun g => F_group g)
  have hB := map_finish_flatten groups
  have hC : finish (summarize ((groups.map List.flatten).map
      fun leaf => finish (summarize leaf))) =
      finish (summarize (groups.map List.flatten).flatten) :=
    F_group (groups.map List.flatten)
  show finish (summarize (groups.map fun group =>
        finish (summarize (group.map fun leaf => finish (summarize leaf))))) =
      finish (summarize groups.flatten.flatten)
  rw [hA, hB, hC, flatten_map_flatten]

/-! ### ProjectionChain -/

private theorem compose_project (p q : Projection) (row : List (Option Int)) :
    project (composeSlots p q) row = project q (project p row) := by
  show List.map (fun (i : Option Nat) => i.bind fun (k : Nat) => (row[k]?).join)
        (List.map (fun (i : Option Nat) => i.bind fun (k : Nat) => (p[k]?).join) q) =
      List.map (fun (i : Option Nat) => i.bind fun (k : Nat) =>
          ((List.map (fun (j : Option Nat) => j.bind fun (m : Nat) => (row[m]?).join) p)[k]?).join) q
  rw [List.map_map]
  refine congrArg (fun f => List.map f q) (funext fun j => ?_)
  cases j with
  | none => rfl
  | some k =>
      simp only [List.getElem?_map]
      show ((p[k]?).join).bind (fun (m : Nat) => (row[m]?).join) =
        (Option.map (fun (j : Option Nat) => j.bind (fun (m : Nat) => (row[m]?).join))
          (p[k]? : Option (Option Nat))).join
      generalize hm : (p[k]? : Option (Option Nat)) = pm
      cases pm with
      | none => rfl
      | some pk => cases pk <;> rfl

/-- Projection-list fusion composes source slots exactly. -/
theorem sound_ProjectionChain : ProjectionChain.Goal := by
  intro ps
  induction ps with
  | nil => intro row p; rfl
  | cons q qs ih =>
      intro row p
      rw [List.foldl_cons, List.foldl_cons]
      exact (ih row (composeSlots p q)).trans
        (congrArg (fun r => List.foldl (fun r q => project q r) r qs) (compose_project p q row))

/-! ### MemoMisses -/

/-- `eraseDups` commutes with filtering by a decidable predicate. -/
private theorem ed_len_cons (k : Nat) (L : List Nat) :
    (k :: L).eraseDups.length
        = (L.filter fun m => !(m == k)).eraseDups.length + 1 := by
  rw [List.eraseDups_cons, List.length_cons]

private theorem TSum_map_summarize (source : Nat → List (Option Int)) (ids : List Nat) :
    TSum (ids.map (fun k => summarize (source k))) = summarize (ids.flatMap source) := by
  have inner : ∀ x, (source x).foldl (fun a v => plus a (weighted 1 v)) zero =
      summarize (source x) := by
    intro x; rw [foldl_plus_acc, plus_zero']
  refine Eq.trans ((foldl_TSum ids (fun k => summarize (source k))).symm) ?_
  refine Eq.trans (congrArg (fun h : Nat → Totals =>
      ids.foldl (fun a x => plus a (h x)) zero) (funext fun x => (inner x).symm)) ?_
  refine Eq.trans (summarize_flatMap ids source zero).symm ?_
  exact Eq.trans (foldl_plus_acc (ids.flatMap source) zero) (plus_zero' _)

/-- Replay with an initially valid cache: the accumulator folds every occurrence and
the call counter grows exactly on first occurrences of keys missing from the cache. -/
private theorem replay_spec (source : Nat → List (Option Int)) (F : Nat → Totals)
    (_hF : ∀ k, F k = summarize (source k)) :
    ∀ (ids : List Nat) (cache₀ : Std.HashMap Nat Totals) (acc₀ : Totals) (calls₀ : Nat),
      (∀ j a, cache₀[j]? = some a → a = F j) →
      ((ids.foldl (fun (cache,acc,calls) k => match cache[k]? with
            | some a => (cache,plus acc a,calls)
            | none => (cache.insert k (F k),plus acc (F k),calls+1))
            (cache₀,acc₀,calls₀)).2.1
          = plus acc₀ (TSum (ids.map F)) ∧
        (ids.foldl (fun (cache,acc,calls) k => match cache[k]? with
            | some a => (cache,plus acc a,calls)
            | none => (cache.insert k (F k),plus acc (F k),calls+1))
            (cache₀,acc₀,calls₀)).2.2
          = calls₀ + ((ids.filter (fun m => cache₀[m]? = none)).eraseDups).length) := by
  intro ids
  induction ids with
  | nil =>
      intro cache₀ acc₀ calls₀ hval
      exact ⟨(plus_zero acc₀).symm, (Nat.add_zero calls₀).symm⟩
  | cons k ids ih =>
      intro cache₀ acc₀ calls₀ hval
      rw [List.foldl_cons]
      cases hget : (cache₀[k]? : Option Totals) with
      | some x =>
          have hstep : (match (cache₀, acc₀, calls₀) with
                | (cache, acc, calls) =>
                    match cache[k]? with
                    | some a => (cache, plus acc a, calls)
                    | none => (cache.insert k (F k), plus acc (F k), calls + 1))
              = (cache₀, plus acc₀ x, calls₀) := by
            show (match cache₀[k]? with
                  | some a => (cache₀, plus acc₀ a, calls₀)
                  | none => (cache₀.insert k (F k), plus acc₀ (F k), calls₀ + 1))
              = (cache₀, plus acc₀ x, calls₀)
            rw [hget]
          have hfn : List.filter (fun m => cache₀[m]? = none) (k :: ids)
              = List.filter (fun m => cache₀[m]? = none) ids := by
            have hk : ¬(decide (cache₀[k]? = none) = true) := by
              intro he
              exact Option.some_ne_none x (hget.symm.trans (of_decide_eq_true he))
            rw [List.filter_cons, ite_eq_right hk]
          obtain ⟨h1, h2⟩ := ih cache₀ (plus acc₀ x) calls₀ hval
          rw [hstep, h1, h2, List.map_cons, TSum_cons, ← plus_assoc,
            show F k = x from (hval k x hget).symm, hfn]
          exact ⟨rfl, rfl⟩
      | none =>
          have hstep : (match (cache₀, acc₀, calls₀) with
                | (cache, acc, calls) =>
                    match cache[k]? with
                    | some a => (cache, plus acc a, calls)
                    | none => (cache.insert k (F k), plus acc (F k), calls + 1))
              = (cache₀.insert k (F k), plus acc₀ (F k), calls₀ + 1) := by
            show (match cache₀[k]? with
                  | some a => (cache₀, plus acc₀ a, calls₀)
                  | none => (cache₀.insert k (F k), plus acc₀ (F k), calls₀ + 1))
              = (cache₀.insert k (F k), plus acc₀ (F k), calls₀ + 1)
            rw [hget]
          have hval₁ : ∀ j a, (cache₀.insert k (F k))[j]? = some a → a = F j := by
            intro j a h
            rw [Std.HashMap.getElem?_insert] at h
            by_cases hb : (k == j) = true
            · rw [ite_eq_left hb] at h
              have hkj : k = j := natbeq_true hb
              subst hkj
              exact (Option.some.inj h).symm
            · rw [ite_eq_right hb] at h
              exact hval j a h
          have hfp : List.filter (fun m => cache₀[m]? = none) (k :: ids)
              = k :: List.filter (fun m => cache₀[m]? = none) ids := by
            have hk : decide (cache₀[k]? = none) = true := decide_eq_true hget
            rw [List.filter_cons, ite_eq_left hk]
          have hf1 : List.filter (fun m => (cache₀.insert k (F k))[m]? = none) ids
              = List.filter (fun m => !(m == k))
                (List.filter (fun m => cache₀[m]? = none) ids) := by
            rw [List.filter_filter]
            refine List.filter_congr (fun m _ => ?_)
            rw [Std.HashMap.getElem?_insert]
            by_cases hmk : m = k
            · subst hmk
              simp
            · have hkm : ¬(k = m) := fun he => hmk he.symm
              simp [hmk, hkm]
          obtain ⟨h1, h2⟩ := ih (cache₀.insert k (F k)) (plus acc₀ (F k)) (calls₀+1) hval₁
          rw [hstep, h1, h2, List.map_cons, TSum_cons, ← plus_assoc]
          rw [hf1, hfp, ed_len_cons]
          exact ⟨rfl, by omega⟩

/-- Memoized replay returns the bag sum and counts exactly the distinct cold leaves. -/
theorem sound_MemoMisses : MemoMisses.Goal := by
  intro plan source
  have hval : ∀ j a, ({} : Std.HashMap Nat Totals)[j]? = some a → a = summarize (source j) := by
    intro j a h
    exact absurd h (by simp)
  obtain ⟨h1, h2⟩ := replay_spec source (fun k => summarize (source k)) (fun _ => rfl)
    (leaves plan) ({} : Std.HashMap Nat Totals) zero 0 hval
  refine ⟨h1.trans (by rw [plus_zero', TSum_map_summarize, ← run_eq_flatMap]), ?_⟩
  · exact h2.trans (by
      rw [Nat.zero_add,
        filter_all_true (p := fun m => ({} : Std.HashMap Nat Totals)[m]? = none)
          (l := leaves plan) (fun m _ => by rw [Std.HashMap.getElem?_empty]; decide)])

/-! ### RangeMultiplicity -/

private theorem fil_map_snd (cuts : List (Int × Int)) (p : Int → Bool) :
    List.filter p (cuts.map Prod.snd) = (cuts.filter fun i => p i.2).map Prod.snd := by
  induction cuts with
  | nil => rfl
  | cons i cs ih =>
      rw [List.map_cons, List.filter_cons, ih]
      cases h : p i.2 <;> simp [h]

private theorem fil_map_fst (cuts : List (Int × Int)) (p : Int → Bool) :
    List.filter p (cuts.map Prod.fst) = (cuts.filter fun i => p i.1).map Prod.fst := by
  induction cuts with
  | nil => rfl
  | cons i cs ih =>
      rw [List.map_cons, List.filter_cons, ih]
      cases h : p i.1 <;> simp [h]

private theorem hN (cuts : List (Int × Int)) (v : Int)
    (hcut : ∀ i ∈ cuts, i.1 ≤ i.2) :
    ((cuts.map Prod.fst).filter fun k => decide (k ≤ v)).length
        - ((cuts.map Prod.snd).filter fun k => decide (k ≤ v)).length
      = (cuts.filter fun i => i.1 ≤ v && v < i.2).length := by
  have hlenP : ((cuts.map Prod.fst).filter fun k => decide (k ≤ v)).length
      = ((cuts.filter fun i => decide (i.1 ≤ v)).map Prod.fst).length :=
    congrArg List.length (fil_map_fst cuts (fun k => decide (k ≤ v)))
  have hlenS : ((cuts.map Prod.snd).filter fun k => decide (k ≤ v)).length
      = ((cuts.filter fun i => decide (i.2 ≤ v)).map Prod.snd).length :=
    congrArg List.length (fil_map_snd cuts (fun k => decide (k ≤ v)))
  have hpart : List.Perm
      ((cuts.filter fun i => decide (i.1 ≤ v)).filter (fun i => decide (i.2 ≤ v))
        ++ (cuts.filter fun i => decide (i.1 ≤ v)).filter
            (fun i => !(decide (i.2 ≤ v))))
      (cuts.filter fun i => decide (i.1 ≤ v)) :=
    filter_not_partition (fun i => decide (i.2 ≤ v))
      (cuts.filter fun i => decide (i.1 ≤ v))
  have hQ : (cuts.filter fun i => decide (i.1 ≤ v)).filter (fun i => decide (i.2 ≤ v))
      = cuts.filter fun i => decide (i.2 ≤ v) := by
    rw [List.filter_filter]
    refine List.filter_congr (fun i hi => ?_)
    have hb : i.1 ≤ i.2 := hcut i hi
    show (decide (i.2 ≤ v) && decide (i.1 ≤ v)) = decide (i.2 ≤ v)
    cases h2 : decide (i.2 ≤ v) with
    | true =>
        have h1 : decide (i.1 ≤ v) = true :=
          decide_eq_true (Int.le_trans hb (of_decide_eq_true h2))
        rw [h1]
        exact rfl
    | false => rw [Bool.false_and]
  have hnotQ : (cuts.filter fun i => decide (i.1 ≤ v)).filter
      (fun i => !(decide (i.2 ≤ v)))
      = cuts.filter fun i => i.1 ≤ v && v < i.2 := by
    rw [List.filter_filter]
    refine List.filter_congr (fun i hi => ?_)
    have hb : i.1 ≤ i.2 := hcut i hi
    show (!decide (i.2 ≤ v) && decide (i.1 ≤ v))
        = (decide (i.1 ≤ v) && decide (v < i.2))
    cases h1 : decide (i.1 ≤ v) with
    | true =>
        simp only [Bool.and_true, Bool.true_and]
        cases h2 : decide (i.2 ≤ v) with
        | true =>
            rw [Bool.not_true, decide_eq_false (fun hlt =>
              Int.lt_irrefl v (Int.lt_of_lt_of_le hlt (of_decide_eq_true h2)))]
        | false =>
            rw [Bool.not_false, decide_eq_true (Int.not_le.mp (fun hh =>
              Bool.false_ne_true (h2.symm.trans (decide_eq_true hh))))]
    | false => simp only [Bool.and_false, Bool.false_and]
  rw [hlenP, hlenS, List.length_map, List.length_map, ← hpart.length_eq,
    List.length_append, hQ, hnotQ]
  omega

private theorem filter_mergeSort_length {α : Type} (l : List α)
    (le : α → α → Bool) (p : α → Bool) :
    ((List.mergeSort (le := le) l).filter p).length = (l.filter p).length := by
  exact ((List.mergeSort_perm l le).filter p).length_eq

/-- Endpoint takeWhile counts reduce to filter counts on the unsorted endpoints. -/
private theorem hTw (cuts : List (Int × Int)) (x : Int) :
    (((cuts.map Prod.fst).mergeSort (fun a b => decide (a ≤ b))).takeWhile
        (fun v => decide (v ≤ x))).length
        - (((cuts.map Prod.snd).mergeSort (fun a b => decide (a ≤ b))).takeWhile
            (fun v => decide (v ≤ x))).length
      = ((cuts.map Prod.fst).filter fun v => decide (v ≤ x)).length
        - ((cuts.map Prod.snd).filter fun v => decide (v ≤ x)).length := by
  have hle : ∀ {a b : Int}, (fun x y => decide (x ≤ y)) a b = true →
      ¬((fun v => decide (v ≤ x)) a = true) → ¬((fun v => decide (v ≤ x)) b = true) := by
    intro a b hab hna hb
    exact hna (decide_eq_true (Int.le_trans (of_decide_eq_true hab) (of_decide_eq_true hb)))
  have hf := takeWhile_eq_filter_of_sorted (fun a b => decide (a ≤ b))
      (fun v => decide (v ≤ x))
      ((cuts.map Prod.fst).mergeSort (fun a b => decide (a ≤ b))) hle
      (mergeSort_pairwise (fun a b => decide (a ≤ b))
        (fun a b c h₁ h₂ =>
          decide_eq_true (Int.le_trans (of_decide_eq_true h₁) (of_decide_eq_true h₂)))
        (decide_rel_total Int.le_total) (cuts.map Prod.fst))
  have hs := takeWhile_eq_filter_of_sorted (fun a b => decide (a ≤ b))
      (fun v => decide (v ≤ x))
      ((cuts.map Prod.snd).mergeSort (fun a b => decide (a ≤ b))) hle
      (mergeSort_pairwise (fun a b => decide (a ≤ b))
        (fun a b c h₁ h₂ =>
          decide_eq_true (Int.le_trans (of_decide_eq_true h₁) (of_decide_eq_true h₂)))
        (decide_rel_total Int.le_total) (cuts.map Prod.snd))
  rw [hf, hs, filter_mergeSort_length (cuts.map Prod.fst) (fun a b => decide (a ≤ b))
      (fun v => decide (v ≤ x)),
    filter_mergeSort_length (cuts.map Prod.snd) (fun a b => decide (a ≤ b))
      (fun v => decide (v ≤ x))]

private theorem weighted_zero (v : Option Int) : weighted (0 : Nat) v = zero := by
  cases v <;> simp [weighted, zero]

private theorem fil_cons_none (vs : List (Option Int)) (q : Int → Bool) :
    (none :: vs).filter (fun v => v.any q) = vs.filter (fun v => v.any q) := by
  rw [List.filter_cons]
  simp

private theorem fil_cons_some (vs : List (Option Int)) (q : Int → Bool) (y : Int)
    (h : q y = true) :
    (some y :: vs).filter (fun v => v.any q) = some y :: vs.filter (fun v => v.any q) := by
  have hc : (fun v => v.any q) (some y) = true := h
  rw [List.filter_cons, ite_eq_left hc]

private theorem fil_cons_no (vs : List (Option Int)) (q : Int → Bool) (y : Int)
    (h : q y = false) :
    (some y :: vs).filter (fun v => v.any q) = vs.filter (fun v => v.any q) := by
  have hc : ¬((fun v => v.any q) (some y) = true) := fun hh =>
    Bool.false_ne_true (h ▸ hh)
  rw [List.filter_cons, ite_eq_right hc]

private theorem single_interval (rows : List (Option Int)) (q : Int → Bool) :
    ∀ (z : Totals),
      (rows.filter fun v => v.any q).foldl (fun a v => plus a (weighted 1 v)) z
        = (rows.filterMap id).foldl
            (fun a x => plus a (weighted (if q x then 1 else 0) (some x))) z := by
  intro z
  induction rows generalizing z with
  | nil => rfl
  | cons v vs ih =>
      cases v with
      | none =>
          rw [fil_cons_none vs q]
          rw [show ((none :: vs).filterMap id) = vs.filterMap id from rfl]
          exact ih z
      | some y =>
          cases hq : q y with
          | true =>
              rw [fil_cons_some vs q y hq,
                show (some y :: vs).filterMap id = y :: vs.filterMap id from rfl,
                List.foldl_cons, List.foldl_cons, ih, ite_eq_left hq]
          | false =>
              rw [fil_cons_no vs q y hq,
                show (some y :: vs).filterMap id = y :: vs.filterMap id from rfl,
                List.foldl_cons, ih,
                ite_eq_right (fun hh => Bool.false_ne_true (hq ▸ hh)),
                weighted_zero, plus_zero]

private theorem hlen (q : Int × Int → Bool) (i : Int × Int) (cs : List (Int × Int)) :
    ((i :: cs).filter q).length = (if q i then 1 else 0) + (cs.filter q).length := by
  rw [List.filter_cons]
  cases h : q i <;> simp <;> omega

private theorem swap_sum (rows : List (Option Int)) :
    ∀ (cuts : List (Int × Int)) (z : Totals),
      List.foldl (fun a v => plus a (weighted 1 v)) z
        (cuts.flatMap fun i => rows.filter fun v => v.any fun x => i.1 ≤ x && x < i.2)
        = (rows.filterMap id).foldl
            (fun a x =>
              plus a (weighted ((cuts.filter fun i => i.1 ≤ x && x < i.2).length) (some x))) z := by
  intro cuts
  induction cuts with
  | nil =>
      intro z
      have h0 : List.flatMap (fun i => rows.filter fun v => v.any fun x => i.1 ≤ x && x < i.2)
          ([] : List (Int × Int)) = [] := rfl
      have hnil : List.foldl (fun a v => plus a (weighted 1 v)) z ([] : List (Option Int)) = z :=
        rfl
      have hstepf : (fun a x =>
          plus a (weighted ((([] : List (Int × Int))).filter fun i =>
            i.1 ≤ x && x < i.2).length (some x))) = (fun a x => a) := by
        funext a x
        show plus a (weighted (0 : Nat) (some x)) = a
        rw [weighted_zero, plus_zero]
      rw [h0, hnil, hstepf, foldl_id]
  | cons i cs ih =>
      intro z
      rw [List.flatMap_cons, List.foldl_append, ih,
        single_interval rows (fun x => i.1 ≤ x && x < i.2) z]
      rw [← foldl_fuse (rows.filterMap id)
        (fun x => weighted (if (i.1 ≤ x && x < i.2) then 1 else 0) (some x))
        (fun x => weighted ((cs.filter fun k => k.1 ≤ x && x < k.2).length) (some x)) z]
      refine congrArg (fun s : Totals → Int → Totals => (rows.filterMap id).foldl s z) ?_
      refine funext (fun acc => funext (fun v => ?_))
      rw [hlen (fun k => k.1 ≤ v && v < k.2) i cs, weighted_add]


/-! ### SpoolBudget -/

private theorem spool_lt (b w r u : Nat) (h : w + (u - 1) * r < (u - 1) * b) :
    b + w + (u - 1) * r ≤ u * b := by
  cases u with
  | zero => omega
  | succ v =>
      have hsub : Nat.succ v - 1 = v := by omega
      rw [hsub] at h ⊢
      rw [Nat.succ_mul]
      omega

private theorem spool_ge (b w r u : Nat) (h : ¬(w + (u - 1) * r < (u - 1) * b)) :
    u * b ≤ b + w + (u - 1) * r := by
  cases u with
  | zero => rw [Nat.zero_mul]; exact Nat.zero_le _
  | succ v =>
      have hsub : Nat.succ v - 1 = v := by omega
      rw [hsub] at h ⊢
      rw [Nat.succ_mul]
      omega

private theorem foldl_le2 {stepL stepR : Nat → Nat → Nat}
    (h1 : ∀ n k, stepL n k ≤ stepR n k)
    (h2 : ∀ x y k, x ≤ y → stepR x k ≤ stepR y k) :
    ∀ (l : List Nat) (a b : Nat), a ≤ b → l.foldl stepL a ≤ l.foldl stepR b := by
  intro l
  induction l with
  | nil => intro a b hab; exact hab
  | cons t rest ih =>
      intro a b hab
      rw [List.foldl_cons, List.foldl_cons]
      exact ih (stepL a t) (stepR b t) (Nat.le_trans (h1 a t) (h2 a b t hab))

/-- The cost-derived spool policy is no more expensive than any other policy. -/
theorem sound_SpoolBudget : SpoolBudget.Goal := by
  intro uses build write read policy
  have hcase : ∀ k : Nat,
      (if decide (write k + (uses.count k - 1) * read k < (uses.count k - 1) * build k)
          then build k + write k + (uses.count k - 1) * read k
        else uses.count k * build k)
        ≤ (if policy k then build k + write k + (uses.count k - 1) * read k
          else uses.count k * build k) := by
    intro k
    by_cases hlt :
        decide (write k + (uses.count k - 1) * read k < (uses.count k - 1) * build k) = true
    · by_cases hpo : policy k = true
      · rw [ite_eq_left hlt, ite_eq_left hpo]
        exact Nat.le_refl _
      · rw [ite_eq_left hlt, ite_eq_right hpo]
        exact spool_lt (build k) (write k) (read k) (uses.count k) (of_decide_eq_true hlt)
    · by_cases hpo : policy k = true
      · rw [ite_eq_right hlt, ite_eq_left hpo]
        exact spool_ge (build k) (write k) (read k) (uses.count k)
          (fun hh => hlt (decide_eq_true hh))
      · rw [ite_eq_right hlt, ite_eq_right hpo]
        exact Nat.le_refl _
  show (uses.eraseDups.foldl (fun n k =>
        n + (if decide (write k + (uses.count k - 1) * read k < (uses.count k - 1) * build k)
            then build k + write k + (uses.count k - 1) * read k
          else uses.count k * build k)) 0)
      ≤ (uses.eraseDups.foldl (fun n k =>
          n + (if policy k then build k + write k + (uses.count k - 1) * read k
            else uses.count k * build k)) 0)
  refine foldl_le2 ?_ ?_ uses.eraseDups 0 0 (Nat.le_refl 0)
  · intro n k
    exact Nat.add_le_add_left (hcase k) n
  · intro x y k hx
    exact Nat.add_le_add_right hx _

/-! ### GroupSignature -/

/-- Duplicate-free GROUP BY slots induce exactly the same tuple equality. -/
theorem sound_GroupSignature : GroupSignature.Goal := by
  intro slots x y
  show (slots.eraseDups.map (fun i => i.bind fun k => (x[k]?).join))
        = (slots.eraseDups.map (fun i => i.bind fun k => (y[k]?).join)) ↔
      (slots.map (fun i => i.bind fun k => (x[k]?).join))
        = (slots.map (fun i => i.bind fun k => (y[k]?).join))
  rw [map_congr_iff, map_congr_iff]
  constructor
  · intro h a ha
    exact h a (List.mem_eraseDups.2 ha)
  · intro h a ha
    exact h a (List.mem_eraseDups.1 ha)

private theorem histogram_foldl_add' {α : Type} [BEq α] [Hashable α] [LawfulBEq α]
    [LawfulHashable α] (xs : List α) (f : α → Nat) :
    ((histogram xs).toList).foldl (fun n w => n + f w.1 * w.2) 0 = (xs.map f).sum :=
  histogram_toList_foldl_add xs f

/-! ### CanonicalFilters -/

private theorem map_comp {α β γ : Type} (xs : List α) (f : α → β) (g : β → γ) :
    (xs.map f).map g = xs.map (fun x => g (f x)) := by
  induction xs with
  | nil => rfl
  | cons x rest ih =>
      show g (f x) :: (List.map f rest).map g = g (f x) :: List.map (fun x => g (f x)) rest
      rw [ih]

private theorem all_eq_of_mem {α : Type} (l m : List α) (p : α → Bool)
    (h : ∀ a, a ∈ l ↔ a ∈ m) : l.all p = m.all p := by
  cases h1 : l.all p with
  | true =>
      have hm : m.all p = true := List.all_eq_true.mpr fun a ha =>
        List.all_eq_true.mp h1 a (h a |>.mpr ha)
      exact hm.symm
  | false =>
      obtain ⟨a, ha, hp⟩ := List.all_eq_false.mp h1
      have hm : m.all p = false := List.all_eq_false.mpr ⟨a, h a |>.mp ha, hp⟩
      exact hm.symm


theorem sound_CanonicalFilters : CanonicalFilters.Goal := by
  intro branches rows atom
  have hmem : ∀ (ids : List Nat) (r : Nat),
      (ids.eraseDups.mergeSort).all (fun i => atom i r == some true)
        = ids.all (fun i => atom i r == some true) := by
    intro ids r
    refine all_eq_of_mem ids.eraseDups.mergeSort ids (fun i => atom i r == some true) ?_
    intro a
    rw [List.mem_mergeSort, List.mem_eraseDups]
  have hkey : ∀ ids : List Nat,
      rows.countP (fun r => (ids.eraseDups.mergeSort).all (fun i => atom i r == some true))
        = rows.countP (fun r => ids.all (fun i => atom i r == some true)) := by
    intro ids
    exact congrArg (List.countP (l := rows)) (funext fun r => hmem ids r)
  show (((histogram (branches.map fun ids => ids.eraseDups.mergeSort)).toList).foldl
          (fun n ids_w =>
            n + (rows.countP fun r => ids_w.1.all fun i => atom i r == some true) * ids_w.2) 0)
      = (branches.flatMap fun ids =>
          rows.filter fun r => ids.all fun x => atom x r == some true).length
  rw [histogram_foldl_add' (α := List Nat)
        (xs := branches.map fun ids =>
          ids.eraseDups.mergeSort fun a b => decide (a ≤ b))
        (f := fun ids => rows.countP (fun r => ids.all (fun i => atom i r == some true)))]
  rw [map_comp]
  have hmap : (branches.map fun ids =>
        rows.countP (fun r => (ids.eraseDups.mergeSort).all (fun i => atom i r == some true)))
      = branches.map fun ids => rows.countP (fun r => ids.all (fun i => atom i r == some true)) :=
    map_congr_mem branches
      (fun ids => rows.countP (fun r => (ids.eraseDups.mergeSort).all
        (fun i => atom i r == some true)))
      (fun ids => rows.countP (fun r => ids.all (fun i => atom i r == some true)))
      fun ids _ => hkey ids
  rw [List.length_flatMap, hmap]
  refine congrArg List.sum
    (map_congr_mem branches
      (fun ids => rows.countP (fun r => ids.all (fun x => atom x r == some true)))
      (fun ids => (rows.filter (fun r => ids.all (fun x => atom x r == some true))).length)
      fun ids _ => by rw [List.countP_eq_length_filter])

/-! ### SharedProjection -/

/-- Distinct composed slot vectors suffice to reproduce every consumer's value
bag after a shared upstream projection. -/
theorem sound_SharedProjection : SharedProjection.Goal := by
  intro p qs rows value
  have hstepc : (fun (n : Nat) (qw : Projection × Nat) =>
        n + qw.2 * ((rows.map (project qw.1)).count value))
      = (fun (n : Nat) (qw : Projection × Nat) =>
        n + ((rows.map (project qw.1)).count value) * qw.2) := by
    funext n qw
    rw [Nat.mul_comm]
  show (((histogram (qs.map fun q => composeSlots p q)).toList).foldl
          (fun n qw => n + qw.2 * ((rows.map (project qw.1)).count value)) 0)
      = (qs.flatMap fun q => (rows.map (project p)).map (project q)).count value
  rw [hstepc,
    histogram_foldl_add' (xs := qs.map fun q => composeSlots p q)
      (f := fun q => (rows.map (project q)).count value),
    map_comp]
  rw [List.count_flatMap]
  refine congrArg List.sum (map_congr_mem qs
    (fun q => (rows.map (project (composeSlots p q))).count value)
    (fun q => List.count value ((rows.map (project p)).map (project q)))
    fun q _ => ?_)
  refine congrArg (List.count value) ?_
  rw [map_comp]
  refine map_congr_mem rows (fun row => project (composeSlots p q) row)
    (fun row => project q (project p row)) (fun row _ => ?_)
  exact compose_project p q row

theorem sound_RangeMultiplicity : RangeMultiplicity.Goal := by
  intro cuts rows hcut
  have hn : ∀ (x : Int),
      (((cuts.map Prod.fst).mergeSort (fun a b => decide (a ≤ b))).takeWhile
            (fun v => decide (v ≤ x))).length
          - (((cuts.map Prod.snd).mergeSort (fun a b => decide (a ≤ b))).takeWhile
              (fun v => decide (v ≤ x))).length
        = (cuts.filter fun i => i.1 ≤ x && x < i.2).length := by
    intro x
    rw [hTw cuts x, hN cuts x hcut]
  have hconv : (rows.filterMap id).foldl
          (fun a x =>
            plus a (weighted
              ((((cuts.map Prod.fst).mergeSort (fun a b => decide (a ≤ b))).takeWhile
                      (fun v => decide (v ≤ x))).length
                  - (((cuts.map Prod.snd).mergeSort (fun a b => decide (a ≤ b))).takeWhile
                      (fun v => decide (v ≤ x))).length)
              (some x)))
          zero
      = (rows.filterMap id).foldl
          (fun a x =>
            plus a (weighted ((cuts.filter fun i => i.1 ≤ x && x < i.2).length)
              (some x)))
          zero := by
    refine congrArg (fun s : Totals → Int → Totals => (rows.filterMap id).foldl s zero) ?_
    refine funext (fun acc => funext (fun x => ?_))
    rw [hn x]
  show (rows.filterMap id).foldl
          (fun a x =>
            plus a (weighted
              ((((cuts.map Prod.fst).mergeSort (fun a b => decide (a ≤ b))).takeWhile
                      (fun v => decide (v ≤ x))).length
                  - (((cuts.map Prod.snd).mergeSort (fun a b => decide (a ≤ b))).takeWhile
                      (fun v => decide (v ≤ x))).length)
              (some x)))
          zero
      = summarize (cuts.flatMap fun lohi =>
          rows.filter fun v => v.any fun x => lohi.1 ≤ x && x < lohi.2)
  rw [hconv, (swap_sum rows cuts zero).symm]
  rfl

end TinylambOpt.ResearchProofs.CloudBI.NestedBI
