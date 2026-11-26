import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `Contiguous.PositiveGapRule` — `∀ fee xs, let ks := points xs let gaps := ks.zip
  (ks.drop 1) cost (price fee) (choose (price fee) xs) = ks.length + (if ks.isEmpty
  then 0 else fee) + (gaps.map (fun (a,b) => min fee (b-a-1))).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Contiguous_PositiveGapRule

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans.Contiguous

/-!
The exhaustive planner `choose` returns a minimum-cost cover of the sorted
distinct points.  For the contiguous byte model a cover is a partition into
consecutive extents; each gap between neighbouring points is either bridged
(paying the gap as overread) or split (paying one extra seek fee).  Hence the
optimum pays `fee` once plus `min fee gap` for every gap, which is exactly the
closed form in the goal.
-/

private def target (fee : Nat) (ks : List Nat) : Nat :=
  ks.length + (if ks.isEmpty then 0 else fee) +
    ((ks.zip (ks.drop 1)).map (fun (a, b) => min fee (b - a - 1))).sum

private theorem target_one (fee x : Nat) : target fee [x] = fee + 1 := by
  simp [target, Nat.add_comm]

private theorem target_cons_cons (fee x y : Nat) (ys : List Nat) :
    target fee (x :: y :: ys) = target fee (y :: ys) + 1 + min fee (y - x - 1) := by
  simp [target]
  omega

-- ── normalized points are strictly increasing ──

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  cases xs with
  | nil => simp
  | cons x xs =>
    rw [List.eraseDups_cons, List.nodup_cons]
    constructor
    · intro hx
      have hxf : x ∈ List.filter (fun b => !b == x) xs := List.mem_eraseDups.mp hx
      have hx' := List.mem_filter.mp hxf
      have hxx : x ∈ xs.eraseDups := List.mem_eraseDups.mpr hx'.1
      simp at hx'
    · exact eraseDups_nodup (List.filter (fun b => !b == x) xs)
termination_by xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b => !b == x) xs)

private theorem pairwise_le_to_lt {l : List Nat}
    (hle : l.Pairwise (fun a b => a ≤ b)) (hnd : l.Nodup) :
    l.Pairwise (fun a b => a < b) := by
  induction l with
  | nil => exact .nil
  | cons a l ih =>
    rw [List.pairwise_cons] at hle
    rw [List.nodup_cons] at hnd
    exact .cons (by
      intro b hb
      exact Nat.lt_of_le_of_ne (hle.1 b hb) (by
        intro hab
        apply hnd.1
        simpa [hab] using hb)) (ih hle.2 hnd.2)

private theorem points_strict (xs : List Nat) :
    (points xs).Pairwise (fun a b => a < b) := by
  unfold points
  let le : Nat → Nat → Bool := fun a b => decide (a ≤ b)
  have hpairBool : (xs.eraseDups.mergeSort le).Pairwise (fun a b => le a b = true) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp only [le, decide_eq_true_eq] at hab hbc ⊢
      omega
    · intro a b
      simp only [le, Bool.or_eq_true, decide_eq_true_eq]
      omega
  have hpairProp : (xs.eraseDups.mergeSort le).Pairwise (fun a b => a ≤ b) :=
    hpairBool.imp (by intro a b h; simpa [le] using h)
  have hperm := List.mergeSort_perm xs.eraseDups le
  have hnd : (xs.eraseDups.mergeSort le).Nodup :=
    (hperm.nodup_iff).mpr (eraseDups_nodup xs)
  exact pairwise_le_to_lt hpairProp hnd

-- ── `choose` is the minimum-cost cover ──

private theorem covers_nonempty (xs : List Nat) : ∃ ps, ps ∈ covers xs := by
  induction xs with
  | nil => exact ⟨[], by simp [covers]⟩
  | cons x xs ih =>
    obtain ⟨q, hq⟩ := ih
    cases q with
    | nil =>
      exact ⟨[(x, x)], by
        rw [covers]
        exact List.mem_flatMap.mpr ⟨[], hq, by simp⟩⟩
    | cons e rest =>
      exact ⟨(x, x) :: e :: rest, by
        rw [covers]
        exact List.mem_flatMap.mpr ⟨e :: rest, hq, by simp⟩⟩

private theorem covers_ne_nil (xs : List Nat) : covers xs ≠ [] := by
  obtain ⟨ps, hps⟩ := covers_nonempty xs
  intro h
  simp [h] at hps

private theorem pairwise_head?_le {α : Type} {le : α → α → Bool}
    (hrefl : ∀ x, le x x = true)
    {l : List α} {a b : α} (hpw : l.Pairwise (fun x y => le x y = true))
    (hhead : l.head? = some a) (hmem : b ∈ l) : le a b = true := by
  cases l with
  | nil => simp at hmem
  | cons x xs =>
    simp at hhead
    subst hhead
    rw [List.pairwise_cons] at hpw
    rcases List.mem_cons.mp hmem with rfl | hmem
    · exact hrefl _
    · exact hpw.1 b hmem

private theorem choose_mem_covers (f : Price) (xs : List Nat) :
    choose f xs ∈ covers (points xs) := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hperm := List.mergeSort_perm (covers (points xs)) le
  have hne' : (covers (points xs)).mergeSort le ≠ [] := by
    intro h
    have := hperm.symm.length_eq
    rw [h] at this
    simp at this
    exact covers_ne_nil (points xs) this
  cases h : (covers (points xs)).mergeSort le with
  | nil => exact absurd h hne'
  | cons hd tl =>
    have hmem : hd ∈ (covers (points xs)).mergeSort le := by simp [h]
    rw [List.mem_mergeSort] at hmem
    simpa [le, h] using hmem

private theorem choose_le (f : Price) (xs : List Nat) (ps : List Extent)
    (hmem : ps ∈ covers (points xs)) :
    cost f (choose f xs) ≤ cost f ps := by
  unfold choose
  let le := fun a b => decide (cost f a ≤ cost f b)
  have hpair : List.Pairwise (fun a b => le a b = true)
      ((covers (points xs)).mergeSort le) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp only [le, decide_eq_true_eq] at hab hbc ⊢
      exact Nat.le_trans hab hbc
    · intro a b
      simp only [le, Bool.or_eq_true, decide_eq_true_eq]
      exact Nat.le_total (cost f a) (cost f b)
  have hmem' : ps ∈ (covers (points xs)).mergeSort le := List.mem_mergeSort.mpr hmem
  cases h : (covers (points xs)).mergeSort le with
  | nil => simp [h] at hmem'
  | cons hd tl =>
    simp only [List.head?_cons, Option.getD_some]
    have hle := pairwise_head?_le (fun x => by simp [le]) hpair
      (show ((covers (points xs)).mergeSort le).head? = some hd by simp [h]) hmem'
    simpa [le] using hle

-- ── structural decomposition of covers ──

private theorem covers_cons_mem {x : Nat} {rest : List Nat} {ps : List Extent}
    (h : ps ∈ covers (x :: rest)) :
    ∃ q ∈ covers rest,
      (q = [] → ps = [(x, x)]) ∧
      (∀ c b r, q = (c, b) :: r → ps = (x, x) :: q ∨ ps = (x, b) :: r) := by
  simp only [covers, List.mem_flatMap] at h
  obtain ⟨q, hq, hmem⟩ := h
  refine ⟨q, hq, ?_, ?_⟩
  · intro hqnil
    subst q
    simpa using hmem
  · intro c b r hqeq
    subst q
    simp only [List.mem_cons] at hmem
    rcases hmem with hmem | hmem
    · left; simpa using hmem
    · right; simpa using hmem

private theorem covers_start (xs : List Nat) (ps : List Extent)
    (h : ps ∈ covers xs) :
    xs = [] ∨ ∃ b r, ps = (xs.head?.getD 0, b) :: r := by
  cases xs with
  | nil => exact .inl rfl
  | cons x rest =>
    right
    obtain ⟨q, hq, hnil, hcons⟩ := covers_cons_mem h
    cases hq' : q with
    | nil =>
      refine ⟨x, [], ?_⟩
      have hp := hnil hq'
      simp [hp]
    | cons e r =>
      rcases hcons e.1 e.2 r (by simpa using hq') with hps | hps
      · exact ⟨x, q, hps⟩
      · exact ⟨e.2, r, hps⟩

private theorem covers_nil_only {rest : List Nat} {q : List Extent}
    (h : q ∈ covers rest) (hqnil : q = []) : rest = [] := by
  cases rest with
  | nil => rfl
  | cons x xs =>
    obtain ⟨r, hr, hnil, hcons⟩ := covers_cons_mem h
    cases hr' : r with
    | nil =>
      have hp := hnil hr'
      simp [hp] at hqnil
    | cons e r' =>
      rcases hcons e.1 e.2 r' (by simpa using hr') with hps | hps <;> simp [hps] at hqnil

private theorem covers_le (xs : List Nat) (hstr : xs.Pairwise (fun a b => a < b))
    (ps : List Extent) (h : ps ∈ covers xs) :
    ∀ a b, (a, b) ∈ ps → a ≤ b := by
  induction xs generalizing ps with
  | nil =>
    simp [covers] at h
    subst ps
    simp
  | cons x rest ih =>
    obtain ⟨q, hq, hnil, hcons⟩ := covers_cons_mem h
    have hrest : rest.Pairwise (fun a b => a < b) := by
      rw [List.pairwise_cons] at hstr
      exact hstr.2
    have ihq := ih hrest q hq
    intro a b hab
    cases hq' : q with
    | nil =>
      have hp := hnil hq'
      simp [hp] at hab
      omega
    | cons e r =>
      rcases hcons e.1 e.2 r (by simpa using hq') with hps | hps
      · rw [hps] at hab
        simp only [List.mem_cons] at hab
        rcases hab with hab | hab
        · have ha : a = x := congrArg Prod.fst hab
          have hb : b = x := congrArg Prod.snd hab
          omega
        · exact ihq a b hab
      · rw [hps] at hab
        simp only [List.mem_cons] at hab
        rcases hab with hab | hab
        · have he := ihq e.1 e.2 (by simp [hq'])
          cases rest with
          | nil => simp [covers, hq'] at hq
          | cons y ys =>
            have hxy : x < y := by
              rw [List.pairwise_cons] at hstr
              exact hstr.1 y (by simp)
            have he1y : e.1 = y := by
              obtain ⟨b', r', hqeq⟩ := (covers_start (y :: ys) q hq).resolve_left (by simp)
              rw [hq'] at hqeq
              have heq : e = ((y :: ys).head?.getD 0, b') := by
                injection hqeq with h1
              simpa using congrArg Prod.fst heq
            have ha : a = x := congrArg Prod.fst hab
            have hb : b = e.2 := congrArg Prod.snd hab
            omega
        · exact ihq a b (by simp [hq', hab])

-- ── every cover costs at least the closed form ──

private theorem lower_bound (fee : Nat) :
    ∀ ks : List Nat, ks.Pairwise (fun a b => a < b) →
      ∀ ps ∈ covers ks, target fee ks ≤ cost (price fee) ps := by
  intro ks hstr
  induction ks with
  | nil =>
    intro ps hps
    simp [covers] at hps
    subst ps
    simp [target, cost]
  | cons x rest ih =>
    intro ps hps
    have hrest : rest.Pairwise (fun a b => a < b) := by
      rw [List.pairwise_cons] at hstr
      exact hstr.2
    obtain ⟨q, hq, hnil, hcons⟩ := covers_cons_mem hps
    cases hq' : q with
    | nil =>
      have hrestnil := covers_nil_only hq hq'
      subst rest
      have hp := hnil hq'
      rw [hp]
      simp [target_one, cost, price, span]
    | cons e r =>
      rcases hcons e.1 e.2 r (by simpa using hq') with hpssplit | hpsbridge
      · rw [hpssplit]
        cases rest with
        | nil => simp [covers, hq'] at hq
        | cons y ys =>
          have hih := ih hrest q hq
          have ht := target_cons_cons fee x y ys
          rw [ht]
          have hcost : cost (price fee) ((x, x) :: q) =
              fee + 1 + cost (price fee) q := by
            simp [cost, price, span]
          rw [hcost]
          omega
      · rw [hpsbridge]
        cases rest with
        | nil => simp [covers, hq'] at hq
        | cons y ys =>
          have hih := ih hrest q hq
          have ht := target_cons_cons fee x y ys
          rw [ht]
          have hxy : x < y := by
            rw [List.pairwise_cons] at hstr
            exact hstr.1 y (by simp)
          have he1y : e.1 = y := by
            obtain ⟨b', r', hqeq⟩ := (covers_start (y :: ys) q hq).resolve_left (by simp)
            rw [hq'] at hqeq
            have heq : e = ((y :: ys).head?.getD 0, b') := by
              injection hqeq with h1
            simpa using congrArg Prod.fst heq
          have he12 : e.1 ≤ e.2 :=
            covers_le (y :: ys) hrest q hq e.1 e.2 (by simp [hq'])
          have hcostq : cost (price fee) q =
              fee + (e.2 + 1 - e.1) + cost (price fee) r := by
            rw [hq']
            simp [cost, price, span]
          have hcostps : cost (price fee) ((x, e.2) :: r) =
              fee + (e.2 + 1 - x) + cost (price fee) r := by
            simp [cost, price, span]
          rw [hcostq] at hih
          rw [hcostps]
          omega

-- ── a cover attaining the closed form ──

private theorem covers_cons_split {x : Nat} {rest : List Nat} {q : List Extent}
    (hq : q ∈ covers rest) : (x, x) :: q ∈ covers (x :: rest) := by
  rw [covers, List.mem_flatMap]
  exact ⟨q, hq, by cases q <;> simp⟩

private theorem covers_cons_bridge {x : Nat} {rest : List Nat} {q : List Extent}
    (hq : q ∈ covers rest) {b : Nat} {r : List Extent}
    (hqeq : q = (rest.head?.getD 0, b) :: r) : (x, b) :: r ∈ covers (x :: rest) := by
  rw [covers, List.mem_flatMap]
  refine ⟨q, hq, ?_⟩
  cases hrest : rest with
  | nil =>
    rw [hrest] at hq hqeq
    simp [covers] at hq
    rw [hq] at hqeq
    simp at hqeq
  | cons y ys =>
    rw [hqeq]
    simp

private def good (fee : Nat) : List Nat → List Extent
  | [] => []
  | [x] => [(x, x)]
  | x :: y :: ys =>
      if min fee (y - x - 1) = fee then (x, x) :: good fee (y :: ys)
      else
        match good fee (y :: ys) with
        | [] => [(x, x)]
        | (_, b) :: r => (x, b) :: r

private theorem good_spec (fee : Nat) :
    ∀ ks : List Nat, ks.Pairwise (fun a b => a < b) →
      good fee ks ∈ covers ks ∧
      cost (price fee) (good fee ks) = target fee ks ∧
      (ks = [] ∨ ∃ b r, good fee ks = (ks.head?.getD 0, b) :: r) ∧
      (∀ a t, (a, t) ∈ good fee ks → a ≤ t) := by
  intro ks hstr
  induction ks with
  | nil =>
    simp [good, covers, target, cost]
  | cons x rest ih =>
    cases rest with
    | nil =>
      simp [good, covers, target_one, cost, price, span]
    | cons y ys =>
      have hrest : (y :: ys).Pairwise (fun a b => a < b) := by
        rw [List.pairwise_cons] at hstr
        exact hstr.2
      have hxy : x < y := by
        rw [List.pairwise_cons] at hstr
        exact hstr.1 y (by simp)
      obtain ⟨hmem, hcost, hstart, hle⟩ := ih hrest
      obtain ⟨b, r, hqeq⟩ := hstart.resolve_left (by simp)
      by_cases hsplit : min fee (y - x - 1) = fee
      · refine ⟨?_, ?_, ?_, ?_⟩
        · simpa [good, hsplit] using (covers_cons_split hmem)
        · simp [good, hsplit]
          rw [show cost (price fee) ((x, x) :: good fee (y :: ys)) =
              fee + 1 + cost (price fee) (good fee (y :: ys)) by
                simp [cost, price, span]]
          rw [hcost, target_cons_cons]
          omega
        · right
          refine ⟨x, good fee (y :: ys), ?_⟩
          simp [good, hsplit]
        · intro a t hat
          simp [good, hsplit] at hat
          rcases hat with ⟨rfl, rfl⟩ | hat
          · omega
          · exact hle a t hat
      · have hmin : min fee (y - x - 1) = y - x - 1 := by
          by_cases hl : fee ≤ y - x - 1
          · have : min fee (y - x - 1) = fee := Nat.min_eq_left hl
            contradiction
          · exact Nat.min_eq_right (by omega)
        have hyb : y ≤ b := hle y b (by rw [hqeq]; simp)
        have htarget : target fee (y :: ys) =
            fee + (b + 1 - y) + cost (price fee) r := by
          have hc : cost (price fee) (good fee (y :: ys)) =
              fee + (b + 1 - y) + cost (price fee) r := by
            rw [hqeq]
            simp [cost, price, span]
          rw [← hcost, hc]
        refine ⟨?_, ?_, ?_, ?_⟩
        · simpa [good, hsplit, hqeq] using (covers_cons_bridge hmem hqeq)
        · have hcgood : cost (price fee) ((x, b) :: r) =
              fee + (b + 1 - x) + cost (price fee) r := by
            simp [cost, price, span]
          simp [good, hsplit, hqeq]
          rw [hcgood]
          rw [target_cons_cons, hmin, htarget]
          omega
        · right
          refine ⟨b, r, ?_⟩
          simp [good, hsplit, hqeq]
        · intro a t hat
          simp [good, hsplit, hqeq] at hat
          rcases hat with ⟨rfl, rfl⟩ | hat
          · omega
          · exact hle a t (by rw [hqeq]; simp [hat])

-- ── main theorem ──

theorem sound_PositiveGapRule : Contiguous.PositiveGapRule.Goal := by
  intro fee xs
  let ks := points xs
  have hstr : ks.Pairwise (fun a b => a < b) := by
    simpa [ks] using points_strict xs
  obtain ⟨hmem, hcost, _, _⟩ := good_spec fee ks hstr
  have hupper : cost (price fee) (choose (price fee) xs) ≤ target fee ks := by
    rw [← hcost]
    exact choose_le (price fee) xs (good fee ks) (by simpa [ks] using hmem)
  have hmemChoose : choose (price fee) xs ∈ covers ks := by
    simpa [ks] using choose_mem_covers (price fee) xs
  have hlower := lower_bound fee ks hstr (choose (price fee) xs) hmemChoose
  change cost (price fee) (choose (price fee) xs) = target fee ks
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Contiguous_PositiveGapRule
