import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `IntervalDeletes.FooterRuns` — `∀ n rs, let gaps := (pieces n rs).filter (fun r =>
  depth rs r.1 == 0) ((live n rs).length,(live n rs).sum) = ((gaps.map (fun (a,b) =>
  b-a)).sum,(gaps.map (fun (a,b) => choose2 b-choose2 a)).sum)`
- `IntervalDeletes.LiveSelect` — `∀ n rs (k : Nat), seek ((pieces n rs).filter (fun r
  => !blocked rs r.1)) k = (live n rs)[k]?`
- `IntervalDeletes.OverlapDebt` — `∀ n a b, (live n (a++b)).length + ((live n a).countP
  (blocked b)) = (live n a).length`
- `IntervalDeletes.RebaseMask` — `∀ base n rs, ((List.range n).filter (fun i =>
  !blocked rs (base+i))) = live n (rs.map (fun (a,b) => (a-base,b-base)))`
- `IntervalDeletes.SignedEndpoints` — `∀ rs i, blocked rs i = decide (0 < depth rs i)`
-/

namespace TinylambOpt.ResearchProofs.Atlas.SnapshotMaintenance.IntervalDeletes

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.IntervalDeletes
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ### Signed endpoints -/

private theorem span_depth_contrib (a b i : Nat) :
    ((if a < b && a ≤ i then 1 else 0 : Nat) : Int) -
      ((if a < b && b ≤ i then 1 else 0 : Nat) : Int) =
        ((if hit (a, b) i then 1 else 0 : Nat) : Int) := by
  by_cases hab : a < b <;> by_cases hai : a ≤ i <;> by_cases hbi : b ≤ i <;>
    by_cases hib : i < b <;> simp [hit, hab, hai, hbi, hib] <;> omega

private theorem depth_eq_count_hit (rs : List Span) (i : Nat) :
    depth rs i = (rs.countP (fun r => hit r i) : Int) := by
  induction rs with
  | nil => simp [depth]
  | cons r rs ih =>
      rcases r with ⟨a, b⟩
      simp only [depth, List.countP_cons]
      have hcast (p : Bool) (c : Nat) :
          ((c + (if p then 1 else 0) : Nat) : Int) =
            ((if p then 1 else 0 : Nat) : Int) + (c : Int) := by
        cases p <;> omega
      rw [hcast (a < b && a ≤ i) (rs.countP (fun r : Span => r.1 < r.2 && r.1 ≤ i))]
      rw [hcast (a < b && b ≤ i) (rs.countP (fun r : Span => r.1 < r.2 && r.2 ≤ i))]
      rw [hcast (hit (a, b) i) (rs.countP (fun r : Span => hit r i))]
      rw [show ∀ A B C D : Int, (A + B) - (C + D) = (A - C) + (B - D) by
        intro A B C D; omega]
      rw [span_depth_contrib a b i]
      rw [show ((rs.countP (fun r : Span => r.1 < r.2 && r.1 ≤ i) : Int) -
            (rs.countP (fun r : Span => r.1 < r.2 && r.2 ≤ i) : Int)) =
            (rs.countP (fun r : Span => hit r i) : Int) by simpa [depth] using ih]

private theorem decide_pos_int (n : Nat) : decide (0 < (n : Int)) = decide (0 < n) := by
  by_cases h : 0 < n <;> simp [h] <;> omega

theorem sound_SignedEndpoints : SignedEndpoints.Goal := by
  intro rs i
  rw [depth_eq_count_hit]
  change blocked rs i = decide (0 < (rs.countP (fun r => hit r i) : Int))
  rw [decide_pos_int]
  unfold blocked
  induction rs with
  | nil => simp
  | cons r rs ih =>
      cases h : hit r i <;> simp [h, ih]

/-! ### Deletion union -/

private theorem filter_and_not_count (xs : List Nat) (p q : Nat → Bool) :
    (xs.filter (fun x => p x && !q x)).length + (xs.filter p).countP q =
      (xs.filter p).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      cases hp : p x <;> cases hq : q x <;> simp [hp, hq, ih] <;> omega

private theorem blocked_append (a b : List Span) (i : Nat) :
    blocked (a ++ b) i = (blocked a i || blocked b i) := by
  simp [blocked]

theorem sound_OverlapDebt : OverlapDebt.Goal := by
  intro n a b
  unfold live
  have hba : ∀ i, blocked (a ++ b) i = (blocked a i || blocked b i) := blocked_append a b
  simpa [hba] using
    (filter_and_not_count (List.range n) (fun i => !blocked a i) (fun i => blocked b i))

/-! ### Rebasing a mask -/

private theorem bool_eq_of_iff {u v : Bool} (h : (u = true) ↔ (v = true)) : u = v := by
  cases u <;> cases v <;> simp_all

private theorem hit_rebase (base i a b : Nat) :
    hit (a - base, b - base) i = hit (a, b) (base + i) := by
  apply bool_eq_of_iff
  simp [hit]
  constructor <;> intro h <;> rcases h with ⟨h1, h2⟩ <;> constructor
  · by_cases ha : a ≤ base <;> omega
  · by_cases hb : b ≤ base <;> omega
  · by_cases ha : a ≤ base <;> omega
  · by_cases hb : b ≤ base <;> omega

private theorem blocked_rebase (base i : Nat) (rs : List Span) :
    blocked (rs.map (fun p : Span => (p.1 - base, p.2 - base))) i = blocked rs (base + i) := by
  induction rs with
  | nil => simp [blocked]
  | cons r rs ih =>
      rcases r with ⟨a, b⟩
      simp only [blocked, List.map_cons, List.any_cons]
      rw [hit_rebase base i a b]
      have ih' : (List.map (fun p : Span => (p.1 - base, p.2 - base)) rs).any
          (fun x => hit x i) = rs.any (fun x => hit x (base + i)) := by
        simpa [blocked] using ih
      rw [ih']

theorem sound_RebaseMask : RebaseMask.Goal := by
  intro base n rs
  unfold live
  apply List.filter_congr
  intro i hi
  rw [blocked_rebase base i rs]

/-! ### Knot structure -/

private theorem pairwise_eraseDups_sorted : ∀ l : List Nat, l.Pairwise (· ≤ ·) →
    l.eraseDups.Pairwise (· < ·)
  | [], h => by simp
  | a :: as, h => by
      have hrel : ∀ b ∈ as, a ≤ b := fun b hb => List.rel_of_pairwise_cons h hb
      have htail : as.Pairwise (· ≤ ·) := h.of_cons
      rw [List.eraseDups_cons]
      constructor
      · intro b hb
        rw [List.mem_eraseDups] at hb
        rw [List.mem_filter] at hb
        have hab_le := hrel b hb.1
        have hne : a ≠ b := by
          intro heq
          have hbeq : b == a := by simp [heq]
          have hnot : (!b == a) = true := hb.2
          simp [hbeq] at hnot
        omega
      · exact pairwise_eraseDups_sorted (as.filter (fun b => !b == a)) (htail.filter _)
termination_by l => l.length
decreasing_by
  have hle := List.length_filter_le (fun b : Nat => !b == a) as
  have hlen : (a :: as).length = as.length + 1 := by simp
  omega

private theorem knots_pairwise (n rs) : (knots n rs).Pairwise (· < ·) := by
  unfold knots
  let L : List Nat := 0 :: n :: rs.flatMap (fun (a, b) => [min a n, min b n])
  change (L.mergeSort (fun a b => decide (a ≤ b))).eraseDups.Pairwise (· < ·)
  apply pairwise_eraseDups_sorted
  have hsorted : (L.mergeSort (fun a b => decide (a ≤ b))).Pairwise
      (fun a b => (decide (a ≤ b) : Bool) = true) := by
    apply List.pairwise_mergeSort
    · intro a b c hab hbc
      simp at hab hbc ⊢
      omega
    · intro a b
      by_cases h : a ≤ b
      · simp [h]
      · have hba : b ≤ a := by omega
        simp [h, hba]
  simpa using hsorted

private theorem knots_mem_zero (n rs) : 0 ∈ knots n rs := by
  unfold knots
  simp

private theorem knots_mem_n (n rs) : n ∈ knots n rs := by
  unfold knots
  simp

private theorem knots_le_n (n rs) : ∀ x ∈ knots n rs, x ≤ n := by
  intro x hx
  unfold knots at hx
  rw [List.mem_eraseDups, List.mem_mergeSort] at hx
  simp only [List.mem_cons] at hx
  rcases hx with hx0 | hxn | hxflat
  · subst x
    omega
  · subst x
    exact Nat.le_refl n
  · rcases List.mem_flatMap.mp hxflat with ⟨p, hp, hxmem⟩
    rcases p with ⟨a, b⟩
    simp only [List.mem_cons, List.mem_nil_iff, or_false] at hxmem
    rcases hxmem with hxeq | hxeq
    · subst x
      exact Nat.min_le_right a n
    · subst x
      exact Nat.min_le_right b n

private theorem endpoint_in_knots (n rs) (r : Span) (hr : r ∈ rs) :
    min r.1 n ∈ knots n rs ∧ min r.2 n ∈ knots n rs := by
  rcases r with ⟨a, b⟩
  unfold knots
  constructor
  · rw [List.mem_eraseDups, List.mem_mergeSort]
    simp only [List.mem_cons]
    right; right
    exact List.mem_flatMap.mpr ⟨(a, b), hr, by simp⟩
  · rw [List.mem_eraseDups, List.mem_mergeSort]
    simp only [List.mem_cons]
    right; right
    exact List.mem_flatMap.mpr ⟨(a, b), hr, by simp⟩

private theorem piece_knots (n rs a b) (hp : (a, b) ∈ pieces n rs) :
    a ∈ knots n rs ∧ b ∈ knots n rs := by
  unfold pieces at hp
  rcases List.of_mem_zip hp with ⟨ha, hb⟩
  exact ⟨ha, List.mem_of_mem_drop hb⟩

private theorem piece_right_le_n (n rs a b) (hp : (a, b) ∈ pieces n rs) : b ≤ n :=
  knots_le_n n rs b (piece_knots n rs a b hp).2

private theorem head_le_mem {ks : List Nat} (hpair : ks.Pairwise (· < ·)) {x : Nat}
    (hx : x ∈ ks) : ks.head?.getD 0 ≤ x := by
  cases ks with
  | nil => simp at hx
  | cons y ys =>
      simp only [List.head?_cons, Option.getD_some]
      rcases List.mem_cons.mp hx with hxy | hxys
      · subst x
        omega
      · have hrel := List.rel_of_pairwise_cons hpair hxys
        omega

private theorem mem_le_last {ks : List Nat} (hpair : ks.Pairwise (· < ·)) {x : Nat}
    (hx : x ∈ ks) : x ≤ ks.getLast?.getD 0 := by
  cases ks with
  | nil => simp at hx
  | cons y ys =>
      cases ys with
      | nil =>
          simp at hx
          subst x
          simp
      | cons z zs =>
          have hyz : y < z := (List.pairwise_cons.mp hpair).1 z (by simp)
          have htail : (z :: zs).Pairwise (· < ·) := (List.pairwise_cons.mp hpair).2
          rcases List.mem_cons.mp hx with hxy | hxys
          · subst x
            have hzl := mem_le_last htail (by simp : z ∈ z :: zs)
            have hy_le : y ≤ (z :: zs).getLast?.getD 0 := by omega
            simpa [List.getLast?_cons_cons] using hy_le
          · have hxl := mem_le_last htail hxys
            simpa [List.getLast?_cons_cons] using hxl

private theorem knots_head_zero (n rs) : (knots n rs).head?.getD 0 = 0 := by
  let ks := knots n rs
  have hpair : ks.Pairwise (· < ·) := knots_pairwise n rs
  have h0 : 0 ∈ ks := knots_mem_zero n rs
  exact Nat.le_antisymm (head_le_mem hpair h0) (Nat.zero_le _)

private theorem last_eq_of_mem_le (ks : List Nat) (n : Nat)
    (hpair : ks.Pairwise (· < ·)) (hn : n ∈ ks) (hle : ∀ x ∈ ks, x ≤ n) :
    ks.getLast?.getD 0 = n := by
  revert hpair hn hle
  induction ks with
  | nil =>
      intro hpair hn hle
      simp at hn
  | cons y ys ih =>
      intro hpair hn hle
      cases ys with
      | nil =>
          simp at hn
          subst n
          simp
      | cons z zs =>
          have hyz : y < z := (List.pairwise_cons.mp hpair).1 z (by simp)
          have htail : (z :: zs).Pairwise (· < ·) := (List.pairwise_cons.mp hpair).2
          have hn_tail : n ∈ z :: zs := by
            rcases List.mem_cons.mp hn with hyn | hnt
            · subst n
              have hzle := hle z (by simp)
              omega
            · exact hnt
          have hle_tail : ∀ x ∈ z :: zs, x ≤ n := by
            intro x hx
            exact hle x (by simp [hx])
          have ih' : (z :: zs).getLast?.getD 0 = n := ih htail hn_tail hle_tail
          simpa [List.getLast?_cons_cons] using ih'

private theorem knots_last_n (n rs) : (knots n rs).getLast?.getD 0 = n :=
  last_eq_of_mem_le (knots n rs) n (knots_pairwise n rs) (knots_mem_n n rs) (knots_le_n n rs)

private theorem knot_outside (ks : List Nat) (hpair : ks.Pairwise (· < ·)) :
    ∀ {x a b : Nat}, x ∈ ks → (a, b) ∈ ks.zip (ks.drop 1) → x ≤ a ∨ b ≤ x := by
  induction ks with
  | nil =>
      intro x a b hx hp
      simp at hp
  | cons y ys ih =>
      cases ys with
      | nil =>
          intro x a b hx hp
          simp at hp
      | cons z zs =>
          have htail : (z :: zs).Pairwise (· < ·) := (List.pairwise_cons.mp hpair).2
          have hyz : y < z := (List.pairwise_cons.mp hpair).1 z (by simp)
          intro x a b hx hp
          simp only [List.drop_succ_cons, List.drop_zero, List.zip_cons_cons, List.mem_cons] at hp
          rcases hp with hp | hp'
          · cases hp
            rcases List.mem_cons.mp hx with hxy | hxtail
            · subst x
              left
              exact Nat.le_refl y
            · right
              have hzle := head_le_mem htail hxtail
              simpa using hzle
          · rcases List.mem_cons.mp hx with hxy | hxtail
            · subst x
              left
              have haz := head_le_mem htail (List.of_mem_zip hp').1
              have hhead : (z :: zs).head?.getD 0 = z := by simp
              omega
            · exact ih htail hxtail hp'

private def pieceRange (p : Nat × Nat) : List Nat :=
  (List.range (p.2 - p.1)).map (fun x => p.1 + x)

private theorem mem_pieceRange_bounds {a b i : Nat} (hi : i ∈ pieceRange (a, b)) :
    a ≤ i ∧ i < b := by
  unfold pieceRange at hi
  rw [List.mem_map] at hi
  rcases hi with ⟨j, hj, rfl⟩
  rw [List.mem_range] at hj
  constructor <;> omega

private theorem endpoint_cmp_eq (n : Nat) (rs : List Span) (a b i x : Nat)
    (hp : (a, b) ∈ pieces n rs) (hi1 : a ≤ i) (hi2 : i < b)
    (hxknot : min x n ∈ knots n rs) : (x ≤ i) = (x ≤ a) := by
  have hbn : b ≤ n := piece_right_le_n n rs a b hp
  have hout := knot_outside (knots n rs) (knots_pairwise n rs) hxknot hp
  rcases hout with hxa | hbx
  · have ha_lt_n : a < n := by omega
    have hxk_lt : min x n < n := by omega
    have hxlt : x < n := by
      by_cases hlt : x < n
      · exact hlt
      · have hnx : n ≤ x := Nat.le_of_not_gt hlt
        have hxeq : min x n = n := Nat.min_eq_right hnx
        rw [hxeq] at hxk_lt
        omega
    have hxeq : min x n = x := Nat.min_eq_left (Nat.le_of_lt hxlt)
    have hxa' : x ≤ a := by omega
    have hxi' : x ≤ i := by omega
    have hi_eq : (x ≤ i) = true := by simp [hxi']
    have ha_eq : (x ≤ a) = true := by simp [hxa']
    rw [hi_eq, ha_eq]
  · have hxi_false : ¬ x ≤ i := by
      by_cases hxlt : x < n
      · have hxeq : min x n = x := Nat.min_eq_left (Nat.le_of_lt hxlt)
        omega
      · have hnx : n ≤ x := Nat.le_of_not_gt hxlt
        omega
    have hxa_false : ¬ x ≤ a := by
      by_cases hxlt : x < n
      · have hxeq : min x n = x := Nat.min_eq_left (Nat.le_of_lt hxlt)
        omega
      · have hnx : n ≤ x := Nat.le_of_not_gt hxlt
        omega
    have hi_eq : (x ≤ i) = false := by simp [hxi_false]
    have ha_eq : (x ≤ a) = false := by simp [hxa_false]
    rw [hi_eq, ha_eq]

private theorem countP_bool_eq_of_forall {α : Type} (f g : α → Bool) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : l.countP f = l.countP g := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.countP_cons]
      rw [h x (by simp)]
      exact congrArg (fun z => z + (if g x then 1 else 0))
        (ih (by intro y hy; exact h y (by simp [hy])))

private theorem depth_on_piece (n rs) :
    ∀ p ∈ pieces n rs, ∀ i, p.1 ≤ i → i < p.2 → depth rs i = depth rs p.1 := by
  intro p hp i hi1 hi2
  unfold depth
  congr 1
  · exact congrArg (fun z : Nat => (z : Int)) (countP_bool_eq_of_forall
      (fun r : Span => r.1 < r.2 && r.1 ≤ i)
      (fun r : Span => r.1 < r.2 && r.1 ≤ p.1) rs (by
        intro r hr
        have hcmp := endpoint_cmp_eq n rs p.1 p.2 i r.1 hp hi1 hi2
          (endpoint_in_knots n rs r hr).1
        by_cases hrd : r.1 < r.2
        · simp [hrd, hcmp]
        · simp [hrd]))
  · exact congrArg (fun z : Nat => (z : Int)) (countP_bool_eq_of_forall
      (fun r : Span => r.1 < r.2 && r.2 ≤ i)
      (fun r : Span => r.1 < r.2 && r.2 ≤ p.1) rs (by
        intro r hr
        have hcmp := endpoint_cmp_eq n rs p.1 p.2 i r.2 hp hi1 hi2
          (endpoint_in_knots n rs r hr).2
        by_cases hrd : r.1 < r.2
        · simp [hrd, hcmp]
        · simp [hrd]))

/-! ### Range partition by knots -/

private theorem range_split (a b last : Nat) (hab : a ≤ b) (hbl : b ≤ last) :
    pieceRange (a, b) ++ (List.range (last - b)).map (fun x => b + x) =
      (List.range (last - a)).map (fun x => a + x) := by
  unfold pieceRange
  have hdecomp : last - a = (b - a) + (last - b) := by omega
  rw [hdecomp, List.range_add, List.map_append]
  congr 1
  rw [List.map_map]
  apply List.map_congr_left
  intro x hx
  simp only [Function.comp_apply]
  omega

private theorem partition_list (ks : List Nat) (h : ks.Pairwise (· < ·)) :
    (ks.zip (ks.drop 1)).flatMap pieceRange =
      (List.range (ks.getLast?.getD 0 - ks.head?.getD 0)).map (fun x => ks.head?.getD 0 + x) := by
  induction ks with
  | nil => simp
  | cons a ks ih =>
      cases ks with
      | nil => simp
      | cons b bs =>
          have hab : a < b := (List.pairwise_cons.mp h).1 b (by simp)
          have htail : (b :: bs).Pairwise (· < ·) := (List.pairwise_cons.mp h).2
          have ih' := ih htail
          simp only [List.drop_succ_cons, List.drop_zero] at ih'
          simp only [List.drop_succ_cons, List.drop_zero, List.zip_cons_cons, List.flatMap_cons]
          rw [ih']
          have hlast : (a :: b :: bs).getLast?.getD 0 = (b :: bs).getLast?.getD 0 := by simp
          have hlast' : (b :: bs).getLast?.getD 0 = (b :: bs).getLast?.getD 0 := rfl
          rw [hlast]
          have hlead : (a :: b :: bs).head?.getD 0 = a := by simp
          have hlead_tail : (b :: bs).head?.getD 0 = b := by simp
          rw [hlead, hlead_tail]
          exact range_split a b ((b :: bs).getLast?.getD 0) (Nat.le_of_lt hab)
            (mem_le_last htail (by simp : b ∈ b :: bs))

private theorem pieces_flatMap_eq_range (n rs) :
    (pieces n rs).flatMap pieceRange = List.range n := by
  unfold pieces
  let ks := knots n rs
  have hpair : ks.Pairwise (· < ·) := knots_pairwise n rs
  have hhead : ks.head?.getD 0 = 0 := knots_head_zero n rs
  have hlast : ks.getLast?.getD 0 = n := knots_last_n n rs
  change (ks.zip (ks.drop 1)).flatMap pieceRange = List.range n
  rw [partition_list ks hpair, hhead, hlast]
  simp

private theorem not_blocked_eq (rs : List Span) (i : Nat) :
    (!(blocked rs i)) = (depth rs i == 0) := by
  have hdnonneg : 0 ≤ depth rs i := by
    rw [depth_eq_count_hit]
    exact Int.natCast_nonneg _
  rw [sound_SignedEndpoints rs i]
  by_cases h : depth rs i == 0
  · have hd0 : depth rs i = 0 := beq_iff_eq.mp h
    simp [hd0]
  · have hdne : depth rs i ≠ 0 := by intro he; exact h (by simp [he])
    have hdpos : 0 < depth rs i := by omega
    simp [h, hdpos]

private theorem filter_flatMap_pieces (ps : List (Nat × Nat)) (f : Nat → Bool)
    (hconst : ∀ p ∈ ps, ∀ i ∈ pieceRange p, f i = f p.1) :
    (ps.flatMap pieceRange).filter f = (ps.filter (fun p => f p.1)).flatMap pieceRange := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      have hp := hconst p (by simp)
      have htail : ∀ q ∈ ps, ∀ i ∈ pieceRange q, f i = f q.1 := by
        intro q hq i hi
        exact hconst q (by simp [hq]) i hi
      simp only [List.flatMap_cons]
      rw [List.filter_append]
      by_cases hpv : f p.1
      · simp [hpv]
        have hf_all : (pieceRange p).filter f = pieceRange p := by
          apply List.filter_eq_self.mpr
          intro i hi
          rw [hp i hi]
          exact hpv
        rw [hf_all, ih htail]
      · simp [hpv]
        have hf_nil : (pieceRange p).filter f = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro i hi
          rw [hp i hi]
          exact hpv
        rw [hf_nil, ih htail]
        simp

private theorem live_decomp (n rs) :
    live n rs = ((pieces n rs).filter (fun p => !blocked rs p.1)).flatMap pieceRange := by
  unfold live
  rw [← pieces_flatMap_eq_range n rs]
  apply filter_flatMap_pieces
  intro p hp i hi
  have hib := mem_pieceRange_bounds hi
  have hdep := depth_on_piece n rs p hp i hib.1 hib.2
  rw [sound_SignedEndpoints rs i, sound_SignedEndpoints rs p.1, hdep]

/-! ### choose2 arithmetic -/

private theorem two_dvd_mul_consec (n : Nat) : 2 ∣ n * (n + 1) := by
  by_cases h : n % 2 = 0
  · apply Nat.dvd_of_mod_eq_zero
    rw [Nat.mul_mod, h]
    simp
  · have h1 : n % 2 = 1 := by omega
    have hdiv : 2 ∣ n + 1 := by
      apply Nat.dvd_of_mod_eq_zero
      rw [Nat.add_mod, h1]
    rcases hdiv with ⟨k, hk⟩
    refine ⟨n * k, ?_⟩
    rw [hk]
    simp [Nat.mul_assoc, Nat.mul_comm]

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  cases n with
  | zero => simp [choose2]
  | succ m =>
      unfold choose2
      simp only [Nat.add_sub_cancel_right]
      have hdA : 2 ∣ (m + 2) * (m + 1) := by
        simpa [Nat.mul_comm, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using
          (two_dvd_mul_consec (m + 1))
      have hdB : 2 ∣ (m + 1) * m := by simpa [Nat.mul_comm] using two_dvd_mul_consec m
      have hA : 2 * (((m + 2) * (m + 1)) / 2) = (m + 2) * (m + 1) :=
        Nat.mul_div_cancel_left' hdA
      have hB : 2 * (((m + 1) * m) / 2) = (m + 1) * m :=
        Nat.mul_div_cancel_left' hdB
      have h2 : 2 * (((m + 2) * (m + 1)) / 2) =
          2 * ((((m + 1) * m) / 2) + (m + 1)) := by
        rw [hA]
        calc
          (m + 2) * (m + 1) = (m + 1) * m + 2 * (m + 1) := by
            rw [Nat.add_mul, Nat.mul_comm m (m + 1)]
          _ = 2 * (((m + 1) * m) / 2) + 2 * (m + 1) := by omega
          _ = 2 * ((((m + 1) * m) / 2) + (m + 1)) := by omega
      exact Nat.mul_right_cancel (by decide : 0 < 2) (by simpa [Nat.mul_comm] using h2)

private theorem choose2_mono {a b : Nat} (hab : a ≤ b) : choose2 a ≤ choose2 b := by
  induction b with
  | zero =>
      have ha0 : a = 0 := by omega
      subst a
      exact Nat.le_refl _
  | succ b ih =>
      by_cases h : a ≤ b
      · have ih' := ih h
        rw [choose2_succ]
        omega
      · have haeq : a = b + 1 := by omega
        subst a
        exact Nat.le_refl _

private theorem sum_pieceRange_len (a d : Nat) :
    (pieceRange (a, a + d)).sum = choose2 (a + d) - choose2 a := by
  induction d with
  | zero => simp [pieceRange, choose2]
  | succ d ih =>
      rw [show a + (d + 1) = (a + d) + 1 by omega]
      have hstep : pieceRange (a, (a + d) + 1) = pieceRange (a, a + d) ++ [a + d] := by
        unfold pieceRange
        rw [show (a + d + 1) - a = d + 1 by omega, List.range_succ, List.map_append]
        simp
      rw [hstep, List.sum_append, ih, choose2_succ]
      simp only [List.sum_singleton]
      have hmono : choose2 a ≤ choose2 (a + d) := choose2_mono (by omega)
      rw [Nat.sub_add_comm hmono]

private theorem sum_pieceRange (a b : Nat) :
    (pieceRange (a, b)).sum = choose2 b - choose2 a := by
  by_cases hab : a ≤ b
  · have h := sum_pieceRange_len a (b - a)
    have hb : a + (b - a) = b := Nat.add_sub_of_le hab
    simpa [hb] using h
  · have hba : b ≤ a := Nat.le_of_not_ge hab
    have hz : b - a = 0 := Nat.sub_eq_zero_of_le hba
    have hc : choose2 b ≤ choose2 a := choose2_mono hba
    simp [pieceRange, hz, Nat.sub_eq_zero_of_le hc]

private theorem length_flatMap_pieceRange (ps : List (Nat × Nat)) :
    (ps.flatMap pieceRange).length = (ps.map (fun p => p.2 - p.1)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp [pieceRange, ih]

private theorem sum_flatMap_pieceRange (ps : List (Nat × Nat)) :
    (ps.flatMap pieceRange).sum = (ps.map (fun p => choose2 p.2 - choose2 p.1)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      rcases p with ⟨a, b⟩
      simp [ih, sum_pieceRange]

/-! ### Footer runs -/

theorem sound_FooterRuns : FooterRuns.Goal := by
  intro n rs
  dsimp only
  let gaps := (pieces n rs).filter (fun p => depth rs p.1 == 0)
  change ((live n rs).length, (live n rs).sum) =
    ((gaps.map (fun p => p.2 - p.1)).sum,
     (gaps.map (fun p => choose2 p.2 - choose2 p.1)).sum)
  have hg : gaps = (pieces n rs).filter (fun p => !blocked rs p.1) := by
    dsimp [gaps]
    apply List.filter_congr
    intro p hp
    exact (not_blocked_eq rs p.1).symm
  rw [live_decomp n rs]
  rw [Prod.mk.injEq]
  constructor
  · simpa [hg] using length_flatMap_pieceRange gaps
  · simpa [hg] using sum_flatMap_pieceRange gaps

/-! ### Selecting a live row -/

private theorem pieceRange_get (a b k : Nat) :
    (pieceRange (a, b))[k]? = if k < b - a then some (a + k) else none := by
  by_cases h : k < b - a <;> simp [pieceRange, h]

private theorem seek_pieces (ps : List (Nat × Nat)) (k : Nat) :
    seek ps k = (ps.flatMap pieceRange)[k]? := by
  induction ps generalizing k with
  | nil => simp [seek]
  | cons p ps ih =>
      rcases p with ⟨a, b⟩
      change (if k < b - a then some (a + k) else seek ps (k - (b - a))) =
        (((a, b) :: ps).flatMap pieceRange)[k]?
      simp only [List.flatMap_cons]
      rw [List.getElem?_append]
      have hlen : (pieceRange (a, b)).length = b - a := by simp [pieceRange]
      rw [hlen, pieceRange_get]
      by_cases hk : k < b - a
      · simp [hk]
      · simpa [hk] using ih (k - (b - a))

theorem sound_LiveSelect : LiveSelect.Goal := by
  intro n rs k
  rw [live_decomp n rs]
  exact seek_pieces ((pieces n rs).filter (fun p => !blocked rs p.1)) k

end TinylambOpt.ResearchProofs.Atlas.SnapshotMaintenance.IntervalDeletes
