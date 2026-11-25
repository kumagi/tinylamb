import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.Atlas_ExecutorResources_B

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction

/-!
Batch B: the five `Gather` goals and five `LaneCompaction` goals of
`TinylambOpt.ResearchGoals.Atlas.ExecutorResources`.

All ten goals are already covered by verified proofs in
`TinylambOpt/ResearchProofs/Atlas_ExecutorResources_Gather.lean` and
`TinylambOpt/ResearchProofs/Atlas_ExecutorResources_LaneCompaction.lean`
(`lake env lean` exit 0 on both). Because those proof modules are not built
into `.lake` (and `lake build` is disallowed for agents), the verified proof
terms — private helpers included — are reproduced verbatim below with only the
ten public theorem names changed to the required `<Leaf>_sound` form and the
goal statements written out fully.

Per-goal status: ALREADY_PROVED (all ten).
-/

/-! ===== Gather ===== -/

private theorem nodup_eraseDups [BEq α] [LawfulBEq α] : ∀ xs : List α, xs.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro h
        have hf : a ∈ List.filter (fun b => ! b == a) as := (List.mem_eraseDups.mp h)
        have hp := (List.mem_filter.mp hf).2
        simp at hp
      · exact nodup_eraseDups (List.filter (fun b => ! b == a) as)
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem eraseDups_len_le [BEq α] : ∀ xs : List α, xs.eraseDups.length ≤ xs.length
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      simp only [List.length_cons]
      exact Nat.succ_le_succ (Nat.le_trans (eraseDups_len_le (List.filter (fun b => ! b == a) as))
        (List.length_filter_le _ _))
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem eraseDups_comp_map_le [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    [BEq γ] [LawfulBEq γ] (f : α → β) (g : β → γ) (xs : List α) :
    (xs.map (fun x => g (f x))).eraseDups.length ≤ (xs.map f).eraseDups.length := by
  have hperm : (xs.map (fun x => g (f x))).eraseDups.Perm
      (((xs.map f).eraseDups.map g).eraseDups) := by
    apply (List.perm_ext_iff_of_nodup (nodup_eraseDups _) (nodup_eraseDups _)).mpr
    intro z
    simp only [List.mem_eraseDups, List.mem_map]
    constructor
    · rintro ⟨a, ha, rfl⟩
      exact ⟨f a, ⟨a, ha, rfl⟩, rfl⟩
    · rintro ⟨_, ⟨a, ha, rfl⟩, rfl⟩
      exact ⟨a, ha, rfl⟩
  have hlen := hperm.length_eq
  rw [hlen]
  simpa using (eraseDups_len_le ((xs.map f).eraseDups.map g))

private theorem sum_map_le_length_mul {α : Type} (l : List α) (f : α → Nat) (B : Nat)
    (h : ∀ x ∈ l, f x ≤ B) : (l.map f).sum ≤ l.length * B := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have ha := h a (by simp)
      have ht : ∀ x ∈ as, f x ≤ B := by intro x hx; exact h x (by simp [hx])
      have hi := ih ht
      rw [Nat.succ_mul]
      omega

private theorem runCost_le_length_mul (xs : List Nat) (s p : Nat) :
    runCost xs s p ≤ xs.length * (s + p) := by
  cases xs with
  | nil => simp [runCost]
  | cons x rest =>
      simp only [runCost]
      have hsum := sum_map_le_length_mul ((x::rest).zip rest)
          (fun q : Nat × Nat => p + min s (p*(q.2-q.1-1))) (s+p)
          (by intro q hq; have hm : min s (p*(q.2-q.1-1)) ≤ s := Nat.min_le_left _ _; omega)
      have hlen : ((x::rest).zip rest).length = rest.length := by simp [List.length_zip]
      rw [hlen] at hsum
      simp only [List.length_cons]
      rw [Nat.succ_mul]
      omega

private theorem getLastD_cons_cons (x y : Nat) (ys : List Nat) :
    (x::y::ys).getLast?.getD 0 = (y::ys).getLast?.getD 0 := by
  cases ys with
  | nil => simp
  | cons z zs => simp [List.getLast?_cons]

private theorem mem_le_getLastD_of_pairwise (l : List Nat) (hs : l.Pairwise (fun a b => a ≤ b)) :
    ∀ a ∈ l, a ≤ l.getLast?.getD 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      cases xs with
      | nil => simp at ha ⊢; omega
      | cons y ys =>
          rw [getLastD_cons_cons]
          have hpw_tail : (y::ys).Pairwise (fun a b => a ≤ b) := (List.pairwise_cons.mp hs).2
          cases ha with
          | head =>
              have hxy : x ≤ y := (List.pairwise_cons.mp hs).1 y (by simp)
              have hyL := ih hpw_tail y (by simp)
              omega
          | tail _ htail =>
              exact ih hpw_tail a htail

private theorem runCost_cons_cons (x y : Nat) (ys : List Nat) (s p : Nat) :
    runCost (x::y::ys) s p = (p + min s (p*(y-x-1))) + runCost (y::ys) s p := by
  simp [runCost, List.zip_cons_cons]
  omega

private theorem runCost_le_span (xs : List Nat) (s p : Nat) :
    xs.Pairwise (fun a b => a ≤ b) → xs.Nodup →
    runCost xs s p ≤ s + p*(xs.getLast?.getD 0 + 1 - xs.head?.getD 0) := by
  induction xs with
  | nil => intro hs hn; simp [runCost]
  | cons x xs ih =>
      intro hs hn
      cases xs with
      | nil => simp [runCost]
      | cons y ys =>
          have hpw := (List.pairwise_cons.mp hs).2
          have hxy : x ≤ y := (List.pairwise_cons.mp hs).1 y (by simp)
          have hn_tail : (y::ys).Nodup := (List.nodup_cons.mp hn).2
          have hxnot : x ∉ y::ys := (List.nodup_cons.mp hn).1
          have hxy_lt : x < y := by
            have : x ≠ y := by intro h; subst y; exact hxnot (by simp)
            omega
          have ih' := ih hpw hn_tail
          rw [runCost_cons_cons]
          have hterm : p + min s (p*(y-x-1)) ≤ p*(y-x) := by
            have hmin : min s (p*(y-x-1)) ≤ p*(y-x-1) := Nat.min_le_right _ _
            have hmul : p * (y-x) = p*(y-x-1) + p := by
              calc
                p * (y-x) = p * ((y-x-1)+1) := by congr 1 <;> omega
                _ = p*(y-x-1) + p*1 := by rw [Nat.mul_add]
                _ = p*(y-x-1) + p := by rw [Nat.mul_one]
            omega
          let L := (y::ys).getLast?.getD 0
          have ih'' : runCost (y::ys) s p ≤ s + p*(L+1-y) := by simpa [L] using ih'
          have hyL : y ≤ L := mem_le_getLastD_of_pairwise (y::ys) hpw y (by simp)
          rw [getLastD_cons_cons]
          simp only [List.head?_cons, Option.getD_some]
          change p + min s (p * (y - x - 1)) + runCost (y :: ys) s p ≤ s + p * (L + 1 - x)
          have hsum : p*(y-x) + p*(L+1-y) = p*((y-x)+(L+1-y)) := by rw [Nat.mul_add]
          have hdiff : (y-x)+(L+1-y) = L+1-x := by omega
          calc
            p + min s (p * (y - x - 1)) + runCost (y :: ys) s p
                ≤ p*(y-x) + runCost (y::ys) s p := Nat.add_le_add_right hterm _
            _ ≤ p*(y-x) + (s + p*(L+1-y)) := Nat.add_le_add_left ih'' _
            _ = s + (p*(y-x) + p*(L+1-y)) := by omega
            _ = s + p*((y-x)+(L+1-y)) := by rw [hsum]
            _ = s + p*(L+1-x) := by rw [hdiff]

theorem CostEnvelope_sound : TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather.CostEnvelope.Goal := by
  intro ids cap setup price hcap
  let ps := (pages cap ids).mergeSort
  dsimp only
  apply Nat.le_min.mpr
  constructor
  · exact runCost_le_length_mul ps setup price
  · have hperm : ps.Perm (pages cap ids) := by
      dsimp [ps]
      exact List.mergeSort_perm _ _
    have hnodup : ps.Nodup := hperm.symm.nodup (nodup_eraseDups _)
    have hsorted : ps.Pairwise (fun a b => a ≤ b) := by
      have htotal : ∀ a b : Nat, (decide (a ≤ b) || decide (b ≤ a)) = true := by
        intro a b
        by_cases h : a ≤ b
        · simp [h]
        · have hba : b ≤ a := by omega
          simp [h, hba]
      have hpw := List.pairwise_mergeSort
        (le := fun a b : Nat => decide (a ≤ b))
        (by intro a b c hab hbc; simp at hab hbc ⊢; omega) htotal (pages cap ids)
      simpa [ps] using hpw
    change runCost ps setup price ≤ setup + price*(ps.getLast?.getD 0 + 1 - ps.head?.getD 0)
    exact runCost_le_span ps setup price hsorted hnodup

private def startsAux (prev : Option Nat) (l : List Nat) : Nat :=
  match l with
  | [] => 0
  | x::xs => (if (prev.map (fun p => p+1)) == some x then 0 else 1) + startsAux (some x) xs

private def bridge (prev : Option Nat) (l : List Nat) : Nat :=
  match prev with
  | none => 0
  | some p => if l.contains (p+1) then 1 else 0

private def cntFrom (prev : Option Nat) (start : Nat) (l : List Nat) : Nat :=
  (l.zipIdx start).countP (fun (v,i) =>
    !(((if i == start then prev else some (l[i - start - 1]?.getD v)).map (fun p => p+1)) == some v))

private theorem cntFrom_eq_startsAux (prev : Option Nat) (start : Nat) (l : List Nat) :
    cntFrom prev start l = startsAux prev l := by
  induction l generalizing start prev with
  | nil => simp [cntFrom, startsAux]
  | cons z zs ih =>
      unfold cntFrom
      rw [List.zipIdx_cons, List.countP_cons]
      have hfirst :
          (!(((if start == start then prev else some ((z::zs)[start - start - 1]?.getD z)).map (fun p => p+1)) == some z)) =
          (!(((prev.map (fun p => p+1)) == some z))) := by simp
      simp only [hfirst]
      have htail :
          (List.filter (fun x : Nat × Nat => !(((if x.snd == start then prev else some ((z::zs)[x.snd - start - 1]?.getD x.fst)).map (fun p => p+1)) == some x.fst)) (zs.zipIdx (start+1))) =
          (List.filter (fun x : Nat × Nat => !(((if x.snd == start+1 then some z else some (zs[x.snd - (start+1) - 1]?.getD x.fst)).map (fun p => p+1)) == some x.fst)) (zs.zipIdx (start+1))) := by
        apply List.filter_congr
        intro x hx
        have hle : start+1 ≤ x.snd := (List.mk_mem_zipIdx_iff_le_and_getElem?_sub.mp hx).1
        have hstart_false : (x.snd == start) = false := by
          by_cases h : x.snd == start
          · have heq : x.snd = start := by simpa using h
            omega
          · exact Bool.eq_false_of_ne_true h
        by_cases hfirst2 : x.snd == start+1
        · have hs : x.snd = start + 1 := by simpa using hfirst2
          have hidx0 : x.snd - start - 1 = 0 := by omega
          simp [hs]
        · have hne : x.snd ≠ start+1 := by intro hh; exact hfirst2 (by simp [hh])
          have hgt : start+1 < x.snd := by omega
          have hget : (z::zs)[x.snd - start - 1]? = zs[x.snd - (start+1) - 1]? := by
            have hj : x.snd - start - 1 = (x.snd - (start+1) - 1) + 1 := by omega
            rw [hj, List.getElem?_cons_succ]
          rw [hstart_false]
          simp [hfirst2]
          rw [hget]
      have htailc :
          (zs.zipIdx (start+1)).countP (fun x : Nat × Nat => !(((if x.snd == start then prev else some ((z::zs)[x.snd - start - 1]?.getD x.fst)).map (fun p => p+1)) == some x.fst)) =
          (zs.zipIdx (start+1)).countP (fun x : Nat × Nat => !(((if x.snd == start+1 then some z else some (zs[x.snd - (start+1) - 1]?.getD x.fst)).map (fun p => p+1)) == some x.fst)) := by
        rw [List.countP_eq_length_filter, List.countP_eq_length_filter, htail]
      rw [htailc]
      change cntFrom (some z) (start+1) zs +
          (if (!((prev.map (fun p => p+1)) == some z)) then 1 else 0) =
        (if ((prev.map (fun p => p+1)) == some z) then 0 else 1) + startsAux (some z) zs
      rw [ih (some z) (start+1)]
      cases hc : ((prev.map (fun p => p+1)) == some z) <;> simp <;> omega

private theorem extentsFilter_eq_cntFrom (l : List Nat) :
    (l.zipIdx.filter (fun (v,i) => i == 0 || (l[i-1]?.getD v)+1 != v)).length = cntFrom none 0 l := by
  unfold cntFrom
  rw [List.countP_eq_length_filter]
  apply congrArg List.length
  apply List.filter_congr
  rintro ⟨v,i⟩ hi
  by_cases h : i == 0
  · have hz : i = 0 := by simpa using h
    simp [hz]
  · have hne : i ≠ 0 := by intro hh; exact h (by simp [hh])
    have hpos : 0 < i := by omega
    simp [h]
    rfl

private theorem startsAux_aux (prev : Option Nat) (l : List Nat)
    (hprev : ∀ x ∈ l, ∀ p, prev = some p → p < x)
    (hs : l.Pairwise (fun a b => a ≤ b)) (hn : l.Nodup) :
    startsAux prev l + l.countP (fun p => l.contains (p+1)) + bridge prev l = l.length := by
  induction l generalizing prev with
  | nil => cases prev <;> simp [startsAux, bridge]
  | cons x xs ih =>
      have hpx_all : ∀ p, prev = some p → p < x := hprev x (by simp)
      cases xs with
      | nil =>
          cases hp : prev with
          | none => simp [startsAux, bridge]
          | some p =>
              have hpx : p < x := hpx_all p hp
              by_cases hEq : p+1 = x
              · subst x
                simp [startsAux, bridge]
              · have hbool : (p+1 == x) = false := by simp [hEq]
                simp [startsAux, bridge, hbool]
                exact hEq
      | cons y ys =>
          have hpw_tail : (y::ys).Pairwise (fun a b => a ≤ b) := (List.pairwise_cons.mp hs).2
          have hn_tail : (y::ys).Nodup := (List.nodup_cons.mp hn).2
          have hxnot : x ∉ y::ys := (List.nodup_cons.mp hn).1
          have hprev_tail : ∀ z ∈ y::ys, ∀ p, (some x) = some p → p < z := by
            intro z hz p hp
            have hxp : x = p := by simpa using hp
            subst p
            have hxz : x ≤ z := by
              rcases List.mem_cons.mp hz with hzy | hzys
              · subst z
                exact (List.pairwise_cons.mp hs).1 y (by simp)
              · exact (List.pairwise_cons.mp hs).1 z (by simp [hzys])
            have hne : z ≠ x := by intro hh; subst z; exact hxnot hz
            omega
          have ih' := ih (some x) hprev_tail hpw_tail hn_tail
          have hheadbridge : bridge prev (x::y::ys) = (if (prev.map (fun p => p+1)) == some x then 1 else 0) := by
            cases hp : prev with
            | none => simp [bridge]
            | some p =>
                have hpx : p < x := hpx_all p hp
                have hmin : ∀ z ∈ y::ys, y ≤ z := by
                  intro z hz
                  rcases List.mem_cons.mp hz with hzy | hzys
                  · subst z
                    exact Nat.le_refl y
                  · exact (List.pairwise_cons.mp hpw_tail).1 z hzys
                have hxylt : x < y := by
                  have hxy : x ≤ y := (List.pairwise_cons.mp hs).1 y (by simp)
                  have hne : x ≠ y := by intro hh; subst y; exact hxnot (by simp)
                  omega
                have hnot : p+1 ∉ y::ys := by
                  intro hm
                  have hy_le : y ≤ p+1 := hmin (p+1) hm
                  omega
                have htailnot : (y::ys).contains (p+1) = false := by simpa using hnot
                have hb : bridge (some p) (x::y::ys) = (if p+1 == x then 1 else 0) := by
                  simp [bridge]
                  have hney : p+1 ≠ y := by intro hh; apply hnot; simp [hh]
                  have hne_ys : p+1 ∉ ys := by intro hh; apply hnot; simp [hh]
                  simp [hney, hne_ys]
                simpa [hp] using hb
          have hpred : ∀ q ∈ y::ys, (x::y::ys).contains (q+1) = (y::ys).contains (q+1) := by
            intro q hq
            have hxq : x < q := by
              have hxleq : x ≤ q := (List.pairwise_cons.mp hs).1 q hq
              have hne : q ≠ x := by intro hh; subst q; exact hxnot hq
              omega
            by_cases hEq : (q+1 == x) = true
            · have heq : q+1 = x := by simpa using hEq
              omega
            · have hbool : (q+1 == x) = false := Bool.eq_false_of_ne_true hEq
              simp
              intro hh
              omega
          have hcp : (y::ys).countP (fun p => (x::y::ys).contains (p+1)) =
              (y::ys).countP (fun p => (y::ys).contains (p+1)) := by
            rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
            apply congrArg List.length
            apply List.filter_congr
            intro q hq
            exact hpred q hq
          have hhead : (x::y::ys).contains (x+1) = (y::ys).contains (x+1) := by
            have hne : x+1 ≠ x := by omega
            simp
          have hcnthead : (x::y::ys).countP (fun p => (x::y::ys).contains (p+1)) =
              (y::ys).countP (fun p => (y::ys).contains (p+1)) +
                (if (y::ys).contains (x+1) then 1 else 0) := by
            rw [List.countP_cons, hcp, hhead]
          simp [bridge] at ih'
          rw [startsAux]
          simp only [List.length_cons]
          rw [hheadbridge, hcnthead]
          rw [← ih']
          unfold startsAux
          split <;> simp <;> omega

theorem ExtentCount_sound : TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather.ExtentCount.Goal := by
  intro cap ids hcap
  let ps := pages cap ids
  let sorted := ps.mergeSort
  have hperm : sorted.Perm ps := by
    dsimp [sorted]
    exact List.mergeSort_perm _ _
  have hnodup : sorted.Nodup := hperm.symm.nodup (nodup_eraseDups _)
  have hsorted : sorted.Pairwise (fun a b => a ≤ b) := by
    have htotal : ∀ a b : Nat, (decide (a ≤ b) || decide (b ≤ a)) = true := by
      intro a b
      by_cases h : a ≤ b
      · simp [h]
      · have hba : b ≤ a := by omega
        simp [h, hba]
    have hpw := List.pairwise_mergeSort
      (le := fun a b : Nat => decide (a ≤ b))
      (by intro a b c hab hbc; simp at hab hbc ⊢; omega) htotal ps
    simpa [sorted] using hpw
  have hstart : startsAux none sorted + sorted.countP (fun p => sorted.contains (p+1)) = sorted.length := by
    have h := startsAux_aux none sorted (by intro x hx p hp; cases hp) hsorted hnodup
    simpa [bridge] using h
  have hpred_eq : (fun p : Nat => sorted.contains (p+1)) = (fun p : Nat => ps.contains (p+1)) := by
    funext p
    exact hperm.contains_eq
  rw [hpred_eq] at hstart
  have hcnt : sorted.countP (fun p => ps.contains (p+1)) = ps.countP (fun p => ps.contains (p+1)) := by
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
    exact (hperm.filter (fun p => ps.contains (p+1))).length_eq
  dsimp [extents]
  change (sorted.zipIdx.filter (fun (v,i) => i == 0 || (sorted[i-1]?.getD v)+1 != v)).length +
      ps.countP (fun p => ps.contains (p+1)) = ps.length
  rw [extentsFilter_eq_cntFrom sorted, cntFrom_eq_startsAux none 0 sorted]
  rw [← hcnt]
  rw [hstart, hperm.length_eq]

theorem CoarserPages_sound : TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather.CoarserPages.Goal := by
  intro ids cap n hcap hn
  dsimp [pages]
  simpa [Nat.div_div_eq_div_mul] using
    (eraseDups_comp_map_le (fun id : Nat => id / cap) (fun p : Nat => p / n) ids)

theorem ScatterBack_sound : TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather.ScatterBack.Goal := by
  intro payload ids
  dsimp
  apply List.map_congr_left
  intro i hi
  have hiaddr : i ∈ ids.mergeSort.eraseDups := by
    rw [List.mem_eraseDups]
    simpa using hi
  have hidxlt : ids.mergeSort.eraseDups.idxOf i < ids.mergeSort.eraseDups.length :=
    List.idxOf_lt_length_of_mem hiaddr
  have hget : ids.mergeSort.eraseDups[ids.mergeSort.eraseDups.idxOf i] = i :=
    List.getElem_idxOf hidxlt
  have hcache : (ids.mergeSort.eraseDups.map (fun i => payload[i]?))[ids.mergeSort.eraseDups.idxOf i]? = some (payload[i]?) := by
    simp [List.getElem?_eq_getElem hidxlt, hget]
  rw [hcache]
  cases payload[i]? <;> simp


private def midP (s t : Nat) (q : Nat) : Bool := decide (s ≤ q ∧ q < t)
private def outP (s t : Nat) (q : Nat) : Bool := !(midP s t q)


private theorem term_mono (a b c : Nat) (hbc : b ≤ c) (s p : Nat) :
    p + min s (p*(b-a-1)) ≤ p + min s (p*(c-a-1)) := by
  have hsub : b - a - 1 ≤ c - a - 1 := by omega
  have hmul : p*(b-a-1) ≤ p*(c-a-1) := Nat.mul_le_mul_left p hsub
  have hmin : min s (p*(b-a-1)) ≤ min s (p*(c-a-1)) := by
    by_cases hle : s ≤ p*(b-a-1)
    · have hsle : s ≤ p*(c-a-1) := Nat.le_trans hle hmul
      rw [Nat.min_eq_left hle, Nat.min_eq_left hsle]
      exact Nat.le_refl s
    · by_cases hle2 : s ≤ p*(c-a-1)
      · rw [Nat.min_eq_left hle2]
        omega
      · rw [Nat.min_eq_right (by omega : p*(c-a-1) ≤ s)]
        rw [Nat.min_eq_right (by omega : p*(b-a-1) ≤ s)]
        exact hmul
  exact Nat.add_le_add_left hmin p

private theorem term_le_setup_page (a b s p : Nat) :
    p + min s (p*(b-a-1)) ≤ s + p := by omega

private theorem term_le_gap (a b s p : Nat) (hab : a < b) :
    p + min s (p*(b-a-1)) ≤ p*(b-a) := by
  have hmin : min s (p*(b-a-1)) ≤ p*(b-a-1) := Nat.min_le_right _ _
  have hmul : p*(b-a) = p*(b-a-1)+p := by
    calc p*(b-a) = p*((b-a-1)+1) := by congr 1 <;> omega
         _ = p*(b-a-1)+p*1 := by rw [Nat.mul_add]
         _ = p*(b-a-1)+p := by rw [Nat.mul_one]
  omega

private theorem page_le_span (p s t : Nat) (h : s < t) : p ≤ p*(t-s) := by
  have h1 : 1 ≤ t-s := by omega
  simpa using Nat.mul_le_mul_left p h1

private theorem runCost_mid_bound (l : List Nat) (s0 p s t : Nat)
    (hord : l.Pairwise (fun a b => a ≤ b)) (hnd : l.Nodup) :
    runCost l s0 p ≤ runCost (l.filter (outP s t)) s0 p +
      (let m := l.filter (midP s t)
       if m.isEmpty then 0 else s0 + p*(t - m.head?.getD 0)) := by
  induction l with
  | nil => simp [runCost]
  | cons x xs ih =>
      have hxs_ord : xs.Pairwise (fun a b => a ≤ b) := (List.pairwise_cons.mp hord).2
      have hxs_nd : xs.Nodup := (List.nodup_cons.mp hnd).2
      cases xs with
      | nil =>
          by_cases hx : s ≤ x ∧ x < t
          · simp [runCost, midP, outP, hx]
            have h1 : 1 ≤ t - x := by omega
            simpa using Nat.mul_le_mul_left p h1
          · simp [runCost, midP, outP, hx]
      | cons y ys =>
          have ih' := ih hxs_ord hxs_nd
          by_cases hx : s ≤ x ∧ x < t
          · by_cases hy : s ≤ y ∧ y < t
            · simp [midP, outP, hx, hy] at ih' ⊢
              have hxy : x < y := by
                have hxy_le : x ≤ y := (List.pairwise_cons.mp hord).1 y (by simp)
                have hne : x ≠ y := by intro he; subst y; exact (List.nodup_cons.mp hnd).1 (by simp)
                omega
              rw [runCost_cons_cons]
              have hterm := term_le_gap x y s0 p hxy
              have hdiff : (y-x)+(t-y) = t-x := by omega
              have hsum : p*(y-x) + p*(t-y) = p*(t-x) := by
                rw [← Nat.mul_add, hdiff]
              calc
                p + min s0 (p*(y-x-1)) + runCost (y::ys) s0 p
                    ≤ (p + min s0 (p*(y-x-1))) + (runCost (List.filter (outP s t) ys) s0 p + (s0 + p*(t-y))) :=
                      Nat.add_le_add_left ih' _
                _ = runCost (List.filter (outP s t) ys) s0 p + s0 +
                      ((p + min s0 (p*(y-x-1))) + p*(t-y)) := by omega
                _ ≤ runCost (List.filter (outP s t) ys) s0 p + (s0 + p*(t-x)) := by
                      have htail : (p + min s0 (p*(y-x-1))) + p*(t-y) ≤ p*(t-x) := by
                        calc
                          (p + min s0 (p*(y-x-1))) + p*(t-y) ≤ p*(y-x) + p*(t-y) :=
                            Nat.add_le_add_right hterm _
                          _ = p*(t-x) := hsum
                      have hh := Nat.add_le_add_left htail (runCost (List.filter (outP s t) ys) s0 p + s0)
                      simpa [Nat.add_assoc] using hh
            · -- x mid, y out
              have hxy : x < y := by
                have hxy_le : x ≤ y := (List.pairwise_cons.mp hord).1 y (by simp)
                have hne : x ≠ y := by intro he; subst y; exact (List.nodup_cons.mp hnd).1 (by simp)
                omega
              have hyge : t ≤ y := by
                by_cases hylt : y < t
                · exact False.elim (hy ⟨by omega, hylt⟩)
                · omega
              have htail_nomid : List.filter (midP s t) (y::ys) = [] := by
                apply List.filter_eq_nil_iff.mpr
                intro q hq hqm
                have hyq : y ≤ q := by
                  cases hq with
                  | head => exact Nat.le_refl y
                  | tail _ hqys => exact (List.pairwise_cons.mp hxs_ord).1 q hqys
                have hn : ¬ (s ≤ q ∧ q < t) := by intro h; omega
                have hp' : s ≤ q ∧ q < t := by simpa [midP] using hqm
                exact hn hp'
              have htail_out : List.filter (outP s t) (y::ys) = y::ys := by
                apply List.filter_eq_self.mpr
                intro q hq
                have hyq : y ≤ q := by
                  cases hq with
                  | head => exact Nat.le_refl y
                  | tail _ hqys => exact (List.pairwise_cons.mp hxs_ord).1 q hqys
                have hn : ¬ (s ≤ q ∧ q < t) := by intro h; omega
                have hmfalse : midP s t q = false := by
                  apply Bool.eq_false_of_ne_true
                  intro hp
                  have hp' : s ≤ q ∧ q < t := by simpa [midP] using hp
                  exact hn hp'
                simp [outP, hmfalse]
              simp [midP, outP, hx, htail_nomid, htail_out] at ih' ⊢
              rw [runCost_cons_cons]
              have htx : x < t := hx.2
              have hle : p + min s0 (p*(y-x-1)) ≤ s0 + p := term_le_setup_page x y s0 p
              have h1 : 1 ≤ t - x := by omega
              have hps : p ≤ p*(t-x) := by simpa using Nat.mul_le_mul_left p h1
              omega
          · by_cases hy : s ≤ y ∧ y < t
            · -- x out, y mid
              have hfirst : (List.filter (midP s t) (x::y::ys)).head?.getD 0 =
                  (List.filter (midP s t) (y::ys)).head?.getD 0 := by
                simp [midP, hx, hy]
              simp [midP, outP, hx, hy] at ih' ⊢
              rw [runCost_cons_cons]
              let ot := List.filter (outP s t) (y::ys)
              have hcomp : p + min s0 (p*(y-x-1)) + runCost ot s0 p ≤ runCost (x::ot) s0 p := by
                cases hot : ot with
                | nil =>
                    simp [runCost]
                    exact term_le_setup_page x y s0 p
                | cons z zs =>
                    have hzot : z ∈ ot := by rw [hot]; simp
                    have hzmem : z ∈ y::ys := (List.mem_filter.mp hzot).1
                    have hzpred : outP s t z = true := (List.mem_filter.mp hzot).2
                    have hyz : y ≤ z := by
                      cases hzmem with
                      | head => exact Nat.le_refl y
                      | tail _ hzys => exact (List.pairwise_cons.mp hxs_ord).1 z hzys
                    have hmfalse : midP s t z = false := by simpa [outP] using hzpred
                    have hzout : ¬ (s ≤ z ∧ z < t) := by
                      intro hzmid
                      have hp : midP s t z = true := by simpa [midP] using hzmid
                      rw [hp] at hmfalse
                      cases hmfalse
                    have hzt : t ≤ z := by
                      have hsyle : s ≤ y := hy.1
                      omega
                    have hzy : y < z := by
                      have : z ≠ y := by intro he; subst z; exact hzout hy
                      omega
                    have hm := term_mono x y z (Nat.le_of_lt hzy) s0 p
                    rw [runCost_cons_cons]
                    exact Nat.add_le_add_right hm _
              have ih'' : runCost (y::ys) s0 p ≤ runCost ot s0 p + (s0 + p*(t-y)) := by
                simpa [ot, outP, midP, hy, Nat.add_assoc] using ih'
              calc
                p + min s0 (p*(y-x-1)) + runCost (y::ys) s0 p
                    ≤ (p + min s0 (p*(y-x-1))) + (runCost ot s0 p + (s0 + p*(t-y))) :=
                      Nat.add_le_add_left ih'' _
                _ = (p + min s0 (p*(y-x-1)) + runCost ot s0 p) + (s0 + p*(t-y)) := by simp [Nat.add_assoc]
                _ ≤ runCost (x::ot) s0 p + (s0 + p*(t-y)) := Nat.add_le_add_right hcomp _
                _ = runCost (x :: List.filter (outP s t) ys) s0 p + (s0 + p*(t-y)) := by
                      simp [ot, outP, midP, hy]
            · -- both out
              have hfirst_tail : (List.filter (midP s t) (y::ys)).head?.getD 0 =
                  (List.filter (midP s t) (x::y::ys)).head?.getD 0 := by
                simp [midP, hx, hy]
              simp [midP, outP, hx, hy] at ih' ⊢
              let ot := List.filter (outP s t) (y::ys)
              have hcomp : p + min s0 (p*(y-x-1)) + runCost ot s0 p ≤ runCost (x::ot) s0 p := by
                cases hot : ot with
                | nil =>
                    simp [runCost]
                    exact term_le_setup_page x y s0 p
                | cons z zs =>
                    have hz : z = y := by
                      have hhead : ot.head?.getD 0 = y := by simp [ot, outP, midP, hy]
                      rw [hot] at hhead
                      simpa using hhead
                    subst z
                    rw [runCost_cons_cons]
                    exact Nat.le_refl _
              rw [runCost_cons_cons]
              have houtl : runCost (x :: y :: List.filter (outP s t) ys) s0 p = runCost (x::ot) s0 p := by
                simp [ot, outP, midP, hy]
              rw [houtl]
              have ih'' : runCost (y::ys) s0 p ≤ runCost ot s0 p +
                    (if ∀ (a : Nat), a ∈ ys → s ≤ a → t ≤ a then 0 else s0 + p*(t - (List.find? (midP s t) ys).getD 0)) := by
                simpa [ot, outP, midP, hy, Nat.add_assoc] using ih'
              calc
                p + min s0 (p*(y-x-1)) + runCost (y::ys) s0 p
                    ≤ (p + min s0 (p*(y-x-1))) + (runCost ot s0 p + (if ∀ (a : Nat), a ∈ ys → s ≤ a → t ≤ a then 0 else s0 + p*(t - (List.find? (midP s t) ys).getD 0))) :=
                      Nat.add_le_add_left ih'' _
                _ = (p + min s0 (p*(y-x-1)) + runCost ot s0 p) +
                      (if ∀ (a : Nat), a ∈ ys → s ≤ a → t ≤ a then 0 else s0 + p*(t - (List.find? (midP s t) ys).getD 0)) := by simp [Nat.add_assoc]
                _ ≤ runCost (x::ot) s0 p +
                      (if ∀ (a : Nat), a ∈ ys → s ≤ a → t ≤ a then 0 else s0 + p*(t - (List.find? (midP s t) ys).getD 0)) :=
                      Nat.add_le_add_right hcomp _


theorem OptimalRead_sound : TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Gather.OptimalRead.Goal := by
  intro ids cap setup price ext hcap hext hcov
  let ps := pages cap ids
  let sorted := ps.mergeSort
  have hperm : sorted.Perm ps := by dsimp [sorted]; exact List.mergeSort_perm _ _
  have hnodup : sorted.Nodup := by
    apply hperm.symm.nodup
    dsimp [ps]
    exact nodup_eraseDups _
  have hsorted : sorted.Pairwise (fun a b => a ≤ b) := by
    have htotal : ∀ a b : Nat, (decide (a ≤ b) || decide (b ≤ a)) = true := by
      intro a b
      by_cases h : a ≤ b
      · simp [h]
      · have hba : b ≤ a := by omega
        simp [h, hba]
    have hpw := List.pairwise_mergeSort
      (le := fun a b : Nat => decide (a ≤ b))
      (by intro a b c hab hbc; simp at hab hbc ⊢; omega) htotal ps
    simpa [sorted] using hpw
  have hmain : ∀ (ext0 : List (Nat × Nat)) (l : List Nat),
      (∀ e ∈ ext0, e.1 < e.2) → l.Pairwise (fun a b => a ≤ b) → l.Nodup →
      (∀ p ∈ l, covers ext0 p = true) →
      runCost l setup price ≤ readCost setup price ext0 := by
    intro ext0
    induction ext0 with
    | nil =>
        intro l he hs hn hc
        have lempty : l = [] := by
          cases l with
          | nil => rfl
          | cons p ps =>
              have h := hc p (by simp)
              simp [covers] at h
        subst l
        simp [runCost, readCost]
    | cons e rest ih =>
        intro l he hs hn hc
        rcases e with ⟨s,t⟩
        have hst : s < t := he (s,t) (by simp)
        let ol := l.filter (outP s t)
        let ml := l.filter (midP s t)
        have hb : runCost l setup price ≤ runCost ol setup price +
            (if ml.isEmpty then 0 else setup + price*(t - ml.head?.getD 0)) := by
          simpa [ol, ml] using runCost_mid_bound l setup price s t hs hn
        have hcharge : runCost l setup price ≤ runCost ol setup price + (setup + price*(t-s)) := by
          by_cases hm : ml.isEmpty
          · simp [hm] at hb
            omega
          · simp [hm] at hb
            have hmem : ml.head?.getD 0 ∈ ml := by
              cases hm0 : ml with
              | nil => exact False.elim (hm (by simp [hm0]))
              | cons z zs => simp
            have hmid : midP s t (ml.head?.getD 0) = true := (List.mem_filter.mp hmem).2
            have hprop : s ≤ ml.head?.getD 0 ∧ ml.head?.getD 0 < t := by simpa [midP] using hmid
            have hd : t - ml.head?.getD 0 ≤ t - s := Nat.sub_le_sub_left hprop.1 t
            have hmch : setup + price*(t - ml.head?.getD 0) ≤ setup + price*(t-s) :=
              Nat.add_le_add_left (Nat.mul_le_mul_left price hd) _
            exact Nat.le_trans hb (by simpa [Nat.add_assoc] using Nat.add_le_add_left hmch (runCost ol setup price))
        have hcov_ol : ∀ p ∈ ol, covers rest p = true := by
          intro p hp
          have hp_in_l : p ∈ l := (List.mem_filter.mp hp).1
          have hp_out : outP s t p = true := (List.mem_filter.mp hp).2
          have hc0 := hc p hp_in_l
          simp [covers] at hc0
          rcases hc0 with hfirst | hrest
          · have hp_mid : s ≤ p ∧ p < t := by simpa using hfirst
            have hmfalse : midP s t p = false := by simpa [outP] using hp_out
            have hptrue : midP s t p = true := by simpa [midP] using hp_mid
            rw [hptrue] at hmfalse
            cases hmfalse
          · simpa [covers] using hrest
        have ih' := ih ol (by intro e he'; exact he e (by simp [he']))
            (hs.filter (outP s t)) (hn.filter (outP s t)) hcov_ol
        calc
          runCost l setup price ≤ runCost ol setup price + (setup + price*(t-s)) := hcharge
          _ ≤ readCost setup price rest + (setup + price*(t-s)) := Nat.add_le_add_right ih' _
          _ = readCost setup price ((s,t)::rest) := by
                simp [readCost, Nat.add_assoc, Nat.add_comm]
  have hp_in_ps : ∀ p ∈ sorted, p ∈ ps := by
    intro p hp
    exact (List.Perm.mem_iff hperm).mp hp
  simpa [ps, sorted] using hmain ext (pages cap ids |>.mergeSort)
      (by intro e he; exact hext e he) hsorted hnodup (by intro p hp; exact hcov p (hp_in_ps p hp))


/-! ===== LaneCompaction ===== -/

private def slotsAt (m : Flags) (k : Nat) : List Nat :=
  ((m.zipIdx k).filter Prod.fst).map Prod.snd

private theorem zipIdx_shift (xs : List α) (k j : Nat) :
    xs.zipIdx (k + j) = (xs.zipIdx k).map (fun p => (p.1, p.2 + j)) := by
  induction xs generalizing k j with
  | nil => simp
  | cons a as ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      have hidx : (k + j) + 1 = (k + 1) + j := by omega
      rw [hidx, ih (k + 1) j]
      simp

private theorem zipIdx_shift_one (xs : List α) :
    xs.zipIdx 1 = (xs.zipIdx).map (fun p => (p.1, p.2 + 1)) := by
  simpa using zipIdx_shift xs 0 1

private theorem slotsAt_cons (ok : Bool) (m : Flags) (k : Nat) :
    slotsAt (ok :: m) k = (if ok then [k] else []) ++ slotsAt m (k + 1) := by
  unfold slotsAt
  rw [List.zipIdx_cons]
  cases ok <;> simp

private theorem slots_aux (m : Flags) (k : Nat) :
    slotsAt m k = (slots m).map (fun i => k + i) := by
  induction m generalizing k with
  | nil => simp [slotsAt, slots]
  | cons ok m ih =>
      rw [slotsAt_cons]
      cases ok
      · rw [ih (k + 1)]
        have hf : slots (false :: m) = slotsAt m 1 := by
          simp [slots, slotsAt, List.zipIdx_cons]
        rw [hf, ih 1]
        rw [List.map_map]
        simp only [Function.comp_def]
        apply List.map_congr_left
        intro i hi
        omega
      · rw [ih (k + 1)]
        have ht : slots (true :: m) = 0 :: slotsAt m 1 := by
          simp [slots, slotsAt, List.zipIdx_cons]
        rw [ht, ih 1]
        simp only [ite_true, List.map_cons]
        rw [List.map_map]
        simp only [Function.comp_def]
        simp
        congr 1
        intro i hi
        omega

private theorem slots_cons_false (m : Flags) : slots (false :: m) = (slots m).map Nat.succ := by
  have hf : slots (false :: m) = slotsAt m 1 := by
    simp [slots, slotsAt, List.zipIdx_cons]
  rw [hf, slots_aux]
  apply List.map_congr_left
  intro i hi
  omega

private theorem slots_cons_true (m : Flags) : slots (true :: m) = 0 :: (slots m).map Nat.succ := by
  have ht : slots (true :: m) = 0 :: slotsAt m 1 := by
    simp [slots, slotsAt, List.zipIdx_cons]
  rw [ht, slots_aux]
  congr 1
  apply List.map_congr_left
  intro i hi
  omega

private theorem count_take_cons (ok : Bool) (m : Flags) (i : Nat) :
    ((ok :: m).take (i + 1)).count true = (m.take i).count true + (if ok then 1 else 0) := by
  cases ok <;> simp

private def recoverFrom {α : Type} (m : Flags) (dense : List α) (shift : Nat) : List (Option α) :=
  m.zipIdx.map (fun (ok, i) => if ok then dense[(m.take i).count true + shift]? else none)

private theorem recover_eq_recoverFrom {α : Type} (m : Flags) (dense : List α) :
    recover m dense = recoverFrom m dense 0 := by
  simp [recover, recoverFrom]

private theorem recoverFrom_cons (ok : Bool) (m : Flags) {α : Type} (dense : List α) (shift : Nat) :
    recoverFrom (ok :: m) dense shift =
      (if ok then dense[shift]? else none) ::
        recoverFrom m dense (shift + (if ok then 1 else 0)) := by
  unfold recoverFrom
  rw [List.zipIdx_cons]
  simp only [List.map_cons]
  congr 1
  · cases ok <;> simp
  · rw [zipIdx_shift_one]
    simp only [List.map_map]
    apply List.map_congr_left
    intro p hp
    rcases p with ⟨ok', i⟩
    by_cases hok' : ok'
    · by_cases hok : ok <;> simp [hok', hok] <;> congr 1 <;> omega
    · simp [hok']

private theorem getElem?_append_add {α : Type} (pre xs : List α) (j : Nat) :
    (pre ++ xs)[pre.length + j]? = xs[j]? := by
  rw [List.getElem?_append]
  have hnot : ¬ pre.length + j < pre.length := by omega
  simp [hnot]

private theorem recoverFrom_eq_recover_of_append (m : Flags) (pre second : List Bool) :
    recoverFrom m (pre ++ second) pre.length = recover m second := by
  rw [recover_eq_recoverFrom]
  unfold recoverFrom
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨ok, i⟩
  by_cases hok : ok
  · simp [hok]
    rw [Nat.add_comm]
    exact getElem?_append_add pre second ((m.take i).count true)
  · simp [hok]

private theorem recover_cons_false (m : Flags) (second : List Bool) :
    recover (false :: m) second = none :: recover m second := by
  rw [recover_eq_recoverFrom, recover_eq_recoverFrom]
  rw [recoverFrom_cons]
  simp

private theorem recover_cons_true (m : Flags) (b : Bool) (second : List Bool) :
    recover (true :: m) (b :: second) = some b :: recover m second := by
  rw [recover_eq_recoverFrom, recover_eq_recoverFrom]
  rw [recoverFrom_cons]
  simp
  rw [show b :: second = [b] ++ second by rfl]
  exact recoverFrom_eq_recover_of_append m [b] second

private theorem pack_cons_true {α : Type} (m : Flags) (x : α) (v : List α) :
    pack (true :: m) (x :: v) = x :: pack m v := by
  simp [pack]

private theorem pack_cons_false {α : Type} (m : Flags) (x : α) (v : List α) :
    pack (false :: m) (x :: v) = pack m v := by
  simp [pack]

private theorem recoverFrom_pack_append {α : Type} (m : Flags) (v : List α)
    (h : m.length = v.length) (pre : List α) :
    recoverFrom m (pre ++ pack m v) pre.length =
      (m.zip v).map (fun (ok, x) => if ok then some x else none) := by
  induction m generalizing v pre with
  | nil =>
      cases v with
      | nil => simp [recoverFrom]
      | cons x v' => simp at h
  | cons ok m ih =>
      cases v with
      | nil => simp at h
      | cons x v' =>
          have hv : m.length = v'.length := by simpa using h
          cases ok
          · rw [recoverFrom_cons]
            simp [pack_cons_false, ih v' hv pre]
          · rw [recoverFrom_cons]
            simp only [ite_true, pack_cons_true]
            have hget : (pre ++ (x :: pack m v'))[pre.length]? = some x := by
              rw [List.getElem?_append]
              simp
            rw [hget]
            rw [show pre ++ x :: pack m v' = (pre ++ [x]) ++ pack m v' by simp]
            rw [show pre.length + 1 = (pre ++ [x]).length by simp]
            rw [ih v' hv (pre ++ [x])]
            simp

private theorem pack_sum_partition (m : Flags) (widths : List Nat)
    (h : m.length = widths.length) :
    widths.sum = (pack m widths).sum + (pack (m.map (!·)) widths).sum := by
  induction m generalizing widths with
  | nil =>
      cases widths with
      | nil => simp [pack]
      | cons w ws => simp at h
  | cons ok m ih =>
      cases widths with
      | nil => simp at h
      | cons w ws =>
          have hw : m.length = ws.length := by simpa using h
          specialize ih ws hw
          cases ok
          · simp [pack_cons_false, pack_cons_true]
            omega
          · simp [pack_cons_true, pack_cons_false]
            omega

private def stopAt (m : Flags) (k : Nat) : Nat := ((slots m)[k]?).getD m.length

private theorem stopAt_cons_false (m : Flags) (k : Nat) :
    stopAt (false :: m) k = stopAt m k + 1 := by
  unfold stopAt
  rw [slots_cons_false]
  simp only [List.length_cons, List.getElem?_map]
  cases (slots m)[k]? with
  | none => simp
  | some i => simp

private theorem stopAt_cons_true_zero (m : Flags) : stopAt (true :: m) 0 = 0 := by
  unfold stopAt
  rw [slots_cons_true]
  simp

private theorem stopAt_cons_true_succ (m : Flags) (k : Nat) :
    stopAt (true :: m) (k + 1) = stopAt m k + 1 := by
  unfold stopAt
  rw [slots_cons_true]
  simp only [List.getElem?_cons_succ, List.length_cons, List.getElem?_map]
  cases (slots m)[k]? with
  | none => simp
  | some i => simp

theorem MaskCompose_sound : TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction.MaskCompose.Goal := by
  intro v m second hlen hsecond
  induction m generalizing v second with
  | nil =>
      cases v with
      | nil =>
          cases second with
          | nil => simp [pack, recover]
          | cons b bs => simp at hsecond
      | cons x xs => simp at hlen
  | cons ok m ih =>
      cases v with
      | nil => simp at hlen
      | cons x v' =>
          have hv : m.length = v'.length := by simpa using hlen
          cases ok
          · have hs : second.length = m.count true := by simpa using hsecond
            rw [pack_cons_false, recover_cons_false]
            simp [pack_cons_false]
            exact ih v' second hv hs
          · cases second with
            | nil => simp at hsecond
            | cons b second' =>
                have hs : second'.length = m.count true := by simpa using hsecond
                rw [pack_cons_true, recover_cons_true]
                simp
                cases b
                · simp [pack_cons_false]
                  exact ih v' second' hv hs
                · simp [pack_cons_true]
                  exact ih v' second' hv hs

theorem ChunkOffsets_sound : TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction.ChunkOffsets.Goal := by
  intro a b
  change slotsAt (a ++ b) 0 = slots a ++ (slots b).map (fun i => a.length + i)
  rw [show slotsAt (a ++ b) 0 = slotsAt a 0 ++ slotsAt b a.length by
    unfold slotsAt
    rw [List.zipIdx_append]
    simp]
  rw [show slotsAt a 0 = slots a by rfl, slots_aux]

theorem ExpandBack_sound : TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction.ExpandBack.Goal := by
  intro m v hlen
  simpa [recover_eq_recoverFrom] using (recoverFrom_pack_append m v hlen [])

theorem DecodeSavings_sound : TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction.DecodeSavings.Goal := by
  intro m widths hlen
  have hsum := pack_sum_partition m widths hlen
  omega

theorem EarlyDemand_sound : TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction.EarlyDemand.Goal := by
  intro m xs k hlen
  change (pack m xs).take k = pack (m.take (stopAt m k)) (xs.take (stopAt m k))
  induction m generalizing xs k with
  | nil =>
      cases xs with
      | nil => simp [stopAt, pack, slots]
      | cons x xs' => simp at hlen
  | cons ok m ih =>
      cases xs with
      | nil => simp at hlen
      | cons x xs' =>
          have hxs : m.length = xs'.length := by simpa using hlen
          cases ok
          · rw [pack_cons_false, stopAt_cons_false]
            rw [show (false :: m).take (stopAt m k + 1) = false :: m.take (stopAt m k) by simp]
            rw [show (x :: xs').take (stopAt m k + 1) = x :: xs'.take (stopAt m k) by simp]
            rw [pack_cons_false]
            exact ih xs' k hxs
          · cases k with
            | zero =>
                rw [stopAt_cons_true_zero]
                simp [pack]
            | succ k' =>
                rw [pack_cons_true, stopAt_cons_true_succ]
                rw [show (true :: m).take (stopAt m k' + 1) = true :: m.take (stopAt m k') by simp]
                rw [show (x :: xs').take (stopAt m k' + 1) = x :: xs'.take (stopAt m k') by simp]
                rw [pack_cons_true]
                simpa using ih xs' k' hxs


end TinylambOpt.ResearchProofs.DevinBatch.Atlas_ExecutorResources_B
