import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `ExceptionOverlay.OverrideHead` — `∀ n d es i x k, i<n → let old := (es.lookup
  i).getD d bag.mass (n,d,(i,x)::es) k + (decide (old=k)).toNat = bag.mass (n,d,es) k
  + (decide (x=k)).toNat`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_OverrideHead

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

/-! ## Generic list lemmas -/

private theorem count_map_eq_countP_map (l : List Nat) (f : Nat → Nat) (k : Nat) :
    (l.map f).count k = l.countP (fun i => f i == k) := by
  rw [List.count_eq_countP]
  induction l with
  | nil => simp
  | cons a as ih => simp [ih, List.countP_cons]

private theorem countP_split {α : Type} (l : List α) (p q : α → Bool) :
    l.countP p = (l.filter q).countP p + (l.filter (fun x => !q x)).countP p := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases hq : q a = true
      · simp [List.countP_cons, hq, ih]
        omega
      · have hqfalse : q a = false := Bool.eq_false_of_ne_true hq
        simp [List.countP_cons, hqfalse, ih]
        omega

private theorem count_map_point_update (l : List Nat) (f g : Nat → Nat) (i k : Nat)
    (hnd : l.Nodup) (hi : i ∈ l) (hfg : ∀ j ∈ l, j ≠ i → f j = g j) :
    (l.map g).count k + (decide (f i = k)).toNat =
      (l.map f).count k + (decide (g i = k)).toNat := by
  induction l generalizing f g i k with
  | nil => simp at hi
  | cons a tl ih =>
      rw [List.nodup_cons] at hnd
      rcases List.mem_cons.mp hi with rfl | hitl
      · have htlm : tl.map f = tl.map g := by
          apply List.map_congr_left
          intro j hj
          exact hfg j (by simp [hj]) (by
            intro hji
            exact hnd.1 (hji ▸ hj))
        rw [List.map_cons, List.map_cons, List.count_cons, List.count_cons, htlm]
        by_cases hfk : f i = k <;> by_cases hgk : g i = k <;> simp [hfk, hgk] <;> omega
      · have hai : a ≠ i := by
          intro h
          subst a
          exact hnd.1 hitl
        have hfa : f a = g a := hfg a (by simp) hai
        have ih' := ih f g i k hnd.2 hitl (fun j hj hji => hfg j (by simp [hj]) hji)
        rw [List.map_cons, List.map_cons, List.count_cons, List.count_cons, hfa]
        omega

/-! ## Lookup / dictionary facts -/

private theorem lookup_isSome_iff_mem_fst (es : List (Nat × Nat)) (i : Nat) :
    (es.lookup i).isSome = true ↔ i ∈ es.map Prod.fst := by
  induction es with
  | nil => simp
  | cons q qs ih =>
      rcases q with ⟨j, v⟩
      rw [List.lookup_cons]
      by_cases hij : i == j
      · have hijEq : i = j := beq_iff_eq.mp hij
        simp [hijEq]
      · have hneq : i ≠ j := by intro h; exact hij (beq_iff_eq.mpr h)
        simp [hij, hneq, ih]

private theorem eraseDups_nodup (l : List Nat) : l.eraseDups.Nodup := by
  induction hl : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
    cases l with
    | nil => simp
    | cons a as =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro hb
        rw [List.mem_eraseDups, List.mem_filter] at hb
        have hneq : (!a == a) = true := hb.2
        simp at hneq
      · apply ih (as.filter (fun b => !b == a)).length
        · have hle := List.length_filter_le (fun b => !b == a) as
          simp only [List.length_cons] at hl
          omega
        · rfl

private theorem nodup_filter_bool {α : Type} (l : List α) (p : α → Bool) (hn : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hn
      rcases hn with ⟨hanot, htnd⟩
      by_cases ha : p a = true
      · rw [List.filter_cons_of_pos ha]
        exact List.nodup_cons.mpr ⟨by
          intro h
          exact hanot (List.mem_filter.mp h).1, ih htnd⟩
      · rw [List.filter_cons_of_neg ha]
        exact ih htnd

private theorem good_perm_ps (n : Nat) (es : List (Nat × Nat)) :
    ((List.range n).filter (fun i => (es.lookup i).isSome)).Perm
      ((es.map Prod.fst).eraseDups.filter (fun i => i < n)) := by
  apply (List.perm_ext_iff_of_nodup
    (nodup_filter_bool _ _ List.nodup_range)
    (nodup_filter_bool _ _ (eraseDups_nodup _))).mpr
  intro i
  simp only [List.mem_filter, List.mem_range, List.mem_eraseDups]
  rw [lookup_isSome_iff_mem_fst]
  simp [decide_eq_true_eq, and_comm]

/-! ## The histogram of an exception overlay equals the count of its expansion -/

private theorem mass_eq_count (n d : Nat) (es : List (Nat × Nat)) (k : Nat) :
    bag.mass (n, d, es) k = (bag.expand (n, d, es)).count k := by
  unfold bag
  dsimp only
  rw [count_map_eq_countP_map]
  let ps := (es.map Prod.fst).eraseDups.filter (fun i => i < n)
  let good := (List.range n).filter (fun i => (es.lookup i).isSome)
  let bad := (List.range n).filter (fun i => !(es.lookup i).isSome)
  have hperm : good.Perm ps := good_perm_ps n es
  have hsplit := countP_split (List.range n) (fun i => (es.lookup i).getD d == k)
      (fun i => (es.lookup i).isSome)
  have hgood : good.countP (fun i => (es.lookup i).getD d == k) =
      ps.countP (fun i => es.lookup i == some k) := by
    rw [List.Perm.countP_eq (fun i => (es.lookup i).getD d == k) hperm]
    apply List.countP_congr
    intro i hi
    have himap : i ∈ es.map Prod.fst := by
      have hi' : i ∈ (es.map Prod.fst).eraseDups := (List.mem_filter.mp hi).1
      exact List.mem_eraseDups.mp hi'
    have hisome : (es.lookup i).isSome = true := (lookup_isSome_iff_mem_fst es i).mpr himap
    rcases hlk : es.lookup i with _ | v
    · rw [hlk] at hisome
      simp at hisome
    · simp
  have hbad : bad.countP (fun i => (es.lookup i).getD d == k) =
      (if k == d then bad.length else 0) := by
    have hcong : bad.countP (fun i => (es.lookup i).getD d == k) =
        bad.countP (fun i => d == k) := by
      apply List.countP_congr
      intro i hi
      have hnot : (es.lookup i).isSome = false := by
        have ht := (List.mem_filter.mp hi).2
        revert ht
        cases (es.lookup i).isSome <;> simp
      rcases hlk : es.lookup i with _ | v
      · simp
      · simp [hlk] at hnot
    rw [hcong]
    by_cases hkd : k == d
    · have hkdeq : k = d := beq_iff_eq.mp hkd
      subst k
      simp
    · have hdkfalse : (d == k) = false := by
        apply Bool.eq_false_of_ne_true
        intro h
        exact hkd (beq_iff_eq.mpr (beq_iff_eq.mp h).symm)
      simp [hdkfalse, hkd]
  have hlen_split := countP_split (List.range n) (fun _ : Nat => true)
      (fun i => (es.lookup i).isSome)
  have hlen_eq : n = good.length + bad.length := by
    simpa [good, bad] using hlen_split
  have hbadlen : bad.length = n - ps.length := by
    have hlenp := hperm.length_eq
    omega
  dsimp [ps, good, bad] at *
  rw [hsplit, hgood, hbad]
  by_cases hkd : k == d
  · have hkdeq : k = d := beq_iff_eq.mp hkd
    subst k
    simp only [beq_self_eq_true, ↓reduceIte]
    omega
  · simp [hkd]

/-! ## Goal -/

theorem sound_OverrideHead : OverrideHead.Goal := by
  intro n d es i x k hi
  dsimp
  rw [mass_eq_count, mass_eq_count]
  have hexpand : bag.expand (n, d, (i, x) :: es) =
      (List.range n).map (fun j => if j == i then x else (es.lookup j).getD d) := by
    unfold bag
    dsimp only
    apply List.map_congr_left
    intro j hj
    by_cases hji : j == i
    · have hji' : j = i := beq_iff_eq.mp hji
      subst j
      simp
    · simp [List.lookup_cons, hji]
  rw [hexpand]
  let f : Nat → Nat := fun j => (es.lookup j).getD d
  let g : Nat → Nat := fun j => if j == i then x else (es.lookup j).getD d
  change ((List.range n).map g).count k + (decide ((es.lookup i).getD d = k)).toNat =
    ((List.range n).map f).count k + (decide (x = k)).toNat
  have hmem : i ∈ List.range n := List.mem_range.mpr hi
  have hfg : ∀ j ∈ List.range n, j ≠ i → f j = g j := by
    intro j hj hji
    have hji' : (j == i) = false := beq_eq_false_iff_ne.mpr hji
    dsimp [f, g]
    rw [hji']
    simp
  have hfi : f i = (es.lookup i).getD d := rfl
  have hgi : g i = x := by simp [g]
  simpa [hfi, hgi] using
    (count_map_point_update (List.range n) f g i k List.nodup_range hmem hfg)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_OverrideHead
