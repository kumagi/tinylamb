import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Grammar_LongPower

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar

/-! Exhaustive check over short binary words (length <= 3, repetitions up to 6) found no counterexample; the goal is true. The proof below establishes the general boundary-hit counting lemma and then proves the power formula by induction. -/

private def hit (s p : Word) (i : Nat) : Bool := p.isPrefixOf (s.drop i)

private theorem and_true {a b : Bool} (h : a && b = true) : a = true ∧ b = true := by
  rw [Bool.and_eq_true] at h
  exact h

private theorem hit_iff (s p : Word) (i : Nat) :
    hit s p i = true ↔ ∃ t : Word, p ++ t = s.drop i := by
  simp [hit, List.isPrefixOf_iff_prefix, List.IsPrefix]

private theorem hit_length (s p : Word) (i : Nat) (hp : p ≠ []) (h : hit s p i = true) :
    i + p.length ≤ s.length := by
  rcases (hit_iff s p i).mp h with ⟨t, ht⟩
  have hlen := congrArg List.length ht
  simp at hlen
  have hppos : 0 < p.length := by
    cases p with
    | nil => simp at hp
    | cons x xs => simp
  omega

private theorem isPrefixOf_eq_take (p s : Word) (hle : p.length ≤ s.length) :
    p.isPrefixOf s = true ↔ s.take p.length = p := by
  constructor
  · intro h
    rcases (List.isPrefixOf_iff_prefix.mp h) with ⟨t, ht⟩
    have hd : s.drop p.length = t := by
      rw [← ht]
      simp
    rw [← ht]
    simp [List.take_append_of_le_length (Nat.le_refl p.length)]
  · intro h
    apply List.isPrefixOf_iff_prefix.mpr
    refine ⟨s.drop p.length, ?_⟩
    calc
      p ++ s.drop p.length = s.take p.length ++ s.drop p.length := by rw [h]
      _ = s := List.take_append_drop p.length s

private theorem prefix_append_iff_of_le (p s t : Word) (hle : p.length ≤ s.length) :
    p.isPrefixOf (s ++ t) = true ↔ p.isPrefixOf s = true := by
  rw [isPrefixOf_eq_take p (s ++ t) (by simp; omega),
      isPrefixOf_eq_take p s hle]
  rw [List.take_append_of_le_length hle]

private theorem hit_append_of_le (s t p : Word) (i : Nat) (hi : i + p.length ≤ s.length) :
    hit (s ++ t) p i = hit s p i := by
  have hi' : i ≤ s.length := by omega
  have hdrop : (s ++ t).drop i = s.drop i ++ t := by
    rw [List.drop_append]
    have hsub : i - s.length = 0 := by omega
    rw [hsub]
    simp
  rw [hit, hdrop]
  exact Bool.eq_iff_iff.mpr (prefix_append_iff_of_le p (s.drop i) t (by simp; omega))

private theorem hit_append_right (s t p : Word) (j : Nat) (hj : j ≤ t.length) :
    hit (s ++ t) p (s.length + j) = hit t p j := by
  have hdrop : (s ++ t).drop (s.length + j) = t.drop j := by
    rw [List.drop_append]
    have hsub : s.length + j - s.length = j := by omega
    rw [hsub]
    rw [List.drop_eq_nil_of_le (by omega : s.length ≤ s.length + j)]
    simp
  simp [hit, hdrop]

private theorem positions_length_count (s p : Word) :
    (positions s p).length = (List.range (s.length + 1)).countP (hit s p) := by
  unfold positions hit
  rw [← List.countP_eq_length_filter]

private theorem countP_split (l : List Nat) (P Q : Nat → Bool) :
    l.countP P = l.countP (fun i => P i && Q i) + l.countP (fun i => P i && !Q i) := by
  induction l with
  | nil => simp
  | cons i is ih =>
      simp only [List.countP_cons]
      by_cases hP : P i = true <;> by_cases hQ : Q i = true <;>
        simp [hP, hQ] at ih ⊢ <;> omega

private theorem filter_range_shift (n s : Nat) (Q : Nat → Bool) :
    ((List.range (s + n)).filter (fun i => decide (s ≤ i) && Q (i - s))).map (fun i => i - s) =
      (List.range n).filter Q := by
  induction s with
  | zero =>
      simp
  | succ s ih =>
      rw [Nat.succ_add, List.range_succ_eq_map]
      simp only [List.filter_cons, List.filter_map]
      have hhead : (decide (s + 1 ≤ 0) && Q (0 - (s + 1))) = false := by simp
      rw [hhead]
      simp
      have hfilter :
          (List.range (s + n)).filter
              ((fun i => decide (s + 1 ≤ i) && Q (i - (s + 1))) ∘ Nat.succ) =
            (List.range (s + n)).filter (fun i => decide (s ≤ i) && Q (i - s)) := by
        apply List.filter_congr
        intro i hi
        simp only [Function.comp_apply]
        have hle : decide (s + 1 ≤ i + 1) = decide (s ≤ i) := by
          by_cases hs : s ≤ i
          · have hsi : s + 1 ≤ i + 1 := by omega
            simp [hs, hsi]
          · have hsi : ¬ s + 1 ≤ i + 1 := by omega
            simp [hs, hsi]
        have hsub : (i + 1) - (s + 1) = i - s := by omega
        rw [hle, hsub]
      rw [hfilter]
      change ((List.range (s + n)).filter (fun i => decide (s ≤ i) && Q (i - s))).map
          (fun i => (i + 1) - (s + 1)) = (List.range n).filter Q
      have hsub : ∀ i : Nat, (i + 1) - (s + 1) = i - s := by omega
      simp only [hsub]
      exact ih

private theorem prefix_take_boundary (sj b p : Word) (hp : p ≠ [])
    (hspos : 0 < sj.length) (hk : sj.length < p.length) (hkle : p.length ≤ sj.length + b.length) :
    p.isPrefixOf (sj ++ b.take (p.length - 1)) = true ↔ p.isPrefixOf (sj ++ b) = true := by
  have hppos : 0 < p.length := by
    cases p with
    | nil => simp at hp
    | cons x xs => simp
  have hkt : p.length - sj.length ≤ p.length - 1 := by omega
  have hkb : p.length - sj.length ≤ b.length := by
    omega
  have hkmin : p.length - sj.length ≤ min b.length (p.length - 1) :=
    Nat.le_min.mpr ⟨hkb, hkt⟩
  have hlenTake : p.length ≤ (sj ++ b.take (p.length - 1)).length := by
    rw [List.length_append, List.length_take]
    omega
  have hlenFull : p.length ≤ (sj ++ b).length := by simp; omega
  rw [isPrefixOf_eq_take p _ hlenTake, isPrefixOf_eq_take p _ hlenFull]
  rw [List.take_append, List.take_append]
  have hsjt : sj.take p.length = sj := by
    rw [List.take_eq_take_min]
    have hmin : min p.length sj.length = sj.length := Nat.min_eq_right (by omega)
    rw [hmin, List.take_length]
  rw [hsjt]
  simp [List.take_take, Nat.min_comm, Nat.min_eq_left hkt]

private theorem seam_len (a b p : Word) :
    (seamWord a b p).length = min a.length (p.length - 1) + min b.length (p.length - 1) := by
  unfold seamWord
  rw [List.length_append, List.length_drop, List.length_take]
  have hsub : a.length - (a.length - (p.length - 1)) = min a.length (p.length - 1) := by omega
  rw [hsub]
  rw [Nat.min_comm (p.length - 1) b.length]

private theorem len_sh_add_lim (a p : Word) :
    a.length = (a.length - min a.length (p.length - 1)) + min a.length (p.length - 1) := by
  omega

private theorem cross_hit_iff (a b p : Word) (hp : p ≠ []) (i j : Nat)
    (hij : i = (a.length - min a.length (p.length - 1)) + j)
    (hi : i < a.length) (hcross : a.length < i + p.length) :
    hit (a ++ b) p i = true ↔ hit (seamWord a b p) p j = true := by
  let lim := min a.length (p.length - 1)
  let sh := a.length - lim
  let sj := (a.drop sh).drop j
  have hppos : 0 < p.length := by
    cases p with
    | nil => simp at hp
    | cons x xs => simp
  have hp2 : 1 < p.length := by
    by_cases h1 : p.length = 1
    · have hc : a.length < i + 1 := by simpa [h1] using hcross
      omega
    · omega
  have hlimpos : 0 < lim := by
    dsimp [lim]
    by_cases hle : a.length ≤ p.length - 1
    · rw [Nat.min_eq_left hle]
      omega
    · have hle' : p.length - 1 ≤ a.length := by omega
      rw [Nat.min_eq_right hle']
      omega
  have hshlim : a.length = sh + lim := by
    dsimp [sh, lim]
    exact len_sh_add_lim a p
  have hjlim : j < lim := by omega
  have hshle : sh ≤ i := by omega
  have hji : i - sh = j := by omega
  have hdropa : a.drop i = sj := by
    dsimp [sj]
    rw [List.drop_drop]
    congr 1
  have hdropcat : (a ++ b).drop i = sj ++ b := by
    rw [List.drop_append]
    have hsub : i - a.length = 0 := by omega
    rw [hsub]
    simp [hdropa]
  have hidx : a.length - (p.length - 1) = sh := by
    dsimp [sh, lim]
    by_cases hle : a.length ≤ p.length - 1
    · rw [Nat.min_eq_left hle]
      omega
    · have hle' : p.length - 1 ≤ a.length := by omega
      rw [Nat.min_eq_right hle']
  have hdroplen : (a.drop sh).length = lim := by
    dsimp [sh, lim]
    simp
    omega
  have hsjlen : sj.length = lim - j := by
    dsimp [sj]
    simp
    omega
  have hseamdrop : (seamWord a b p).drop j = sj ++ b.take (p.length - 1) := by
    unfold seamWord
    rw [hidx]
    rw [List.drop_append]
    have hsub : j - (a.drop sh).length = 0 := by omega
    rw [hsub]
    simp [sj]
  have hsjpos : 0 < sj.length := by omega
  have hsjlt : sj.length < p.length := by omega
  constructor
  · intro hleft
    have hleft' : p.isPrefixOf (sj ++ b) = true := by
      simpa [hit, hdropcat] using hleft
    have hkle : p.length ≤ sj.length + b.length := by
      simpa using (List.isPrefixOf_iff_prefix.mp hleft').length_le
    have heq : p.isPrefixOf (sj ++ b.take (p.length - 1)) = true :=
      (prefix_take_boundary sj b p hp hsjpos hsjlt hkle).mpr hleft'
    simpa [hit, hseamdrop] using heq
  · intro hright
    have hright' : p.isPrefixOf (sj ++ b.take (p.length - 1)) = true := by
      simpa [hit, hseamdrop] using hright
    have hkleR : p.length ≤ (sj ++ b.take (p.length - 1)).length :=
      (List.isPrefixOf_iff_prefix.mp hright').length_le
    have hkle : p.length ≤ sj.length + b.length := by
      simp at hkleR ⊢
      omega
    have heq : p.isPrefixOf (sj ++ b) = true :=
      (prefix_take_boundary sj b p hp hsjpos hsjlt hkle).mp hright'
    simpa [hit, hdropcat] using heq

private theorem cross_count_eq (a b p : Word) (hp : p ≠ []) :
    (List.range a.length).countP
        (fun i => hit (a ++ b) p i && !decide (i + p.length ≤ a.length)) =
      crossing a b p := by
  let lim := min a.length (p.length - 1)
  let sh := a.length - lim
  let seam := seamWord a b p
  let Q : Nat → Bool := fun j => hit seam p j && decide (lim < j + p.length)
  let pred : Nat → Bool := fun i => hit (a ++ b) p i && !decide (i + p.length ≤ a.length)
  have hppos : 0 < p.length := by
    cases p with
    | nil => simp at hp
    | cons x xs => simp
  have hlimle : lim + 1 ≤ p.length := by
    dsimp [lim]
    by_cases hle : a.length ≤ p.length - 1
    · rw [Nat.min_eq_left hle]; omega
    · have hle' : p.length - 1 ≤ a.length := by omega
      rw [Nat.min_eq_right hle']; omega
  have hpred_eq : (List.range a.length).filter pred =
      (List.range a.length).filter (fun i => decide (sh ≤ i) && Q (i - sh)) := by
    apply List.filter_congr
    intro i hi
    have hilt : i < a.length := by simpa [List.mem_range] using hi
    have hshlim : a.length = sh + lim := by
      dsimp [sh, lim]
      exact len_sh_add_lim a p
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro hpred
      have hhit : hit (a ++ b) p i = true := by
        simpa [pred] using (and_true hpred).1
      have hnotbool : (!decide (i + p.length ≤ a.length)) = true :=
        (and_true hpred).2
      have hdecfalse : decide (i + p.length ≤ a.length) = false := by
        cases h : decide (i + p.length ≤ a.length) <;> simp [h] at hnotbool ⊢
      have hnle : ¬ i + p.length ≤ a.length := by
        intro hle
        have hbtrue : decide (i + p.length ≤ a.length) = true := by simp [hle]
        simp [hbtrue] at hdecfalse
      have hcross0 : a.length < i + p.length := by omega
      have hshle : sh ≤ i := by omega
      have hjlt : i - sh < lim := by omega
      have hcross : a.length < i + p.length := by omega
      have hlimlt : lim < (i - sh) + p.length := by omega
      have hseamhit : hit seam p (i - sh) = true := by
        have hc := cross_hit_iff a b p hp i (i - sh) (by omega) hilt hcross
        simpa [seam] using (hc.mp hhit)
      simp [Q, hseamhit, hshle, hlimlt]
    · intro hq
      have hle_bool : decide (sh ≤ i) = true := by
        simpa using (and_true hq).1
      have shle : sh ≤ i := by exact decide_eq_true_eq.mp hle_bool
      have hq' : Q (i - sh) = true := (and_true hq).2
      have hseamhit : hit seam p (i - sh) = true := by
        simpa [Q] using (and_true hq').1
      have hlimlt : lim < (i - sh) + p.length := by
        have hb := (and_true hq').2
        exact decide_eq_true_eq.mp hb
      have hcross : a.length < i + p.length := by omega
      have hnle : ¬ i + p.length ≤ a.length := by omega
      have hhit : hit (a ++ b) p i = true := by
        have hc := cross_hit_iff a b p hp i (i - sh) (by omega) hilt hcross
        exact hc.mpr (by simpa [seam] using hseamhit)
      simp [pred, hhit, hnle]
  have hlen : a.length = sh + lim := by
    dsimp [sh, lim]
    exact len_sh_add_lim a p
  have hlist : ((List.range a.length).filter pred).map (fun i => i - sh) =
      (List.range lim).filter Q := by
    rw [hpred_eq, hlen]
    exact filter_range_shift lim sh Q
  have hleft :
      (List.range a.length).countP pred = (List.range lim).countP Q := by
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
    rw [← hlist, List.length_map]
  rw [show (fun i => hit (a ++ b) p i && !decide (i + p.length ≤ a.length)) = pred by rfl]
  rw [hleft]
  unfold crossing positions
  rw [List.countP_filter]
  have hseamlen : seam.length = lim + min b.length (p.length - 1) := by
    dsimp [seam, lim]
    exact seam_len a b p
  have hlim_le : lim ≤ seam.length + 1 := by omega
  have hrange : List.range (seam.length + 1) =
      List.range lim ++ (List.range (seam.length + 1 - lim)).map (fun k => lim + k) := by
    have hr := List.range_add (n := lim) (m := seam.length + 1 - lim)
    have hs : lim + (seam.length + 1 - lim) = seam.length + 1 := by omega
    simpa [hs] using hr
  rw [hrange, List.countP_append, List.countP_map]
  have hzero : (List.range (seam.length + 1 - lim)).countP
      (((fun j => decide (j < lim) && decide (lim < j + p.length) &&
          p.isPrefixOf (seam.drop j)) ∘ (fun k => lim + k))) = 0 := by
    apply List.countP_eq_zero.mpr
    intro k hk
    simp only [Function.comp_apply]
    simp
    intro hlt
    omega
  rw [hzero, Nat.add_zero]
  apply List.countP_congr
  intro j hj
  have hjlt : j < lim := by simpa [List.mem_range] using hj
  dsimp [Q]
  by_cases hlimlt : lim < j + p.length <;>
    simp [hit, List.isPrefixOf_iff_prefix, hjlt, hlimlt, lim, seam,
      Bool.and_comm, Bool.and_left_comm, Bool.and_assoc]

theorem sound_BoundaryHits : BoundaryHits.Goal := by
  intro a b p hp
  let P : Nat → Bool := hit (a ++ b) p
  have hlen : (a ++ b).length + 1 = a.length + (b.length + 1) := by simp; omega
  have hsplitRange : List.range ((a ++ b).length + 1) =
      List.range a.length ++ (List.range (b.length + 1)).map (fun j => a.length + j) := by
    rw [hlen]
    exact List.range_add
  have htotal : (positions (a ++ b) p).length =
      (List.range a.length).countP P +
        (List.range (b.length + 1)).countP (fun j => P (a.length + j)) := by
    rw [positions_length_count]
    change (List.range ((a ++ b).length + 1)).countP P = _
    rw [hsplitRange, List.countP_append, List.countP_map]
    rfl
  have hright : (List.range (b.length + 1)).countP (fun j => P (a.length + j)) =
      (positions b p).length := by
    rw [positions_length_count]
    apply List.countP_congr
    intro j hj
    have hjlt : j < b.length + 1 := by simpa [List.mem_range] using hj
    have hjle : j ≤ b.length := by omega
    change (P (a.length + j) = true) ↔ (hit b p j = true)
    dsimp [P]
    rw [hit_append_right a b p j hjle]
  have hposa : (positions a p).length = (List.range a.length).countP (hit a p) := by
    rw [positions_length_count]
    rw [List.range_succ, List.countP_append]
    simp [hit, hp]
  have hinside :
      (List.range a.length).countP (fun i => P i && decide (i + p.length ≤ a.length)) =
        (positions a p).length := by
    rw [hposa]
    apply List.countP_congr
    intro i hi
    change (P i && decide (i + p.length ≤ a.length) = true) ↔ (hit a p i = true)
    by_cases hle : i + p.length ≤ a.length
    · have hlebool : decide (i + p.length ≤ a.length) = true := by simp [hle]
      simp [hlebool]
      dsimp [P]
      rw [hit_append_of_le a b p i hle]
    · have hlebool : decide (i + p.length ≤ a.length) = false := by simp [hle]
      simp [hlebool]
      have hn : ¬ hit a p i = true := by
        intro hhit
        have hb := hit_length a p i hp hhit
        omega
      exact Bool.eq_false_of_ne_true hn
  have hsplit := countP_split (List.range a.length) P
      (fun i => decide (i + p.length ≤ a.length))
  rw [htotal, hsplit, hinside, cross_count_eq a b p hp, hright]
  omega

private theorem take_drop_filterMap (xs : Word) (l w : Nat) :
    (xs.drop l).take w = (List.range w).filterMap (fun j => xs[l + j]?) := by
  induction w with
  | zero => simp
  | succ w ih =>
      rw [List.take_add_one]
      rw [List.range_succ, List.filterMap_append, ih]
      simp only [List.getElem?_drop]
      cases h : xs[l + w]? with
      | none => simp [h]
      | some x => simp [h]

private theorem length_expand : ∀ g : Text, length g = (expand g).length
  | .literal s => rfl
  | .concat a b => by simp [length, expand, length_expand a, length_expand b]
  | .repeat n a => by simp [length, expand, length_expand a]

private theorem get_replicate_flatten (xs : Word) (n i : Nat) :
    ((List.replicate n xs).flatten)[i]? =
      if i < n * xs.length then xs[i % xs.length]? else none := by
  induction n generalizing i with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons]
      rw [List.getElem?_append]
      by_cases hx : xs.length = 0
      · have hnil : xs = [] := List.eq_nil_of_length_eq_zero hx
        subst xs
        simp
      · by_cases hi : i < xs.length
        · have hpos : 0 < n + 1 := by omega
          have hleL : xs.length ≤ (n + 1) * xs.length := Nat.le_mul_of_pos_left xs.length hpos
          have hlt : i < (n + 1) * xs.length := by omega
          simp [hi, hlt, Nat.mod_eq_of_lt hi]
        · have hge : xs.length ≤ i := by omega
          rw [if_neg hi]
          have hsucc : (n + 1) * xs.length = n * xs.length + xs.length := by
            rw [Nat.succ_mul]
          have hcond : (i < (n + 1) * xs.length) ↔ (i - xs.length < n * xs.length) := by
            rw [hsucc]
            omega
          have hiadd : xs.length + (i - xs.length) = i := by omega
          have hmod : i % xs.length = (i - xs.length) % xs.length := by
            rw [← hiadd, Nat.add_mod]
            simp [Nat.mod_mod]
          by_cases hlt : i < (n + 1) * xs.length
          · have hlt' : i - xs.length < n * xs.length := hcond.mp hlt
            simpa [hlt, hlt', hmod] using (ih (i - xs.length))
          · have hlt' : ¬ i - xs.length < n * xs.length := by
              intro h; exact hlt (hcond.mpr h)
            simpa [hlt, hlt'] using (ih (i - xs.length))

private theorem atChar_eq_get : ∀ (g : Text) (i : Nat), atChar g i = (expand g)[i]?
  | .literal s, i => rfl
  | .concat a b, i => by
      dsimp [atChar, expand]
      have hlen_eq : (expand a).length = length a := (length_expand a).symm
      by_cases hi : i < length a
      · rw [if_pos hi, atChar_eq_get a]
        rw [List.getElem?_append_left]
        simpa [hlen_eq] using hi
      · rw [if_neg hi]
        have hage : (expand a).length ≤ i := by simpa [hlen_eq] using hi
        rw [List.getElem?_append_right hage]
        simpa [hlen_eq] using (atChar_eq_get b (i - length a))
  | .repeat n a, i => by
      dsimp [atChar, expand]
      rw [length_expand a]
      rw [get_replicate_flatten]
      by_cases hi : i < n * (expand a).length
      · simp [hi, atChar_eq_get a]
      · simp [hi]

private theorem rep_append_comm (s : Word) (n : Nat) :
    (List.replicate n s).flatten ++ s = s ++ (List.replicate n s).flatten := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons]
      calc
        (s ++ (List.replicate n s).flatten) ++ s =
            s ++ ((List.replicate n s).flatten ++ s) := by rw [List.append_assoc]
        _ = s ++ (s ++ (List.replicate n s).flatten) := by rw [ih]

private theorem rep_succ (s : Word) (n : Nat) :
    expand (.repeat (n + 1) (.literal s)) = expand (.repeat n (.literal s)) ++ s := by
  dsimp [expand]
  rw [List.replicate_succ, List.flatten_cons]
  exact (rep_append_comm s n).symm

private theorem crossing_empty (s p : Word) : crossing [] s p = 0 := by
  unfold crossing seamWord
  simp

private theorem seam_append_of_le (a s p : Word) (hp : p ≠ []) (hle : p.length ≤ s.length) :
    seamWord (a ++ s) s p = seamWord s s p := by
  have hpos : 0 < p.length := by
    cases p with
    | nil => simp at hp
    | cons x xs => simp
  have hm1 : p.length - 1 ≤ s.length := by omega
  have hmin : min s.length (p.length - 1) = p.length - 1 := Nat.min_eq_right hm1
  have hmincat : min (a.length + s.length) (p.length - 1) = p.length - 1 :=
    Nat.min_eq_right (by omega)
  unfold seamWord
  simp only [List.length_append]
  have hsub : a.length + s.length - (p.length - 1) =
      a.length + (s.length - (p.length - 1)) := by omega
  rw [hsub, List.drop_append]
  have hsub2 : a.length + (s.length - (p.length - 1)) - a.length =
      s.length - (p.length - 1) := by omega
  rw [hsub2]
  rw [List.drop_eq_nil_of_le (by omega : a.length ≤ a.length + (s.length - (p.length - 1)))]
  simp

private theorem crossing_append_of_le (a s p : Word) (hp : p ≠ []) (hle : p.length ≤ s.length) :
    crossing (a ++ s) s p = crossing s s p := by
  unfold crossing
  dsimp only
  rw [seam_append_of_le a s p hp hle]
  have hm1 : p.length - 1 ≤ s.length := by omega
  have hmin : min s.length (p.length - 1) = p.length - 1 := Nat.min_eq_right hm1
  have hmincat : min (a ++ s).length (p.length - 1) = p.length - 1 :=
    Nat.min_eq_right (by simp; omega)
  rw [hmin, hmincat]

private theorem crossing_rep_succ (s p : Word) (hp : p ≠ []) (hle : p.length ≤ s.length)
    (n : Nat) : crossing (expand (.repeat (n + 1) (.literal s))) s p = crossing s s p := by
  cases n with
  | zero => simp [expand]
  | succ k =>
      rw [rep_succ]
      exact crossing_append_of_le (expand (.repeat (k + 1) (.literal s))) s p hp hle

theorem sound_LongPower : LongPower.Goal := by
  intro s p n hp hle
  induction n with
  | zero =>
      simp [expand, positions, hp]
  | succ n ih =>
      rw [rep_succ]
      rw [sound_BoundaryHits (expand (.repeat n (.literal s))) s p hp]
      rw [ih]
      by_cases hn : n = 0
      · subst n
        simp [expand, positions, hp, crossing_empty]
      · obtain ⟨k, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hn
        rw [crossing_rep_succ s p hp hle k]
        simp only [Nat.succ_eq_add_one, Nat.add_sub_cancel_right, Nat.add_mul, Nat.one_mul]
        omega

theorem sound_Grammar_LongPower : Grammar.LongPower.Goal := sound_LongPower

end TinylambOpt.ResearchProofs.Atlas.Goal_Grammar_LongPower
