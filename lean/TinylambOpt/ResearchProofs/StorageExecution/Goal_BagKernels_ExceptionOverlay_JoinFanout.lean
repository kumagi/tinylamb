import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_JoinFanout

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

private theorem nodup_eraseDups : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp [List.eraseDups]
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro hm
        rw [List.mem_eraseDups, List.mem_filter] at hm
        have hbad : (!a == a) = true := hm.2
        simp at hbad
      · exact nodup_eraseDups (as.filter (fun b => !b == a))
termination_by l => l.length
decreasing_by
  simp_wf
  have hle : (as.filter (fun b => !b == a)).length ≤ as.length := List.length_filter_le _ _
  omega

private theorem filter_false_eq_nil (l : List Nat) : l.filter (fun _ => false) = [] := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem sum_map_zero (l : List Nat) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem succ_minus_arith (n len cnt : Nat) (bd bv : Bool) :
    (if bd then Nat.succ n - (len + 1) else 0) + (cnt + (if bv then 1 else 0)) =
      ((if bd then n - len else 0) + cnt) + (if bv then 1 else 0) := by
  have hsub : Nat.succ n - (len + 1) = n - len := by
    rw [Nat.add_one len, Nat.succ_sub_succ]
  cases bd <;> cases bv <;> simp [hsub] <;> omega

private theorem succ_minus_arith_nodel (n len cnt : Nat) (hle : len ≤ n) (bd : Bool) :
    (if bd then Nat.succ n - len else 0) + cnt =
      ((if bd then n - len else 0) + cnt) + (if bd then 1 else 0) := by
  have hs : Nat.succ n - len = (n - len) + 1 := by
    rw [Nat.succ_sub hle]
  cases bd <;> simp [hs] <;> omega

private theorem filter_lt_succ_countP (L : List Nat) (hL : L.Nodup) (n : Nat) (g : Nat → Bool) :
    (L.filter (fun i => i < Nat.succ n)).countP g =
      (L.filter (fun i => i < n)).countP g + (if n ∈ L then (g n).toNat else 0) := by
  rw [List.countP_filter, List.countP_filter]
  induction L with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hL
      by_cases ha : a = n
      · subst a
        have hp : ∀ i ∈ as, (g i && i < Nat.succ n) ↔ (g i && i < n) := by
          intro i hi
          have hin : i ≠ n := by
            intro h
            subst i
            exact hL.1 hi
          have hltbool : (decide (i < Nat.succ n)) = (decide (i < n)) := by
            by_cases hlt : i < n
            · have hlts : i < Nat.succ n := Nat.lt_succ_of_lt hlt
              simp [hlt, hlts]
            · have hnlt : ¬ i < Nat.succ n := by omega
              simp [hlt, hnlt]
          rw [hltbool]
        rw [List.countP_cons, List.countP_cons, List.countP_congr hp]
        have hnfalse : ¬ n < n := Nat.lt_irrefl n
        cases hgn : g n <;> simp [hgn, hnfalse]
      · have hhead : (if g a && a < Nat.succ n then 1 else 0) =
            (if g a && a < n then 1 else 0) := by
          by_cases halt : a < n
          · have halts : a < Nat.succ n := Nat.lt_succ_of_lt halt
            simp [halt, halts]
          · have halt' : ¬ a < Nat.succ n := by omega
            simp [halt, halt']
        have hna : n ≠ a := Ne.symm ha
        have hncons : (n ∈ a :: as) = (n ∈ as) := by simp [ha, hna]
        rw [List.countP_cons, List.countP_cons, ih hL.2]
        rw [hhead]
        simp only [hncons]
        omega

private theorem filter_lt_length_le (L : List Nat) (hL : L.Nodup) (n : Nat) :
    (L.filter (fun i => i < n)).length ≤ n := by
  induction n with
  | zero => simp
  | succ n ih =>
      have h := filter_lt_succ_countP L hL n (fun _ : Nat => true)
      simp only [List.countP_true] at h
      by_cases hn : n ∈ L
      · simp [hn] at h
        omega
      · simp [hn] at h
        omega

private theorem lookup_value_mem_keys (d n : Nat) (es : List (Nat × Nat)) :
    (es.lookup n).getD d ∈ d :: es.map Prod.snd := by
  cases h : es.lookup n with
  | none => simp
  | some v =>
      rw [List.lookup_eq_some_iff] at h
      rcases h with ⟨l₁, l₂, hes, _⟩
      subst es
      simp

private theorem sum_indicator_nodup (l : List Nat) (hl : l.Nodup) (v : Nat) (f : Nat → Nat) :
    (l.map (fun k => (if k == v then 1 else 0) * f k)).sum = if v ∈ l then f v else 0 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      simp only [List.map_cons, List.sum_cons]
      rw [ih hl.2]
      by_cases hav : a = v
      · subst a
        have hvas : v ∉ as := hl.1
        simp [hvas]
      · have hva : v ≠ a := Ne.symm hav
        have hneq : (a == v) = false := by simpa [beq_eq_false_iff_ne] using hav
        simp [hneq, hva]

private theorem sum_add_mul (l : List Nat) (m ind f : Nat → Nat) :
    (l.map (fun k => (m k + ind k) * f k)).sum =
      (l.map (fun k => m k * f k)).sum + (l.map (fun k => ind k * f k)).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [Nat.add_mul, ih]
      omega

private theorem mass_succ (n d k : Nat) (es : List (Nat × Nat)) :
    bag.mass (Nat.succ n, d, es) k =
      bag.mass (n, d, es) k + (if k == (es.lookup n).getD d then 1 else 0) := by
  let L : List Nat := (es.map Prod.fst).eraseDups
  have hL : L.Nodup := by
    dsimp [L]
    exact nodup_eraseDups (es.map Prod.fst)
  let g : Nat → Bool := fun i => es.lookup i == some k
  have hlen := filter_lt_succ_countP L hL n (fun _ : Nat => true)
  have hcnt := filter_lt_succ_countP L hL n g
  have hlen' : (L.filter (fun i => i < Nat.succ n)).length =
      (L.filter (fun i => i < n)).length + (if n ∈ L then 1 else 0) := by
    simpa using hlen
  have hcnt' : (L.filter (fun i => i < Nat.succ n)).countP g =
      (L.filter (fun i => i < n)).countP g + (if n ∈ L then (g n).toNat else 0) := by
    simpa using hcnt
  have hlen_le : (L.filter (fun i => i < n)).length ≤ n := filter_lt_length_le L hL n
  change
    (if k == d then Nat.succ n - (L.filter (fun i => i < Nat.succ n)).length else 0) +
        (L.filter (fun i => i < Nat.succ n)).countP g =
      ((if k == d then n - (L.filter (fun i => i < n)).length else 0) +
        (L.filter (fun i => i < n)).countP g) +
        (if k == (es.lookup n).getD d then 1 else 0)
  rw [hlen', hcnt']
  by_cases hnL : n ∈ L
  · have hmemmap : n ∈ es.map Prod.fst := by simpa [L] using hnL
    have his : (es.lookup n).isSome := by
      rw [List.lookup_isSome_iff]
      rcases List.mem_map.mp hmemmap with ⟨p, hp, hpn⟩
      exact ⟨p, hp, by simp [hpn]⟩
    cases hlook : es.lookup n with
    | none =>
        rw [hlook] at his
        simp at his
    | some v =>
        have hgn : (g n).toNat = if k == v then 1 else 0 := by
          by_cases hkv : k == v
          · simp [g, hlook, Bool.beq_comm (a := v) (b := k), hkv]
          · have hvk : v ≠ k := by
              intro h
              apply hkv
              simp [h]
            simp [g, hlook, hvk, hkv]
        simp only [hnL, hgn, hlook]
        exact succ_minus_arith n (L.filter (fun i => i < n)).length
          ((L.filter (fun i => i < n)).countP g) (k == d) (k == v)
  · have hlooknone : es.lookup n = none := by
      rw [List.lookup_eq_none_iff]
      intro p hp
      have hnmap : n ∉ es.map Prod.fst := by
        intro hm
        exact hnL (by simpa [L] using hm)
      have hnp : n ≠ p.1 := by
        intro h
        exact hnmap (List.mem_map.mpr ⟨p, hp, h.symm⟩)
      simp [hnp]
    have hgn : (g n).toNat = 0 := by simp [g, hlooknone]
    simp only [hnL, hgn, hlooknone]
    exact succ_minus_arith_nodel n (L.filter (fun i => i < n)).length
      ((L.filter (fun i => i < n)).countP g) hlen_le (k == d)

private theorem weighted_flat : ∀ (p : Nat × Nat × List (Nat × Nat)) (f : Nat → Nat),
    weighted bag p f = ((bag.expand p).map f).sum := by
  rintro ⟨n, d, es⟩ f
  induction n generalizing d es f with
  | zero =>
      have hm : ∀ k, bag.mass (0, d, es) k = 0 := by
        intro k
        dsimp [bag]
        have hf : (fun i : Nat => decide (i < 0)) = (fun _ : Nat => false) := by
          funext i
          simp
        rw [hf, filter_false_eq_nil]
        simp
      unfold weighted
      have hmapped :
          (bag.keys (0, d, es)).eraseDups.map (fun k => bag.mass (0, d, es) k * f k) =
          (bag.keys (0, d, es)).eraseDups.map (fun _ => 0) := by
        apply List.map_congr_left
        intro k hk
        rw [hm k]
        simp
      rw [hmapped]
      change (List.map (fun _ : Nat => 0) (bag.keys (0, d, es)).eraseDups).sum = 0
      exact sum_map_zero _
  | succ n ih =>
      let v := (es.lookup n).getD d
      have hv : v ∈ d :: es.map Prod.snd := lookup_value_mem_keys d n es
      have hmass : ∀ k, bag.mass (Nat.succ n, d, es) k =
          bag.mass (n, d, es) k + (if k == v then 1 else 0) := by
        intro k
        simpa [v] using mass_succ n d k es
      have hkeys : bag.keys (Nat.succ n, d, es) = bag.keys (n, d, es) := rfl
      have hsum_extra :
          ((bag.keys (n, d, es)).eraseDups.map (fun k => (if k == v then 1 else 0) * f k)).sum = f v := by
        rw [sum_indicator_nodup ((bag.keys (n, d, es)).eraseDups) (nodup_eraseDups _) v f]
        simp [bag, hv]
      unfold weighted
      rw [hkeys]
      have hmap :
          ((bag.keys (n, d, es)).eraseDups.map (fun k => bag.mass (Nat.succ n, d, es) k * f k)).sum =
          ((bag.keys (n, d, es)).eraseDups.map (fun k => bag.mass (n, d, es) k * f k)).sum +
          ((bag.keys (n, d, es)).eraseDups.map (fun k => (if k == v then 1 else 0) * f k)).sum := by
        have hmapped :
            (bag.keys (n, d, es)).eraseDups.map (fun k => bag.mass (Nat.succ n, d, es) k * f k) =
            (bag.keys (n, d, es)).eraseDups.map
              (fun k => (bag.mass (n, d, es) k + (if k == v then 1 else 0)) * f k) := by
          apply List.map_congr_left
          intro k hk
          rw [hmass k]
        rw [hmapped]
        exact sum_add_mul (bag.keys (n, d, es)).eraseDups (bag.mass (n, d, es))
          (fun k => (if k == v then 1 else 0)) f
      rw [hmap, hsum_extra]
      change weighted bag (n, d, es) f + f v = ((bag.expand (Nat.succ n, d, es)).map f).sum
      rw [ih d es f]
      have hexpand : bag.expand (Nat.succ n, d, es) = bag.expand (n, d, es) ++ [v] := by
        simp [bag, List.range_succ, v]
      rw [hexpand]
      simp

private theorem bool_sum_filter (ys : List Nat) (P : Nat → Bool) :
    ((ys.filter P).map (fun y => (P y).toNat)).sum = (ys.filter P).length := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases h : P y
      · simp [h, ih, Nat.add_comm]
      · simp [h, ih]

private theorem sum_bool_toNat_filter (ys : List Nat) (P : Nat → Bool) :
    (ys.map (fun y => (P y).toNat)).sum = ((ys.filter P).map (fun y => (P y).toNat)).sum := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases h : P y
      · simp [h, ih]
      · simp [h, ih]

private theorem length_pairs (xs ys : List Nat) (P : Nat → Nat → Bool) :
    (pairs xs ys P).length =
      (xs.map (fun x => ((ys.filter (P x)).map (fun y => (P x y).toNat)).sum)).sum := by
  induction xs with
  | nil => simp [pairs]
  | cons x xs ih =>
      simp [pairs, ih, bool_sum_filter, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c s
  let P : Nat → Nat → Bool := fun x y => x ≤ y && y ≤ x + s
  have hleft :
      weighted bag a (fun x => weighted bag c (fun y => (decide (x ≤ y ∧ y ≤ x + s)).toNat)) =
        ((bag.expand a).map (fun x =>
          (((bag.expand c).filter (P x)).map (fun y => (P x y).toNat)).sum)).sum := by
    rw [weighted_flat]
    congr 1
    apply List.map_congr_left
    intro x hx
    rw [weighted_flat]
    have hinner :
        ((bag.expand c).map (fun y => (decide (x ≤ y ∧ y ≤ x + s)).toNat)).sum =
          ((bag.expand c).map (fun y => (P x y).toNat)).sum := by
      congr 1
    rw [hinner]
    exact sum_bool_toNat_filter (bag.expand c) (P x)
  rw [hleft, length_pairs]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_JoinFanout
