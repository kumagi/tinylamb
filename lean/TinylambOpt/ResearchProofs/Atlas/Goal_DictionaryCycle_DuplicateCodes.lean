import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryCycle_DuplicateCodes

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DictionaryCycle

#eval do
  let vals : List Int := [-2, 0, 1]
  let mut dicts : List (List Int) := [[]]
  for _ in [0, 1, 2, 3] do
    dicts := dicts.flatMap (fun d => vals.map (fun x => d ++ [x]))
  let mut bad : Array (Nat × List Int × Int × Nat × Nat) := #[]
  for n in (List.range 13 : List Nat) do
    for d in dicts do
      for v in ([-3, -2, -1, 0, 1, 2, 3] : List Int) do
        if d ≠ [] then
          let c : Config := ⟨n, 0, 0, 0, 0, 0, d⟩
          let lhs := occurrence layout c v
          let rhs := (c.n / c.dict.length) * c.dict.count v +
            (c.dict.take (c.n % c.dict.length)).count v
          if lhs != rhs then bad := bad.push (n, d, v, lhs, rhs)
  pure (List.take 20 bad.toList)

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  rw [← filterMap_some_eq_map (List.range n) g]
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ]
      simp only [List.filterMap_append]
      rw [ih (fun i hi => h i (by omega))]
      simp [h n (by omega)]

private theorem rows_dictionary (c : Config) (hne : c.dict ≠ []) :
    rows layout c = (List.range c.n).map (fun i =>
      (i, c.dict.get ⟨i % c.dict.length, Nat.mod_lt i (List.ne_nil_iff_length_pos.mp hne)⟩)) := by
  unfold rows
  apply range_filterMap_all
  intro i hi
  simp only [layout]
  rw [List.getElem?_eq_getElem (Nat.mod_lt i (List.ne_nil_iff_length_pos.mp hne))]
  rfl

private theorem countP_map_snd {α β : Type} [BEq β] (l : List α) (f : α → β) (v : β) :
    (l.map (fun x => (x, f x))).countP (fun r => r.2 == v) =
      l.countP (fun x => f x == v) := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp only [List.map_cons, List.countP_cons, ih]

private theorem count_take_succ (d : List Int) (r : Nat) (hr : r < d.length) (v : Int) :
    (d.take (r + 1)).count v =
      (d.take r).count v + (if d.get ⟨r, hr⟩ == v then 1 else 0) := by
  calc
    List.count v (d.take (r + 1)) =
        List.count v (d.take r ++ [d.get ⟨r, hr⟩]) := by
          rw [List.take_add_one]
          have hg : d[r]? = some (d.get ⟨r, hr⟩) := List.getElem?_eq_getElem hr
          rw [hg]
          simp
    _ = List.count v (d.take r) + (if d.get ⟨r, hr⟩ == v then 1 else 0) := by
          rw [List.count_append]
          simp [List.count_cons]

private theorem succ_eq_mul_succ (L n : Nat) (h : n % L + 1 = L) :
    n + 1 = L * (n / L + 1) := by
  have hn : L * (n / L) + n % L = n := Nat.div_add_mod n L
  calc
    n + 1 = (L * (n / L) + n % L) + 1 := by rw [hn]
    _ = L * (n / L) + L := by omega
    _ = L * (n / L + 1) := by rw [Nat.mul_add]; simp

private theorem succ_eq_add_mul (L n : Nat) :
    n + 1 = (n % L + 1) + L * (n / L) := by
  have hn : L * (n / L) + n % L = n := Nat.div_add_mod n L
  calc
    n + 1 = (L * (n / L) + n % L) + 1 := by rw [hn]
    _ = (n % L + 1) + L * (n / L) := by omega

private theorem div_succ_wrap (L n : Nat) (hL : 0 < L) (h : n % L + 1 = L) :
    (n + 1) / L = n / L + 1 := by
  rw [succ_eq_mul_succ L n h]
  simpa using (Nat.add_mul_div_left 0 (n / L + 1) hL)

private theorem mod_succ_wrap (L n : Nat) (_hL : 0 < L) (h : n % L + 1 = L) :
    (n + 1) % L = 0 := by
  rw [succ_eq_mul_succ L n h]
  simp

private theorem div_succ_nonwrap (L n : Nat) (hL : 0 < L) (h : n % L + 1 < L) :
    (n + 1) / L = n / L := by
  rw [succ_eq_add_mul L n]
  rw [Nat.add_mul_div_left (n % L + 1) (n / L) hL]
  rw [Nat.div_eq_of_lt h]
  omega

private theorem mod_succ_nonwrap (L n : Nat) (_hL : 0 < L) (h : n % L + 1 < L) :
    (n + 1) % L = n % L + 1 := by
  rw [succ_eq_add_mul L n]
  rw [Nat.add_mul_mod_self_left (n % L + 1) L (n / L)]
  exact Nat.mod_eq_of_lt h

private theorem periodic_count (d : List Int) (hne : d ≠ []) (n : Nat) (v : Int) :
    (List.range n).countP (fun i =>
      d.get ⟨i % d.length, Nat.mod_lt i (List.ne_nil_iff_length_pos.mp hne)⟩ == v) =
      (n / d.length) * d.count v + (d.take (n % d.length)).count v := by
  have hpos : 0 < d.length := List.ne_nil_iff_length_pos.mp hne
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, ih]
      simp only [List.countP_cons, List.countP_nil]
      by_cases hwrap : n % d.length + 1 = d.length
      · rw [div_succ_wrap d.length n hpos hwrap, mod_succ_wrap d.length n hpos hwrap]
        simp only [List.take_zero, List.count_nil, Nat.add_zero]
        have htake : d.take (n % d.length + 1) = d := by
          rw [hwrap]
          exact List.take_length
        have hcnt : d.count v =
            (d.take (n % d.length)).count v +
              (if d.get ⟨n % d.length, Nat.mod_lt n hpos⟩ == v then 1 else 0) := by
          calc
            d.count v = List.count v (d.take (n % d.length + 1)) := by rw [htake]
            _ = (d.take (n % d.length)).count v +
                (if d.get ⟨n % d.length, Nat.mod_lt n hpos⟩ == v then 1 else 0) := by
                  exact count_take_succ d (n % d.length) (Nat.mod_lt n hpos) v
        rw [hcnt]
        rw [Nat.add_mul]
        simp
        omega
      · have hlt : n % d.length + 1 < d.length := by
          have hm := Nat.mod_lt n hpos
          omega
        rw [div_succ_nonwrap d.length n hpos hlt, mod_succ_nonwrap d.length n hpos hlt]
        rw [count_take_succ d (n % d.length) (Nat.mod_lt n hpos) v]
        omega

theorem sound_DictionaryCycle_DuplicateCodes : DictionaryCycle.DuplicateCodes.Goal := by
  intro c v hne
  unfold occurrence
  rw [rows_dictionary c hne]
  rw [countP_map_snd]
  exact periodic_count c.dict hne c.n v

end TinylambOpt.ResearchProofs.Atlas.Goal_DictionaryCycle_DuplicateCodes
