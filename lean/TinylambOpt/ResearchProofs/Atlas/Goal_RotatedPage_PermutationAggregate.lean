import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `RotatedPage.PermutationAggregate` — `∀ c, ((rows layout c).map Prod.snd).Perm ((rows
  LinearOrdinal.layout c).map Prod.snd)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RotatedPage_PermutationAggregate

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RotatedPage

/-! Brute-force sanity check over small configurations. -/
#eval do
  let mut bad : Array (Nat × Nat × Int × Int) := #[]
  for n in List.range 12 do
    for p in List.range 12 do
      for b in [-2, 0, 3] do
        for st in [-3, 0, 2] do
          let c : Config := ⟨n, 0, p, b, st, 0, []⟩
          let lhs := (rows layout c).map Prod.snd
          let rhs := (rows LinearOrdinal.layout c).map Prod.snd
          if !decide (List.Perm lhs rhs) then bad := bad.push (n, p, b, st)
  pure bad.toList

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

/-! The physical-to-logical index map is a rotation of `[0,n)`. -/
private def rot (n s i : Nat) : Nat := (i + s) % n

/-! Its inverse on `[0,n)`: unwind a wrapped index by `n`. -/
private def unrot (n s x : Nat) : Nat :=
  if x < s then x + n - s else x - s

private theorem unrot_lt (n s x : Nat) (_hn : 0 < n) (hs : s < n) (hx : x < n) :
    unrot n s x < n := by
  unfold unrot
  by_cases h : x < s <;> simp [h] <;> omega

private theorem rot_unrot (n s x : Nat) (_hn : 0 < n) (hs : s < n) (hx : x < n) :
    rot n s (unrot n s x) = x := by
  unfold unrot
  by_cases h : x < s
  · simp [h]
    unfold rot
    have hsum : (x + n - s) + s = x + n := by omega
    rw [hsum]
    calc
      (x + n) % n = (x + n * 1) % n := by rw [Nat.mul_one]
      _ = x % n := Nat.add_mul_mod_self_left x n 1
      _ = x := Nat.mod_eq_of_lt hx
  · simp [h]
    unfold rot
    have hsum : (x - s) + s = x := by omega
    rw [hsum]
    exact Nat.mod_eq_of_lt hx

private theorem unrot_rot (n s i : Nat) (_hn : 0 < n) (hs : s < n) (hi : i < n) :
    unrot n s (rot n s i) = i := by
  unfold unrot rot
  by_cases hlt : i + s < n
  · have hmod : (i + s) % n = i + s := Nat.mod_eq_of_lt hlt
    rw [hmod]
    have hnot : ¬ i + s < s := by omega
    simp [hnot]
  · have hge : n ≤ i + s := by omega
    have hmod : (i + s) % n = i + s - n := by
      rw [Nat.mod_eq_sub_mod hge]
      exact Nat.mod_eq_of_lt (by omega)
    rw [hmod]
    have hlt' : i + s - n < s := by omega
    simp [hlt']
    omega

private theorem rot_inj (n s : Nat) (hn : 0 < n) (hs : s < n) {a b : Nat}
    (ha : a < n) (hb : b < n) (h : rot n s a = rot n s b) : a = b := by
  have hh := congrArg (unrot n s) h
  simpa [unrot_rot n s a hn hs ha, unrot_rot n s b hn hs hb] using hh

private theorem map_rot_nodup {l : List Nat} (n s : Nat) (hn : 0 < n) (hs : s < n)
    (hmem : ∀ i ∈ l, i < n) (hnd : l.Nodup) : (l.map (rot n s)).Nodup := by
  induction l with
  | nil => simp
  | cons a t ih =>
      rw [List.map_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hm
        obtain ⟨b, hb, heq⟩ := List.mem_map.mp hm
        have ha : a < n := hmem a (by simp)
        have hb' : b < n := hmem b (by simp [hb])
        have hba : b = a := rot_inj n s hn hs hb' ha heq
        exact (List.nodup_cons.mp hnd).1 (by simpa [hba] using hb)
      · exact ih (fun i hi => hmem i (by simp [hi])) (List.nodup_cons.mp hnd).2

private theorem rot_perm (n s : Nat) (hs : s < n) :
    List.Perm ((List.range n).map (rot n s)) (List.range n) := by
  by_cases hn0 : n = 0
  · subst n
    simp
  have hn : 0 < n := Nat.pos_of_ne_zero hn0
  have hnodup : ((List.range n).map (rot n s)).Nodup :=
    map_rot_nodup n s hn hs (fun i hi => List.mem_range.mp hi) List.nodup_range
  apply (List.perm_ext_iff_of_nodup hnodup List.nodup_range).mpr
  intro x
  constructor
  · intro hx
    obtain ⟨i, hi, hix⟩ := List.mem_map.mp hx
    have hlt : rot n s i < n := Nat.mod_lt _ hn
    simpa [hix] using hlt
  · intro hx
    have hx' : x < n := List.mem_range.mp hx
    refine List.mem_map.mpr ⟨unrot n s x, List.mem_range.mpr (unrot_lt n s x hn hs hx'), ?_⟩
    exact rot_unrot n s x hn hs hx'

private theorem rows_rotated (c : Config) :
    (rows layout c).map Prod.snd =
      (List.range c.n).map (fun i : Nat =>
        c.base + c.step * ((rot c.n (c.split % c.n) i : Nat) : Int)) := by
  have hrows : rows layout c =
      (List.range c.n).map (fun i : Nat =>
        (i, c.base + c.step * ((rot c.n (c.split % c.n) i : Nat) : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * ((rot c.n (c.split % c.n) i : Nat) : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [layout, rot]
  rw [hrows]
  simp [List.map_map, Function.comp_def]

private theorem rows_linearOrdinal (c : Config) :
    (rows LinearOrdinal.layout c).map Prod.snd =
      (List.range c.n).map (fun (j : Nat) => c.base + c.step * (j : Int)) := by
  have hrows : rows LinearOrdinal.layout c =
      (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * (i : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [LinearOrdinal.layout]
  rw [hrows]
  simp [List.map_map, Function.comp_def]

theorem sound_RotatedPage_PermutationAggregate : RotatedPage.PermutationAggregate.Goal := by
  intro c
  rw [rows_rotated c, rows_linearOrdinal c]
  by_cases hn : c.n = 0
  · simp [hn]
  · have hs : c.split % c.n < c.n := Nat.mod_lt _ (Nat.pos_of_ne_zero hn)
    have hp := List.Perm.map (fun j : Nat => c.base + c.step * (j : Int))
      (rot_perm c.n (c.split % c.n) hs)
    simpa [Function.comp_def] using hp

end TinylambOpt.ResearchProofs.Atlas.Goal_RotatedPage_PermutationAggregate
