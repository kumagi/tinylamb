import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Boustrophedon_FullTiles

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.Boustrophedon

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

/-- The ordinal permutation induced by a serpentine traversal. -/
private def boustro (w : Nat) (i : Nat) : Nat :=
  let k := i / w
  let r := i % w
  k * w + if k % 2 == 0 then r else w - 1 - r

/-- Reversing the same full tile twice restores the original offset. -/
private theorem boustro_invol (w : Nat) (hw : 0 < w) (i : Nat) :
    boustro w (boustro w i) = i := by
  let k := i / w
  let r := i % w
  have hr : r < w := Nat.mod_lt _ hw
  let s := if k % 2 == 0 then r else w - 1 - r
  have hs : s < w := by
    dsimp [s]
    split
    · exact hr
    · omega
  have hbi : boustro w i = k * w + s := rfl
  rw [hbi]
  have hdiv : (k * w + s) / w = k := by
    rw [Nat.add_comm (k * w) s, Nat.mul_comm k w,
      Nat.add_mul_div_left s k hw, Nat.div_eq_of_lt hs, Nat.zero_add]
  have hmod : (k * w + s) % w = s := by
    rw [Nat.add_comm (k * w) s, Nat.mul_comm k w,
      Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hs]
  unfold boustro
  rw [hdiv, hmod]
  have hkmod : k % 2 = 0 ∨ k % 2 = 1 := by
    have hlt : k % 2 < 2 := Nat.mod_lt _ (by decide)
    omega
  rcases hkmod with hk | hk
  · have hcond : (k % 2 == 0) = true := by simp [hk]
    simpa [k, r, s, hcond, Nat.mul_comm] using (Nat.div_add_mod i w)
  · have hcond : (k % 2 == 0) = false := by simp [hk]
    have hrr : w - 1 - (w - 1 - r) = r := by omega
    simpa [k, r, s, hcond, hrr, Nat.mul_comm] using (Nat.div_add_mod i w)

private theorem boustro_inj (w : Nat) (hw : 0 < w) {a b : Nat}
    (h : boustro w a = boustro w b) : a = b := by
  have hh := congrArg (boustro w) h
  simpa [boustro_invol w hw] using hh

/-- Serpentine reordering never leaves its tile. -/
private theorem boustro_div (w : Nat) (hw : 0 < w) (i : Nat) :
    boustro w i / w = i / w := by
  let s := if i / w % 2 == 0 then i % w else w - 1 - i % w
  have hs : s < w := by
    dsimp [s]
    split
    · exact Nat.mod_lt _ hw
    · omega
  change (i / w * w + s) / w = i / w
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_div_left s (i / w) hw, Nat.div_eq_of_lt hs, Nat.zero_add]

private theorem full_tile_decomp (n w : Nat) (hn : n % w = 0) : n = (n / w) * w := by
  have hd := Nat.div_add_mod n w
  rw [hn, Nat.add_zero] at hd
  rw [Nat.mul_comm] at hd
  exact hd.symm

/-- An index below `n` stays below `n` when the last tile is complete. -/
private theorem boustro_bound (n w : Nat) (hw : 0 < w) (hn : n % w = 0) {i : Nat}
    (hi : i < n) : boustro w i < n := by
  have hq := full_tile_decomp n w hn
  have hklt : i / w < n / w := by
    rw [Nat.div_lt_iff_lt_mul hw]
    rw [← hq]
    exact hi
  calc
    boustro w i = (i / w) * w + (boustro w i) % w := by
      have hdec := Nat.div_add_mod (boustro w i) w
      rw [boustro_div w hw i] at hdec
      simpa [Nat.mul_comm] using hdec.symm
    _ < ((i / w) + 1) * w := by
      rw [Nat.add_mul, Nat.one_mul]
      exact Nat.add_lt_add_left (Nat.mod_lt _ hw) ((i / w) * w)
    _ ≤ n := by
      rw [hq]
      exact Nat.mul_le_mul_right w (by omega)

/-- With complete tiles the serpentine map permutes `[0,n)`. -/
private theorem boustro_perm (n w : Nat) (hw : 0 < w) (hn : n % w = 0) :
    List.Perm ((List.range n).map (boustro w)) (List.range n) := by
  have hnd : ((List.range n).map (boustro w)).Nodup := by
    exact List.Pairwise.map (R := fun a b : Nat => a ≠ b) (S := fun a b : Nat => a ≠ b)
      (boustro w) (fun a b hne hEq => hne (boustro_inj w hw hEq)) List.nodup_range
  apply (List.perm_ext_iff_of_nodup hnd List.nodup_range).mpr
  intro x
  constructor
  · intro hx
    obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hx
    exact List.mem_range.mpr (boustro_bound n w hw hn (List.mem_range.mp hi))
  · intro hx
    refine List.mem_map.mpr ⟨boustro w x,
      List.mem_range.mpr (boustro_bound n w hw hn (List.mem_range.mp hx)), ?_⟩
    exact boustro_invol w hw x

private theorem rows_boustrophedon (c : Config) :
    (rows Boustrophedon.layout c).map Prod.snd =
      (List.range c.n).map (fun i : Nat =>
        c.base + c.step * ((boustro (width c) i : Nat) : Int)) := by
  have hrows : rows Boustrophedon.layout c =
      (List.range c.n).map (fun i : Nat =>
        (i, c.base + c.step * ((boustro (width c) i : Nat) : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * ((boustro (width c) i : Nat) : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [Boustrophedon.layout, boustro]
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

theorem sound_Boustrophedon_FullTiles : Boustrophedon.FullTiles.Goal := by
  intro c hn
  rw [rows_boustrophedon c, rows_linearOrdinal c]
  by_cases hs : c.step = 0
  · simp [hs]
  · have hw : 0 < width c := by unfold width; omega
    have hp := boustro_perm c.n (width c) hw hn
    have hval := List.Perm.map (fun j : Nat => c.base + c.step * (j : Int)) hp
    simpa [boustro, Function.comp_def] using hval

end TinylambOpt.ResearchProofs.Atlas.Goal_Boustrophedon_FullTiles
