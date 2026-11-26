import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# BoundedCartesianProductTiles

For a positive tile size, flattening the tiles reconstructs the exact Cartesian product
in its original order, with every tile within the size cap.

A join can schedule bounded batches of matching pairs even when one key produces a large
cross product.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.BoundedCartesianProductTiles

open TinylambOpt.ResearchGoals.QueryRewrites

abbrev Pair := Int × Int

def pairsOf (xs ys : List Int) : List Pair := xs.flatMap (fun x => ys.map (fun y => (x, y)))

theorem pairs_length : ∀ (xs ys : List Int), (pairsOf xs ys).length = xs.length * ys.length
  | [], ys => by simp [pairsOf]
  | x :: xs, ys => by
      rw [show pairsOf (x :: xs) ys = (ys.map (fun y => (x, y))) ++ pairsOf xs ys by rfl]
      rw [List.length_append, pairs_length xs ys]
      simp [Nat.add_mul, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

theorem pairs_get (xs ys : List Int) : ∀ p : Nat,
    (pairsOf xs ys)[p]? =
      Option.bind xs[p / ys.length]? (fun x => Option.map (fun y => (x, y)) ys[p % ys.length]?) := by
  induction xs with
  | nil =>
      intro p
      simp [pairsOf]
  | cons x xs ih =>
      intro p
      unfold pairsOf
      rw [List.flatMap_cons, List.getElem?_append, List.length_map, List.getElem?_map]
      by_cases hp : p < ys.length
      · rw [if_pos hp]
        have hdiv : p / ys.length = 0 := Nat.div_eq_of_lt hp
        have hmod : p % ys.length = p := Nat.mod_eq_of_lt hp
        rw [hdiv, hmod]
        simp
      · rw [if_neg hp]
        change (pairsOf xs ys)[p - ys.length]? =
          (x :: xs)[p / ys.length]?.bind fun x => Option.map (fun y => (x, y)) ys[p % ys.length]?
        rw [ih (p - ys.length)]
        by_cases hn : ys.length = 0
        · simp [hn]
        · have hnpos : 0 < ys.length := Nat.pos_of_ne_zero hn
          have hpn : ys.length ≤ p := Nat.le_of_not_gt hp
          have hpeq : p = (p - ys.length) + ys.length := (Nat.sub_add_cancel hpn).symm
          have hdiv0 : (p - ys.length + ys.length * 1) / ys.length =
              (p - ys.length) / ys.length + 1 := by
            rw [Nat.add_mul_div_left (p - ys.length) 1 hnpos]
          have hmod0 : (p - ys.length + ys.length * 1) % ys.length =
              (p - ys.length) % ys.length := by
            rw [Nat.add_mul_mod_self_left]
          have hdiv : p / ys.length = (p - ys.length) / ys.length + 1 := by
            rw [hpeq]
            simpa using hdiv0
          have hmod : p % ys.length = (p - ys.length) % ys.length := by
            rw [hpeq]
            simpa using hmod0
          rw [hdiv, hmod]
          simp

theorem tile_elem (xs ys : List Int) (p : Nat) (hp : p < (pairsOf xs ys).length) :
    (do
      pure (← xs[p / ys.length]?, ← ys[p % ys.length]?)) = (pairsOf xs ys)[p]? := by
  have hlen := pairs_length xs ys
  have hnpos : 0 < ys.length := by
    by_cases hn : 0 < ys.length
    · exact hn
    · have hz : ys.length = 0 := Nat.eq_zero_of_not_pos hn
      rw [hz, Nat.mul_zero] at hlen
      omega
  have hxidx : p / ys.length < xs.length := by
    rw [Nat.div_lt_iff_lt_mul hnpos]
    simpa [hlen, Nat.mul_comm] using hp
  have hyidx : p % ys.length < ys.length := Nat.mod_lt p hnpos
  have hxs : xs[p / ys.length]? = some (xs[p / ys.length]) := List.getElem?_eq_getElem hxidx
  have hys : ys[p % ys.length]? = some (ys[p % ys.length]) := List.getElem?_eq_getElem hyidx
  rw [pairs_get]
  simp [hxs, hys]

theorem filterMap_range_eq_map {α β : Type} (w : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ j, j < w → f j = some (g j)) :
    (List.range w).filterMap f = (List.range w).map g := by
  induction w with
  | zero => simp
  | succ w ih =>
      rw [List.range_succ, List.filterMap_append, List.map_append]
      rw [ih (fun j hj => h j (by omega))]
      simp [h w (by omega)]

def tileOrd (N width t : Nat) : List Nat :=
  (List.range (min width (N - t * width))).map (fun j => t * width + j)

theorem tile_eq_pairs (xs ys : List Int) (cap t : Nat) :
    (List.range (min cap ((pairsOf xs ys).length - t * cap))).filterMap
        (fun j => do pure (← xs[(t * cap + j) / ys.length]?, ← ys[(t * cap + j) % ys.length]?)) =
      (tileOrd (pairsOf xs ys).length cap t).map (fun p => ((pairsOf xs ys)[p]?).getD default) := by
  unfold tileOrd
  rw [List.map_map]
  apply filterMap_range_eq_map (α := Pair) (β := Pair)
  intro j hj
  have hjrem : j < (pairsOf xs ys).length - t * cap := (Nat.lt_min.mp hj).2
  have hp : t * cap + j < (pairsOf xs ys).length := by omega
  rw [tile_elem xs ys (t * cap + j) hp]
  simp [List.getElem?_eq_getElem hp]

theorem flatMap_map_map {α β γ : Type} (l : List α) (f : α → List β) (g : β → γ) :
    l.flatMap (fun a => (f a).map g) = (l.flatMap f).map g := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

theorem range_map_getElem {α : Type} [Inhabited α] (l : List α) :
    (List.range l.length).map (fun i => (l[i]?).getD default) = l := by
  apply List.ext_getElem?
  intro i
  rw [List.getElem?_map]
  by_cases hi : i < l.length
  · rw [List.getElem?_range hi]
    simp [List.getElem?_eq_getElem hi]
  · have hr : (List.range l.length)[i]? = none := by
      simp [List.getElem?_range, hi]
    simp [hi, hr]

theorem range_pairwise_lt : ∀ n : Nat, (List.range n).Pairwise (fun a b : Nat => a < b)
  | 0 => by simp
  | n + 1 => by
      rw [List.range_succ, List.pairwise_append]
      refine ⟨range_pairwise_lt n, by simp, ?_⟩
      intro a ha b hb
      have ha' : a < n := by simpa using ha
      simp at hb
      omega

theorem tileOrd_pairwise (N width t : Nat) :
    (tileOrd N width t).Pairwise (fun a b : Nat => a < b) := by
  unfold tileOrd
  exact List.Pairwise.map (fun j => t * width + j) (by intro a b h; omega)
    (range_pairwise_lt _)

theorem tilesOrd_prefix_pairwise (N width k : Nat) :
    ((List.range k).flatMap (fun t => tileOrd N width t)).Pairwise (fun a b : Nat => a < b) := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [List.range_succ, List.flatMap_append, List.pairwise_append]
      refine ⟨ih, by simpa using tileOrd_pairwise N width k, ?_⟩
      intro a ha b hb
      rcases List.mem_flatMap.mp ha with ⟨u, hu, hau⟩
      have hu' : u < k := by simpa using hu
      unfold tileOrd at hau
      rcases List.mem_map.mp hau with ⟨ja, hja, haj⟩
      have hja' : ja < min width (N - u * width) := by simpa using hja
      have hjaw : ja < width := (Nat.lt_min.mp hja').1
      have hb' : b ∈ tileOrd N width k := by simpa using hb
      unfold tileOrd at hb'
      rcases List.mem_map.mp hb' with ⟨jb, hjb, hbj⟩
      have huk : (u + 1) * width ≤ k * width :=
        Nat.mul_le_mul_right width (Nat.succ_le_of_lt hu')
      have hleft : u * width + ja < (u + 1) * width := by
        rw [Nat.succ_mul]
        omega
      have hright : k * width ≤ k * width + jb := Nat.le_add_right _ _
      rw [← haj, ← hbj]
      omega

theorem not_mem_erase_self_of_nodup {α : Type} [BEq α] [LawfulBEq α] {a : α} {l : List α}
    (hn : l.Nodup) : a ∉ l.erase a := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        have hn' := List.nodup_cons.mp hn
        simpa [List.erase_cons_head] using hn'.1
      · have hne : a ≠ b := by
          intro h
          subst b
          simp at hba
        have hn' := List.nodup_cons.mp hn
        rw [List.erase_cons_tail hba]
        intro hmem
        rcases List.mem_cons.mp hmem with hab | hmem
        · exact hne hab
        · exact ih hn'.2 hmem

theorem nodup_erase {α : Type} [BEq α] [LawfulBEq α] {a : α} {l : List α}
    (hn : l.Nodup) : (l.erase a).Nodup := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        have hn' := List.nodup_cons.mp hn
        simpa [List.erase_cons_head] using hn'.2
      · have hn' := List.nodup_cons.mp hn
        rw [List.erase_cons_tail hba, List.nodup_cons]
        constructor
        · intro hbmem
          exact hn'.1 (List.mem_of_mem_erase hbmem)
        · exact ih hn'.2

theorem nodup_perm_ext {α : Type} [BEq α] [LawfulBEq α] (l₁ l₂ : List α)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hmem : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hl2 : l₂ = [] := by
        cases l₂ with
        | nil => rfl
        | cons b bs =>
            have : b ∈ b :: bs := by simp
            have : b ∈ ([] : List α) := (hmem b).mpr this
            simp at this
      subst l₂
      exact List.Perm.nil
  | cons a as ih =>
      rw [List.cons_perm_iff_perm_erase]
      constructor
      · exact (hmem a).mp (by simp)
      · have hnd' := List.nodup_cons.mp hnd₁
        apply ih (l₂.erase a) hnd'.2
        · exact nodup_erase hnd₂
        · intro x
          constructor
          · intro hx
            have hxa : x ≠ a := by
              intro h
              subst x
              exact hnd'.1 hx
            have hx2 : x ∈ l₂ := (hmem x).mp (by simp [hx])
            simpa [List.mem_erase_of_ne hxa] using hx2
          · intro hx
            have hxa : x ≠ a := by
              intro h
              subst x
              exact not_mem_erase_self_of_nodup hnd₂ hx
            have hx2 : x ∈ l₂ := List.mem_of_mem_erase hx
            have hx1 : x ∈ a :: as := (hmem x).mpr hx2
            rcases List.mem_cons.mp hx1 with rfl | hxas
            · contradiction
            · exact hxas

theorem mem_tileOrd {N width t x : Nat} :
    x ∈ tileOrd N width t ↔ ∃ j, j < min width (N - t * width) ∧ x = t * width + j := by
  unfold tileOrd
  constructor
  · intro hx
    rcases List.mem_map.mp hx with ⟨j, hj, hxj⟩
    exact ⟨j, by simpa using hj, hxj.symm⟩
  · rintro ⟨j, hj, rfl⟩
    exact List.mem_map.mpr ⟨j, by simpa using hj, rfl⟩

theorem mem_tileOrd_lt {N width t x : Nat} (hx : x ∈ tileOrd N width t) : x < N := by
  rcases (mem_tileOrd.mp hx) with ⟨j, hj, rfl⟩
  have hjrem : j < N - t * width := by omega
  omega

theorem nodup_map_add {L : List Nat} (hn : L.Nodup) (c : Nat) :
    (L.map fun j => c + j).Nodup := by
  induction L with
  | nil => simp
  | cons a as ih =>
      have hn' := List.nodup_cons.mp hn
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, hba⟩
        have hba' : a = b := by omega
        exact hn'.1 (by simpa [hba'] using hb)
      · exact ih hn'.2

theorem tileOrd_nodup (N width t : Nat) : (tileOrd N width t).Nodup := by
  unfold tileOrd
  exact nodup_map_add List.nodup_range (t * width)

theorem mem_flatMap_tilesOrd_lt {N width k x : Nat}
    (hx : x ∈ (List.range k).flatMap (fun t => tileOrd N width t)) : x < N := by
  rcases List.mem_flatMap.mp hx with ⟨t, ht, hxt⟩
  exact mem_tileOrd_lt hxt

theorem tilesOrd_prefix_nodup (N width k : Nat) :
    ((List.range k).flatMap (fun t => tileOrd N width t)).Nodup := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [List.range_succ, List.flatMap_append, List.nodup_append]
      constructor
      · exact ih
      · constructor
        · simpa using tileOrd_nodup N width k
        · intro a ha b hb hEq
          rcases List.mem_flatMap.mp ha with ⟨u, hu, hau⟩
          rcases (mem_tileOrd.mp hau) with ⟨ja, hja, rfl⟩
          have hb' : b ∈ tileOrd N width k := by
            simpa using hb
          rcases (mem_tileOrd.mp hb') with ⟨jb, hjb, hbeq⟩
          subst b
          have huk : u < k := by simpa using hu
          have hjaw : ja < width := by omega
          have hu1 : (u + 1) * width ≤ k * width :=
            Nat.mul_le_mul_right width (Nat.succ_le_of_lt huk)
          have hleft : u * width + ja < (u + 1) * width := by
            rw [Nat.succ_mul]
            omega
          have hright : k * width ≤ k * width + jb := Nat.le_add_right _ _
          omega

theorem ceil_mul_sub_cancel {N width : Nat} (hw : 0 < width) (hN : 0 < N) :
    (N + width - 1) - (width - 1) = N := by
  omega

theorem mem_flatMap_tilesOrd {N width x : Nat} (hw : 0 < width) :
    x ∈ (List.range ((N + width - 1) / width)).flatMap (fun t => tileOrd N width t) ↔ x < N := by
  constructor
  · exact mem_flatMap_tilesOrd_lt
  · intro hx
    have hN : 0 < N := by omega
    let t := x / width
    let j := x % width
    have htceil : t < (N + width - 1) / width := by
      rw [Nat.lt_div_iff_mul_lt hw]
      rw [ceil_mul_sub_cancel hw hN]
      have htmul : t * width ≤ x := by
        dsimp [t]
        exact Nat.div_mul_le_self x width
      omega
    have hjw : j < width := by
      dsimp [j]
      exact Nat.mod_lt x hw
    have hdecomp : width * t + j = x := by
      dsimp [t, j]
      exact Nat.div_add_mod x width
    have hdecomp' : t * width + j = x := by simpa [Nat.mul_comm] using hdecomp
    have hjrem : j < N - t * width := by omega
    have hjmin : j < min width (N - t * width) := Nat.lt_min.mpr ⟨hjw, hjrem⟩
    apply List.mem_flatMap.mpr
    exact ⟨t, List.mem_range.mpr htceil, mem_tileOrd.mpr ⟨j, hjmin, hdecomp'.symm⟩⟩

theorem tilesOrd_perm_range (N width : Nat) (hw : 0 < width) :
    ((List.range ((N + width - 1) / width)).flatMap (fun t => tileOrd N width t)).Perm (List.range N) := by
  apply nodup_perm_ext
    ((List.range ((N + width - 1) / width)).flatMap (fun t => tileOrd N width t))
    (List.range N) (tilesOrd_prefix_nodup N width ((N + width - 1) / width)) List.nodup_range
  intro x
  simpa [List.mem_range] using (mem_flatMap_tilesOrd (N := N) (width := width) (x := x) hw)

theorem tilesOrd_eq_range (N width : Nat) (hw : 0 < width) :
    (List.range ((N + width - 1) / width)).flatMap (fun t => tileOrd N width t) = List.range N := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
  · intro a b ha hb hab hba
    exact Nat.le_antisymm hab hba
  · exact (tilesOrd_prefix_pairwise N width ((N + width - 1) / width)).imp (by omega)
  · exact (range_pairwise_lt N).imp (by omega)
  · exact tilesOrd_perm_range N width hw

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.BoundedCartesianProductTiles.Goal := by
  intro xs ys cap hcap
  let pairs := pairsOf xs ys
  let len := pairs.length
  let T := (len + cap - 1) / cap
  let tiles := (List.range T).map (fun t =>
    (List.range (min cap (len - t * cap))).filterMap (fun j =>
      do pure (← xs[(t * cap + j) / ys.length]?, ← ys[(t * cap + j) % ys.length]?)))
  let g : Nat → Pair := fun p => (pairs[p]?).getD default
  have hflat : tiles.flatten = pairs := by
    have hfun : ∀ t : Nat,
        (List.range (min cap (len - t * cap))).filterMap
            (fun j => do pure (← xs[(t * cap + j) / ys.length]?, ← ys[(t * cap + j) % ys.length]?)) =
          (tileOrd len cap t).map g := by
      intro t
      dsimp [len, pairs, g]
      exact tile_eq_pairs xs ys cap t
    dsimp [tiles]
    change (List.range T).flatMap (fun t =>
        (List.range (min cap (len - t * cap))).filterMap
          (fun j => do pure (← xs[(t * cap + j) / ys.length]?, ← ys[(t * cap + j) % ys.length]?))) = pairs
    rw [show (List.range T).flatMap (fun t =>
          (List.range (min cap (len - t * cap))).filterMap
            (fun j => do pure (← xs[(t * cap + j) / ys.length]?, ← ys[(t * cap + j) % ys.length]?))) =
        (List.range T).flatMap (fun t => (tileOrd len cap t).map g) by
          apply congrArg (fun f : Nat → List Pair => (List.range T).flatMap f)
          funext t
          exact hfun t]
    rw [flatMap_map_map]
    rw [tilesOrd_eq_range len cap hcap]
    dsimp [g, pairs]
    exact range_map_getElem pairs
  constructor
  · exact hflat
  · intro tile ht
    rcases List.mem_map.mp ht with ⟨t, htT, rfl⟩
    exact Nat.le_trans (List.length_filterMap_le _ _) (by
      change (List.range (min cap ((pairsOf xs ys).length - t * cap))).length ≤ cap
      rw [List.length_range]
      exact Nat.min_le_left _ _)

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.BoundedCartesianProductTiles

