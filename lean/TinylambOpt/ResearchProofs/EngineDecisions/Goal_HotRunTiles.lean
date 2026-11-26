import TinylambOpt.ResearchGoals.EngineDecisions.Executor

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `HotRunTiles` — Split a single hot key across workers in pair-ordinal space and union
  matched occurrence bits *after* residual evaluation before outer emission.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Goal_HotRunTiles
open TinylambOpt.ResearchGoals.EngineDecisions.Executor.HotRunTiles

private def allPairs (l r : Nat) : List PairId :=
  (List.range l).flatMap fun i => (List.range r).map fun j => (i,j)

private def tileOrd (N width t : Nat) : List Nat :=
  (List.range (min width (N - t*width))).map fun j => t*width+j

private theorem not_mem_erase_self_of_nodup {α : Type} [BEq α] [LawfulBEq α] {a : α} {l : List α}
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

private theorem nodup_erase {α : Type} [BEq α] [LawfulBEq α] {a : α} {l : List α}
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

private theorem nodup_perm_ext {α : Type} [BEq α] [LawfulBEq α] (l₁ l₂ : List α)
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
              intro h; subst x; exact hnd'.1 hx
            have hx2 : x ∈ l₂ := (hmem x).mp (by simp [hx])
            simpa [List.mem_erase_of_ne hxa] using hx2
          · intro hx
            have hxa : x ≠ a := by
              intro h; subst x
              exact not_mem_erase_self_of_nodup hnd₂ hx
            have hx2 : x ∈ l₂ := List.mem_of_mem_erase hx
            have hx1 : x ∈ a :: as := (hmem x).mpr hx2
            rcases List.mem_cons.mp hx1 with rfl | hxas
            · contradiction
            · exact hxas

private theorem tile_eq_ord (l r width t : Nat) :
    tile l r width t = (tileOrd (l*r) width t).map (fun x => (x/r, x%r)) := by
  simp [tile, tileOrd]

private theorem mem_tileOrd {N width t x : Nat} :
    x ∈ tileOrd N width t ↔ ∃ j, j < min width (N - t*width) ∧ x = t*width+j := by
  constructor
  · intro hx
    rcases List.mem_map.mp hx with ⟨j, hj, hxj⟩
    exact ⟨j, by simpa using hj, hxj.symm⟩
  · rintro ⟨j, hj, rfl⟩
    exact List.mem_map.mpr ⟨j, by simpa using hj, rfl⟩

private theorem mem_tileOrd_lt {N width t x : Nat} (hx : x ∈ tileOrd N width t) : x < N := by
  rcases (mem_tileOrd.mp hx) with ⟨j, hj, rfl⟩
  have hjrem : j < N - t*width := by omega
  omega

private theorem mem_tileOrd_div_eq {N width t x : Nat} (hw : 0 < width)
    (hx : x ∈ tileOrd N width t) : x / width = t := by
  rcases (mem_tileOrd.mp hx) with ⟨j, hj, rfl⟩
  have hjw : j < width := by omega
  apply Nat.div_eq_of_lt_le
  · omega
  · have : t*width+j < t*width+width := Nat.add_lt_add_left hjw (t*width)
    simpa [Nat.succ_mul, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using this

private theorem nodup_map_add {L : List Nat} (hn : L.Nodup) (c : Nat) :
    (L.map fun j => c+j).Nodup := by
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

private theorem tileOrd_nodup (N width t : Nat) : (tileOrd N width t).Nodup := by
  exact nodup_map_add List.nodup_range (t*width)

private theorem mem_flatMap_tilesOrd_lt {N width k x : Nat}
    (hx : x ∈ (List.range k).flatMap (fun t => tileOrd N width t)) : x < N := by
  rcases List.mem_flatMap.mp hx with ⟨t, ht, hxt⟩
  exact mem_tileOrd_lt hxt

private theorem tilesOrd_prefix_nodup (N width k : Nat) :
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
          have hb' : b ∈ tileOrd N width k := by simpa using hb
          rcases (mem_tileOrd.mp hb') with ⟨jb, hjb, hbeq⟩
          subst b
          have huk : u < k := by simpa using (List.mem_range.mp hu)
          have hjaw : ja < width := by omega
          have hu1 : (u+1)*width ≤ k*width := Nat.mul_le_mul_right width (Nat.succ_le_of_lt huk)
          have hleft : u*width+ja < (u+1)*width := by
            rw [Nat.succ_mul]
            omega
          have hright : k*width ≤ k*width+jb := Nat.le_add_right _ _
          omega

private theorem ceil_mul_sub_cancel {N width : Nat} (hw : 0 < width) (hN : 0 < N) :
    (N + width - 1) - (width - 1) = N := by
  omega

private theorem mem_flatMap_tilesOrd {N width x : Nat} (hw : 0 < width) :
    x ∈ (List.range ((N+width-1)/width)).flatMap (fun t => tileOrd N width t) ↔ x < N := by
  constructor
  · exact mem_flatMap_tilesOrd_lt
  · intro hx
    have hN : 0 < N := by omega
    let t := x / width
    let j := x % width
    have htceil : t < (N+width-1)/width := by
      rw [Nat.lt_div_iff_mul_lt hw]
      rw [ceil_mul_sub_cancel hw hN]
      have htmul : t*width ≤ x := by
        dsimp [t]
        exact Nat.div_mul_le_self x width
      omega
    have hjw : j < width := by
      dsimp [j]
      exact Nat.mod_lt x hw
    have hdecomp : width*t + j = x := by
      dsimp [t, j]
      exact Nat.div_add_mod x width
    have hdecomp' : t*width + j = x := by simpa [Nat.mul_comm] using hdecomp
    have hjrem : j < N - t*width := by omega
    have hjmin : j < min width (N - t*width) := Nat.lt_min.mpr ⟨hjw, hjrem⟩
    apply List.mem_flatMap.mpr
    exact ⟨t, List.mem_range.mpr htceil, mem_tileOrd.mpr ⟨j, hjmin, hdecomp'.symm⟩⟩

private theorem tilesOrd_perm_range (N width : Nat) (hw : 0 < width) :
    ((List.range ((N+width-1)/width)).flatMap (fun t => tileOrd N width t)).Perm (List.range N) := by
  apply nodup_perm_ext ((List.range ((N+width-1)/width)).flatMap (fun t => tileOrd N width t))
    (List.range N) (tilesOrd_prefix_nodup N width ((N+width-1)/width)) List.nodup_range
  intro x
  simpa [List.mem_range] using (mem_flatMap_tilesOrd (N := N) (width := width) (x := x) hw)

private theorem ord_mod_of_lt (r j : Nat) (hr : 0 < r) (hj : j < r) :
    (r*l + j) / r = l := by
  rw [show r*l+j = j+r*l by omega]
  rw [Nat.add_mul_div_left (x := j) (z := l) (y := r) hr]
  simp [Nat.div_eq_of_lt hj]


private theorem cons_append_perm {β : Type} (a : β) (bs as : List β) :
    (a :: (bs ++ as)).Perm (bs ++ (a :: as)) := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      rw [List.cons_append, List.cons_append]
      exact (List.Perm.swap b a (bs ++ as)).trans (List.Perm.cons b ih)

private theorem append_comm_perm {β : Type} (as bs : List β) : (as ++ bs).Perm (bs ++ as) := by
  induction as with
  | nil => simpa using List.Perm.nil
  | cons a as ih =>
      rw [List.cons_append]
      exact (List.Perm.cons a ih).trans (cons_append_perm a bs as)

private theorem perm_flatMap {α β : Type} (f : α → List β) :
    ∀ {l₁ l₂ : List α}, l₁.Perm l₂ → (l₁.flatMap f).Perm (l₂.flatMap f) := by
  intro l₁ l₂ hp
  induction hp with
  | nil => exact List.Perm.nil
  | cons x p ih => simpa [List.flatMap_cons] using List.Perm.append_left (f x) ih
  | swap x y l =>
      simpa [List.flatMap_cons, List.append_assoc] using
        (List.Perm.append_right (List.flatMap f l) (append_comm_perm (f y) (f x)))
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem nodup_map_of_injective {α β : Type} (f : α → β) {l : List α}
    (hn : l.Nodup) (hinj : ∀ a b, f a = f b → a = b) : (l.map f).Nodup := by
  rw [List.nodup_iff_pairwise_ne] at hn ⊢
  exact List.Pairwise.map f (fun a b hab hf => hab (hinj a b hf)) hn

private theorem allPairs_nodup (l r : Nat) : (allPairs l r).Nodup := by
  induction l with
  | zero => simp [allPairs]
  | succ l ih =>
      rw [allPairs, List.range_succ, List.flatMap_append, List.nodup_append]
      constructor
      · exact ih
      · constructor
        · simpa using (nodup_map_of_injective (fun j : Nat => (l, j)) List.nodup_range (by
            intro a b h
            exact congrArg Prod.snd h))
        · intro a ha b hb hEq
          have hb' : b ∈ (List.range r).map (fun j : Nat => (l, j)) := by simpa using hb
          rcases List.mem_map.mp hb' with ⟨j, hj, rfl⟩
          have ha' : a ∈ allPairs l r := ha
          rcases List.mem_flatMap.mp ha' with ⟨i, hi, hai⟩
          rcases List.mem_map.mp hai with ⟨k, hk, rfl⟩
          have hil : i < l := by simpa using hi
          simp at hEq
          omega

private theorem mem_allPairs (l r i j : Nat) :
    (i, j) ∈ allPairs l r ↔ i < l ∧ j < r := by
  simp [allPairs]

private theorem mem_hitsF (l r : Nat) (pass : PairId → Bool) (i j : Nat) :
    (i, j) ∈ (allPairs l r).filter pass ↔ i < l ∧ j < r ∧ pass (i,j) = true := by
  simp [mem_allPairs, and_assoc]

private theorem hitsF_any_left_true (l r : Nat) (pass : PairId → Bool) (i : Nat) :
    ((allPairs l r).filter pass).any (fun p : PairId => p.1 == i) = true ↔
      i < l ∧ ∃ j, j < r ∧ pass (i,j) = true := by
  rw [List.any_eq_true]
  constructor
  · rintro ⟨p, hp, hpi⟩
    have hp' := (mem_hitsF l r pass p.1 p.2).mp hp
    rcases p with ⟨a, b⟩
    simp at hpi
    subst a
    exact ⟨hp'.1, b, hp'.2.1, hp'.2.2⟩
  · rintro ⟨hil, j, hjr, hp⟩
    exact ⟨(i,j), (mem_hitsF l r pass i j).mpr ⟨hil, hjr, hp⟩, by simp⟩

private theorem hitsF_any_left_false (l r : Nat) (pass : PairId → Bool) (i : Nat) :
    ((allPairs l r).filter pass).any (fun p : PairId => p.1 == i) = false ↔
      i < l → ∀ j, j < r → pass (i,j) = false := by
  rw [Bool.eq_false_iff]
  constructor
  · intro h hil j hjr
    have hn : ¬(i < l ∧ ∃ j, j < r ∧ pass (i,j) = true) := by
      intro hx
      exact h ((hitsF_any_left_true l r pass i).mpr hx)
    cases hpass : pass (i,j) <;> simp_all
  · intro h htrue
    rcases (hitsF_any_left_true l r pass i).mp htrue with ⟨hil, j, hjr, hp⟩
    have hb := h hil j hjr
    simp [hb] at hp

private theorem hitsF_any_right_true (l r : Nat) (pass : PairId → Bool) (j : Nat) :
    ((allPairs l r).filter pass).any (fun p : PairId => p.2 == j) = true ↔
      j < r ∧ ∃ i, i < l ∧ pass (i,j) = true := by
  rw [List.any_eq_true]
  constructor
  · rintro ⟨p, hp, hpj⟩
    have hp' := (mem_hitsF l r pass p.1 p.2).mp hp
    rcases p with ⟨a, b⟩
    simp at hpj
    subst b
    exact ⟨hp'.2.1, a, hp'.1, hp'.2.2⟩
  · rintro ⟨hjr, i, hil, hp⟩
    exact ⟨(i,j), (mem_hitsF l r pass i j).mpr ⟨hil, hjr, hp⟩, by simp⟩

private theorem hitsF_any_right_false (l r : Nat) (pass : PairId → Bool) (j : Nat) :
    ((allPairs l r).filter pass).any (fun p : PairId => p.2 == j) = false ↔
      j < r → ∀ i, i < l → pass (i,j) = false := by
  rw [Bool.eq_false_iff]
  constructor
  · intro h hjr i hil
    have hn : ¬(j < r ∧ ∃ i, i < l ∧ pass (i,j) = true) := by
      intro hx
      exact h ((hitsF_any_right_true l r pass j).mpr hx)
    cases hpass : pass (i,j) <;> simp_all
  · intro h htrue
    rcases (hitsF_any_right_true l r pass j).mp htrue with ⟨hjr, i, hil, hp⟩
    have hi := h hjr i hil
    simp [hi] at hp

private def ordPairs (l r : Nat) : List PairId :=
  (List.range (l*r)).map fun x => (x/r, x%r)

private theorem ordPairs_perm_allPairs (l r : Nat) : (ordPairs l r).Perm (allPairs l r) := by
  by_cases hr0 : r = 0
  · subst r
    simp [ordPairs, allPairs]
  · have hr : 0 < r := Nat.pos_of_ne_zero hr0
    induction l with
    | zero => simp [ordPairs, allPairs]
    | succ l ih =>
        have hrange : List.range ((l+1)*r) = List.range (l*r) ++ (List.range r).map (fun j => l*r+j) := by
          have hmul : (l+1)*r = l*r + r := by simpa using Nat.succ_mul l r
          rw [hmul, List.range_add]
        rw [ordPairs, hrange, List.map_append]
        rw [allPairs, List.range_succ, List.flatMap_append]
        have ih' :
            (List.map (fun x : Nat => (x/r, x%r)) (List.range (l*r))).Perm
              (List.flatMap (fun i : Nat => List.map (fun j : Nat => (i,j)) (List.range r)) (List.range l)) := by
          simpa [ordPairs, allPairs] using ih
        have htailEq :
            List.map (fun x : Nat => (x/r, x%r)) (List.map (fun j : Nat => l*r+j) (List.range r)) =
              List.flatMap (fun i : Nat => List.map (fun j : Nat => (i,j)) (List.range r)) [l] := by
          simp [List.map_map, List.flatMap_cons]
          intro j hj
          constructor
          · rw [Nat.mul_comm l r]
            exact ord_mod_of_lt (l := l) r j hr hj
          · exact Nat.mod_eq_of_lt hj
        have htail :
            (List.map (fun x : Nat => (x/r, x%r)) (List.map (fun j : Nat => l*r+j) (List.range r))).Perm
              (List.flatMap (fun i : Nat => List.map (fun j : Nat => (i,j)) (List.range r)) [l]) := by
          rw [htailEq]
        exact (List.Perm.append_right _ ih').trans
          (List.Perm.append_left _ htail)

private theorem flatMap_tiles_perm_ordPairs (l r width : Nat) (hw : 0 < width) :
    ((List.range ((l*r+width-1)/width)).flatMap (fun t => tile l r width t)).Perm (ordPairs l r) := by
  have h := List.Perm.map (fun x : Nat => (x/r, x%r)) (tilesOrd_perm_range (l*r) width hw)
  simpa [ordPairs, tile_eq_ord, List.map_flatMap] using h

private theorem finish_nodup (l r : Nat) {hits : List PairId} (hn : hits.Nodup) :
    (finish l r hits).Nodup := by
  let matched : List Out := hits.map (fun p : PairId => (some p.1, some p.2))
  let left : List Out :=
    ((List.range l).filter (fun i => !hits.any (fun p : PairId => p.1 == i))).map
      (fun i => (some i, none))
  let right : List Out :=
    ((List.range r).filter (fun j => !hits.any (fun p : PairId => p.2 == j))).map
      (fun j => (none, some j))
  have hM : matched.Nodup := by
    exact nodup_map_of_injective (fun p : PairId => (some p.1, some p.2)) hn (by
      intro a b h
      have h1 : a.1 = b.1 := by simpa using congrArg (fun o : Out => o.1) h
      have h2 : a.2 = b.2 := by simpa using congrArg (fun o : Out => o.2) h
      exact Prod.ext h1 h2)
  have hL : left.Nodup := by
    apply nodup_map_of_injective (fun i : Nat => (some i, none))
    · exact List.filter_sublist.nodup List.nodup_range
    · intro a b h
      simp at h
      exact h
  have hR : right.Nodup := by
    apply nodup_map_of_injective (fun j : Nat => (none, some j))
    · exact List.filter_sublist.nodup List.nodup_range
    · intro a b h
      simp at h
      exact h
  have hLR : (left ++ right).Nodup := by
    rw [List.nodup_append]
    refine ⟨hL, hR, ?_⟩
    intro x hx y hy hEq
    rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
    rcases List.mem_map.mp hy with ⟨j, hj, rfl⟩
    simp at hEq
  have hMLR : ∀ x, x ∈ matched → ∀ y, y ∈ left ++ right → x ≠ y := by
    intro x hx y hy hEq
    rcases List.mem_map.mp hx with ⟨p, hp, rfl⟩
    rcases List.mem_append.mp hy with hy | hy
    · rcases List.mem_map.mp hy with ⟨i, hi, rfl⟩
      simp at hEq
    · rcases List.mem_map.mp hy with ⟨j, hj, rfl⟩
      simp at hEq
  change (matched ++ left ++ right).Nodup
  rw [List.append_assoc, List.nodup_append]
  exact ⟨hM, hLR, hMLR⟩

private def jsOf (l r : Nat) (pass : PairId → Bool) (i : Nat) : List Nat :=
  (List.range r).filter (fun j => pass (i,j))

private def outerBlock (l r : Nat) (pass : PairId → Bool) (i : Nat) : List Out :=
  if (jsOf l r pass i).isEmpty then [(some i, none)]
  else (jsOf l r pass i).map (fun j => (some i, some j))

private theorem outerBlock_nodup (l r : Nat) (pass : PairId → Bool) (i : Nat) :
    (outerBlock l r pass i).Nodup := by
  by_cases h : (jsOf l r pass i).isEmpty
  · simp [outerBlock, h]
  · simp [outerBlock, h]
    apply nodup_map_of_injective (fun j : Nat => (some i, some j))
    · exact List.filter_sublist.nodup List.nodup_range
    · intro a b hEq
      simp at hEq
      exact hEq

private theorem mem_outerBlock_fst (l r : Nat) (pass : PairId → Bool) (i : Nat) {o : Out}
    (ho : o ∈ outerBlock l r pass i) : o.1 = some i := by
  by_cases h : (jsOf l r pass i).isEmpty
  · have ho' : o = (some i, none) := by simpa [outerBlock, h] using ho
    subst o
    simp
  · rcases List.mem_map.mp (by simpa [outerBlock, h] using ho) with ⟨j, hj, rfl⟩
    simp

private theorem outerFirst_nodup (l r : Nat) (pass : PairId → Bool) :
    ((List.range l).flatMap (fun i => outerBlock l r pass i)).Nodup := by
  induction l with
  | zero => simp
  | succ l ih =>
      rw [List.range_succ, List.flatMap_append, List.nodup_append]
      constructor
      · exact ih
      · constructor
        · simpa using outerBlock_nodup (l+1) r pass l
        · intro a ha b hb hEq
          rcases List.mem_flatMap.mp ha with ⟨u, hu, hau⟩
          have hb' : b ∈ outerBlock (l+1) r pass l := by simpa using hb
          have haf := mem_outerBlock_fst l r pass u hau
          have hbf := mem_outerBlock_fst (l+1) r pass l hb'
          have hfst := congrArg Prod.fst hEq
          rw [haf, hbf] at hfst
          simp at hfst
          have hu : u < l := by simpa using hu
          omega

private theorem outer_nodup (l r : Nat) (pass : PairId → Bool) :
    (outerRef l r pass).Nodup := by
  unfold outerRef
  change (((List.range l).flatMap (fun i => outerBlock l r pass i)) ++
    ((List.range r).filter (fun j => !(List.range l).any (fun i => pass (i,j)))).map (fun j => (none, some j))).Nodup
  rw [List.nodup_append]
  constructor
  · exact outerFirst_nodup l r pass
  · constructor
    · apply nodup_map_of_injective (fun j : Nat => (none, some j))
      · exact List.filter_sublist.nodup List.nodup_range
      · intro a b h
        simp at h
        exact h
    · intro x hx y hy hEq
      rcases List.mem_flatMap.mp hx with ⟨i, hi, hxi⟩
      have hxf := mem_outerBlock_fst l r pass i hxi
      rcases List.mem_map.mp hy with ⟨j, hj, rfl⟩
      subst x
      simp at hxf

private theorem jsOf_isEmpty_iff (l r : Nat) (pass : PairId → Bool) (i : Nat) :
    (jsOf l r pass i).isEmpty = true ↔ ∀ j, j < r → pass (i,j) = false := by
  constructor
  · intro h j hj
    cases hpass : pass (i,j) with
    | false => rfl
    | true =>
        have hnil : jsOf l r pass i = [] := List.isEmpty_iff.mp h
        have hmem : j ∈ jsOf l r pass i := by simpa [jsOf, hj, hpass]
        simp [hnil] at hmem
  · intro h
    rw [List.isEmpty_iff, List.eq_nil_iff_forall_not_mem]
    intro j hjmem
    have hjr : j < r := by
      have := (List.mem_filter.mp hjmem).1
      simpa [jsOf] using this
    have hptrue : pass (i,j) = true := by
      have := (List.mem_filter.mp hjmem).2
      simpa [jsOf] using this
    have hpfalse := h j hjr
    simp [hpfalse] at hptrue

private theorem mem_outerBlock_some_some (l r : Nat) (pass : PairId → Bool) (i a b : Nat) :
    (some a, some b) ∈ outerBlock l r pass i ↔ a = i ∧ b < r ∧ pass (i,b) = true := by
  by_cases h : (jsOf l r pass i).isEmpty
  · have hnil : jsOf l r pass i = [] := List.isEmpty_iff.mp h
    have hno : ¬(a = i ∧ b < r ∧ pass (i,b) = true) := by
      rintro ⟨hai, hbr, hp⟩
      subst a
      have hmem : b ∈ jsOf l r pass i := by simpa [jsOf, hbr, hp]
      simp [hnil] at hmem
    simp [outerBlock, h, hnil, hno]
  · have hmem : b ∈ jsOf l r pass i ↔ b < r ∧ pass (i,b) = true := by simp [jsOf]
    rw [outerBlock, ite_eq_right h]
    constructor
    · intro hb
      rcases List.mem_map.mp hb with ⟨j, hj, hEqj⟩
      have hji : b = j := by simpa using (congrArg Prod.snd hEqj).symm
      have hai : a = i := by simpa using (congrArg Prod.fst hEqj).symm
      subst j
      subst a
      exact ⟨rfl, hmem.mp hj⟩
    · rintro ⟨hai, hbr, hp⟩
      subst a
      exact List.mem_map.mpr ⟨b, hmem.mpr ⟨hbr, hp⟩, rfl⟩

private theorem mem_outerBlock_some_none (l r : Nat) (pass : PairId → Bool) (i a : Nat) :
    (some a, none) ∈ outerBlock l r pass i ↔ a = i ∧ ∀ j, j < r → pass (i,j) = false := by
  by_cases h : (jsOf l r pass i).isEmpty
  · have hiff := (jsOf_isEmpty_iff l r pass i).mp h
    rw [outerBlock, ite_eq_left h]
    constructor
    · intro hm
      have hEq : ((some a, none) : Out) = (some i, none) := by simpa using hm
      have hai : a = i := by simpa using congrArg Prod.fst hEq
      exact ⟨hai, hiff⟩
    · rintro ⟨hai, _⟩
      subst a
      simp
  · have hno : ¬(∀ j, j < r → pass (i,j) = false) := by
      intro hn
      exact h ((jsOf_isEmpty_iff l r pass i).mpr hn)
    simp [outerBlock, h, hno]

private theorem any_perm_eq {α : Type} (p : α → Bool) {l₁ l₂ : List α} (h : l₁.Perm l₂) :
    l₁.any p = l₂.any p := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h₁
    apply List.any_eq_true.mpr
    rcases List.any_eq_true.mp h₁ with ⟨x, hx, hp⟩
    exact ⟨x, h.mem_iff.mp hx, hp⟩
  · intro h₂
    apply List.any_eq_true.mpr
    rcases List.any_eq_true.mp h₂ with ⟨x, hx, hp⟩
    exact ⟨x, h.mem_iff.mpr hx, hp⟩

private theorem finish_perm_of_perm (l r : Nat) {h₁ h₂ : List PairId} (h : h₁.Perm h₂) :
    (finish l r h₁).Perm (finish l r h₂) := by
  have hm : (h₁.map (fun p : PairId => (some p.1, some p.2))).Perm
      (h₂.map (fun p : PairId => (some p.1, some p.2))) := h.map _
  have hanyL : ∀ i, h₁.any (fun p : PairId => p.1 == i) = h₂.any (fun p : PairId => p.1 == i) := by
    intro i
    exact any_perm_eq (fun p : PairId => p.1 == i) h
  have hanyR : ∀ j, h₁.any (fun p : PairId => p.2 == j) = h₂.any (fun p : PairId => p.2 == j) := by
    intro j
    exact any_perm_eq (fun p : PairId => p.2 == j) h
  have hleftEq :
      (List.range l).filter (fun i => !h₁.any (fun p : PairId => p.1 == i)) =
        (List.range l).filter (fun i => !h₂.any (fun p : PairId => p.1 == i)) := by
    apply List.filter_congr
    intro i hi
    rw [hanyL i]
  have hrightEq :
      (List.range r).filter (fun j => !h₁.any (fun p : PairId => p.2 == j)) =
        (List.range r).filter (fun j => !h₂.any (fun p : PairId => p.2 == j)) := by
    apply List.filter_congr
    intro j hj
    rw [hanyR j]
  unfold finish
  rw [hleftEq, hrightEq]
  exact List.Perm.append_right _ (List.Perm.append_right _ hm)

private theorem mem_finish_filtered (l r : Nat) (pass : PairId → Bool) (o : Out) :
    o ∈ finish l r ((allPairs l r).filter pass) ↔
      match o with
      | (some i, some j) => i < l ∧ j < r ∧ pass (i,j) = true
      | (some i, none) => i < l ∧ ∀ j, j < r → pass (i,j) = false
      | (none, some j) => j < r ∧ ∀ i, i < l → pass (i,j) = false
      | (none, none) => False := by
  rcases o with ⟨oi, oj⟩
  cases oi with
  | none =>
      cases oj with
      | none =>
          constructor
          · intro h
            rcases List.mem_append.mp h with hml | hright
            · rcases List.mem_append.mp hml with hmatched | hleft
              · rcases List.mem_map.mp hmatched with ⟨p, hp, hEq⟩
                simp at hEq
              · rcases List.mem_map.mp hleft with ⟨i, hi, hEq⟩
                simp at hEq
            · rcases List.mem_map.mp hright with ⟨j, hj, hEq⟩
              simp at hEq
          · intro hfalse
            exact False.elim hfalse
      | some j =>
          constructor
          · intro h
            rcases List.mem_append.mp h with hml | hright
            · rcases List.mem_append.mp hml with hmatched | hleft
              · rcases List.mem_map.mp hmatched with ⟨p, hp, hEq⟩
                simp at hEq
              · rcases List.mem_map.mp hleft with ⟨i, hi, hEq⟩
                simp at hEq
            · rcases List.mem_map.mp hright with ⟨k, hk, hEq⟩
              have hkj : k = j := by simpa using congrArg Prod.snd hEq
              subst k
              have hjr : j < r := by
                have := (List.mem_filter.mp hk).1
                simpa using this
              have hnany : ((allPairs l r).filter pass).any (fun p : PairId => p.2 == j) = false := by
                simpa using (List.mem_filter.mp hk).2
              have hno := (hitsF_any_right_false l r pass j).mp hnany hjr
              exact ⟨hjr, hno⟩
          · rintro ⟨hjr, hno⟩
            apply List.mem_append.mpr
            right
            apply List.mem_map.mpr
            refine ⟨j, ?_, rfl⟩
            apply List.mem_filter.mpr
            constructor
            · simpa using hjr
            · have hnany : ((allPairs l r).filter pass).any (fun p : PairId => p.2 == j) = false :=
                (hitsF_any_right_false l r pass j).mpr (fun _ => hno)
              simpa using hnany
  | some i =>
      cases oj with
      | none =>
          constructor
          · intro h
            rcases List.mem_append.mp h with hml | hright
            · rcases List.mem_append.mp hml with hmatched | hleft
              · rcases List.mem_map.mp hmatched with ⟨p, hp, hEq⟩
                simp at hEq
              · rcases List.mem_map.mp hleft with ⟨a, ha, hEq⟩
                have hai : a = i := by simpa using congrArg Prod.fst hEq
                have hil : a < l := by
                  have := (List.mem_filter.mp ha).1
                  simpa using this
                have hnany : ((allPairs l r).filter pass).any (fun p : PairId => p.1 == a) = false := by
                  simpa using (List.mem_filter.mp ha).2
                have hno := (hitsF_any_left_false l r pass a).mp hnany hil
                subst a
                exact ⟨hil, hno⟩
            · rcases List.mem_map.mp hright with ⟨j, hj, hEq⟩
              simp at hEq
          · rintro ⟨hil, hno⟩
            apply List.mem_append.mpr
            left
            apply List.mem_append.mpr
            right
            apply List.mem_map.mpr
            refine ⟨i, ?_, rfl⟩
            apply List.mem_filter.mpr
            constructor
            · simpa using hil
            · have hnany : ((allPairs l r).filter pass).any (fun p : PairId => p.1 == i) = false :=
                (hitsF_any_left_false l r pass i).mpr (fun _ => hno)
              simpa using hnany
      | some j =>
          constructor
          · intro h
            rcases List.mem_append.mp h with hml | hright
            · rcases List.mem_append.mp hml with hmatched | hleft
              · rcases List.mem_map.mp hmatched with ⟨p, hp, hEq⟩
                have hp' := (mem_hitsF l r pass p.1 p.2).mp hp
                rcases p with ⟨a, b⟩
                have hai : a = i := by simpa using congrArg (fun o : Out => o.1) hEq
                have hbj : b = j := by simpa using congrArg (fun o : Out => o.2) hEq
                simpa [hai, hbj] using hp'
              · rcases List.mem_map.mp hleft with ⟨a, ha, hEq⟩
                simp at hEq
            · rcases List.mem_map.mp hright with ⟨b, hb, hEq⟩
              simp at hEq
          · rintro ⟨hil, hjr, hp⟩
            apply List.mem_append.mpr
            left
            apply List.mem_append.mpr
            left
            apply List.mem_map.mpr
            exact ⟨(i,j), (mem_hitsF l r pass i j).mpr ⟨hil, hjr, hp⟩, rfl⟩

private theorem mem_outer (l r : Nat) (pass : PairId → Bool) (o : Out) :
    o ∈ outerRef l r pass ↔
      match o with
      | (some i, some j) => i < l ∧ j < r ∧ pass (i,j) = true
      | (some i, none) => i < l ∧ ∀ j, j < r → pass (i,j) = false
      | (none, some j) => j < r ∧ ∀ i, i < l → pass (i,j) = false
      | (none, none) => False := by
  rcases o with ⟨oi, oj⟩
  cases oi with
  | none =>
      cases oj with
      | none =>
          constructor
          · intro h
            rcases List.mem_append.mp h with hfirst | hright
            · rcases List.mem_flatMap.mp hfirst with ⟨i, hi, hblock⟩
              have hf := mem_outerBlock_fst l r pass i hblock
              simp at hf
            · rcases List.mem_map.mp hright with ⟨j, hj, hEq⟩
              simp at hEq
          · intro hfalse
            exact False.elim hfalse
      | some j =>
          constructor
          · intro h
            rcases List.mem_append.mp h with hfirst | hright
            · rcases List.mem_flatMap.mp hfirst with ⟨i, hi, hblock⟩
              have hf := mem_outerBlock_fst l r pass i hblock
              simp at hf
            · rcases List.mem_map.mp hright with ⟨k, hk, hEq⟩
              have hkj : k = j := by simpa using congrArg Prod.snd hEq
              subst k
              have hjr : j < r := by
                have := (List.mem_filter.mp hk).1
                simpa using this
              have hanyfalse : (List.range l).any (fun i => pass (i,j)) = false := by
                simpa using (List.mem_filter.mp hk).2
              have hno : ∀ i, i < l → pass (i,j) = false := by
                intro i hil
                have hnot := (List.any_eq_false.mp hanyfalse) i (by simpa using hil)
                cases hpass : pass (i,j) <;> simp_all
              exact ⟨hjr, hno⟩
          · rintro ⟨hjr, hno⟩
            apply List.mem_append.mpr
            right
            apply List.mem_map.mpr
            refine ⟨j, ?_, rfl⟩
            apply List.mem_filter.mpr
            constructor
            · simpa using hjr
            · have hanyfalse : (List.range l).any (fun i => pass (i,j)) = false := by
                apply List.any_eq_false.mpr
                intro i hilmem
                have hil : i < l := by simpa using hilmem
                have hpfalse := hno i hil
                simp [hpfalse]
              simpa using hanyfalse
  | some i =>
      cases oj with
      | none =>
          constructor
          · intro h
            rcases List.mem_append.mp h with hfirst | hright
            · rcases List.mem_flatMap.mp hfirst with ⟨a, ha, hab⟩
              have hal : a < l := by simpa using ha
              have hb := (mem_outerBlock_some_none l r pass a i).mp hab
              have hia : i = a := by omega
              exact ⟨by omega, by simpa [hia] using hb.2⟩
            · rcases List.mem_map.mp hright with ⟨b, hb, hEq⟩
              simp at hEq
          · rintro ⟨hil, hno⟩
            apply List.mem_append.mpr
            left
            apply List.mem_flatMap.mpr
            exact ⟨i, by simpa using hil, (mem_outerBlock_some_none l r pass i i).mpr ⟨rfl, hno⟩⟩
      | some j =>
          constructor
          · intro h
            rcases List.mem_append.mp h with hfirst | hright
            · rcases List.mem_flatMap.mp hfirst with ⟨a, ha, hab⟩
              have hb := (mem_outerBlock_some_some l r pass a i j).mp hab
              have hia : i = a := by omega
              exact ⟨by simpa [hia] using ha, by simpa [hia] using hb.2.1, by simpa [hia] using hb.2.2⟩
            · rcases List.mem_map.mp hright with ⟨b, hb, hEq⟩
              simp at hEq
          · rintro ⟨hil, hjr, hp⟩
            apply List.mem_append.mpr
            left
            apply List.mem_flatMap.mpr
            exact ⟨i, by simpa using hil, (mem_outerBlock_some_some l r pass i i j).mpr ⟨rfl, hjr, hp⟩⟩

private theorem finish_filtered_nodup (l r : Nat) (pass : PairId → Bool) :
    (finish l r ((allPairs l r).filter pass)).Nodup := by
  exact finish_nodup l r (List.filter_sublist.nodup (allPairs_nodup l r))

private theorem finish_filtered_perm_outer (l r : Nat) (pass : PairId → Bool) :
    (finish l r ((allPairs l r).filter pass)).Perm (outerRef l r pass) := by
  apply nodup_perm_ext _ _ (finish_filtered_nodup l r pass) (outer_nodup l r pass)
  intro o
  exact (mem_finish_filtered l r pass o).trans (mem_outer l r pass o).symm

theorem sound : Goal := by
  intro l r width order pass hw horder
  let hits := order.flatMap fun t => (tile l r width t).filter pass
  let canon := (List.range ((l*r+width-1)/width)).flatMap (fun t => tile l r width t)
  have hfilterFlat : ∀ xs : List Nat,
      (xs.flatMap (fun t => tile l r width t)).filter pass =
        xs.flatMap (fun t => (tile l r width t).filter pass) := by
    intro xs
    induction xs with
    | nil => simp
    | cons t ts ih => simp [List.flatMap_cons, ih]
  have horderFlat : hits.Perm (canon.filter pass) := by
    dsimp [hits, canon]
    have hf := perm_flatMap (fun t => tile l r width t) horder
    simpa [hfilterFlat order] using (List.Perm.filter pass hf)
  have hcanon : canon.Perm (allPairs l r) := by
    dsimp [canon]
    exact (flatMap_tiles_perm_ordPairs l r width hw).trans (ordPairs_perm_allPairs l r)
  have hhits : hits.Perm ((allPairs l r).filter pass) := horderFlat.trans (List.Perm.filter pass hcanon)
  change (finish l r hits).Perm (outerRef l r pass)
  exact (finish_perm_of_perm l r hhits).trans (finish_filtered_perm_outer l r pass)

end TinylambOpt.ResearchProofs.EngineDecisions.Goal_HotRunTiles
