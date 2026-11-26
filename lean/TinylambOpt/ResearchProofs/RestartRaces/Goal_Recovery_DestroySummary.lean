import TinylambOpt.ResearchGoals.RestartRaces.Recovery
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Recovery_DestroySummary

open TinylambOpt.ResearchGoals.RestartRaces.Recovery.DestroySummary

private def maxD (l : List Nat) : Nat := (l.max?).getD 0

private theorem maxD_eq_foldl (l : List Nat) : maxD l = l.foldl max 0 := by
  cases l with
  | nil => simp [maxD, List.max?]
  | cons a as => simp [maxD, List.max?, Nat.max_eq_right (Nat.zero_le a)]

private theorem init_le_foldl_max (l : List Nat) (c : Nat) : c ≤ l.foldl max c := by
  induction l generalizing c with
  | nil => simp
  | cons a l ih =>
      simp only [List.foldl_cons]
      exact Nat.le_trans (Nat.le_max_left c a) (ih (max c a))

private theorem mem_le_foldl_max (l : List Nat) (c x : Nat) (h : x ∈ l) : x ≤ l.foldl max c := by
  induction l generalizing c x with
  | nil => simp at h
  | cons a l ih =>
      simp only [List.mem_cons] at h
      simp only [List.foldl_cons]
      rcases h with hxa | hx
      · subst x
        exact Nat.le_trans (Nat.le_max_right c a) (init_le_foldl_max l (max c a))
      · exact ih (max c a) x hx

private theorem foldl_max_le (l : List Nat) (c t : Nat) (h : ∀ x ∈ l, x ≤ t) (hc : c ≤ t) :
    l.foldl max c ≤ t := by
  induction l generalizing c with
  | nil => exact hc
  | cons a l ih =>
      simp only [List.foldl_cons]
      exact ih (max c a) (fun x hx => h x (by simp [hx]))
        (Nat.max_le.mpr ⟨hc, h a (by simp)⟩)

private theorem mem_le_maxD (l : List Nat) (x : Nat) (h : x ∈ l) : x ≤ maxD l := by
  rw [maxD_eq_foldl]
  exact mem_le_foldl_max l 0 x h

private theorem maxD_le (l : List Nat) (t : Nat) (h : ∀ x ∈ l, x ≤ t) : maxD l ≤ t := by
  rw [maxD_eq_foldl]
  exact foldl_max_le l 0 t h (Nat.zero_le t)

private def ownersOf (rs : List Rec) : List Nat :=
  rs.filterMap fun r => match r.op with | .commit => some r.owner | _ => none

private def destroysOf (rs : List Rec) (owner page : Nat) : List Nat :=
  rs.filterMap fun r => match r.op with
    | .destroy p => if r.owner == owner && p == page then some r.lsn else none
    | _ => none

private def committedDestroys (rs : List Rec) (commits : List Nat) (page : Nat) : List Nat :=
  rs.filterMap fun r => match r.op with
    | .destroy p => if p == page && commits.contains r.owner then some r.lsn else none
    | _ => none

private theorem ownersOf_append (a b : List Rec) : ownersOf (a ++ b) = ownersOf a ++ ownersOf b := by
  simp [ownersOf, List.filterMap_append]

private theorem destroysOf_append (a b : List Rec) (o p : Nat) :
    destroysOf (a ++ b) o p = destroysOf a o p ++ destroysOf b o p := by
  simp [destroysOf, List.filterMap_append]

private theorem committedDestroys_append (a b : List Rec) (cs : List Nat) (p : Nat) :
    committedDestroys (a ++ b) cs p = committedDestroys a cs p ++ committedDestroys b cs p := by
  simp [committedDestroys, List.filterMap_append]

private theorem ownersOf_flatten (chunks : List (List Rec)) :
    ownersOf chunks.flatten = chunks.flatMap ownersOf := by
  induction chunks with
  | nil => simp [ownersOf]
  | cons c cs ih =>
      simp [List.flatten_cons, List.flatMap_cons, ownersOf_append, ih]

private theorem destroysOf_flatten (chunks : List (List Rec)) (o p : Nat) :
    destroysOf chunks.flatten o p = chunks.flatMap (fun seg => destroysOf seg o p) := by
  induction chunks with
  | nil => simp [destroysOf]
  | cons c cs ih =>
      simp [List.flatten_cons, List.flatMap_cons, destroysOf_append, ih]

private theorem committedDestroys_flatten (chunks : List (List Rec)) (cs : List Nat) (p : Nat) :
    committedDestroys chunks.flatten cs p = chunks.flatMap (fun seg => committedDestroys seg cs p) := by
  induction chunks with
  | nil => simp [committedDestroys]
  | cons c cs ih =>
      simp [List.flatten_cons, List.flatMap_cons, committedDestroys_append, ih]

private theorem foldl_max_map_maxD (chunks : List (List Nat)) :
    maxD (chunks.map maxD) = maxD chunks.flatten := by
  -- prove by le_antisymm via membership
  apply Nat.le_antisymm
  · apply maxD_le
    intro x hx
    obtain ⟨seg, hseg, rfl⟩ := List.mem_map.mp hx
    apply maxD_le
    intro y hy
    apply mem_le_maxD chunks.flatten
    rw [List.mem_flatten]
    exact ⟨seg, hseg, by simpa using hy⟩
  · apply maxD_le
    intro x hx
    rw [List.mem_flatten] at hx
    obtain ⟨seg, hseg, hxseg⟩ := hx
    exact Nat.le_trans (mem_le_maxD seg x hxseg)
      (mem_le_maxD (chunks.map maxD) (maxD seg) (List.mem_map_of_mem hseg))

private theorem maxD_flatMap {α : Type} (xs : List α) (f : α → List Nat) :
    maxD (xs.map (fun a => maxD (f a))) = maxD (xs.flatMap f) := by
  apply Nat.le_antisymm
  · apply maxD_le
    intro x hx
    obtain ⟨a, ha, rfl⟩ := List.mem_map.mp hx
    apply maxD_le
    intro y hy
    apply mem_le_maxD (xs.flatMap f)
    rw [List.mem_flatMap]
    exact ⟨a, ha, hy⟩
  · apply maxD_le
    intro x hx
    rw [List.mem_flatMap] at hx
    obtain ⟨a, ha, hxa⟩ := hx
    exact Nat.le_trans (mem_le_maxD (f a) x hxa)
      (mem_le_maxD (xs.map (fun a => maxD (f a))) (maxD (f a)) (List.mem_map_of_mem ha))

private theorem maxD_committed (rs : List Rec) (commits : List Nat) (page : Nat) :
    maxD (commits.map fun id => maxD (destroysOf rs id page)) =
      maxD (committedDestroys rs commits page) := by
  apply Nat.le_antisymm
  · apply maxD_le
    intro x hx
    obtain ⟨id, hid, rfl⟩ := List.mem_map.mp hx
    apply maxD_le
    intro y hy
    rw [destroysOf, List.mem_filterMap] at hy
    obtain ⟨r, hr, hry⟩ := hy
    cases hrOp : r.op with
    | destroy p =>
        have hry' : (r.owner = id ∧ p = page) ∧ r.lsn = y := by simpa [hrOp] using hry
        have hc : r.owner ∈ commits := by simpa [hry'.1.1] using hid
        have hrhs : y ∈ committedDestroys rs commits page := by
          rw [committedDestroys, List.mem_filterMap]
          refine ⟨r, hr, ?_⟩
          simp [hrOp, hry'.1.1, hry'.1.2, hry'.2, hc, hid]
        exact mem_le_maxD (committedDestroys rs commits page) y hrhs
    | write => simp [hrOp] at hry
    | commit => simp [hrOp] at hry
  · apply maxD_le
    intro x hx
    rw [committedDestroys, List.mem_filterMap] at hx
    obtain ⟨r, hr, hry⟩ := hx
    cases hrOp : r.op with
    | destroy p =>
        have hry' : (p = page ∧ r.owner ∈ commits) ∧ r.lsn = x := by simpa [hrOp] using hry
        have hxown : x ∈ destroysOf rs r.owner page := by
          rw [destroysOf, List.mem_filterMap]
          refine ⟨r, hr, ?_⟩
          simp [hrOp, hry'.1.1, hry'.2]
        exact Nat.le_trans (mem_le_maxD (destroysOf rs r.owner page) x hxown)
          (mem_le_maxD (commits.map fun id => maxD (destroysOf rs id page))
            (maxD (destroysOf rs r.owner page)) (List.mem_map_of_mem hry'.1.2))
    | write => simp [hrOp] at hry
    | commit => simp [hrOp] at hry

theorem sound : Goal := by
  intro chunks page
  let rs := chunks.flatten
  let commits := chunks.flatMap ownersOf
  have hfirst : (chunks.map high).max?.getD 0 = high rs := by
    change maxD (chunks.map (fun seg => maxD (seg.map Rec.owner))) = maxD (rs.map Rec.owner)
    rw [maxD_flatMap chunks (fun seg => seg.map Rec.owner)]
    have hflat : chunks.flatMap (fun seg => seg.map Rec.owner) = rs.map Rec.owner := by
      subst rs
      induction chunks with
      | nil => simp
      | cons c cs ih => simp [List.flatten_cons, List.flatMap_cons, ih]
    rw [hflat]
  have hsecond : ((commits.map fun id => (chunks.map (fun seg => floorAt seg id page)).max?.getD 0).max?.getD 0) =
      ((rs.filterMap fun r => match r.op with
        | .destroy p => if p == page && (owners rs).contains r.owner then some r.lsn else none
        | _ => none).max?).getD 0 := by
    have hfloor : ∀ id, (chunks.map (fun seg => floorAt seg id page)).max?.getD 0 = maxD (destroysOf rs id page) := by
      intro id
      change maxD (chunks.map (fun seg => maxD (destroysOf seg id page))) = maxD (destroysOf rs id page)
      rw [maxD_flatMap chunks (fun seg => destroysOf seg id page)]
      rw [destroysOf_flatten]
    have hcom : commits = owners rs := by
      change chunks.flatMap ownersOf = ownersOf chunks.flatten
      rw [ownersOf_flatten]
    rw [show (commits.map fun id => (chunks.map (fun seg => floorAt seg id page)).max?.getD 0) =
        commits.map (fun id => maxD (destroysOf rs id page)) by
          apply List.map_congr_left
          intro id hid
          exact hfloor id]
    rw [hcom]
    change maxD (owners rs |>.map fun id => maxD (destroysOf rs id page)) =
      maxD (committedDestroys rs (owners rs) page)
    exact maxD_committed rs (owners rs) page
  exact Prod.ext hfirst hsecond

end TinylambOpt.ResearchProofs.RestartRaces.Recovery_DestroySummary
