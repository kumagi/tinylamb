import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `ThresholdViews.ReplayLedger` — `∀ old p k, indicator (active old k) + weight
  (changes old p) k = indicator (active (old++p) k)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ThresholdViews_ReplayLedger

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews

/-! `ThresholdViews.ReplayLedger` claims that a stored signed delta for a key
exactly repairs the old indicator into the new one.  This is true: each key
touched by the patch contributes exactly one signed change, and untouched
keys keep their weight (hence their active bit). -/

private def elems : List (Nat × Int) :=
  [(0,-2),(0,-1),(0,0),(0,1),(0,2),(1,-1),(1,1)]

private def bagsUpTo : Nat → List Bag
  | 0 => [[]]
  | n+1 => bagsUpTo n ++ (bagsUpTo n).flatMap (fun xs => elems.map (fun e => e :: xs))

private def hcheck (old p : Bag) (k : Nat) : Bool :=
  decide (indicator (active old k) + weight (changes old p) k =
    indicator (active (old ++ p) k))

private def checkAll : Bool :=
  (bagsUpTo 2).all (fun old => (bagsUpTo 2).all (fun p =>
    [0,1,2].all (fun k => hcheck old p k)))

#guard checkAll

private theorem mem_keys (xs : Bag) (k : Nat) : k ∈ keys xs ↔ k ∈ xs.map Prod.fst := by
  simp [keys]

private theorem weight_append (xs ys : Bag) (k : Nat) :
    weight (xs ++ ys) k = weight xs k + weight ys k := by
  simp [weight]

private theorem weight_of_not_mem (xs : Bag) (k : Nat) (h : k ∉ xs.map Prod.fst) :
    weight xs k = 0 := by
  induction xs with
  | nil => simp [weight]
  | cons r rs ih =>
      simp only [List.map_cons, List.mem_cons] at h
      have hrk : k ≠ r.1 := by intro he; exact h (Or.inl he)
      have hrs : k ∉ rs.map Prod.fst := by intro hk; exact h (Or.inr hk)
      have hr : r.1 ≠ k := by intro he; exact hrk he.symm
      have ih' : ((rs.filter (fun r => r.1 == k)).map Prod.snd).sum = 0 := by
        simpa [weight] using ih hrs
      simp [weight, hr, ih']

private theorem weight_of_not_keys (xs : Bag) (k : Nat) (h : k ∉ keys xs) :
    weight xs k = 0 := by
  exact weight_of_not_mem xs k (by simpa using (not_congr (mem_keys xs k)).mp h)

private theorem active_append_of_weight_zero (xs ys : Bag) (k : Nat)
    (h : weight ys k = 0) : active (xs ++ ys) k = active xs k := by
  simp [active, weight_append, h]

private def delta (old patch : Bag) (k : Nat) : Int :=
  indicator (active (old ++ patch) k) - indicator (active old k)

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  refine Nat.strongRecOn (motive := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup)
    l.length ?_ l rfl
  intro n ih l hl
  cases l with
  | nil => simp
  | cons a as =>
      rw [List.eraseDups_cons]
      rw [List.nodup_cons]
      constructor
      · intro hm
        have hfil : a ∈ (as.filter fun b => !(b == a)) := List.mem_eraseDups.mp hm
        have hc := (List.mem_filter.mp hfil).2
        simp at hc
      · have hlt : (as.filter fun b => !(b == a)).length < n := by
          rw [← hl]
          exact Nat.lt_succ_of_le (List.length_filter_le _ as)
        exact ih _ hlt (as.filter fun b => !(b == a)) rfl

private theorem weight_map_nodup (f : Nat → Int) (ks : List Nat) (hnd : ks.Nodup) (k : Nat) :
    (((ks.map (fun k => (k, f k))).filter (fun r => r.1 == k)).map Prod.snd).sum =
      if k ∈ ks then f k else 0 := by
  induction ks with
  | nil => simp
  | cons a as ih =>
      have hnot : a ∉ as := (List.nodup_cons.mp hnd).1
      have hndas : as.Nodup := (List.nodup_cons.mp hnd).2
      simp only [List.map_cons, List.filter_cons, List.mem_cons]
      by_cases hak : a = k
      · subst a
        have hknot : k ∉ as := by simpa using hnot
        simp [hknot, ih hndas]
      · have hbe : (a == k) = false := by simp [hak]
        have hkne : k ≠ a := by intro he; exact hak he.symm
        simp [hbe, hkne, ih hndas]

private theorem weight_changes (old p : Bag) (k : Nat) :
    weight (changes old p) k = if k ∈ keys p then delta old p k else 0 := by
  unfold changes weight keys
  exact weight_map_nodup (delta old p) ((p.map Prod.fst).eraseDups) (nodup_eraseDups _) k

theorem sound_ThresholdViews_ReplayLedger :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.ReplayLedger.Goal := by
  intro old p k
  rw [weight_changes]
  by_cases hk : k ∈ keys p
  · rw [ite_eq_left hk]
    dsimp [delta]
    omega
  · rw [ite_eq_right hk]
    have hw : weight p k = 0 := weight_of_not_keys p k hk
    rw [active_append_of_weight_zero old p k hw]
    omega

end TinylambOpt.ResearchProofs.Atlas.Goal_ThresholdViews_ReplayLedger
