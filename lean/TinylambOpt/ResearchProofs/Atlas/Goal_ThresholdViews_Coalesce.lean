import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `ThresholdViews.Coalesce` — `∀ old a b k, weight (changes old (a++b)) k = weight
  (changes old a ++ changes (old++a) b) k`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ThresholdViews_Coalesce

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews

/-!
`ThresholdViews.Coalesce` says that publishing two correction batches in one
step emits the same signed `COUNT DISTINCT` delta as publishing them one after
another.  The statement is true: for each key the emitted delta telescopes,
and a key absent from a batch does not change its active status.
-/

private def genBag : Nat → List Bag
  | 0 => [[]]
  | n + 1 => (genBag n).flatMap (fun xs =>
      [[], [(0, 1)], [(0, -1)], [(1, 1)], [(1, -1)], [(2, 1)]].map (fun ys => ys ++ xs))

private def bruteCoalesce : Bool :=
  let bs := genBag 2
  bs.all (fun old => bs.all (fun a => bs.all (fun b =>
    (List.range 3).all (fun k =>
      weight (changes old (a ++ b)) k == weight (changes old a ++ changes (old ++ a) b) k))))

#guard bruteCoalesce

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

private theorem delta_telescope (old a b : Bag) (k : Nat) :
    delta old a k + delta (old ++ a) b k = delta old (a ++ b) k := by
  dsimp [delta]
  rw [List.append_assoc]
  omega

private theorem delta_of_weight_zero_left (old a b : Bag) (k : Nat)
    (h : weight a k = 0) : delta old (a ++ b) k = delta (old ++ a) b k := by
  dsimp [delta]
  rw [List.append_assoc]
  have hw : weight (old ++ (a ++ b)) k = weight ((old ++ a) ++ b) k := by
    simp [weight_append, h, List.append_assoc]
  have ha : active (old ++ (a ++ b)) k = active ((old ++ a) ++ b) k := by
    simp [active, hw]
  have hb : active (old ++ a) k = active old k := active_append_of_weight_zero old a k h
  rw [ha, hb]

private theorem delta_of_weight_zero_right (old a b : Bag) (k : Nat)
    (h : weight b k = 0) : delta old (a ++ b) k = delta old a k := by
  dsimp [delta]
  have hw : weight (old ++ (a ++ b)) k = weight (old ++ a) k := by
    simp [weight_append, h]
  have ha : active (old ++ (a ++ b)) k = active (old ++ a) k := by
    simp [active, hw]
  rw [ha]

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

private theorem keys_append_mem (a b : Bag) (k : Nat) :
    k ∈ keys (a ++ b) ↔ k ∈ keys a ∨ k ∈ keys b := by
  simp [keys]

theorem sound_ThresholdViews_Coalesce : ThresholdViews.Coalesce.Goal := by
  intro old a b k
  rw [weight_changes old (a ++ b) k]
  rw [weight_append]
  rw [weight_changes old a k]
  rw [weight_changes (old ++ a) b k]
  by_cases ha : k ∈ keys a
  · have hab : k ∈ keys (a ++ b) := (keys_append_mem a b k).mpr (Or.inl ha)
    by_cases hb : k ∈ keys b
    · simp [hab, ha, hb]
      exact (delta_telescope old a b k).symm
    · simp [hab, ha, hb]
      exact delta_of_weight_zero_right old a b k (weight_of_not_keys b k hb)
  · by_cases hb : k ∈ keys b
    · have hab : k ∈ keys (a ++ b) := (keys_append_mem a b k).mpr (Or.inr hb)
      simp [hab, ha, hb]
      exact delta_of_weight_zero_left old a b k (weight_of_not_keys a k ha)
    · have hab : k ∉ keys (a ++ b) := by
        intro hc
        exact ha (((keys_append_mem a b k).mp hc).resolve_right hb)
      simp [hab, ha, hb]

end TinylambOpt.ResearchProofs.Atlas.Goal_ThresholdViews_Coalesce
