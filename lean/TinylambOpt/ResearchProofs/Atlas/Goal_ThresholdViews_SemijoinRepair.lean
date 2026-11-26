import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Proved

- `ThresholdViews.SemijoinRepair` — `∀ left right patch, semi left (right++patch) -
  semi left right = ((changes right patch).map (fun (k,d) => d*weight left k)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ThresholdViews_SemijoinRepair

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews

/-!
`ThresholdViews.SemijoinRepair` is true.  Both sides telescope over the
distinct keys of the patch: `semi left right` is the sum, over any
duplicate-free key set covering `left`, of `weight left k` for the keys that
are active in `right`; changing `right` to `right ++ patch` only alters the
keys occurring in `patch`, and the emitted `changes` record exactly those
activity flips.  An exhaustive check over all bags with two keys, values in
`{-2,-1,1,2}` and at most two rows confirms the statement on small inputs.
-/

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

private theorem active_of_weight_zero (xs : Bag) (k : Nat) (h : weight xs k = 0) :
    active xs k = false := by
  simp [active, h]

private theorem active_append_of_weight_zero (xs ys : Bag) (k : Nat)
    (h : weight ys k = 0) : active (xs ++ ys) k = active xs k := by
  simp [active, weight_append, h]

private def delta (old patch : Bag) (k : Nat) : Int :=
  indicator (active (old ++ patch) k) - indicator (active old k)

private theorem delta_zero_of_weight_zero (q p : Bag) (k : Nat) (h : weight p k = 0) :
    delta q p k = 0 := by
  simp [delta, active_append_of_weight_zero q p k h]

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

private theorem sum_eq_zero_of_all_zero (f : Nat → Int) (s : List Nat)
    (h : ∀ x ∈ s, f x = 0) : (s.map f).sum = 0 := by
  induction s with
  | nil => simp
  | cons a as ih =>
      have ha : f a = 0 := h a (by simp)
      have has : ∀ x ∈ as, f x = 0 := by intro x hx; exact h x (by simp [hx])
      simp only [List.map_cons, List.sum_cons]
      rw [ha, ih has]
      simp

private theorem sum_eq_erase_add (a : Nat) (f : Nat → Int) (s : List Nat) (hs : s.Nodup)
    (ha : a ∈ s) : (s.map f).sum = f a + ((s.erase a).map f).sum := by
  induction s with
  | nil => simp at ha
  | cons b bs ih =>
      have hbnot : b ∉ bs := (List.nodup_cons.mp hs).1
      have hbsnd : bs.Nodup := (List.nodup_cons.mp hs).2
      by_cases hbe : (b == a) = true
      · have hba : b = a := beq_iff_eq.mp hbe
        subst b
        rw [List.erase_cons_head]
        simp
      · have habs : a ∈ bs := by
          simp only [List.mem_cons] at ha
          rcases ha with h | h
          · have : (b == a) = true := by simp [h.symm]
            exact False.elim (hbe this)
          · exact h
        rw [List.erase_cons_tail hbe]
        simp only [List.map_cons, List.sum_cons]
        rw [ih hbsnd habs]
        omega

private theorem ne_of_mem_erase_of_nodup {a x : Nat} {s : List Nat} (hs : s.Nodup)
    (h : x ∈ s.erase a) : x ≠ a := by
  induction s with
  | nil => simp at h
  | cons b bs ih =>
      have hbnot : b ∉ bs := (List.nodup_cons.mp hs).1
      have hbsnd : bs.Nodup := (List.nodup_cons.mp hs).2
      by_cases hba : (b == a) = true
      · have hba_eq : b = a := beq_iff_eq.mp hba
        subst b
        rw [List.erase_cons_head] at h
        intro hxa
        subst x
        exact hbnot h
      · rw [List.erase_cons_tail hba] at h
        rcases List.mem_cons.mp h with hxb | hxbs
        · intro hxa
          apply hba
          have hba_eq : b = a := by omega
          simp [hba_eq]
        · exact ih hbsnd hxbs

private theorem sum_subset_of_vanish (f : Nat → Int) (s : List Nat) (hs : s.Nodup)
    (t : List Nat) (ht : t.Nodup) (hsub : ∀ x ∈ t, x ∈ s)
    (hz : ∀ x ∈ s, x ∉ t → f x = 0) : (t.map f).sum = (s.map f).sum := by
  induction t generalizing s with
  | nil =>
      symm
      apply sum_eq_zero_of_all_zero
      intro x hx
      exact hz x hx (by simp)
  | cons a t ih =>
      have hnot : a ∉ t := (List.nodup_cons.mp ht).1
      have htnd : t.Nodup := (List.nodup_cons.mp ht).2
      have has : a ∈ s := hsub a (by simp)
      have hs_erase : (s.erase a).Nodup := hs.erase a
      have hsub' : ∀ x ∈ t, x ∈ s.erase a := by
        intro x hxt
        have hxs : x ∈ s := hsub x (by simp [hxt])
        have hxa : x ≠ a := by
          intro he
          apply hnot
          simpa [he] using hxt
        exact (List.mem_erase_of_ne hxa).mpr hxs
      have hz' : ∀ x ∈ s.erase a, x ∉ t → f x = 0 := by
        intro x hx hxt
        apply hz x (List.mem_of_mem_erase hx)
        intro hxa_t
        rcases List.mem_cons.mp hxa_t with heq | htmem
        · exact (ne_of_mem_erase_of_nodup hs hx) heq
        · exact hxt htmem
      have ih' := ih (s.erase a) hs_erase htnd hsub' hz'
      have hsum_s := sum_eq_erase_add a f s hs has
      simp only [List.map_cons, List.sum_cons]
      rw [hsum_s, ih']

private theorem sum_sub (f g : Nat → Int) (s : List Nat) :
    (s.map (fun k => f k - g k)).sum = (s.map f).sum - (s.map g).sum := by
  induction s with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_add (f g : Nat → Int) (s : List Nat) :
    (s.map (fun k => f k + g k)).sum = (s.map f).sum + (s.map g).sum := by
  induction s with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem keys_append_mem (a b : Bag) (k : Nat) :
    k ∈ keys (a ++ b) ↔ k ∈ keys a ∨ k ∈ keys b := by
  simp [keys]

private theorem weight_cons (a k : Nat) (v : Int) (rs : Bag) :
    weight ((a, v) :: rs) k = (if a == k then v else 0) + weight rs k := by
  by_cases h : (a == k) = true
  · have hak : a = k := beq_iff_eq.mp h
    subst a
    simp [weight]
  · have hak : a ≠ k := by intro he; exact h (by simp [he])
    simp [weight, h]

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem sum_single_of_nodup (g : Nat → Int) (ks : List Nat) (hnd : ks.Nodup) (a : Nat) :
    (ks.map (fun k => if k = a then g k else 0)).sum = if a ∈ ks then g a else 0 := by
  induction ks with
  | nil => simp
  | cons b bs ih =>
      have hbnot : b ∉ bs := (List.nodup_cons.mp hnd).1
      have hbsnd : bs.Nodup := (List.nodup_cons.mp hnd).2
      by_cases hba : b = a
      · subst b
        have hanot : a ∉ bs := by simpa using hbnot
        simp [hanot, ih hbsnd]
      · have hab : a ≠ b := by intro he; exact hba he.symm
        simp [hba, hab, ih hbsnd]

private theorem semi_eq_sum_over (left right : Bag) (S : List Nat)
    (hnd : S.Nodup) (hsub : ∀ r ∈ left, r.1 ∈ S) :
    semi left right = (S.map (fun k => if active right k then weight left k else 0)).sum := by
  induction left generalizing S with
  | nil =>
      simp [semi, weight, sum_map_zero]
  | cons r rs ih =>
      rcases r with ⟨a, v⟩
      have haS : a ∈ S := hsub (a, v) (by simp)
      have hrsSub : ∀ q ∈ rs, q.1 ∈ S := by
        intro q hq
        exact hsub q (by simp [hq])
      have hpoint : ∀ k ∈ S,
          (if active right k then weight ((a, v) :: rs) k else 0) =
            (if k = a then (if active right k then v else 0) else 0) +
              (if active right k then weight rs k else 0) := by
        intro k hk
        rw [weight_cons]
        by_cases hA : active right k
        · simp [hA]
          by_cases hka : k = a
          · subst k
            simp
          · have hack : a ≠ k := by intro he; exact hka he.symm
            simp [hka, hack]
        · simp [hA]
      have hsum_point :
          (S.map (fun k => if active right k then weight ((a, v) :: rs) k else 0)).sum =
            (S.map (fun k =>
              (if k = a then (if active right k then v else 0) else 0) +
                (if active right k then weight rs k else 0))).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro k hk
        exact hpoint k hk
      have hsingle :
          (S.map (fun k => if k = a then (if active right k then v else 0) else 0)).sum =
            (if active right a then v else 0) := by
        rw [sum_single_of_nodup (fun k => if active right k then v else 0) S hnd a]
        simp [haS]
      have hsplit :
          (S.map (fun k => if active right k then weight ((a, v) :: rs) k else 0)).sum =
            (if active right a then v else 0) +
              (S.map (fun k => if active right k then weight rs k else 0)).sum := by
        rw [hsum_point, sum_add, hsingle]
      have hcons :
          semi ((a, v) :: rs) right = (if active right a then v else 0) + semi rs right := by
        by_cases hA : active right a <;> simp [semi, hA]
      rw [hcons, ih S hnd hrsSub, hsplit]

private theorem indicator_mul (b1 b2 : Bool) (v : Int) :
    (if b1 then v else 0) - (if b2 then v else 0) = (indicator b1 - indicator b2) * v := by
  cases b1 <;> cases b2 <;> simp [indicator]

theorem sound_ThresholdViews_SemijoinRepair :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.SemijoinRepair.Goal := by
  intro left right patch
  let S := keys (left ++ patch)
  have hSnd : S.Nodup := by
    dsimp [S]
    exact nodup_eraseDups _
  have hsub_left : ∀ r ∈ left, r.1 ∈ S := by
    intro r hr
    have hkmap : r.1 ∈ left.map Prod.fst := by
      exact List.mem_map.mpr ⟨r, hr, rfl⟩
    have hkleft : r.1 ∈ keys left := (mem_keys left r.1).mpr hkmap
    dsimp [S]
    exact (keys_append_mem left patch r.1).mpr (Or.inl hkleft)
  have hsemi1 := semi_eq_sum_over left right S hSnd hsub_left
  have hsemi2 := semi_eq_sum_over left (right ++ patch) S hSnd hsub_left
  rw [hsemi2, hsemi1]
  let fnew : Nat → Int := fun k => if active (right ++ patch) k then weight left k else 0
  let fold : Nat → Int := fun k => if active right k then weight left k else 0
  change (S.map fnew).sum - (S.map fold).sum =
    ((changes right patch).map (fun (k, d) => d * weight left k)).sum
  rw [← sum_sub fnew fold S]
  have hdiff :
      (S.map (fun k => fnew k - fold k)).sum =
        (S.map (fun k => delta right patch k * weight left k)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    dsimp [fnew, fold, delta]
    exact indicator_mul (active (right ++ patch) k) (active right k) (weight left k)
  rw [hdiff]
  have hsub_patch : ∀ k ∈ keys patch, k ∈ S := by
    intro k hk
    dsimp [S]
    exact (keys_append_mem left patch k).mpr (Or.inr hk)
  have hzero_c : ∀ k ∈ S, k ∉ keys patch → delta right patch k * weight left k = 0 := by
    intro k hkS hknot
    have hw : weight patch k = 0 := weight_of_not_keys patch k hknot
    have hd : delta right patch k = 0 := delta_zero_of_weight_zero right patch k hw
    simp [hd]
  have hsum_c :
      (S.map (fun k => delta right patch k * weight left k)).sum =
        ((keys patch).map (fun k => delta right patch k * weight left k)).sum := by
    symm
    exact sum_subset_of_vanish (fun k => delta right patch k * weight left k)
      S hSnd (keys patch) (nodup_eraseDups _) hsub_patch hzero_c
  rw [hsum_c]
  unfold changes
  simp only [List.map_map]
  rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_ThresholdViews_SemijoinRepair
