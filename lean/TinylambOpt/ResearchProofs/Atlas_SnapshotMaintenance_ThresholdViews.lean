import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

namespace TinylambOpt.ResearchProofs.Atlas.SnapshotMaintenance.ThresholdViews

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews

set_option maxHeartbeats 0

private theorem countP_eq_sum_indicator {α : Type} (p : α → Bool) (ks : List α) :
    (ks.countP p : Int) = ((ks.map (fun k => indicator (p k))).sum) := by
  induction ks with
  | nil => simp [indicator]
  | cons a as ih =>
      rw [List.countP_cons]
      simp only [List.map_cons, List.sum_cons]
      by_cases h : p a = true
      · simp [h, indicator, ih]
        omega
      · simp [h, indicator, ih]

private theorem count_eq_sum_indicator (xs : Bag) :
    (count xs : Int) = ((keys xs).map (fun k => indicator (active xs k))).sum := by
  unfold count
  exact countP_eq_sum_indicator (active xs) (keys xs)

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

private theorem keys_append_mem (a b : Bag) (k : Nat) :
    k ∈ keys (a ++ b) ↔ k ∈ keys a ∨ k ∈ keys b := by
  simp [keys]

private theorem weight_cons (a k : Nat) (v : Int) (rs : Bag) :
    weight ((a, v) :: rs) k = (if a == k then v else 0) + weight rs k := by
  by_cases h : (a == k) = true
  · have hak : a = k := beq_iff_eq.mp h
    subst a
    simp [weight]
  · simp [weight, h]

private theorem sum_add (f g : Nat → Int) (s : List Nat) :
    (s.map (fun k => f k + g k)).sum = (s.map f).sum + (s.map g).sum := by
  induction s with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

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

private theorem changes_weighted_sum (right patch : Bag) (f : Nat → Int) :
    ((changes right patch).map (fun (k, d) => d * f k)).sum =
      ((keys patch).map (fun k => delta right patch k * f k)).sum := by
  unfold changes
  simp only [List.map_map]
  rfl

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem indicator_mul (b1 b2 : Bool) (v : Int) :
    (if b1 then v else 0) - (if b2 then v else 0) = (indicator b1 - indicator b2) * v := by
  cases b1 <;> cases b2 <;> simp [indicator]

private theorem natAbs_sum_le_sum_natAbs (xs : List Int) :
    xs.sum.natAbs ≤ (xs.map Int.natAbs).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.le_trans (Int.natAbs_add_le x xs.sum) (Nat.add_le_add_left ih x.natAbs)

private theorem keys_old_subset (old patch : Bag) :
    ∀ k ∈ keys old, k ∈ keys (old ++ patch) := by
  intro k hk
  exact (keys_append_mem old patch k).mpr (Or.inl hk)

theorem sound_ZeroCrossing :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.ZeroCrossing.Goal := by
  intro old patch
  rw [count_eq_sum_indicator (old ++ patch), count_eq_sum_indicator old]
  let S := keys (old ++ patch)
  let fnew : Nat → Int := fun k => indicator (active (old ++ patch) k)
  let fold : Nat → Int := fun k => indicator (active old k)
  let c : Nat → Int := fun k => delta old patch k
  change (S.map fnew).sum - ((keys old).map fold).sum =
    ((changes old patch).map Prod.snd).sum
  have hSnd : S.Nodup := by
    dsimp [S]
    exact nodup_eraseDups _
  have hsub_old : ∀ k ∈ keys old, k ∈ S := by
    intro k hk
    dsimp [S]
    exact keys_old_subset old patch k hk
  have hzero_fold : ∀ k ∈ S, k ∉ keys old → fold k = 0 := by
    intro k hkS hknot
    have hw : weight old k = 0 := weight_of_not_keys old k hknot
    have ha : active old k = false := active_of_weight_zero old k hw
    simp [fold, ha, indicator]
  have hsum_fold : (S.map fold).sum = ((keys old).map fold).sum := by
    symm
    exact sum_subset_of_vanish fold S hSnd (keys old) (nodup_eraseDups _) hsub_old hzero_fold
  have hsub_patch : ∀ k ∈ keys patch, k ∈ S := by
    intro k hk
    dsimp [S]
    exact (keys_append_mem old patch k).mpr (Or.inr hk)
  have hzero_c : ∀ k ∈ S, k ∉ keys patch → c k = 0 := by
    intro k hkS hknot
    have hw : weight patch k = 0 := weight_of_not_keys patch k hknot
    dsimp [c]
    exact delta_zero_of_weight_zero old patch k hw
  have hsum_c : (S.map c).sum = ((keys patch).map c).sum := by
    symm
    exact sum_subset_of_vanish c S hSnd (keys patch) (nodup_eraseDups _) hsub_patch hzero_c
  have hdiff : (S.map fnew).sum - ((keys old).map fold).sum = (S.map c).sum := by
    rw [← hsum_fold]
    rw [← sum_sub fnew fold S]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rfl
  rw [hdiff, hsum_c]
  dsimp [c]
  unfold changes
  simp only [List.map_map]
  rfl

theorem sound_Coalesce :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.Coalesce.Goal := by
  intro old a b k
  rw [weight_append]
  rw [weight_changes old (a ++ b) k]
  rw [weight_changes old a k]
  rw [weight_changes (old ++ a) b k]
  have hmem : k ∈ keys (a ++ b) ↔ k ∈ keys a ∨ k ∈ keys b := keys_append_mem a b k
  by_cases hpa : k ∈ keys a
  · by_cases hpb : k ∈ keys b
    · have hpab : k ∈ keys (a ++ b) := hmem.mpr (Or.inl hpa)
      simp [hpa, hpb, hpab]
      exact (delta_telescope old a b k).symm
    · have hpab : k ∈ keys (a ++ b) := hmem.mpr (Or.inl hpa)
      simp [hpa, hpb, hpab]
      have hw : weight b k = 0 := weight_of_not_keys b k hpb
      exact delta_of_weight_zero_right old a b k hw
  · by_cases hpb : k ∈ keys b
    · have hpab : k ∈ keys (a ++ b) := hmem.mpr (Or.inr hpb)
      simp [hpa, hpb, hpab]
      have hw : weight a k = 0 := weight_of_not_keys a k hpa
      exact delta_of_weight_zero_left old a b k hw
    · have hpab : k ∉ keys (a ++ b) := by
        intro hk
        rcases hmem.mp hk with h | h <;> contradiction
      simp [hpa, hpb, hpab]

theorem sound_SemijoinRepair :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.SemijoinRepair.Goal := by
  intro left right patch
  induction left with
  | nil =>
      simp [semi, weight, sum_map_zero]
  | cons r rs ih =>
      rcases r with ⟨a, v⟩
      have hR :
          ((changes right patch).map (fun (k, d) => d * weight ((a, v) :: rs) k)).sum =
            ((changes right patch).map (fun (k, d) => d * weight rs k)).sum +
              delta right patch a * v := by
        rw [changes_weighted_sum, changes_weighted_sum]
        have hsplit : ∀ k, delta right patch k * weight ((a, v) :: rs) k =
            delta right patch k * weight rs k +
              (if k = a then delta right patch k * v else 0) := by
          intro k
          rw [weight_cons]
          by_cases hk : k = a
          · subst k
            simp
            rw [Int.mul_add]
            omega
          · have hak : a ≠ k := by intro he; exact hk he.symm
            simp [hk, hak]
        rw [show (keys patch).map (fun k => delta right patch k * weight ((a, v) :: rs) k) =
              (keys patch).map (fun k =>
                delta right patch k * weight rs k +
                  (if k = a then delta right patch k * v else 0)) by
              apply List.map_congr_left
              intro k hk
              exact hsplit k]
        rw [sum_add]
        rw [sum_single_of_nodup (fun k => delta right patch k * v) (keys patch)
          (nodup_eraseDups _) a]
        by_cases ha : a ∈ keys patch
        · simp [ha]
        · have hw : weight patch a = 0 := weight_of_not_keys patch a ha
          have hd : delta right patch a = 0 := delta_zero_of_weight_zero right patch a hw
          simp [ha, hd]
      have hda : (if a ∈ keys patch then delta right patch a * v else 0) =
          delta right patch a * v := by
        by_cases ha : a ∈ keys patch
        · simp [ha]
        · have hw : weight patch a = 0 := weight_of_not_keys patch a ha
          have hd : delta right patch a = 0 := delta_zero_of_weight_zero right patch a hw
          simp [ha, hd]
      have hfirst : (if active (right ++ patch) a then v else 0) -
            (if active right a then v else 0) = delta right patch a * v := by
        dsimp [delta]
        exact indicator_mul (active (right ++ patch) a) (active right a) v
      have hsemi1 : semi ((a, v) :: rs) (right ++ patch) =
            (if active (right ++ patch) a then v else 0) + semi rs (right ++ patch) := by
        by_cases h : active (right ++ patch) a = true
        · simp [semi, h]
        · have hf : active (right ++ patch) a = false := Bool.eq_false_of_ne_true h
          simp [semi, hf]
      have hsemi2 : semi ((a, v) :: rs) right =
            (if active right a then v else 0) + semi rs right := by
        by_cases h : active right a = true
        · simp [semi, h]
        · have hf : active right a = false := Bool.eq_false_of_ne_true h
          simp [semi, hf]
      rw [hsemi1, hsemi2, hR]
      calc
        ((if active (right ++ patch) a then v else 0) + semi rs (right ++ patch)) -
            ((if active right a then v else 0) + semi rs right) =
            ((if active (right ++ patch) a then v else 0) -
              (if active right a then v else 0)) +
            (semi rs (right ++ patch) - semi rs right) := by omega
        _ = delta right patch a * v +
            ((changes right patch).map (fun (k, d) => d * weight rs k)).sum := by
              rw [hfirst, ih]
        _ = ((changes right patch).map (fun (k, d) => d * weight rs k)).sum +
            delta right patch a * v := by omega

theorem sound_MarginCertificate :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.MarginCertificate.Goal := by
  intro old patch k h
  have hsum : (weight patch k).natAbs ≤
      ((patch.filter (fun r => r.1 == k)).map (fun r => r.2.natAbs)).sum := by
    have hs := natAbs_sum_le_sum_natAbs ((patch.filter (fun r => r.1 == k)).map Prod.snd)
    have hmap : (((patch.filter (fun r => r.1 == k)).map Prod.snd).map Int.natAbs) =
        ((patch.filter (fun r => r.1 == k)).map (fun r => r.2.natAbs)) := by
      rw [List.map_map]
      rfl
    simpa [weight, hmap] using hs
  have hlt : (weight patch k).natAbs < (weight old k).natAbs := by omega
  have hltI : ((weight patch k).natAbs : Int) < ((weight old k).natAbs : Int) :=
    Int.ofNat_lt.mpr hlt
  by_cases hpos : 0 < weight old k
  · have hold : ((weight old k).natAbs : Int) = weight old k :=
      Int.natAbs_of_nonneg (by omega)
    have hp_le : weight patch k ≤ ((weight patch k).natAbs : Int) := Int.le_natAbs
    have hnew : 0 < weight (old ++ patch) k := by
      rw [weight_append]
      omega
    simp [active, hpos, hnew]
  · have hne : weight old k ≠ 0 := by
      intro hz
      rw [hz] at hlt
      have hznonneg : 0 ≤ (weight patch k).natAbs := Nat.zero_le _
      omega
    have holdneg : weight old k < 0 := by omega
    have hold : ((weight old k).natAbs : Int) = -(weight old k) := by
      rw [← Int.natAbs_neg]
      exact Int.natAbs_of_nonneg (by omega)
    have hp_ge : -(weight patch k) ≤ ((weight patch k).natAbs : Int) := by
      have ht : -weight patch k ≤ (((-weight patch k).natAbs : Int)) := Int.le_natAbs
      rwa [Int.natAbs_neg] at ht
    have hnew : ¬ 0 < weight (old ++ patch) k := by
      rw [weight_append]
      omega
    simp [active, hpos, hnew]

theorem sound_ReplayLedger :
    TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.ThresholdViews.ReplayLedger.Goal := by
  intro old p k
  rw [weight_changes]
  by_cases hk : k ∈ keys p
  · simp [hk, delta]
    omega
  · have hw : weight p k = 0 := weight_of_not_keys p k hk
    have ha : active (old ++ p) k = active old k := active_append_of_weight_zero old p k hw
    simp [hk, ha]

end TinylambOpt.ResearchProofs.Atlas.SnapshotMaintenance.ThresholdViews
