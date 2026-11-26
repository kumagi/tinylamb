import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.

Proved

- `Sector.DistinctSectorFloor` — `∀ shift startup xs, (((points xs).map
  (·/2^shift)).eraseDups).length ≤ cost (price shift startup) (choose (price shift
  startup) xs)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_DistinctSectorFloor

open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

-- ── eraseDups length bookkeeping ──

private theorem eraseDups_cons_lower (a : Nat) : ∀ l : List Nat,
    l.eraseDups.length ≤ (a :: l).eraseDups.length := by
  intro l
  refine Nat.strongRecOn l.length
    (motive := fun n => ∀ l : List Nat, l.length = n →
      l.eraseDups.length ≤ (a :: l).eraseDups.length) ?_ l rfl
  intro n ih l hlen
  cases l with
  | nil => simp [List.eraseDups_cons]
  | cons b bs =>
    by_cases hba : b == a
    · have hab : b = a := by
        by_cases hEq : b = a
        · exact hEq
        · have hf : (b == a) = false := beq_eq_false_iff_ne.mpr hEq
          rw [hf] at hba
          cases hba
      subst b
      simp [List.eraseDups_cons]
    · have hbaf : (b == a) = false := by
        rw [← Bool.not_eq_true]
        exact hba
      have hne : b ≠ a := beq_eq_false_iff_ne.mp hbaf
      have hbeq : (a == b) = false := beq_eq_false_iff_ne.mpr (fun h => hne h.symm)
      have hlt : (List.filter (fun x => ! x == b) bs).length < n := by
        have ht := List.length_filter_le (fun x => ! x == b) bs
        simp at hlen
        omega
      have hih := ih _ hlt (List.filter (fun x => ! x == b) bs) rfl
      simp [List.eraseDups_cons, hbeq, hba] at hih ⊢
      have hfilt : List.filter (fun x => ! x == a && ! x == b) bs =
          List.filter (fun x => ! x == b && ! x == a) bs := by
        have hp : (fun x : Nat => ! x == a && ! x == b) =
            (fun x : Nat => ! x == b && ! x == a) := by
          funext x
          cases hxa : x == a <;> cases hxb : x == b <;> rfl
        rw [hp]
      rw [hfilt] at hih
      exact hih

private theorem eraseDups_sublist_mono : ∀ {l₁ l₂ : List Nat}, l₁.Sublist l₂ →
    l₁.eraseDups.length ≤ l₂.eraseDups.length := by
  intro l₁ l₂ h
  refine Nat.strongRecOn l₂.length
    (motive := fun n => ∀ (l₁ l₂ : List Nat), l₂.length = n → l₁.Sublist l₂ →
      l₁.eraseDups.length ≤ l₂.eraseDups.length) ?_ l₁ l₂ rfl h
  intro n ih l₁ l₂ hlen hsub
  cases l₂ with
  | nil =>
    cases hsub
    exact Nat.le_refl _
  | cons a l₂' =>
    cases hsub with
    | cons _ hs =>
      have hlt : l₂'.length < n := by
        simp at hlen
        omega
      have hih := ih l₂'.length hlt l₁ l₂' rfl hs
      exact Nat.le_trans hih (eraseDups_cons_lower a l₂')
    | cons_cons _ hs =>
      rename_i l₁'
      have hlt : (List.filter (fun b => ! b == a) l₂').length < n := by
        have ht := List.length_filter_le (fun b => ! b == a) l₂'
        simp at hlen
        omega
      have hih := ih _ hlt (List.filter (fun b => ! b == a) l₁')
        (List.filter (fun b => ! b == a) l₂') rfl (List.Sublist.filter _ hs)
      simpa [List.eraseDups_cons] using hih

private theorem eraseDups_cons_upper (a : Nat) (l : List Nat) :
    ((a :: l).eraseDups).length ≤ l.eraseDups.length + 1 := by
  rw [List.eraseDups_cons]
  simp only [List.length_cons]
  have hs : (List.filter (fun b => ! b == a) l).Sublist l := List.filter_sublist
  have := eraseDups_sublist_mono hs
  omega

private theorem eraseDups_filter_eq_mem (a : Nat) : ∀ l : List Nat, a ∈ l →
    ((List.filter (fun b => ! b == a) l).eraseDups).length + 1 = l.eraseDups.length := by
  intro l
  refine Nat.strongRecOn l.length
    (motive := fun n => ∀ l : List Nat, l.length = n → a ∈ l →
      ((List.filter (fun b => ! b == a) l).eraseDups).length + 1 = l.eraseDups.length) ?_ l rfl
  intro n ih l hlen ha
  cases l with
  | nil => simp at ha
  | cons b bs =>
    by_cases hba : b = a
    · subst b
      simp [List.eraseDups_cons]
    · have hmem : a ∈ bs := by
        rcases List.mem_cons.mp ha with hEq | hmem
        · exact absurd hEq.symm hba
        · exact hmem
      have hbeq : (b == a) = false := beq_eq_false_iff_ne.mpr hba
      have hne : a ≠ b := fun h => hba h.symm
      have hab : (a == b) = false := beq_eq_false_iff_ne.mpr hne
      have hlt : (List.filter (fun x => ! x == b) bs).length < n := by
        have ht := List.length_filter_le (fun x => ! x == b) bs
        simp at hlen
        omega
      have hmem2 : a ∈ List.filter (fun x => ! x == b) bs := by
        exact List.mem_filter.mpr ⟨hmem, by rw [hab]; rfl⟩
      have hih := ih _ hlt (List.filter (fun x => ! x == b) bs) rfl hmem2
      simp [List.eraseDups_cons, hbeq, hab] at hih ⊢
      have hfilt : List.filter (fun x => ! x == a && ! x == b) bs =
          List.filter (fun x => ! x == b && ! x == a) bs := by
        have hp : (fun x : Nat => ! x == a && ! x == b) =
            (fun x : Nat => ! x == b && ! x == a) := by
          funext x
          cases hxa : x == a <;> cases hxb : x == b <;> rfl
        rw [hp]
      rw [hfilt] at hih
      exact hih

private theorem secCount_cons_le (k a : Nat) (l : List Nat) :
    ((a / k :: l.map (fun x => x / k)).eraseDups).length ≤
      (l.map (fun x => x / k)).eraseDups.length + 1 := by
  exact eraseDups_cons_upper (a / k) (l.map (fun x => x / k))

private theorem secCount_cons_eq (k a : Nat) (l : List Nat)
    (ha : a / k ∈ l.map (fun x => x / k)) :
    ((a / k :: l.map (fun x => x / k)).eraseDups).length =
      (l.map (fun x => x / k)).eraseDups.length := by
  rw [List.eraseDups_cons]
  have h := eraseDups_filter_eq_mem (a / k) (l.map (fun x => x / k)) ha
  omega

-- ── covers bookkeeping ──

private theorem covers_nonempty : (xs : List Nat) → covers xs ≠ []
  | [] => by simp [covers]
  | x :: xs => by
    have ih := covers_nonempty xs
    simp only [covers, ne_eq]
    intro h
    cases hxs : covers xs with
    | nil => exact ih hxs
    | cons p ps =>
      have : (match p with | [] => [[(x, x)]] | (_, b)::rest => [(x, x)::p, (x, b)::rest]) = [] := by
        have hall : ∀ ps' ∈ p :: ps,
            (match ps' with | [] => [[(x, x)]] | (_, b)::rest => [(x, x)::ps', (x, b)::rest]) = [] := by
          simp only [List.flatMap_eq_nil_iff] at h
          rw [hxs] at h
          exact h
        exact hall p (by simp)
      cases p with
      | nil => simp at this
      | cons hd tl => simp at this

private theorem covers_no_empty : (xs : List Nat) → xs ≠ [] →
    ∀ ps ∈ covers xs, ps ≠ []
  | [], h, ps, hps => by exact absurd rfl h
  | x :: xs, h, ps, hps => by
    simp only [covers, List.mem_flatMap] at hps
    obtain ⟨qs, hqs, hmem⟩ := hps
    cases hq : qs with
    | nil =>
      have hps_eq : ps = [(x, x)] := by simpa [hq] using hmem
      simp [hps_eq]
    | cons p rest =>
      cases p with
      | mk c d =>
        simp [hq] at hmem
        rcases hmem with hmem | hmem <;> simp [hmem]

private theorem covers_coords_mem : ∀ (xs : List Nat) (ps : List (Nat × Nat)),
    ps ∈ covers xs → ∀ a b, (a, b) ∈ ps → a ∈ xs ∧ b ∈ xs := by
  intro xs
  induction xs with
  | nil =>
    intro ps hps a b hab
    simp [covers] at hps
    subst ps
    simp at hab
  | cons x xs ih =>
    intro ps hps a b hab
    simp only [covers, List.mem_flatMap] at hps
    obtain ⟨qs, hqs, hmem⟩ := hps
    cases hq : qs with
    | nil =>
      have hps_eq : ps = [(x, x)] := by simpa [hq] using hmem
      subst ps
      simp at hab
      rcases hab with ⟨rfl, rfl⟩
      simp
    | cons p rest =>
      cases p with
      | mk c d =>
        have hres_tail : ∀ a b, (a, b) ∈ qs → a ∈ xs ∧ b ∈ xs := ih qs hqs
        have hcdmem : c ∈ xs ∧ d ∈ xs := hres_tail c d (by simp [hq])
        simp [hq] at hmem
        rcases hmem with hmem | hmem
        · subst ps
          rcases List.mem_cons.mp hab with hEq | hmem_qs
          · rcases hEq
            simp
          · have hres := hres_tail a b (by simpa [hq] using hmem_qs)
            exact ⟨by simp [hres.1], by simp [hres.2]⟩
        · subst ps
          rcases List.mem_cons.mp hab with hEq | hmem_rest
          · rcases hEq
            exact ⟨by simp, by simp [hcdmem.2]⟩
          · have hab_qs : (a, b) ∈ qs := by
              rw [hq]
              exact List.Mem.tail (c, d) hmem_rest
            have hres := hres_tail a b hab_qs
            exact ⟨by simp [hres.1], by simp [hres.2]⟩

private theorem covers_head_fst_le_snd : ∀ (xs : List Nat),
    xs.Pairwise (fun a b => a ≤ b) → xs ≠ [] →
    ∀ ps ∈ covers xs, (ps.head?.getD (0, 0)).1 ≤ (ps.head?.getD (0, 0)).2 := by
  intro xs
  induction xs with
  | nil =>
    intro hp hne
    exact absurd rfl hne
  | cons x xs ih =>
    intro hp hne ps hps
    rw [List.pairwise_cons] at hp
    simp only [covers, List.mem_flatMap] at hps
    obtain ⟨qs, hqs, hmem⟩ := hps
    cases hq : qs with
    | nil =>
      have hxs_nil : xs = [] := by
        by_cases hne2 : xs = []
        · exact hne2
        · exfalso
          exact (covers_no_empty xs hne2 [] (by simpa [hq] using hqs)) rfl
      subst xs
      simp [hq] at hmem
      subst ps
      simp
    | cons p rest =>
      cases p with
      | mk c d =>
        have hcd : c ≤ d := by
          by_cases hxs_nil : xs = []
          · subst xs
            simp [covers] at hqs
            rw [hq] at hqs
            simp at hqs
          · have := ih hp.2 hxs_nil qs hqs
            simpa [hq] using this
        have hd_mem : d ∈ xs := (covers_coords_mem xs qs hqs c d (by simp [hq])).2
        have hxd : x ≤ d := hp.1 d hd_mem
        simp [hq] at hmem
        rcases hmem with hmem | hmem <;> subst ps <;> simp [hcd, hxd]

private theorem choose_mem_covers (f : Price) (xs : List Nat) (hne : covers (points xs) ≠ []) :
    choose f xs ∈ covers (points xs) := by
  unfold choose
  have hperm := List.mergeSort_perm (covers (points xs)) (fun a b => decide (cost f a ≤ cost f b))
  have hne' : (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) ≠ [] := by
    intro h
    have := hperm.symm.length_eq
    rw [h] at this
    simp at this
    exact hne this
  cases h : (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) with
  | nil => exact absurd h hne'
  | cons hd tl =>
    have hmem : hd ∈ (covers (points xs)).mergeSort (fun a b => decide (cost f a ≤ cost f b)) := by
      simp [h]
    rw [List.mem_mergeSort] at hmem
    simpa [h] using hmem

private theorem points_pairwise_le (xs : List Nat) :
    (points xs).Pairwise (fun a b => a ≤ b) := by
  unfold points
  have hpair := List.pairwise_mergeSort (α := Nat)
    (le := fun a b : Nat => decide (a ≤ b))
    (trans := by
      intro a b c hab hbc
      simp only [decide_eq_true_eq] at hab hbc ⊢
      exact Nat.le_trans hab hbc)
    (total := by
      intro a b
      simp only [Bool.or_eq_true, decide_eq_true_eq]
      exact Nat.le_total a b)
    (xs.eraseDups)
  simpa only [decide_eq_true_eq] using hpair

-- ── the floor invariant for every plan in covers ──

private theorem covers_sec_floor (k startup : Nat) :
    ∀ (ks : List Nat), ks.Pairwise (fun a b => a ≤ b) →
    ∀ ps ∈ covers ks,
      ((ks.map (fun x => x / k)).eraseDups).length ≤
        cost (fun a b => startup + pages k a b) ps := by
  intro ks
  induction ks with
  | nil =>
    intro hp ps hps
    simp [covers] at hps
    subst ps
    simp [cost]
  | cons x xs ih =>
    intro hp ps hps
    rw [List.pairwise_cons] at hp
    simp only [covers, List.mem_flatMap] at hps
    obtain ⟨qs, hqs, hmem⟩ := hps
    cases hq : qs with
    | nil =>
      have hps_eq : ps = [(x, x)] := by simpa [hq] using hmem
      subst ps
      have hxs_nil : xs = [] := by
        by_cases hne : xs = []
        · exact hne
        · exfalso
          exact (covers_no_empty xs hne [] (by simpa [hq] using hqs)) rfl
      subst xs
      simp [cost, pages, List.eraseDups_cons]
    | cons p rest =>
      cases p with
      | mk a b =>
        have hqs' : (a, b)::rest ∈ covers xs := by simpa [hq] using hqs
        have ha_mem : a ∈ xs := (covers_coords_mem xs qs hqs a b (by simp [hq])).1
        have hih := ih hp.2 ((a, b)::rest) hqs'
        simp [hq] at hmem
        rcases hmem with hmem | hmem
        · subst ps
          have hsec := secCount_cons_le k x xs
          have hpoint : 1 ≤ startup + pages k x x := by
            simp [pages]
          simp only [cost, List.map, List.sum_cons, List.sum_nil, Nat.add_zero] at hih ⊢
          omega
        · subst ps
          have hxs_ne : xs ≠ [] := by
            intro hnil
            subst xs
            simp [covers] at hqs
            rw [hq] at hqs
            simp at hqs
          have hab : a ≤ b := by
            simpa [hq] using (covers_head_fst_le_snd xs hp.2 hxs_ne qs hqs)
          have hxa : x ≤ a := hp.1 a ha_mem
          by_cases hsec : x / k = a / k
          · have hmem_sec : x / k ∈ xs.map (fun y => y / k) := by
              exact List.mem_map.mpr ⟨a, ha_mem, hsec.symm⟩
            have hsc := secCount_cons_eq k x xs hmem_sec
            have hpages : pages k x b = pages k a b := by
              simp [pages, hsec]
            simp only [cost, List.map, List.sum_cons, List.sum_nil, Nat.add_zero] at hih ⊢
            rw [hsc, hpages]
            exact hih
          · have hdiv_le : x / k ≤ a / k := Nat.div_le_div_right hxa
            have hdiv_lt : x / k < a / k := by omega
            have hadiv_le_b : a / k ≤ b / k := Nat.div_le_div_right hab
            have hpages : pages k a b + 1 ≤ pages k x b := by
              simp only [pages]
              omega
            have hsec_le := secCount_cons_le k x xs
            simp only [cost, List.map, List.sum_cons, List.sum_nil, Nat.add_zero] at hih ⊢
            omega

theorem sound_DistinctSectorFloor : Sector.DistinctSectorFloor.Goal := by
  intro shift startup xs
  let ks := points xs
  have hmem : choose (Sector.price shift startup) xs ∈ covers ks := by
    exact choose_mem_covers (Sector.price shift startup) xs (covers_nonempty ks)
  have hfloor := covers_sec_floor (2 ^ shift) startup ks (points_pairwise_le xs)
    (choose (Sector.price shift startup) xs) hmem
  change ((ks.map (fun x => x / 2 ^ shift)).eraseDups).length ≤
    cost (fun a b => startup + pages (2 ^ shift) a b)
      (choose (Sector.price shift startup) xs)
  exact hfloor

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GatherPlans_Sector_DistinctSectorFloor
