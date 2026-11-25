import TinylambOpt.ResearchGoals.Frontiers.Provenance

/-! Proofs and disproofs for `TinylambOpt.ResearchGoals.Frontiers.Provenance`.

`PackingExact` is a known-false goal (the triangle witnesses `[0,1],[1,2],[0,2]`):
two of them cannot be chosen disjoint, but two deletions are needed to kill all three.

All ten goals are resolved: nine are proved and `PackingExact` is disproved.
`BipartiteCut` is Kőnig's theorem (minimum vertex cover = maximum matching for a
bipartite graph); its proof uses an augmenting-path/Hall-style argument for the
bipartite graph with left vertices `2*x` and right vertices `2*y+1`. -/

namespace TinylambOpt.ResearchProofs.Frontiers.Provenance

open TinylambOpt.ResearchGoals.Frontiers.Provenance

set_option maxHeartbeats 0

private theorem live_true_iff (del w : Witness) : live del w = true ↔ ∀ x ∈ w, x ∉ del := by
  simp [live]

private theorem live_append (a b w : Witness) : live (a ++ b) w = (live a w && live b w) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [live_true_iff] at h
    have h1 : live a w = true := by
      rw [live_true_iff]
      intro x hx hxa
      exact h x hx (List.mem_append.mpr (Or.inl hxa))
    have h2 : live b w = true := by
      rw [live_true_iff]
      intro x hx hxb
      exact h x hx (List.mem_append.mpr (Or.inr hxb))
    simp [h1, h2]
  · intro h
    have h1 : live a w = true := (Bool.and_eq_true_iff.mp h).1
    have h2 : live b w = true := (Bool.and_eq_true_iff.mp h).2
    rw [live_true_iff] at h1 h2 ⊢
    intro x hx hxab
    rcases List.mem_append.mp hxab with hxa | hxb
    · exact h1 x hx hxa
    · exact h2 x hx hxb

private theorem live_witness_append (del p q : Witness) : live del (p ++ q) = (live del p && live del q) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [live_true_iff] at h
    have h1 : live del p = true := by
      rw [live_true_iff]
      intro x hx hxd
      exact h x (List.mem_append.mpr (Or.inl hx)) hxd
    have h2 : live del q = true := by
      rw [live_true_iff]
      intro x hx hxd
      exact h x (List.mem_append.mpr (Or.inr hx)) hxd
    simp [h1, h2]
  · intro h
    have h1 : live del p = true := (Bool.and_eq_true_iff.mp h).1
    have h2 : live del q = true := (Bool.and_eq_true_iff.mp h).2
    rw [live_true_iff] at h1 h2 ⊢
    intro x hx hxd
    rcases List.mem_append.mp hx with hxp | hxq
    · exact h1 x hxp hxd
    · exact h2 x hxq hxd

private theorem live_of_subset {del v w : Witness} (hvw : subset v w = true) (hw : live del w = true) :
    live del v = true := by
  rw [live_true_iff] at hw ⊢
  intro x hx hxd
  have hxw : x ∈ w := List.contains_iff_mem.mp ((List.all_eq_true.mp hvw) x hx)
  exact hw x hxw hxd

private theorem subset_refl (w : Witness) : subset w w = true := by
  rw [subset]
  apply List.all_eq_true.mpr
  intro x hx
  exact List.contains_iff_mem.mpr hx

private theorem subset_trans {u v w : Witness} (huv : subset u v = true) (hvw : subset v w = true) :
    subset u w = true := by
  rw [subset] at huv hvw ⊢
  apply List.all_eq_true.mpr
  intro x hx
  have hxv : x ∈ v := List.contains_iff_mem.mp ((List.all_eq_true.mp huv) x hx)
  exact (List.all_eq_true.mp hvw) x hxv

private theorem subset_eraseDups_left (w : Witness) : subset w.eraseDups w = true := by
  rw [subset]
  apply List.all_eq_true.mpr
  intro x hx
  exact List.contains_iff_mem.mpr (List.mem_eraseDups.mp hx)

private theorem subset_eraseDups_right (w : Witness) : subset w w.eraseDups = true := by
  rw [subset]
  apply List.all_eq_true.mpr
  intro x hx
  exact List.contains_iff_mem.mpr (List.mem_eraseDups.mpr hx)

private theorem live_eraseDups (del w : Witness) : live del w.eraseDups = live del w := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    exact live_of_subset (subset_eraseDups_right w) h
  · intro h
    exact live_of_subset (subset_eraseDups_left w) h

private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List Nat, l.length = n → l.eraseDups.Nodup
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen
    cases l with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        apply List.nodup_cons.mpr
        constructor
        · intro ha
          have hfa : a ∈ as.filter (fun b => !b == a) := List.mem_eraseDups.mp ha
          simp at hfa
        · have hle : (as.filter (fun b => !b == a)).length ≤ as.length :=
            List.length_filter_le (fun b => !b == a) as
          have hlt : (as.filter (fun b => !b == a)).length < n := by
            simp at hlen
            omega
          exact ih _ hlt _ rfl
  exact H l rfl

private theorem length_le_of_nodup_subset {s t : List Nat} (hs : s.Nodup) (hmem : ∀ x ∈ s, x ∈ t) :
    s.length ≤ t.length := by
  let motive : Nat → Prop := fun n => ∀ (s t : List Nat), s.length = n → s.Nodup →
    (∀ x ∈ s, x ∈ t) → s.length ≤ t.length
  have H : motive s.length := by
    apply Nat.strongRecOn
    intro n ih s t hlen hs hmem
    cases s with
    | nil => simp
    | cons a s' =>
        have hs' : s'.Nodup := (List.nodup_cons.mp hs).2
        have ha_not : a ∉ s' := (List.nodup_cons.mp hs).1
        have hat : a ∈ t := hmem a (by simp)
        have hmem' : ∀ x ∈ s', x ∈ t.erase a := by
          intro x hx
          have hxa : x ≠ a := by intro hxa; exact ha_not (by simpa [hxa] using hx)
          exact (List.mem_erase_of_ne hxa).mpr (hmem x (by simp [hx]))
        have hlt : s'.length < n := by
          rw [← hlen]
          simp
        have ih' : s'.length ≤ (t.erase a).length := ih s'.length hlt s' (t.erase a) rfl hs' hmem'
        rw [List.length_erase] at ih'
        simp [hat] at ih'
        have htpos : 0 < t.length := List.length_pos_of_mem hat
        simp
        omega
  exact H s t rfl hs hmem

private theorem length_lt_of_nodup_subset_strict {s t : List Nat} (hs : s.Nodup)
    (hmem : ∀ x ∈ s, x ∈ t) (hex : ∃ x ∈ t, x ∉ s) : s.length < t.length := by
  rcases hex with ⟨x, hxt, hxs⟩
  have hmem2 : ∀ y ∈ s, y ∈ t.erase x := by
    intro y hy
    have hyx : y ≠ x := by intro heq; exact hxs (by simpa [heq] using hy)
    exact (List.mem_erase_of_ne hyx).mpr (hmem y hy)
  have hle := length_le_of_nodup_subset hs hmem2
  have hle' : s.length ≤ t.length - 1 := by
    simpa [hxt] using hle
  have htpos : 0 < t.length := List.length_pos_of_mem hxt
  omega

private theorem strict_subset_length_lt {v w : Witness} (hvw : subset v w = true) (hnsub : (!subset w v) = true) :
    v.eraseDups.length < w.eraseDups.length := by
  have hve : v.eraseDups.Nodup := nodup_eraseDups v
  have hmem : ∀ x ∈ v.eraseDups, x ∈ w.eraseDups := by
    intro x hx
    apply List.mem_eraseDups.mpr
    exact List.contains_iff_mem.mp ((List.all_eq_true.mp hvw) x (List.mem_eraseDups.mp hx))
  apply length_lt_of_nodup_subset_strict hve hmem
  have hnsub' : subset w v = false := Bool.eq_false_of_ne_true (by
    intro htrue
    simp [htrue] at hnsub)
  rw [subset] at hnsub'
  rcases List.all_eq_false.mp hnsub' with ⟨x, hxw, hxnv⟩
  exact ⟨x, List.mem_eraseDups.mpr hxw, by
    intro hxve
    exact hxnv (List.contains_iff_mem.mpr (List.mem_eraseDups.mp hxve))⟩

private theorem minimal_subset_exists (ws : Family) (w : Witness) (hw : w ∈ ws) :
    ∃ u ∈ minimal ws, subset u w = true := by
  let P : Witness → Prop := fun w => w ∈ ws → ∃ u ∈ minimal ws, subset u w = true
  have step : ∀ w : Witness, (∀ v : Witness, v.eraseDups.length < w.eraseDups.length → P v) → P w := by
    intro w ih hw
    by_cases hmin : w ∈ minimal ws
    · exact ⟨w, hmin, subset_refl w⟩
    · have hnot : ws.any (fun v => subset v w && !subset w v) = true := by
        by_cases hb : ws.any (fun v => subset v w && !subset w v) = true
        · exact hb
        · exfalso
          apply hmin
          rw [minimal, List.mem_filter]
          exact ⟨hw, by simp [Bool.eq_false_of_ne_true hb]⟩
      rcases List.any_eq_true.mp hnot with ⟨v, hvws, hvbool⟩
      have hvsub : subset v w = true := (Bool.and_eq_true_iff.mp hvbool).1
      have hvnsub : (!subset w v) = true := (Bool.and_eq_true_iff.mp hvbool).2
      have hlt : v.eraseDups.length < w.eraseDups.length := strict_subset_length_lt hvsub hvnsub
      rcases ih v hlt hvws with ⟨u, humin, husub⟩
      exact ⟨u, humin, subset_trans husub hvsub⟩
  let motive : Nat → Prop := fun n => ∀ w : Witness, w.eraseDups.length = n → P w
  have H : motive w.eraseDups.length := by
    apply Nat.strongRecOn
    intro n ihn w' hlen
    apply step
    intro v hvlt
    apply ihn v.eraseDups.length
    · rw [← hlen]
      exact hvlt
    · rfl
  exact H w rfl hw

theorem sound_Antichain : TinylambOpt.ResearchGoals.Frontiers.Provenance.Antichain.Goal := by
  intro ws del
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [existsAfter, List.any_eq_true] at h ⊢
    rcases h with ⟨w, hwmin, hwlive⟩
    have hwmem : w ∈ ws := by
      rw [minimal, List.mem_filter] at hwmin
      exact hwmin.1
    exact ⟨w, hwmem, hwlive⟩
  · intro h
    rw [existsAfter, List.any_eq_true] at h ⊢
    rcases h with ⟨w, hwmem, hwlive⟩
    rcases minimal_subset_exists ws w hwmem with ⟨u, humin, husub⟩
    exact ⟨u, humin, live_of_subset husub hwlive⟩

private theorem countP_product_single (del p : Witness) (b : Family) :
    List.countP (live del) (List.map (fun q => p ++ q) b) =
      (if live del p = true then List.countP (live del) b else 0) := by
  induction b with
  | nil => simp
  | cons q qs ih =>
      simp only [List.map_cons, List.countP_cons]
      rw [live_witness_append, ih]
      by_cases hp : live del p = true <;> by_cases hq : live del q = true <;> simp [hp, hq]

theorem sound_ProductCount : TinylambOpt.ResearchGoals.Frontiers.Provenance.ProductCount.Goal := by
  intro a b del
  unfold bagAfter product
  induction a with
  | nil => simp
  | cons p ps ih =>
      simp only [List.flatMap_cons, List.countP_append, List.countP_cons]
      rw [countP_product_single, ih]
      by_cases hp : live del p = true
      · simp [hp, Nat.add_mul, Nat.add_comm]
      · simp [hp]

theorem sound_IncidentDelta : TinylambOpt.ResearchGoals.Frontiers.Provenance.IncidentDelta.Goal := by
  intro ws old batch
  unfold bagAfter
  have hpart : ∀ w : Witness,
      (if live old w = true then 1 else 0) =
        (if live (old ++ batch) w = true then 1 else 0) +
          (if (live old w && w.any batch.contains) = true then 1 else 0) := by
    intro w
    rw [live_append]
    by_cases h1 : live old w = true <;> by_cases h2 : w.any batch.contains = true <;>
      simp [h2, live]
  have hsum : List.countP (live old) ws =
      List.countP (live (old ++ batch)) ws +
        List.countP (fun w => live old w && w.any batch.contains) ws := by
    induction ws with
    | nil => simp
    | cons w ws ih =>
        simp only [List.countP_cons]
        rw [ih, hpart w]
        omega
  omega

private theorem beq_comm {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : a = b := beq_iff_eq.mp h
    simp [heq]
  · intro h
    have heq : b = a := beq_iff_eq.mp h
    simp [heq]

private theorem sum_map_eq_count_head {α : Type} [BEq α] [LawfulBEq α] (k : α) (ks : List α) (f : α → Nat) :
    (ks.map f).sum = ks.count k * f k + ((ks.filter (fun b => !(b == k))).map f).sum := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxeq : x = k := beq_iff_eq.mp hx
        subst x
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm]
      · simp [List.count_cons, hx, ih, Nat.add_left_comm]

private theorem dedup_weight_sum (f : Witness → Nat) (keys : Family) :
    ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum := by
  let motive : Nat → Prop := fun n => ∀ keys : Family, keys.length = n →
      ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum
  have H : motive keys.length := by
    apply Nat.strongRecOn
    intro n ih keys hlen
    cases keys with
    | nil => simp
    | cons k ks =>
        let rest := ks.filter (fun b => ! (b == k))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          have hle := List.length_filter_le (fun b : Witness => ! (b == k)) ks
          omega
        have ihrest := ih rest.length hrestlen rest rfl
        rw [List.eraseDups_cons]
        simp only [List.map_cons, List.sum_cons]
        have hhead : (List.count k (k :: ks)) * f k = f k + ks.count k * f k := by
          rw [List.count_cons]
          simp [Nat.add_mul, Nat.add_comm]
        rw [hhead]
        have hrest_count : ∀ a ∈ rest.eraseDups, (List.count a (k :: ks)) = rest.count a := by
          intro a ha
          have hane : (a == k) = false := by
            have hamem : a ∈ rest := List.mem_eraseDups.mp ha
            have hamem' : a ∈ ks.filter (fun b => ! (b == k)) := by simpa [rest] using hamem
            have haf : (!(a == k)) = true := (List.mem_filter.mp hamem').2
            simpa using haf
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hka : (k == a) = false := by simpa [beq_comm k a] using hane
          rw [List.count_cons, hka, hrestc]
          simp
        rw [show ((rest.eraseDups.map fun a => List.count a (k :: ks) * f a).sum) =
             ((rest.eraseDups.map fun a => rest.count a * f a).sum) by
             apply congrArg List.sum
             apply List.map_congr_left
             intro a ha
             rw [hrest_count a ha]]
        rw [ihrest]
        have hks : (ks.map f).sum = ks.count k * f k + (rest.map f).sum := by
          simpa [rest] using sum_map_eq_count_head k ks f
        rw [hks]
        omega
  exact H keys rfl

private theorem sum_filter_eq_sum_ite {α : Type} (L : List α) (p : α → Bool) (g : α → Nat) :
    ((L.filter p).map g).sum = (L.map fun x => if p x = true then g x else 0).sum := by
  induction L with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h, ih]

private theorem live_mergeSort (del w : Witness) :
    live del w.mergeSort = live del w := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [live_true_iff] at h ⊢
    intro x hx hxd
    have hx' : x ∈ w.mergeSort := by
      simpa using (List.mem_mergeSort (le := fun a b : Nat => a <= b) (a := x) (l := w)).mpr hx
    exact h x hx' hxd
  · intro h
    rw [live_true_iff] at h ⊢
    intro x hx hxd
    have hx' : x ∈ w := by
      simpa using (List.mem_mergeSort (le := fun a b : Nat => a <= b) (a := x) (l := w)).mp hx
    exact h x hx' hxd

private theorem live_canon (del w : Witness) :
    live del w.mergeSort.eraseDups = live del w := by
  rw [live_eraseDups, live_mergeSort]

theorem sound_SignatureWeights : TinylambOpt.ResearchGoals.Frontiers.Provenance.SignatureWeights.Goal := by
  intro ws del
  dsimp only
  let keys := ws.map (fun w => w.mergeSort.eraseDups)
  unfold bagAfter
  have hleft : List.countP (live del) ws =
      (keys.map fun k => if live del k = true then 1 else 0).sum := by
    dsimp [keys]
    induction ws with
    | nil => simp
    | cons w ws ih =>
        simp only [List.map_cons, List.sum_cons, List.countP_cons]
        rw [live_canon, ih]
        omega
  have hdist := dedup_weight_sum (fun k : Witness => if live del k = true then 1 else 0) keys
  have hfilter : ((keys.eraseDups.filter (live del)).map keys.count).sum =
      ((keys.eraseDups.map fun k => keys.count k * (if live del k = true then 1 else 0)).sum) := by
    rw [sum_filter_eq_sum_ite]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    by_cases hl : live del k = true <;> simp [hl]
  rw [hleft, ← hdist, ← hfilter]

private theorem existsAfter_product (a b : Family) (del : Witness) :
    existsAfter (product a b) del = (existsAfter a del && existsAfter b del) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [existsAfter, List.any_eq_true] at h
    rcases h with ⟨w, hwprod, hwlive⟩
    rw [product] at hwprod
    rcases List.mem_flatMap.mp hwprod with ⟨p, hpa, hwmap⟩
    rcases List.mem_map.mp hwmap with ⟨q, hqb, rfl⟩
    rw [live_witness_append] at hwlive
    have hpl : live del p = true := (Bool.and_eq_true_iff.mp hwlive).1
    have hql : live del q = true := (Bool.and_eq_true_iff.mp hwlive).2
    apply Bool.and_eq_true_iff.mpr
    constructor
    · rw [existsAfter, List.any_eq_true]
      exact ⟨p, hpa, hpl⟩
    · rw [existsAfter, List.any_eq_true]
      exact ⟨q, hqb, hql⟩
  · intro h
    have ha : existsAfter a del = true := (Bool.and_eq_true_iff.mp h).1
    have hb : existsAfter b del = true := (Bool.and_eq_true_iff.mp h).2
    rw [existsAfter, List.any_eq_true] at ha hb ⊢
    rcases ha with ⟨p, hpa, hpl⟩
    rcases hb with ⟨q, hqb, hql⟩
    refine ⟨p ++ q, ?_, ?_⟩
    · rw [product]
      exact List.mem_flatMap.mpr ⟨p, hpa, List.mem_map.mpr ⟨q, hqb, rfl⟩⟩
    · simpa [live_witness_append] using (Bool.and_eq_true_iff.mpr ⟨hpl, hql⟩)

private theorem cuts_product (a b : Family) (c : Witness) :
    cuts (product a b) c = (cuts a c || cuts b c) := by
  rw [cuts, cuts, cuts, existsAfter_product]
  cases existsAfter a c <;> cases existsAfter b c <;> simp

theorem sound_ProductCut : TinylambOpt.ResearchGoals.Frontiers.Provenance.ProductCut.Goal := by
  intro a b candidates ca cb hca hcb
  unfold cutCost at hca hcb ⊢
  let A := candidates.filter (cuts a)
  let B := candidates.filter (cuts b)
  let P := candidates.filter (cuts (product a b))
  have hA : (A.map List.length).min? = some ca := by simpa [A] using hca
  have hB : (B.map List.length).min? = some cb := by simpa [B] using hcb
  have hAprop := List.min?_eq_some_iff.mp hA
  have hBprop := List.min?_eq_some_iff.mp hB
  have hA_bound : ∀ c ∈ candidates, cuts a c = true → ca ≤ c.length := by
    intro c hcc hcut
    have hcA : c ∈ A := by rw [List.mem_filter]; exact ⟨hcc, hcut⟩
    have hcl : c.length ∈ A.map List.length := List.mem_map.mpr ⟨c, hcA, rfl⟩
    exact hAprop.2 c.length hcl
  have hB_bound : ∀ c ∈ candidates, cuts b c = true → cb ≤ c.length := by
    intro c hcc hcut
    have hcB : c ∈ B := by rw [List.mem_filter]; exact ⟨hcc, hcut⟩
    have hcl : c.length ∈ B.map List.length := List.mem_map.mpr ⟨c, hcB, rfl⟩
    exact hBprop.2 c.length hcl
  apply List.min?_eq_some_iff.mpr
  constructor
  · have hcamem : ca ∈ A.map List.length := hAprop.1
    rcases List.mem_map.mp hcamem with ⟨c, hcA, hclen⟩
    have hcAc := List.mem_filter.mp hcA
    have hcP : c ∈ P := by
      rw [List.mem_filter]
      constructor
      · exact hcAc.1
      · have hcp := cuts_product a b c
        rw [hcAc.2] at hcp
        simpa using hcp
    have hcbmem : cb ∈ B.map List.length := hBprop.1
    rcases List.mem_map.mp hcbmem with ⟨c2, hcB, hclen2⟩
    have hcBc := List.mem_filter.mp hcB
    have hcP2 : c2 ∈ P := by
      rw [List.mem_filter]
      constructor
      · exact hcBc.1
      · have hcp := cuts_product a b c2
        rw [hcBc.2] at hcp
        simp at hcp
        exact hcp
    by_cases hle : ca ≤ cb
    · rw [Nat.min_eq_left hle]
      exact List.mem_map.mpr ⟨c, hcP, hclen⟩
    · have hle' : cb ≤ ca := Nat.le_of_not_ge hle
      rw [Nat.min_eq_right hle']
      exact List.mem_map.mpr ⟨c2, hcP2, hclen2⟩
  · intro n hn
    rcases List.mem_map.mp hn with ⟨c, hcP, hclen⟩
    rw [← hclen]
    have hcPc := List.mem_filter.mp hcP
    have hcp : cuts (product a b) c = true := hcPc.2
    have hor := cuts_product a b c
    rw [hcp] at hor
    rcases Bool.or_eq_true_iff.mp hor.symm with hcutA | hcutB
    · have h1 := hA_bound c hcPc.1 hcutA
      have h2 := Nat.min_le_left ca cb
      omega
    · have h1 := hB_bound c hcPc.1 hcutB
      have h2 := Nat.min_le_right ca cb
      omega

private theorem subset_erase (d : Nat) (del : Witness) : subset (del.erase d) del = true := by
  rw [subset]
  apply List.all_eq_true.mpr
  intro x hx
  exact List.contains_iff_mem.mpr (List.mem_of_mem_erase hx)

private theorem pack_dead_length_le (pack : Family) : ∀ del : Witness,
    pack.Pairwise (fun a b => disjoint a b = true) →
    (∀ w ∈ pack, live del w ≠ true) → pack.length ≤ del.length := by
  induction pack with
  | nil => intro del hdisj hdead; simp
  | cons w ws ih =>
      intro del hdisj hdead
      have hwdead : live del w ≠ true := hdead w (by simp)
      have hwany : w.any del.contains = true := by
        by_cases h : w.any del.contains = true
        · exact h
        · exfalso
          apply hwdead
          rw [live]
          simp [Bool.eq_false_of_ne_true h]
      rcases List.any_eq_true.mp hwany with ⟨d, hdw, hddBool⟩
      have hdd : d ∈ del := List.contains_iff_mem.mp hddBool
      have hpair := List.pairwise_cons.mp hdisj
      have htaildead : ∀ v ∈ ws, live (del.erase d) v ≠ true := by
        intro v hv hlv
        have hdnotv : d ∉ v := by
          intro hdvin
          have hdis := hpair.1 v hv
          rw [disjoint] at hdis
          have hwanyv : w.any v.contains = true :=
            List.any_eq_true.mpr ⟨d, hdw, List.contains_iff_mem.mpr hdvin⟩
          simp [hwanyv] at hdis
        have hdlive : live del v = true := by
          rw [live_true_iff] at hlv ⊢
          intro x hx hxd
          have hxne : x ≠ d := by intro heq; exact hdnotv (by simpa [heq] using hx)
          have hxdel : x ∈ del.erase d := (List.mem_erase_of_ne hxne).mpr hxd
          exact hlv x hx hxdel
        exact hdead v (by simp [hv]) hdlive
      have ih' := ih (del.erase d) hpair.2 htaildead
      rw [List.length_erase] at ih'
      have ih'' : ws.length ≤ del.length - 1 := by simpa [hdd] using ih'
      clear ih'
      have hdelpos : 0 < del.length := List.length_pos_of_mem hdd
      simp
      omega

theorem sound_PackingCertificate : TinylambOpt.ResearchGoals.Frontiers.Provenance.PackingCertificate.Goal := by
  intro ws pack del hmem hdisj hlen
  by_cases hres : existsAfter ws del = true
  · exact hres
  · exfalso
    have hdead : ∀ w ∈ pack, live del w ≠ true := by
      intro w hw hlive
      apply hres
      rw [existsAfter, List.any_eq_true]
      exact ⟨w, hmem w hw, hlive⟩
    have hle := pack_dead_length_le pack del hdisj hdead
    omega

theorem refutation_PackingExact :
    ¬ TinylambOpt.ResearchGoals.Frontiers.Provenance.PackingExact.Goal := by
  intro h
  have hnonempty : ∀ w ∈ ([[0, 1], [1, 2], [0, 2]] : Family), w ≠ [] := by native_decide
  have hres := h [[0, 1], [1, 2], [0, 2]] hnonempty
  have hbad : cutCost ([[0, 1], [1, 2], [0, 2]] : Family)
      (choices ([0, 1, 2] : Witness)) ≠ some (packing ([[0, 1], [1, 2], [0, 2]] : Family)) := by
    native_decide
  exact hbad hres

private theorem mem_of_mem_choices {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    ∀ {ys : List α}, ys ∈ choices xs → ∀ y ∈ ys, y ∈ xs := by
  induction xs with
  | nil =>
      intro ys h
      simp [choices] at h
      subst ys
      simp
  | cons x xs ih =>
      intro ys h
      simp [choices] at h
      rcases h with hrest | ⟨zs, hzmem, hyeq⟩
      · intro y hy
        exact List.mem_cons_of_mem x (ih hrest y hy)
      · intro y hy
        have hyl : y ∈ x :: zs := by simpa [hyeq] using hy
        rw [List.mem_cons] at hyl
        rcases hyl with rfl | hy
        · simp
        · exact List.mem_cons_of_mem x (ih hzmem y hy)

private theorem getElem_mem_zipIdxK {α : Type} (l : List α) (k i : Nat) (hi : i < l.length) :
    (l[i], i + k) ∈ l.zipIdx k := by
  induction l generalizing k i with
  | nil => simp at hi
  | cons a as ih =>
      rw [List.zipIdx_cons]
      cases i with
      | zero => simp
      | succ j =>
          simp only [List.mem_cons]
          right
          have hj : j < as.length := by simpa using hi
          have hrec := ih (k + 1) j hj
          simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hrec

private theorem pairwise_of_zipIdx_condition {ps : Family}
    (h : ps.zipIdx.all (fun (w, i) => (ps.drop (i + 1)).all (fun v => disjoint w v)) = true) :
    ps.Pairwise (fun a b => disjoint a b = true) := by
  rw [List.pairwise_iff_getElem]
  intro i j hi hj hij
  have hzi : (ps[i], i) ∈ ps.zipIdx := by simpa using getElem_mem_zipIdxK ps 0 i hi
  have hcond : (ps.drop (i + 1)).all (fun v => disjoint ps[i] v) = true :=
    List.all_eq_true.mp h (ps[i], i) hzi
  have hjmem : ps[j] ∈ ps.drop (i + 1) := by
    rw [List.mem_drop_iff_getElem]
    refine ⟨j - (i + 1), ?_, ?_⟩
    · omega
    · congr 1
      omega
  exact List.all_eq_true.mp hcond (ps[j]) hjmem

private theorem packing_le_length_of_all_dead (ws : Family) (del : Witness)
    (hdead : ∀ w ∈ ws, live del w ≠ true) : packing ws ≤ del.length := by
  dsimp [packing]
  let good := (choices ws).filter (fun ps =>
    ps.all (fun a => !a.isEmpty) && ps.zipIdx.all (fun (w, i) => (ps.drop (i + 1)).all (disjoint w)))
  change ((good.map List.length).max?).getD 0 ≤ del.length
  cases hm : (good.map List.length).max? with
  | none => simp
  | some k =>
      simp
      have hprop := List.max?_eq_some_iff.mp hm
      rcases List.mem_map.mp hprop.1 with ⟨ps, hpsgood, hpslen⟩
      have hpschoice : ps ∈ choices ws := (List.mem_filter.mp hpsgood).1
      have hpsmem : ∀ w ∈ ps, w ∈ ws := mem_of_mem_choices ws hpschoice
      have hpscond := (List.mem_filter.mp hpsgood).2
      have hzip : ps.zipIdx.all (fun (w, i) => (ps.drop (i + 1)).all (fun v => disjoint w v)) = true :=
        (Bool.and_eq_true_iff.mp hpscond).2
      have hdisj := pairwise_of_zipIdx_condition hzip
      have hle := pack_dead_length_le ps del hdisj (fun w hw => hdead w (hpsmem w hw))
      rw [← hpslen]
      exact hle

theorem sound_GuardedAnti : TinylambOpt.ResearchGoals.Frontiers.Provenance.GuardedAnti.Goal := by
  intro rows alts del cap hcap
  induction rows with
  | nil => simp
  | cons r rs ih =>
      by_cases hr : existsAfter (alts r) del = true
      · simp [hr, ih]
      · have hrfalse : existsAfter (alts r) del = false := Bool.eq_false_of_ne_true hr
        have hdead : ∀ w ∈ alts r, live del w ≠ true := by
          intro w hw hlive
          apply hr
          rw [existsAfter, List.any_eq_true]
          exact ⟨w, hw, hlive⟩
        have hpack := packing_le_length_of_all_dead (alts r) del hdead
        have hlecap : packing (alts r) ≤ cap := by omega
        simp [hrfalse, hlecap, ih]

theorem sound_UniversalImplication :
    TinylambOpt.ResearchGoals.Frontiers.Provenance.UniversalImplication.Goal := by
  intro a b
  constructor
  · intro h p hp
    let del := b.flatten.filter (fun x => !(p.contains x))
    have hp_live : live del p = true := by
      rw [live_true_iff]
      intro x hx hxd
      have hxp : (p.contains x) = true := List.contains_iff_mem.mpr hx
      have hxdel' : x ∈ b.flatten.filter (fun y => !(p.contains y)) := by simpa [del] using hxd
      have hf : (!(p.contains x)) = true := (List.mem_filter.mp hxdel').2
      have hf' : (!(p.contains x)) = false := by
        rw [hxp]
        rfl
      rw [hf'] at hf
      cases hf
    have hexA : existsAfter a del = true := by
      rw [existsAfter, List.any_eq_true]
      exact ⟨p, hp, hp_live⟩
    have hexB := h del hexA
    rw [existsAfter, List.any_eq_true] at hexB
    rcases hexB with ⟨q, hqb, hqlive⟩
    refine ⟨q, hqb, ?_⟩
    rw [subset, List.all_eq_true]
    intro x hxq
    by_cases hxp : x ∈ p
    · exact List.contains_iff_mem.mpr hxp
    · exfalso
      have hxflat : x ∈ b.flatten := List.mem_flatten.mpr ⟨q, hqb, hxq⟩
      have hxpBool : (p.contains x) = false := Bool.eq_false_of_ne_true (by
        intro ht
        exact hxp (List.contains_iff_mem.mp ht))
      have hxdel : x ∈ del := by
        change x ∈ b.flatten.filter (fun y => !(p.contains y))
        exact List.mem_filter.mpr ⟨hxflat, by simpa [hxpBool]⟩
      exact (live_true_iff del q).mp hqlive x hxq hxdel
  · intro h del hexA
    rw [existsAfter, List.any_eq_true] at hexA ⊢
    rcases hexA with ⟨p, hpa, hplive⟩
    rcases h p hpa with ⟨q, hqb, hqsub⟩
    exact ⟨q, hqb, live_of_subset hqsub hplive⟩


private theorem bip_nodup_filter {α : Type} [BEq α] [LawfulBEq α] {l : List α} (h : l.Nodup) (p : α → Bool) :
    (l.filter p).Nodup := by
  exact List.Nodup.sublist List.filter_sublist h

private theorem bip_eraseDups_length_le (l : List Nat) : l.eraseDups.length ≤ l.length := by
  exact length_le_of_nodup_subset (nodup_eraseDups l) (fun x hx => List.mem_eraseDups.mp hx)

private theorem bip_eraseDups_append_length_le (a b : List Nat) :
    (a ++ b).eraseDups.length ≤ a.eraseDups.length + b.eraseDups.length := by
  have hmem : ∀ x ∈ (a++b).eraseDups, x ∈ a.eraseDups ++ b.eraseDups := by
    intro x hx
    have hxab : x ∈ a ++ b := List.mem_eraseDups.mp hx
    rcases List.mem_append.mp hxab with hxa | hxb
    · exact List.mem_append.mpr (Or.inl (List.mem_eraseDups.mpr hxa))
    · exact List.mem_append.mpr (Or.inr (List.mem_eraseDups.mpr hxb))
  have h := length_le_of_nodup_subset (nodup_eraseDups (a++b)) hmem
  simpa using h

private theorem bip_sublist_mem_choices {α : Type} [BEq α] [LawfulBEq α]
    {xs ys : List α} (h : ys.Sublist xs) : ys ∈ choices xs := by
  induction h with
  | slnil => simp [choices]
  | cons a h ih =>
      simp [choices, ih]
  | cons_cons a h ih =>
      simp [choices, ih]

private theorem bip_filter_mem_choices {α : Type} [BEq α] [LawfulBEq α] (xs : List α) (p : α → Bool) :
    xs.filter p ∈ choices xs := bip_sublist_mem_choices List.filter_sublist

private def bip_lefts (E : List (Nat × Nat)) : List Nat := (E.map Prod.fst).eraseDups
private def bip_rights (E : List (Nat × Nat)) : List Nat := (E.map Prod.snd).eraseDups
private def bip_neigh (E : List (Nat × Nat)) (S : List Nat) : List Nat :=
  ((E.filter (fun e => e.1 ∈ S)).map Prod.snd).eraseDups

private theorem bip_mem_neigh (E : List (Nat × Nat)) (S : List Nat) (r : Nat) :
    r ∈ bip_neigh E S ↔ ∃ e ∈ E, e.1 ∈ S ∧ e.2 = r := by
  constructor
  · intro hx
    have hx1 : r ∈ (E.filter (fun e => e.1 ∈ S)).map Prod.snd := List.mem_eraseDups.mp hx
    rcases List.mem_map.mp hx1 with ⟨e, hef, heq⟩
    have he := List.mem_filter.mp hef
    exact ⟨e, he.1, of_decide_eq_true he.2, heq⟩
  · rintro ⟨e, heE, heS, hr⟩
    apply List.mem_eraseDups.mpr
    apply List.mem_map.mpr
    exact ⟨e, List.mem_filter.mpr ⟨heE, decide_eq_true heS⟩, hr⟩

private theorem bip_neigh_nodup (E : List (Nat × Nat)) (S : List Nat) : (bip_neigh E S).Nodup := nodup_eraseDups _

private theorem bip_mem_lefts (E : List (Nat × Nat)) (l : Nat) : l ∈ bip_lefts E ↔ ∃ e ∈ E, e.1 = l := by
  constructor
  · intro hx
    have hx1 : l ∈ E.map Prod.fst := List.mem_eraseDups.mp hx
    rcases List.mem_map.mp hx1 with ⟨e, heE, heq⟩
    exact ⟨e, heE, heq⟩
  · rintro ⟨e, heE, heq⟩
    apply List.mem_eraseDups.mpr
    exact List.mem_map.mpr ⟨e, heE, heq⟩

private theorem bip_mem_rights (E : List (Nat × Nat)) (r : Nat) : r ∈ bip_rights E ↔ ∃ e ∈ E, e.2 = r := by
  constructor
  · intro hx
    have hx1 : r ∈ E.map Prod.snd := List.mem_eraseDups.mp hx
    rcases List.mem_map.mp hx1 with ⟨e, heE, heq⟩
    exact ⟨e, heE, heq⟩
  · rintro ⟨e, heE, heq⟩
    apply List.mem_eraseDups.mpr
    exact List.mem_map.mpr ⟨e, heE, heq⟩


private def bip_ok_exact (E : List (Nat × Nat)) (S : List Nat) (e : Nat × Nat) : Bool :=
  e.1 ∉ S ∧ e.2 ∉ bip_neigh E S

private theorem bip_length_filter_compl {α : Type} (L : List α) (p : α → Bool) :
    (L.filter p).length + (L.filter (fun x => !p x)).length = L.length := by
  induction L with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h]
        omega
      · have hf : p x = false := Bool.eq_false_of_ne_true h
        simp [hf]
        omega

private theorem bip_filter_count_subset {L S : List Nat} (hL : L.Nodup) (hS : S.Nodup)
    (hsub : ∀ x ∈ S, x ∈ L) : (L.filter (fun v => v ∈ S)).length = S.length := by
  have hfN : (L.filter (fun v => v ∈ S)).Nodup := bip_nodup_filter hL _
  have hle1 : S.length ≤ (L.filter (fun v => v ∈ S)).length := by
    apply length_le_of_nodup_subset hS
    intro x hx
    exact List.mem_filter.mpr ⟨hsub x hx, by simp [hx]⟩
  have hle2 : (L.filter (fun v => v ∈ S)).length ≤ S.length := by
    apply length_le_of_nodup_subset hfN
    intro x hx
    exact of_decide_eq_true (List.mem_filter.mp hx).2
  omega

private theorem bip_neigh_filter_left_len (E : List (Nat × Nat)) (S U : List Nat)
    (hU : ∀ x ∈ U, x ∈ S) :
    (bip_neigh (E.filter (fun e => e.1 ∈ S)) U).length = (bip_neigh E U).length := by
  have hmem1 : ∀ x ∈ bip_neigh (E.filter (fun e => e.1 ∈ S)) U, x ∈ bip_neigh E U := by
    intro x hx
    rcases (bip_mem_neigh _ _ _).mp hx with ⟨e, heE, heU, rfl⟩
    exact (bip_mem_neigh _ _ _).mpr ⟨e, (List.mem_filter.mp heE).1, heU, rfl⟩
  have hmem2 : ∀ x ∈ bip_neigh E U, x ∈ bip_neigh (E.filter (fun e => e.1 ∈ S)) U := by
    intro x hx
    rcases (bip_mem_neigh _ _ _).mp hx with ⟨e, heE, heU, rfl⟩
    exact (bip_mem_neigh _ _ _).mpr ⟨e, List.mem_filter.mpr ⟨heE, by simp [hU _ heU]⟩, heU, rfl⟩
  have h1 := length_le_of_nodup_subset (bip_neigh_nodup _ _) hmem1
  have h2 := length_le_of_nodup_subset (bip_neigh_nodup _ _) hmem2
  omega

private theorem bip_neigh_filter_lr_len (E : List (Nat × Nat)) (l0 r0 : Nat) (U : List Nat)
    (hl0 : l0 ∉ U) :
    (bip_neigh (E.filter (fun e => e.1 ≠ l0 ∧ e.2 ≠ r0)) U).length =
      ((bip_neigh E U).erase r0).length := by
  have hmem1 : ∀ x ∈ bip_neigh (E.filter (fun e => e.1 ≠ l0 ∧ e.2 ≠ r0)) U,
      x ∈ (bip_neigh E U).erase r0 := by
    intro x hx
    rcases (bip_mem_neigh _ _ _).mp hx with ⟨e, heE, heU, rfl⟩
    have hfil := List.mem_filter.mp heE
    have hxN : e.2 ∈ bip_neigh E U := (bip_mem_neigh _ _ _).mpr ⟨e, hfil.1, heU, rfl⟩
    have hcond : e.1 ≠ l0 ∧ e.2 ≠ r0 := of_decide_eq_true hfil.2
    exact (List.mem_erase_of_ne hcond.2).mpr hxN
  have hmem2 : ∀ x ∈ (bip_neigh E U).erase r0,
      x ∈ bip_neigh (E.filter (fun e => e.1 ≠ l0 ∧ e.2 ≠ r0)) U := by
    intro x hx
    have hxN : x ∈ bip_neigh E U := List.mem_of_mem_erase hx
    have hxne : x ≠ r0 := by
      intro heq
      subst x
      exact (bip_neigh_nodup E U).not_mem_erase hx
    rcases (bip_mem_neigh _ _ _).mp hxN with ⟨e, heE, heU, hr⟩
    have hl0ne : e.1 ≠ l0 := by
      intro heq
      apply hl0
      simpa [heq] using heU
    have hxne' : e.2 ≠ r0 := by simpa [hr] using hxne
    exact (bip_mem_neigh _ _ _).mpr ⟨e, List.mem_filter.mpr ⟨heE, decide_eq_true ⟨hl0ne, hxne'⟩⟩, heU, hr⟩
  have h1 := length_le_of_nodup_subset (bip_neigh_nodup _ _) hmem1
  have h2 := length_le_of_nodup_subset (List.Nodup.erase r0 (bip_neigh_nodup E U)) hmem2
  omega

private theorem bip_neigh_filter_exact_len (E : List (Nat × Nat)) (S U : List Nat) :
    (bip_neigh (E.filter (bip_ok_exact E S)) U).length =
      ((bip_neigh E U).removeAll (bip_neigh E S)).length := by
  have hmem1 : ∀ x ∈ bip_neigh (E.filter (bip_ok_exact E S)) U,
      x ∈ (bip_neigh E U).removeAll (bip_neigh E S) := by
    intro x hx
    rcases (bip_mem_neigh _ _ _).mp hx with ⟨e, heE, heU, rfl⟩
    have hfil := List.mem_filter.mp heE
    have hcond : e.1 ∉ S ∧ e.2 ∉ bip_neigh E S := of_decide_eq_true hfil.2
    have hxN : e.2 ∈ bip_neigh E U := (bip_mem_neigh _ _ _).mpr ⟨e, hfil.1, heU, rfl⟩
    have hxnot : e.2 ∉ bip_neigh E S := hcond.2
    exact List.mem_filter.mpr ⟨hxN, by simp [hxnot]⟩
  have hmem2 : ∀ x ∈ (bip_neigh E U).removeAll (bip_neigh E S),
      x ∈ bip_neigh (E.filter (bip_ok_exact E S)) U := by
    intro x hx
    have hxF := List.mem_filter.mp hx
    have hxN : x ∈ bip_neigh E U := hxF.1
    have hxF' : x ∈ bip_neigh E U ∧ x ∉ bip_neigh E S := by
      simpa using hxF
    have hxnot : x ∉ bip_neigh E S := hxF'.2
    rcases (bip_mem_neigh _ _ _).mp hxN with ⟨e, heE, heU, hr⟩
    have heSnot : e.1 ∉ S := by
      intro he1
      have : e.2 ∈ bip_neigh E S := (bip_mem_neigh _ _ _).mpr ⟨e, heE, he1, rfl⟩
      exact hxnot (by simpa [hr] using this)
    have hxnot' : e.2 ∉ bip_neigh E S := by simpa [hr] using hxnot
    exact (bip_mem_neigh _ _ _).mpr ⟨e, List.mem_filter.mpr ⟨heE, decide_eq_true ⟨heSnot, hxnot'⟩⟩, heU, hr⟩
  have h1 := length_le_of_nodup_subset (bip_neigh_nodup _ _) hmem1
  have h2 := length_le_of_nodup_subset (bip_nodup_filter (bip_neigh_nodup E U) _) hmem2
  exact Nat.le_antisymm h1 h2

private theorem bip_hall_exact_step (E : List (Nat × Nat)) (L S U : List Nat)
    (_hL : L.Nodup) (hS : S.Nodup) (hU : U.Nodup)
    (hSsub : ∀ x ∈ S, x ∈ L) (hUsub : ∀ x ∈ U, x ∈ L)
    (hSUdisj : ∀ x, x ∈ S → x ∈ U → False)
    (hHall : ∀ T : List Nat, T.Nodup → (∀ x ∈ T, x ∈ L) → T.length ≤ (bip_neigh E T).length)
    (hSeq : (bip_neigh E S).length = S.length) :
    U.length ≤ (bip_neigh (E.filter (bip_ok_exact E S)) U).length := by
  have hSUnodup : (S ++ U).Nodup := by
    rw [List.nodup_append]
    exact ⟨hS, hU, fun a ha b hb heq => hSUdisj a ha (by simpa [heq] using hb)⟩
  have hSUsub : ∀ x ∈ S ++ U, x ∈ L := by
    intro x hx
    rcases List.mem_append.mp hx with hx | hx
    · exact hSsub x hx
    · exact hUsub x hx
  have hH := hHall (S ++ U) hSUnodup hSUsub
  have hlenSU : (S ++ U).length = S.length + U.length := by simp
  have hNnodup : (bip_neigh E (S ++ U)).Nodup := bip_neigh_nodup _ _
  have hNSnodup : (bip_neigh E S).Nodup := bip_neigh_nodup _ _
  let Nout := (bip_neigh E U).removeAll (bip_neigh E S)
  have hNoutnodup : Nout.Nodup := bip_nodup_filter (bip_neigh_nodup E U) _
  have hmem : ∀ x ∈ bip_neigh E (S ++ U), x ∈ bip_neigh E S ++ Nout := by
    intro x hx
    rcases (bip_mem_neigh _ _ _).mp hx with ⟨e, heE, heSU, rfl⟩
    rcases List.mem_append.mp heSU with heS | heU
    · exact List.mem_append.mpr (Or.inl ((bip_mem_neigh _ _ _).mpr ⟨e, heE, heS, rfl⟩))
    · by_cases hxS : e.2 ∈ bip_neigh E S
      · exact List.mem_append.mpr (Or.inl hxS)
      · have hxN : e.2 ∈ bip_neigh E U := (bip_mem_neigh _ _ _).mpr ⟨e, heE, heU, rfl⟩
        exact List.mem_append.mpr (Or.inr (List.mem_filter.mpr ⟨hxN, by simp [hxS]⟩))
  have hle := length_le_of_nodup_subset hNnodup hmem
  have hlenR : (bip_neigh E S ++ Nout).length = (bip_neigh E S).length + Nout.length := by simp
  have hle' : (bip_neigh E (S ++ U)).length ≤ (bip_neigh E S).length + Nout.length := by
    simpa [hlenR] using hle
  have hEq := bip_neigh_filter_exact_len E S U
  have hres : U.length ≤ Nout.length := by omega
  simpa [Nout, hEq] using hres

private theorem bip_hall_strict_step (E : List (Nat × Nat)) (L : List Nat) (l0 r0 : Nat)
    (hl0L : l0 ∈ L) (U : List Nat) (hU : U.Nodup) (hUsub : ∀ x ∈ U, x ∈ L) (hl0notU : l0 ∉ U)
    (hstrict : ∀ S : List Nat, S.Nodup → (∀ x ∈ S, x ∈ L) → S ≠ [] →
        (∃ x ∈ L, x ∉ S) → S.length < (bip_neigh E S).length) :
    U.length ≤ (bip_neigh (E.filter (fun e => e.1 ≠ l0 ∧ e.2 ≠ r0)) U).length := by
  rw [bip_neigh_filter_lr_len E l0 r0 U hl0notU]
  cases U with
  | nil => simp
  | cons u us =>
      have hproper : ∃ x ∈ L, x ∉ (u :: us) := ⟨l0, hl0L, hl0notU⟩
      have hgt := hstrict (u :: us) hU hUsub (by simp) hproper
      rw [List.length_erase]
      by_cases hr : r0 ∈ bip_neigh E (u :: us)
      · simp [hr]
        exact Nat.le_sub_one_of_lt hgt
      · simp [hr]
        exact Nat.le_trans (Nat.le_of_lt hgt) (by omega)


private def bip_validM (E : List (Nat × Nat)) (M : List (Nat × Nat)) : Prop :=
  (∀ e ∈ M, e ∈ E) ∧ M.Pairwise (fun a b => a.1 ≠ b.1 ∧ a.2 ≠ b.2)

private def bip_Hall (L : List Nat) (E : List (Nat × Nat)) : Prop :=
  (∀ e ∈ E, e.1 ∈ L) ∧
    ∀ S : List Nat, S.Nodup → (∀ x ∈ S, x ∈ L) → S.length ≤ (bip_neigh E S).length

private theorem bip_hall_matching : ∀ (L : List Nat) (E : List (Nat × Nat)), L.Nodup → bip_Hall L E →
    ∃ M : List (Nat × Nat), bip_validM E M ∧ M.length = L.length := by
  let P : Nat → Prop := fun n => ∀ (L : List Nat) (E : List (Nat × Nat)), L.Nodup →
    L.length = n → bip_Hall L E → ∃ M : List (Nat × Nat), bip_validM E M ∧ M.length = L.length
  intro L E hL hHall
  have H : P L.length := by
    apply Nat.strongRecOn
    intro n ih L E hL hlen hHall
    cases L with
    | nil =>
        refine ⟨[], ?_, by simp⟩
        constructor
        · intro e he; cases he
        · simp
    | cons l0 Ltail =>
        have hLn : (l0 :: Ltail).Nodup := hL
        have htailnodup : Ltail.Nodup := (List.nodup_cons.mp hLn).2
        have hl0L : l0 ∈ (l0 :: Ltail) := by simp
        by_cases hexact : ∃ S : List Nat, S.Nodup ∧ (∀ x ∈ S, x ∈ (l0 :: Ltail)) ∧ S ≠ [] ∧
            (∃ x ∈ (l0 :: Ltail), x ∉ S) ∧ (bip_neigh E S).length = S.length
        · rcases hexact with ⟨S, hSnodup, hSsub, hSne, hSproper, hSeq⟩
          let E1 := E.filter (fun e => e.1 ∈ S)
          let L2 := (l0 :: Ltail).filter (fun v => !(v ∈ S))
          let E2 := E.filter (bip_ok_exact E S)
          have hSlt : S.length < n := by
            rw [← hlen]
            exact length_lt_of_nodup_subset_strict hSnodup hSsub hSproper
          have hL2nodup : L2.Nodup := bip_nodup_filter hLn _
          have hL2lt : L2.length < n := by
            rw [← hlen]
            apply length_lt_of_nodup_subset_strict hL2nodup
            · intro x hx; exact (List.mem_filter.mp hx).1
            · rcases List.exists_mem_of_ne_nil S hSne with ⟨x, hxS⟩
              exact ⟨x, hSsub x hxS, by
                intro hxL2
                have hxf : (!decide (x ∈ S)) = true := (List.mem_filter.mp hxL2).2
                have hdf : decide (x ∈ S) = false := by simpa using hxf
                have hxnotS : x ∉ S := of_decide_eq_false hdf
                exact hxnotS hxS⟩
          have hE1Hall : bip_Hall S E1 := by
            constructor
            · intro e he
              exact of_decide_eq_true (List.mem_filter.mp he).2
            · intro U hU hUsub
              exact (bip_neigh_filter_left_len E S U hUsub) ▸ hHall.2 U hU (fun x hx => hSsub x (hUsub x hx))
          have hE2Hall : bip_Hall L2 E2 := by
            constructor
            · intro e he
              have hf := List.mem_filter.mp he
              have hc : e.1 ∉ S ∧ e.2 ∉ bip_neigh E S := of_decide_eq_true hf.2
              have heL := hHall.1 e hf.1
              exact List.mem_filter.mpr ⟨heL, decide_eq_true hc.1⟩
            · intro U hU hUsub
              have hSUdisj : ∀ x, x ∈ S → x ∈ U → False := by
                intro x hxS hxU
                have hxL2 : x ∈ L2 := hUsub x hxU
                have hxf : (!decide (x ∈ S)) = true := (List.mem_filter.mp hxL2).2
                have hdf : decide (x ∈ S) = false := by simpa using hxf
                exact (of_decide_eq_false hdf) hxS
              have hUsubL : ∀ x ∈ U, x ∈ (l0 :: Ltail) := fun x hx => (List.mem_filter.mp (hUsub x hx)).1
              exact bip_hall_exact_step E (l0 :: Ltail) S U hLn hSnodup hU hSsub hUsubL hSUdisj
                (fun T hT hTsub => hHall.2 T hT hTsub) hSeq
          rcases ih S.length hSlt S E1 hSnodup rfl hE1Hall with ⟨M1, hM1⟩
          rcases ih L2.length hL2lt L2 E2 hL2nodup rfl hE2Hall with ⟨M2, hM2⟩
          refine ⟨M1 ++ M2, ?_, ?_⟩
          · constructor
            · intro e he
              rcases List.mem_append.mp he with he | he
              · exact (List.mem_filter.mp (hM1.1.1 e he)).1
              · exact (List.mem_filter.mp (hM2.1.1 e he)).1
            · rw [List.pairwise_append]
              refine ⟨hM1.1.2, hM2.1.2, ?_⟩
              intro a ha b hb
              have haE1 := hM1.1.1 a ha
              have hbE2 := hM2.1.1 b hb
              have haS : a.1 ∈ S := of_decide_eq_true (List.mem_filter.mp haE1).2
              have hbc : b.1 ∉ S ∧ b.2 ∉ bip_neigh E S := of_decide_eq_true (List.mem_filter.mp hbE2).2
              have haNS : a.2 ∈ bip_neigh E S := (bip_mem_neigh E S a.2).mpr ⟨a, (List.mem_filter.mp haE1).1, haS, rfl⟩
              exact ⟨fun h => hbc.1 (by simpa [h] using haS), fun h => hbc.2 (by simpa [h] using haNS)⟩
          · have hcount := bip_filter_count_subset hLn hSnodup hSsub
            have hpart := bip_length_filter_compl (l0 :: Ltail) (fun v => v ∈ S)
            have hsum : S.length + L2.length = (l0 :: Ltail).length := by
              dsimp [L2]
              rw [← hcount]
              exact hpart
            rw [List.length_append, hM1.2, hM2.2]
            exact hsum
        · have hstrict : ∀ T : List Nat, T.Nodup → (∀ x ∈ T, x ∈ (l0 :: Ltail)) → T ≠ [] →
              (∃ x ∈ (l0 :: Ltail), x ∉ T) → T.length < (bip_neigh E T).length := by
            intro T hT hTsub hTne hTproper
            have hltlen : T.length < (l0 :: Ltail).length :=
              length_lt_of_nodup_subset_strict hT hTsub hTproper
            have hle := hHall.2 T hT hTsub
            have hne : (bip_neigh E T).length ≠ T.length := by
              intro heq
              apply hexact
              exact ⟨T, hT, hTsub, hTne, hTproper, heq⟩
            omega
          have hpos : 0 < (bip_neigh E [l0]).length := by
            have h := hHall.2 [l0] (by simp) (by intro x hx; simp at hx; simp [hx])
            exact Nat.succ_le_iff.mp h
          have hr0ex : ∃ r0, r0 ∈ bip_neigh E [l0] := by
            cases hn : bip_neigh E [l0] with
            | nil => simp [hn] at hpos
            | cons r rs => exact ⟨r, by simp⟩
          rcases hr0ex with ⟨r0, hr0⟩
          rcases (bip_mem_neigh E [l0] r0).mp hr0 with ⟨e, heE, heL, heq⟩
          have hel0 : e.1 = l0 := by simpa using heL
          have her0 : e.2 = r0 := heq
          let E' := E.filter (fun e => e.1 ≠ l0 ∧ e.2 ≠ r0)
          let L' := (l0 :: Ltail).erase l0
          have hL'nodup : L'.Nodup := List.Nodup.erase l0 hLn
          have hL'lt : L'.length < n := by
            rw [← hlen, List.length_erase]
            have hmeml0 : l0 ∈ (l0 :: Ltail) := by simp
            simp
          have hE'Hall : bip_Hall L' E' := by
            constructor
            · intro f hf
              have hfF := List.mem_filter.mp hf
              have hc : f.1 ≠ l0 ∧ f.2 ≠ r0 := of_decide_eq_true hfF.2
              have hfL := hHall.1 f hfF.1
              exact (List.mem_erase_of_ne hc.1).mpr hfL
            · intro U hU hUsub
              have hl0notU : l0 ∉ U := by
                intro hl
                have hmem : l0 ∈ (l0 :: Ltail).erase l0 := by
                  change l0 ∈ L'
                  exact hUsub l0 hl
                exact hLn.not_mem_erase hmem
              have hUsubL : ∀ x ∈ U, x ∈ (l0 :: Ltail) := by
                intro x hx
                have hxL' := hUsub x hx
                simpa [L'] using (List.mem_of_mem_erase hxL')
              exact bip_hall_strict_step E (l0 :: Ltail) l0 r0 hl0L U hU hUsubL hl0notU hstrict
          rcases ih L'.length hL'lt L' E' hL'nodup rfl hE'Hall with ⟨M', hM'⟩
          refine ⟨(l0, r0) :: M', ?_, ?_⟩
          · constructor
            · intro f hf
              simp only [List.mem_cons] at hf
              rcases hf with rfl | hfM
              · have heqpair : e = (l0, r0) := by
                  cases e
                  simp_all
                simpa [heqpair] using heE
              · exact (List.mem_filter.mp (hM'.1.1 f hfM)).1
            · rw [List.pairwise_cons]
              constructor
              · intro f hfM
                have hfE' := hM'.1.1 f hfM
                have hc : f.1 ≠ l0 ∧ f.2 ≠ r0 := of_decide_eq_true (List.mem_filter.mp hfE').2
                exact ⟨fun h => hc.1 h.symm, fun h => hc.2 h.symm⟩
              · exact hM'.1.2
          · rw [List.length_cons, hM'.2, List.length_erase]
            have hmeml0 : l0 ∈ (l0 :: Ltail) := by simp
            simp [hmeml0]
  exact H L E hL rfl hHall


private theorem bip_choices_nonempty {α : Type} (L : List α) : choices L ≠ [] := by
  induction L with
  | nil => simp [choices]
  | cons x xs ih => simp [choices, ih]

private theorem bip_choices_nodup {α : Type} [BEq α] [LawfulBEq α] {L S : List α}
    (hL : L.Nodup) (hS : S ∈ choices L) : S.Nodup := by
  induction L generalizing S with
  | nil =>
      simp [choices] at hS
      subst S
      simp
  | cons x xs ih =>
      simp [choices] at hS
      rcases hS with hS | ⟨zs, hz, rfl⟩
      · exact ih (List.nodup_cons.mp hL).2 hS
      · apply List.nodup_cons.mpr
        constructor
        · intro hx
          have hx' : x ∈ xs := by
            apply mem_of_mem_choices xs hz
            simpa using hx
          exact (List.nodup_cons.mp hL).1 hx'
        · show zs.Nodup
          exact ih (List.nodup_cons.mp hL).2 hz

private theorem bip_neigh_len_congr (g : List (Nat × Nat)) {S T : List Nat}
    (h : ∀ x, x ∈ S ↔ x ∈ T) : (bip_neigh g S).length = (bip_neigh g T).length := by
  have h1 : ∀ x ∈ bip_neigh g S, x ∈ bip_neigh g T := by
    intro x hx
    rcases (bip_mem_neigh g S x).mp hx with ⟨e, he, hS, rfl⟩
    exact (bip_mem_neigh g T e.2).mpr ⟨e, he, (h e.1).mp hS, rfl⟩
  have h2 : ∀ x ∈ bip_neigh g T, x ∈ bip_neigh g S := by
    intro x hx
    rcases (bip_mem_neigh g T x).mp hx with ⟨e, he, hT, rfl⟩
    exact (bip_mem_neigh g S e.2).mpr ⟨e, he, (h e.1).mpr hT, rfl⟩
  have hl1 := length_le_of_nodup_subset (bip_neigh_nodup g S) h1
  have hl2 := length_le_of_nodup_subset (bip_neigh_nodup g T) h2
  omega

private theorem bip_pairwise_zipIdx {ps : Family} (h : ps.Pairwise (fun a b => disjoint a b = true)) :
    ps.zipIdx.all (fun (w, i) => (ps.drop (i + 1)).all (disjoint w)) = true := by
  rw [List.all_eq_true]
  rintro ⟨w, i⟩ hwi
  rw [List.all_eq_true]
  have hiw := List.mem_zipIdx' hwi
  rw [hiw.2]
  intro v hv
  rcases (List.mem_drop_iff_getElem.mp hv) with ⟨j, hj, hval⟩
  have hj' : i + 1 + j < ps.length := by omega
  have hidx : i < i + 1 + j := by omega
  have hdis := List.pairwise_iff_getElem.mp h i (i + 1 + j) hiw.1 hj' hidx
  rw [hval] at hdis
  exact hdis

private theorem bip_even_odd_ne (x y : Nat) : 2 * x ≠ 2 * y + 1 := by omega

private theorem bip_le_foldl_max (l : List Nat) (a : Nat) : a ≤ l.foldl Nat.max a := by
  induction l generalizing a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      exact Nat.le_trans (Nat.le_max_left a x) (ih (max a x))

private theorem bip_mem_le_foldl_max (l : List Nat) (a x : Nat) (hx : x ∈ l) :
    x ≤ l.foldl Nat.max a := by
  induction l generalizing a with
  | nil => cases hx
  | cons y ys ih =>
      simp only [List.foldl_cons]
      rcases List.mem_cons.mp hx with hyx | hxys
      · subst x
        exact Nat.le_trans (Nat.le_max_right a y) (bip_le_foldl_max ys (max a y))
      · exact ih (max a y) hxys









private theorem bip_eraseDups_sublist {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Sublist l := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Sublist l
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen
    cases l with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        have hlenf : (as.filter (fun b => !b == a)).length < n := by
          rw [← hlen]
          simp
          have hle := List.length_filter_le (fun b => !b == a) as
          omega
        have hs := ih _ hlenf (as.filter (fun b => !b == a)) rfl
        exact List.Sublist.cons_cons a (List.Sublist.trans hs List.filter_sublist)
  exact H l rfl

private theorem bip_nodup_map_inj {α β : Type} {f : α → β} {l : List α}
    (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (h : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨b, hb, hfb⟩
        have hab : b = a := hinj b (by simp [hb]) a (by simp) hfb
        exact (List.nodup_cons.mp h).1 (by simpa [hab] using hb)
      · exact ih (fun x hx y hy heq => hinj x (by simp [hx]) y (by simp [hy]) heq) (List.nodup_cons.mp h).2

private theorem bip_disjoint_pair (l r l' r' : Nat) (hll : l ≠ l') (hrr : r ≠ r')
    (hlr' : l ≠ r') (hrl' : r ≠ l') : disjoint [l, r] [l', r'] = true := by
  simp [disjoint, hll, hrr, hlr', hrl']

private theorem bip_count_filter_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    (xs ys : List α) (hxs : xs.Nodup) (hys : ys.Nodup) (hsub : ∀ y ∈ ys, y ∈ xs) :
    (xs.filter (fun x => ys.contains x)).length = ys.length := by
  induction xs generalizing ys with
  | nil =>
      cases ys with
      | nil => simp
      | cons y yt =>
          have hy : y ∈ ([] : List α) := hsub y (by simp)
          cases hy
  | cons a xs ih =>
      have hxs' : xs.Nodup := (List.nodup_cons.mp hxs).2
      have haxs : a ∉ xs := (List.nodup_cons.mp hxs).1
      by_cases ha : a ∈ ys
      · have hcta : ys.contains a = true := List.contains_iff_mem.mpr ha
        have hys' : (ys.erase a).Nodup := hys.erase a
        have hsub' : ∀ y ∈ ys.erase a, y ∈ xs := by
          intro y hy
          have hya : y ∈ ys := List.mem_of_mem_erase hy
          have hyne : y ≠ a := by
            intro heq
            subst y
            exact hys.not_mem_erase hy
          have hyxs : y ∈ a :: xs := hsub y hya
          rcases List.mem_cons.mp hyxs with rfl | hyx
          · exact False.elim (hyne rfl)
          · exact hyx
        have hrest := ih (ys.erase a) hxs' hys' hsub'
        have hyspos : 0 < ys.length := List.length_pos_of_mem ha
        have hysdecomp : (ys.erase a).length + 1 = ys.length := by
          rw [List.length_erase]
          simp [ha]
          omega
        have hfilter : (a :: xs).filter (fun x => ys.contains x) =
            a :: (xs.filter (fun x => ys.contains x)) := by
          simp [ha]
        have hfilterEq : (xs.filter (fun x => ys.contains x)) =
            (xs.filter (fun x => (ys.erase a).contains x)) := by
          apply List.filter_congr
          intro x hx
          by_cases hxa : x = a
          · subst x
            exact False.elim (haxs hx)
          · have hiff := List.mem_erase_of_ne hxa (l := ys)
            apply Bool.eq_iff_iff.mpr
            simpa [List.contains_iff_mem] using hiff.symm
        rw [hfilter]
        simp only [List.length_cons]
        rw [hfilterEq, hrest]
        omega
      · have hsubxs : ∀ y ∈ ys, y ∈ xs := by
          intro y hy
          have hyxs := hsub y hy
          rcases List.mem_cons.mp hyxs with rfl | hyx
          · exact False.elim (ha hy)
          · exact hyx
        have hnot : (ys.contains a) = false := Bool.eq_false_of_ne_true (by
          intro ht
          exact ha (List.contains_iff_mem.mp ht))
        have hfilter : (a :: xs).filter (fun x => ys.contains x) =
            xs.filter (fun x => ys.contains x) := by
          simp [ha]
        rw [hfilter]
        exact ih ys hxs' hys hsubxs


private theorem bip_eq_of_pairwise_fst {M : List (Nat × Nat)}
    (hM : M.Pairwise (fun a b => a.1 ≠ b.1)) :
    ∀ a ∈ M, ∀ b ∈ M, a.1 = b.1 → a = b := by
  induction M with
  | nil => simp
  | cons e es ih =>
      intro a ha b hb hab
      have hp := List.pairwise_cons.mp hM
      simp only [List.mem_cons] at ha hb
      rcases ha with rfl | ha
      · rcases hb with rfl | hb
        · rfl
        · exfalso
          exact hp.1 b hb hab
      · rcases hb with rfl | hb
        · exfalso
          exact hp.1 a ha hab.symm
        · exact ih hp.2 a ha b hb hab

private theorem bip_eq_of_pairwise_snd {M : List (Nat × Nat)}
    (hM : M.Pairwise (fun a b => a.2 ≠ b.2)) :
    ∀ a ∈ M, ∀ b ∈ M, a.2 = b.2 → a = b := by
  induction M with
  | nil => simp
  | cons e es ih =>
      intro a ha b hb hab
      have hp := List.pairwise_cons.mp hM
      simp only [List.mem_cons] at ha hb
      rcases ha with rfl | ha
      · rcases hb with rfl | hb
        · rfl
        · exfalso
          exact hp.1 b hb hab
      · rcases hb with rfl | hb
        · exfalso
          exact hp.1 a ha hab.symm
        · exact ih hp.2 a ha b hb hab

private theorem bip_pairwise_ne_of_mem {M : List (Nat × Nat)}
    (hM : M.Pairwise (fun a b => a.1 ≠ b.1 ∧ a.2 ≠ b.2)) :
    ∀ a ∈ M, ∀ b ∈ M, a ≠ b → a.1 ≠ b.1 ∧ a.2 ≠ b.2 := by
  induction M with
  | nil => simp
  | cons e es ih =>
      intro a ha b hb hab
      have hp := List.pairwise_cons.mp hM
      simp only [List.mem_cons] at ha hb
      rcases ha with rfl | ha
      · rcases hb with rfl | hb
        · exact False.elim (hab rfl)
        · exact hp.1 b hb
      · rcases hb with rfl | hb
        · have h := hp.1 a ha
          exact ⟨fun hEq => h.1 hEq.symm, fun hEq => h.2 hEq.symm⟩
        · exact ih hp.2 a ha b hb hab



private theorem bip_nil_mem_choices {α : Type} (L : List α) : ([] : List α) ∈ choices L := by
  induction L with
  | nil => simp [choices]
  | cons x xs ih => simpa [choices] using ih

private theorem bip_nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen
    cases l with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        apply List.nodup_cons.mpr
        constructor
        · intro ha
          have hfa : a ∈ as.filter (fun b => !b == a) := List.mem_eraseDups.mp ha
          simp at hfa
        · have hle : (as.filter (fun b => !b == a)).length ≤ as.length :=
            List.length_filter_le (fun b => !b == a) as
          have hlt : (as.filter (fun b => !b == a)).length < n := by
            simp at hlen
            omega
          exact ih _ hlt _ rfl
  exact H l rfl

private theorem bip_nodup_of_pairwise_fst {M : List (Nat × Nat)}
    (h : M.Pairwise (fun a b => a.1 ≠ b.1)) : M.Nodup := by
  induction M with
  | nil => simp
  | cons e es ih =>
      rw [List.nodup_cons]
      have hp := List.pairwise_cons.mp h
      exact ⟨fun he => hp.1 e he rfl, ih hp.2⟩

theorem sound_BipartiteCut : TinylambOpt.ResearchGoals.Frontiers.Provenance.BipartiteCut.Goal := by
  intro edges
  let g : List (Nat × Nat) := edges.map (fun (x, y) => (2 * x, 2 * y + 1))
  let ws : Family := edges.map (fun (x, y) => [2 * x, 2 * y + 1])
  let L := bip_lefts g
  have hLnodup : L.Nodup := by dsimp [L]; exact nodup_eraseDups _
  let diffs := (choices L).map (fun S => S.length - (bip_neigh g S).length)
  let δ := diffs.max?.getD 0
  have hdiffsne : diffs ≠ [] := by
    dsimp [diffs]
    intro h
    have hnil : choices L = [] := List.map_eq_nil_iff.mp h
    exact bip_choices_nonempty L hnil
  have hmax : diffs.max? = some δ := by
    cases hm : diffs.max? with
    | none =>
        have hnil : diffs = [] := List.max?_eq_none_iff.mp hm
        exact False.elim (hdiffsne hnil)
    | some d =>
        dsimp [δ]
        simp [hm]
  have hmaxprop := List.max?_eq_some_iff.mp hmax
  rcases List.mem_map.mp hmaxprop.1 with ⟨Smax, hSmaxC, hSmaxd⟩
  let S0 := if δ = 0 then ([] : List Nat) else Smax
  have hS0C : S0 ∈ choices L := by
    by_cases h : δ = 0
    · simpa [S0, h] using bip_nil_mem_choices L
    · simp [S0, h, hSmaxC]
  have hS0nodup : S0.Nodup := bip_choices_nodup hLnodup hS0C
  have hS0sub : ∀ x ∈ S0, x ∈ L := mem_of_mem_choices L hS0C
  have hS0diff : S0.length - (bip_neigh g S0).length = δ := by
    by_cases h : δ = 0
    · simp [S0, h]
    · simp [S0, h, hSmaxd]
  have hNleS0 : (bip_neigh g S0).length ≤ S0.length := by
    by_cases h : δ = 0
    · have hsemp : S0 = [] := by simp [S0, h]
      have hne : ∀ x, x ∉ bip_neigh g ([] : List Nat) := by
        intro x hx
        rcases (bip_mem_neigh g [] x).mp hx with ⟨e, heg, heS, _⟩
        cases heS
      have hnil : bip_neigh g ([] : List Nat) = [] := by
        cases hm : bip_neigh g ([] : List Nat) with
        | nil => rfl
        | cons x xs => exact False.elim (hne x (by simp [hm]))
      simpa [hsemp] using hnil
    · have hdpos : 0 < δ := Nat.pos_of_ne_zero h
      omega
  let V := ws.flatten.eraseDups
  let Cpred (v : Nat) : Prop := (v ∈ L ∧ v ∉ S0) ∨ v ∈ bip_neigh g S0
  let C := V.filter (fun v => decide (Cpred v))
  have hVnodup : V.Nodup := by dsimp [V]; exact bip_nodup_eraseDups _
  have hCnodup : C.Nodup := by dsimp [C]; exact bip_nodup_filter hVnodup _
  have hCcand : C ∈ choices V := by dsimp [C]; exact bip_filter_mem_choices V _
  have hLeven : ∀ l ∈ L, ∃ x, l = 2 * x := by
    intro l hl
    rcases (bip_mem_lefts g l).mp hl with ⟨e, heg, hel⟩
    rcases List.mem_map.mp heg with ⟨p, hpE, hpe⟩
    rcases p with ⟨x, y⟩
    cases hpe
    simp at hel
    exact ⟨x, hel.symm⟩
  have hNodd : ∀ r ∈ bip_neigh g S0, ∃ y, r = 2 * y + 1 := by
    intro r hr
    rcases (bip_mem_neigh g S0 r).mp hr with ⟨e, heg, heS, her⟩
    rcases List.mem_map.mp heg with ⟨p, hpE, hpe⟩
    rcases p with ⟨x, y⟩
    cases hpe
    simp at her
    exact ⟨y, her.symm⟩
  have hLsubV : ∀ l ∈ L, l ∈ V := by
    intro l hl
    rcases (bip_mem_lefts g l).mp hl with ⟨e, heg, hel⟩
    rcases List.mem_map.mp heg with ⟨p, hpE, hpe⟩
    rcases p with ⟨x, y⟩
    cases hpe
    simp at hel
    apply List.mem_eraseDups.mpr
    exact List.mem_flatten.mpr ⟨[2 * x, 2 * y + 1],
      List.mem_map.mpr ⟨(x, y), hpE, rfl⟩, by simp [hel]⟩
  have hNsubV : ∀ r ∈ bip_neigh g S0, r ∈ V := by
    intro r hr
    rcases (bip_mem_neigh g S0 r).mp hr with ⟨e, heg, heS, her⟩
    rcases List.mem_map.mp heg with ⟨p, hpE, hpe⟩
    rcases p with ⟨x, y⟩
    cases hpe
    simp at her
    apply List.mem_eraseDups.mpr
    exact List.mem_flatten.mpr ⟨[2 * x, 2 * y + 1],
      List.mem_map.mpr ⟨(x, y), hpE, rfl⟩, by simp [her]⟩
  let CL := (L.filter (fun v => !(v ∈ S0))) ++ bip_neigh g S0
  have hCLnodup : CL.Nodup := by
    dsimp [CL]
    rw [List.nodup_append]
    refine ⟨bip_nodup_filter hLnodup _, bip_neigh_nodup g S0, ?_⟩
    intro a ha b hb heq
    rcases hLeven a (List.mem_filter.mp ha).1 with ⟨x, hx⟩
    rcases hNodd b hb with ⟨y, hy⟩
    omega
  have hCLmemC : ∀ v ∈ CL, v ∈ C := by
    intro v hv
    rcases List.mem_append.mp hv with hvL | hvN
    · have hvL' : v ∈ L := (List.mem_filter.mp hvL).1
      have hvnot : v ∉ S0 := by
        have hxf : (!decide (v ∈ S0)) = true := (List.mem_filter.mp hvL).2
        exact of_decide_eq_false (by simpa using hxf)
      dsimp [C]
      exact List.mem_filter.mpr ⟨hLsubV v hvL', decide_eq_true (Or.inl ⟨hvL', hvnot⟩)⟩
    · dsimp [C]
      exact List.mem_filter.mpr ⟨hNsubV v hvN, decide_eq_true (Or.inr hvN)⟩
  have hCmemCL : ∀ v ∈ C, v ∈ CL := by
    intro v hv
    have hvP : Cpred v := of_decide_eq_true (List.mem_filter.mp hv).2
    dsimp [CL]
    rcases hvP with ⟨hvL, hvnot⟩ | hvN
    · apply List.mem_append.mpr
      exact Or.inl (List.mem_filter.mpr ⟨hvL, decide_eq_true (by
        have : decide (v ∈ S0) = false := decide_eq_false hvnot
        simpa using this)⟩)
    · exact List.mem_append.mpr (Or.inr hvN)
  have hCeqCLlen : C.length = CL.length := by
    have h1 := length_le_of_nodup_subset hCnodup hCmemCL
    have h2 := length_le_of_nodup_subset hCLnodup hCLmemC
    omega
  have hCLlen : CL.length = L.length - δ := by
    have hcount := bip_filter_count_subset hLnodup hS0nodup hS0sub
    have hpart := bip_length_filter_compl L (fun v => v ∈ S0)
    dsimp [CL]
    rw [List.length_append]
    omega
  have hClen : C.length = L.length - δ := by omega
  have hcover : ∀ e ∈ g, e.1 ∈ C ∨ e.2 ∈ C := by
    intro e heg
    have he1L : e.1 ∈ L := (bip_mem_lefts g e.1).mpr ⟨e, heg, rfl⟩
    by_cases he1S : e.1 ∈ S0
    · right
      have he2N : e.2 ∈ bip_neigh g S0 := (bip_mem_neigh g S0 e.2).mpr ⟨e, heg, he1S, rfl⟩
      dsimp [C]
      exact List.mem_filter.mpr ⟨hNsubV e.2 he2N, decide_eq_true (Or.inr he2N)⟩
    · left
      dsimp [C]
      exact List.mem_filter.mpr ⟨hLsubV e.1 he1L, decide_eq_true (Or.inl ⟨he1L, he1S⟩)⟩
  have hdead : ∀ w ∈ ws, live C w ≠ true := by
    intro w hw hlive
    rcases List.mem_map.mp hw with ⟨p, hpE, rfl⟩
    have heg : (2 * p.1, 2 * p.2 + 1) ∈ g := by
      dsimp [g]
      exact List.mem_map.mpr ⟨p, hpE, rfl⟩
    have hcov := hcover (2 * p.1, 2 * p.2 + 1) heg
    rcases hcov with h1 | h2
    · exact (live_true_iff C [2 * p.1, 2 * p.2 + 1]).mp hlive (2 * p.1) (by simp) h1
    · exact (live_true_iff C [2 * p.1, 2 * p.2 + 1]).mp hlive (2 * p.2 + 1) (by simp) h2
  have hexfalse : existsAfter ws C = false := by
    apply Bool.eq_false_of_ne_true
    intro ht
    rcases List.any_eq_true.mp ht with ⟨w, hw, hl⟩
    exact hdead w hw hl
  have hcutsC : cuts ws C = true := by simp [cuts, hexfalse]
  let Y := (g.map Prod.snd).foldl Nat.max 0
  let dummy (i : Nat) := 2 * (Y + 1 + i) + 1
  let D := (List.range δ).map dummy
  let Eext := g ++ L.flatMap (fun l => D.map (fun r => (l, r)))
  have hDnodup : D.Nodup := by
    dsimp [D]
    apply bip_nodup_map_inj
    · intro i hi j hj heq
      dsimp [dummy] at heq
      omega
    · exact List.nodup_range
  have hDlen : D.length = δ := by simp [D]
  have hYbound : ∀ e ∈ g, e.2 ≤ Y := by
    intro e heg
    dsimp [Y]
    exact bip_mem_le_foldl_max (g.map Prod.snd) 0 e.2 (List.mem_map.mpr ⟨e, heg, rfl⟩)
  have hDgt : ∀ d ∈ D, 2 * Y + 1 < d := by
    intro d hd
    rcases List.mem_map.mp hd with ⟨i, hi, rfl⟩
    have hi' : i < δ := List.mem_range.mp hi
    dsimp [dummy]
    omega
  have hHallExt : bip_Hall L Eext := by
    constructor
    · intro e he
      rcases List.mem_append.mp he with heg | hnew
      · exact (bip_mem_lefts g e.1).mpr ⟨e, heg, rfl⟩
      · rcases List.mem_flatMap.mp hnew with ⟨l, hlL, hm⟩
        rcases List.mem_map.mp hm with ⟨r, hrD, hpair⟩
        simpa [← hpair] using hlL
    · intro S hS hSsub
      cases S with
      | nil => simp
      | cons s ss =>
          let Su := s :: ss
          let N := bip_neigh g Su
          have hNnodup : N.Nodup := bip_neigh_nodup g Su
          have hdisND : ∀ a ∈ N, ∀ b ∈ D, a ≠ b := by
            intro a ha b hb hab
            have haBound : a ≤ 2 * Y + 1 := by
              rcases (bip_mem_neigh g Su a).mp ha with ⟨e, heg, heS, rfl⟩
              have hy := hYbound e heg
              omega
            have hbGt := hDgt b hb
            omega
          have hNDnodup : (N ++ D).Nodup := by
            rw [List.nodup_append]
            exact ⟨hNnodup, hDnodup, hdisND⟩
          have hmemND : ∀ x ∈ N ++ D, x ∈ bip_neigh Eext Su := by
            intro x hx
            rcases List.mem_append.mp hx with hxN | hxD
            · rcases (bip_mem_neigh g Su x).mp hxN with ⟨e, heg, heS, rfl⟩
              exact (bip_mem_neigh Eext Su e.2).mpr
                ⟨e, List.mem_append.mpr (Or.inl heg), heS, rfl⟩
            · rcases List.mem_map.mp hxD with ⟨i, hiR, rfl⟩
              have hsl : s ∈ L := hSsub s (by simp)
              have hedummy : (s, dummy i) ∈ Eext := by
                apply List.mem_append.mpr
                right
                apply List.mem_flatMap.mpr
                exact ⟨s, hsl, List.mem_map.mpr ⟨dummy i,
                  List.mem_map.mpr ⟨i, hiR, rfl⟩, rfl⟩⟩
              exact (bip_mem_neigh Eext Su (dummy i)).mpr
                ⟨(s, dummy i), hedummy, by simp [Su], rfl⟩
          have hle1 := length_le_of_nodup_subset (bip_nodup_eraseDups (N ++ D))
            (fun x hx => hmemND x (List.mem_eraseDups.mp hx))
          have hNDlen : (N ++ D).eraseDups.length = N.length + δ := by
            have h1 : ∀ x ∈ (N ++ D).eraseDups, x ∈ N ++ D := fun x hx => List.mem_eraseDups.mp hx
            have h2 : ∀ x ∈ N ++ D, x ∈ (N ++ D).eraseDups := fun x hx => List.mem_eraseDups.mpr hx
            have hl1 := length_le_of_nodup_subset (bip_nodup_eraseDups _) h1
            have hl2 := length_le_of_nodup_subset hNDnodup h2
            have hraw : (N ++ D).length = N.length + D.length := by simp
            omega
          have hdiffbound : Su.length - N.length ≤ δ := by
            let S' := L.filter (fun v => v ∈ Su)
            have hS'C : S' ∈ choices L := bip_filter_mem_choices L _
            have hS'N : S'.Nodup := bip_nodup_filter hLnodup _
            have hS'len : S'.length = Su.length := bip_filter_count_subset hLnodup hS hSsub
            have hneighEq : (bip_neigh g S').length = N.length := by
              apply bip_neigh_len_congr
              intro x
              constructor
              · intro hx
                exact of_decide_eq_true (List.mem_filter.mp hx).2
              · intro hx
                exact List.mem_filter.mpr ⟨hSsub x hx, by simp [hx]⟩
            have hd := hmaxprop.2 (S'.length - (bip_neigh g S').length)
              (List.mem_map.mpr ⟨S', hS'C, rfl⟩)
            omega
          have hSle : Su.length ≤ N.length + δ := by omega
          have hleND : N.length + δ ≤ (bip_neigh Eext Su).length := by
            rw [← hNDlen]
            exact hle1
          exact Nat.le_trans (by simpa [Su] using hSle) (by simpa [Su] using hleND)
  rcases bip_hall_matching L Eext hLnodup hHallExt with ⟨Mext, hMext⟩
  let Mdum := Mext.filter (fun e => D.contains e.2)
  let Mreal := Mext.filter (fun e => !(D.contains e.2))
  have hMpart := bip_length_filter_compl Mext (fun e => D.contains e.2)
  have hMdumNodup : (Mdum.map Prod.snd).Nodup := by
    dsimp [Mdum]
    apply bip_nodup_map_inj
    · intro a ha b hb heq
      have haM : a ∈ Mext := (List.mem_filter.mp ha).1
      have hbM : b ∈ Mext := (List.mem_filter.mp hb).1
      have hsnd : Mext.Pairwise (fun a b => a.2 ≠ b.2) :=
        List.Pairwise.imp (fun h => h.2) hMext.1.2
      exact bip_eq_of_pairwise_snd hsnd a haM b hbM heq
    · exact bip_nodup_of_pairwise_fst (List.Pairwise.imp (fun h => h.1)
        (List.Pairwise.filter (fun e : Nat × Nat => D.contains e.2) hMext.1.2))
  have hMdumMemD : ∀ r ∈ Mdum.map Prod.snd, r ∈ D := by
    intro r hr
    rcases List.mem_map.mp hr with ⟨e, heM, rfl⟩
    exact List.contains_iff_mem.mp (List.mem_filter.mp heM).2
  have hMdum_le : Mdum.length ≤ δ := by
    have hle := length_le_of_nodup_subset hMdumNodup hMdumMemD
    rw [hDlen] at hle
    simpa using hle
  have hMreal_valid : bip_validM g Mreal := by
    constructor
    · intro e he
      have heM : e ∈ Mext := (List.mem_filter.mp he).1
      have heE := hMext.1.1 e heM
      rcases List.mem_append.mp heE with heg | hnew
      · exact heg
      · rcases List.mem_flatMap.mp hnew with ⟨l, hlL, hm⟩
        rcases List.mem_map.mp hm with ⟨r, hrD, hpairEq⟩
        have hnot : D.contains e.2 = false := by simpa using (List.mem_filter.mp he).2
        have hcont : D.contains e.2 = true := by
          simpa [← hpairEq] using (List.contains_iff_mem.mpr hrD)
        rw [hnot] at hcont
        cases hcont
    · exact List.Pairwise.filter (fun e : Nat × Nat => !(D.contains e.2)) hMext.1.2
  have hMreal_ge : L.length - δ ≤ Mreal.length := by
    dsimp [Mdum, Mreal] at hMpart hMdum_le ⊢
    omega
  let wit (e : Nat × Nat) : Witness := [e.1, e.2]
  let MW := Mreal.map wit
  have hMrealnodup : Mreal.Nodup := bip_nodup_of_pairwise_fst
    (List.Pairwise.imp (fun h => h.1) hMreal_valid.2)
  have hMWnodup : MW.Nodup := by
    dsimp [MW]
    apply bip_nodup_map_inj
    · intro a ha b hb heq
      rcases a with ⟨al, ar⟩
      rcases b with ⟨bl, br⟩
      have halM : (al, ar) ∈ Mreal := by simpa using ha
      have hblM : (bl, br) ∈ Mreal := by simpa using hb
      dsimp [wit] at heq
      simp at heq
      exact bip_eq_of_pairwise_fst (List.Pairwise.imp (fun h => h.1) hMreal_valid.2)
        (al, ar) halM (bl, br) hblM heq.1
    · exact hMrealnodup
  have hMWsubV : ∀ w ∈ MW, w ∈ ws.eraseDups := by
    intro w hw
    rcases List.mem_map.mp hw with ⟨e, heM, rfl⟩
    have heg := hMreal_valid.1 e heM
    apply List.mem_eraseDups.mpr
    rcases List.mem_map.mp heg with ⟨p, hpE, hpe⟩
    rcases p with ⟨x, y⟩
    simp at hpe
    subst e
    exact List.mem_map.mpr ⟨(x, y), hpE, rfl⟩
  let ps := (ws.eraseDups).filter (fun w => MW.contains w)
  have hpsNodup : ps.Nodup := by dsimp [ps]; exact bip_nodup_filter (bip_nodup_eraseDups _) _
  have hpsC : ps ∈ choices ws := by
    apply bip_sublist_mem_choices
    dsimp [ps]
    exact List.Sublist.trans List.filter_sublist (bip_eraseDups_sublist ws)
  have hpslen : ps.length = MW.length := by
    dsimp [ps]
    exact bip_count_filter_of_nodup ws.eraseDups MW (bip_nodup_eraseDups ws) hMWnodup hMWsubV
  have hMWlen : MW.length = Mreal.length := by simp [MW]
  have hpspair : ps.Pairwise (fun a b => disjoint a b = true) := by
    rw [List.pairwise_iff_getElem]
    intro i j hi hj hij
    have hwi : ps[i] ∈ ps := List.getElem_mem hi
    have hwj : ps[j] ∈ ps := List.getElem_mem hj
    have hwim : ps[i] ∈ MW := List.contains_iff_mem.mp (List.mem_filter.mp hwi).2
    have hwjm : ps[j] ∈ MW := List.contains_iff_mem.mp (List.mem_filter.mp hwj).2
    rcases List.mem_map.mp hwim with ⟨a, haM, haeq⟩
    rcases List.mem_map.mp hwjm with ⟨b, hbM, hbeq⟩
    have hab : a ≠ b := by
      intro heq
      have hw : wit a = wit b := by rw [heq]
      have heqw : ps[i] = ps[j] := by rw [← haeq, ← hbeq, hw]
      have heqij : i = j := (List.Nodup.getElem_inj hpsNodup).mp heqw
      omega
    have hne := bip_pairwise_ne_of_mem hMreal_valid.2 a haM b hbM hab
    have haE := hMreal_valid.1 a haM
    have hbE := hMreal_valid.1 b hbM
    rcases List.mem_map.mp haE with ⟨pa, hpa, hpaEq⟩
    rcases List.mem_map.mp hbE with ⟨pb, hpb, hpbEq⟩
    rcases pa with ⟨xa, ya⟩
    rcases pb with ⟨xb, yb⟩
    simp at hpaEq hpbEq
    subst a
    subst b
    have hcross1 : 2 * xa ≠ 2 * yb + 1 := by omega
    have hcross2 : 2 * ya + 1 ≠ 2 * xb := by omega
    rw [← haeq, ← hbeq]
    exact bip_disjoint_pair (2 * xa) (2 * ya + 1) (2 * xb) (2 * yb + 1)
      hne.1 hne.2 hcross1 hcross2
  let good := (choices ws).filter (fun ps =>
    ps.all (fun a => !a.isEmpty) && ps.zipIdx.all (fun (w, i) =>
      (ps.drop (i + 1)).all (disjoint w)))
  have hpsgood : ps ∈ good := by
    rw [List.mem_filter]
    constructor
    · exact hpsC
    · apply Bool.and_eq_true_iff.mpr
      constructor
      · rw [List.all_eq_true]
        intro w hw
        have hwm : w ∈ MW := List.contains_iff_mem.mp (List.mem_filter.mp hw).2
        rcases List.mem_map.mp hwm with ⟨e, heM, rfl⟩
        simp
      · exact bip_pairwise_zipIdx hpspair
  have hgoodne : good ≠ [] := by
    intro hn
    have : ps ∈ ([] : List Family) := by simp [hn] at hpsgood
    cases this
  have hmaxgood : (good.map List.length).max? = some ((good.map List.length).max?.getD 0) := by
    cases hm : (good.map List.length).max? with
    | none =>
        have hnil : good.map List.length = [] := List.max?_eq_none_iff.mp hm
        have hgoodnil : good = [] := List.map_eq_nil_iff.mp hnil
        exact False.elim (hgoodne hgoodnil)
    | some k => simp
  have hpackprop := List.max?_eq_some_iff.mp hmaxgood
  have hpsmemlen : ps.length ∈ good.map List.length := List.mem_map.mpr ⟨ps, hpsgood, rfl⟩
  have hpackge : Mreal.length ≤ packing ws := by
    have hle := hpackprop.2 ps.length hpsmemlen
    dsimp [packing, good]
    rw [hpslen, hMWlen] at hle
    exact hle
  let cands := (choices V).filter (cuts ws)
  have hCin : C ∈ cands := List.mem_filter.mpr ⟨hCcand, hcutsC⟩
  have hcandsne : cands ≠ [] := by
    intro hn
    have : C ∈ ([] : List Witness) := by simp [hn] at hCin
    cases this
  have hminsome : (cands.map List.length).min? = some ((cands.map List.length).min?.getD 0) := by
    cases hm : (cands.map List.length).min? with
    | none =>
        have hnil : cands.map List.length = [] := List.min?_eq_none_iff.mp hm
        have hcandsnil : cands = [] := List.map_eq_nil_iff.mp hnil
        exact False.elim (hcandsne hcandsnil)
    | some k => simp
  let m := (cands.map List.length).min?.getD 0
  have hminprop := List.min?_eq_some_iff.mp hminsome
  have hCm : C.length ∈ cands.map List.length := List.mem_map.mpr ⟨C, hCin, rfl⟩
  have hmleC : m ≤ C.length := hminprop.2 C.length hCm
  rcases List.mem_map.mp hminprop.1 with ⟨del, hdelc, hdellen⟩
  have hdelc' := List.mem_filter.mp hdelc
  have hdeldead : ∀ w ∈ ws, live del w ≠ true := by
    intro w hw hlive
    have hexT : existsAfter ws del = true := by
      rw [existsAfter, List.any_eq_true]
      exact ⟨w, hw, hlive⟩
    have hb : cuts ws del = false := by simp [cuts, hexT]
    have hbad : (false : Bool) = true := hb.symm.trans hdelc'.2
    cases hbad
  have hweak : packing ws ≤ del.length := packing_le_length_of_all_dead ws del hdeldead
  have hpacklem : packing ws ≤ m := by omega
  have hmeq : m = packing ws := by omega
  have hminfinal : (cands.map List.length).min? = some (packing ws) := by
    rw [hminsome]
    change some m = some (packing ws)
    rw [hmeq]
  dsimp [cutCost, cands]
  exact hminfinal

end TinylambOpt.ResearchProofs.Frontiers.Provenance

