import TinylambOpt.ResearchGoals.Frontiers.Provenance

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.F_Provenance_A

open TinylambOpt.ResearchGoals.Frontiers.Provenance

/-!
Per-goal notes (all five goals are TRUE and already proved in
`TinylambOpt.ResearchProofs.Frontiers.Provenance`; this file re-proves them
self-contained, reusing the same argument structure):

- `Antichain`: `minimal ws` is `ws.filter`d to witnesses with no strict subset
  in `ws`. If `w ∈ ws` is live under `del`, a strictly-descending chain argument
  (`minimal_subset_exists`, measure = `eraseDups.length`) yields `u ∈ minimal ws`
  with `subset u w`; `u` stays live since its IDs are a subset of `w`'s. The
  converse is immediate because `minimal ws ⊆ ws`.
- `ProductCount`: `live del (p ++ q) = live del p && live del q`, so
  `countP` over `a.flatMap (fun p => b.map (p ++ ·))` splits as
  `countP a * countP b` by induction on `a` (`countP_product_single`).
- `SignatureWeights`: `live` is invariant under `mergeSort.eraseDups`
  (`live_canon`), so `bagAfter ws del = countP (live del) (ws.map key)`; the
  weighted dedup identity `dedup_weight_sum` turns that into
  `Σ_{k ∈ keys.eraseDups, live k} keys.count k`.
- `IncidentDelta`: `live (old ++ batch) w = live old w && !(w.any batch.contains)`
  (`live_append`); the per-witness indicator splits `countP (live old)` into the
  `old++batch`-live part plus the `live old && hits batch` part, and `omega`
  handles the Nat subtraction.
- `ProductCut`: `cuts (product a b) c ↔ cuts a c || cuts b c`
  (`existsAfter_product`, `cuts_product`). Any `a`-cutter or `b`-cutter is a
  product-cutter, giving the upper bound `min ca cb`; conversely a product-cutter
  must cut `a` or `b` (else a surviving pair survives the product), so every
  product-cutter has length `≥ min ca cb`. `List.min?_eq_some_iff` packages both
  directions.
-/

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

theorem Antichain_sound : TinylambOpt.ResearchGoals.Frontiers.Provenance.Antichain.Goal := by
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

theorem ProductCount_sound : TinylambOpt.ResearchGoals.Frontiers.Provenance.ProductCount.Goal := by
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

theorem IncidentDelta_sound : TinylambOpt.ResearchGoals.Frontiers.Provenance.IncidentDelta.Goal := by
  intro ws old batch
  unfold bagAfter
  have hpart : ∀ w : Witness,
      (if live old w = true then 1 else 0) =
        (if live (old ++ batch) w = true then 1 else 0) +
          (if (live old w && w.any batch.contains) = true then 1 else 0) := by
    intro w
    rw [live_append]
    by_cases h1 : live old w = true <;> by_cases h2 : w.any batch.contains = true <;>
      simp [h1, h2, live]
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
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · have hkx : (k == x) = false := by simpa [beq_comm k x] using hx
        simp [List.count_cons, hkx, hx, ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

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
          simp [Nat.add_mul, Nat.add_comm, Nat.add_left_comm]
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

theorem SignatureWeights_sound : TinylambOpt.ResearchGoals.Frontiers.Provenance.SignatureWeights.Goal := by
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

theorem ProductCut_sound : TinylambOpt.ResearchGoals.Frontiers.Provenance.ProductCut.Goal := by
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

end TinylambOpt.ResearchProofs.DevinBatch.F_Provenance_A
