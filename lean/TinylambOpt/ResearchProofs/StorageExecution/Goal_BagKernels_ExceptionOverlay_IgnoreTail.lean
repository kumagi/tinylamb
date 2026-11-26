import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `ExceptionOverlay.IgnoreTail` — `∀ n d es i v, n≤i → weighted bag (n,d,es++[(i,v)])
  id = weighted bag (n,d,es) id`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_IgnoreTail

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

private theorem eraseDups_append_single (ks : List Nat) (v : Nat) :
    (ks ++ [v]).eraseDups = ks.eraseDups ++ (if v ∈ ks then [] else [v]) := by
  rw [List.eraseDups_append]
  by_cases h : v ∈ ks
  · have hrem : [v].removeAll ks = [] := by simp [List.removeAll, h]
    simp [hrem, h]
  · have hrem : [v].removeAll ks = [v] := by simp [List.removeAll, h]
    have hsing : [v].eraseDups = [v] := by rw [List.eraseDups_cons]; simp
    simp [hrem, hsing, h]

private theorem sum_eraseDups_append_single (ks : List Nat) (v : Nat) (w : Nat → Nat)
    (hv : v ∈ ks ∨ w v = 0) :
    ((ks ++ [v]).eraseDups.map w).sum = (ks.eraseDups.map w).sum := by
  rw [eraseDups_append_single]
  by_cases h : v ∈ ks
  · simp [h]
  · have hw : w v = 0 := by cases hv with | inl hmem => exact False.elim (h hmem) | inr hz => exact hz
    simp [h, hw]

private theorem filter_eraseDups_append_bound (l : List Nat) (i n : Nat) (hi : n ≤ i) :
    ((l ++ [i]).eraseDups.filter (fun j => j < n)) = (l.eraseDups.filter (fun j => j < n)) := by
  rw [List.eraseDups_append]
  rw [List.filter_append]
  have hz : (([i].removeAll l).eraseDups.filter (fun j => j < n)) = [] := by
    by_cases h : i ∈ l
    · have hrem : [i].removeAll l = [] := by simp_all [List.removeAll]
      simp [hrem]
    · have hrem : [i].removeAll l = [i] := by simp_all [List.removeAll]
      simp [hrem, hi]
  rw [hz, List.append_nil]

private theorem lookup_append_tail (es : List (Nat × Nat)) (j i v : Nat) (hji : j ≠ i) :
    (es ++ [(i, v)]).lookup j = es.lookup j := by
  rw [List.lookup_append]
  have hne : (j != i) = true := by
    by_cases h : j == i
    · have : j = i := by simpa using h
      exact False.elim (hji this)
    · simpa using h
  have hs : List.lookup j [(i, v)] = none := by
    rw [List.lookup_eq_none_iff]
    intro p hp
    simp at hp
    subst p
    simpa using hne
  simp [hs]

private theorem mass_tail_invariant (n d i v : Nat) (es : List (Nat × Nat)) (hi : n ≤ i) (k : Nat) :
    bag.mass (n, d, es ++ [(i, v)]) k = bag.mass (n, d, es) k := by
  unfold bag
  dsimp only
  have hps : ((es.map Prod.fst ++ [i]).eraseDups.filter (fun j => j < n)) =
      ((es.map Prod.fst).eraseDups.filter (fun j => j < n)) :=
    filter_eraseDups_append_bound (es.map Prod.fst) i n hi
  have hcount : ((es.map Prod.fst).eraseDups.filter (fun j => j < n)).countP
        (fun j => (es ++ [(i, v)]).lookup j == some k) =
      ((es.map Prod.fst).eraseDups.filter (fun j => j < n)).countP
        (fun j => es.lookup j == some k) := by
    apply List.countP_congr
    intro j hj
    have hjn : j < n := by simpa using (List.mem_filter.mp hj).2
    have hji : j ≠ i := by omega
    rw [lookup_append_tail es j i v hji]
  simp only [List.map_append, List.map_cons, List.map_nil]
  rw [hps, hcount]

private theorem mass_newkey_zero (n d : Nat) (es : List (Nat × Nat)) {v : Nat}
    (hv : v ∉ d :: es.map Prod.snd) :
    bag.mass (n, d, es) v = 0 := by
  unfold bag
  dsimp only
  have hvd : v ≠ d := by intro heq; exact hv (by simp [heq])
  have hzero : ((es.map Prod.fst).eraseDups.filter (fun j => j < n)).countP
      (fun j => es.lookup j == some v) = 0 := by
    apply List.countP_eq_zero.mpr
    intro j hj
    have hlook : es.lookup j ≠ some v := by
      intro hsome
      rcases List.lookup_eq_some_iff.mp hsome with ⟨l₁, l₂, hes, hfirst⟩
      have hvmem : v ∈ es.map Prod.snd := by
        apply List.mem_map.mpr
        refine ⟨(j, v), ?_, rfl⟩
        rw [hes]
        simp
      exact hv (by simp [hvmem])
    by_cases h : es.lookup j == some v
    · have : es.lookup j = some v := by simpa using h
      exact False.elim (hlook this)
    · simpa using h
  rw [hzero]
  simp [hvd]

private theorem keys_append_tail (n d i v : Nat) (es : List (Nat × Nat)) :
    bag.keys (n, d, es ++ [(i, v)]) = bag.keys (n, d, es) ++ [v] := by
  unfold bag
  simp [List.map_append]

theorem sound_IgnoreTail : IgnoreTail.Goal := by
  intro n d es i v hi
  unfold weighted
  rw [keys_append_tail n d i v es]
  rw [eraseDups_append_single]
  by_cases hv : v ∈ bag.keys (n, d, es)
  · simp [hv]
    congr 1
    apply List.map_congr_left
    intro k hk
    rw [mass_tail_invariant n d i v es hi k]
  · have hvmem : v ∉ (bag.keys (n, d, es)).eraseDups := by
      simpa [List.mem_eraseDups] using hv
    have hz : bag.mass (n, d, es) v = 0 := by
      apply mass_newkey_zero
      simpa [bag] using hvmem
    have hz' : bag.mass (n, d, es ++ [(i, v)]) v = 0 := by
      rw [mass_tail_invariant n d i v es hi v]
      exact hz
    simp [hv, hz']
    congr 1
    apply List.map_congr_left
    intro k hk
    rw [mass_tail_invariant n d i v es hi k]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_IgnoreTail
