import TinylambOpt.ResearchGoals.ClickstreamLayout.Execution

/-!
These models describe encoded clickstream columns and the predicates and aggregates that
can consume them.

Proved

- `ConcatSearch` — May a scanner search one stream and then consult the ends only for
  hits? Reject cross-row byte matches, handle empty patterns and preserve row order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickstreamLayout.ConcatenatedPieceSearch

open TinylambOpt.ResearchGoals.ClickstreamLayout.Execution
open ConcatSearch

private theorem foldl_add : ∀ (ys : List Nat) (s : Nat), ys.foldl (·+·) s = s + ys.sum := by
  intro ys
  induction ys with
  | nil => intro s; simp
  | cons y ys ih =>
    intro s
    simp only [List.foldl_cons, List.sum_cons]
    rw [ih (s + y)]
    omega

private theorem scanl_prefix (ys : List Nat) (s i : Nat) (hi : i ≤ ys.length) :
    (ys.scanl (·+·) s)[i]? = some (s + (ys.take i).sum) := by
  rw [List.getElem?_scanl, if_pos hi, foldl_add]

private theorem scanl_bound : ∀ (ys : List Nat) (s v : Nat),
    v ∈ ys.scanl (·+·) s → s ≤ v := by
  intro ys
  induction ys with
  | nil =>
    intro s v hv
    simp at hv
    omega
  | cons y ys ih =>
    intro s v hv
    simp only [List.scanl_cons, List.mem_cons] at hv
    rcases hv with hv | hv
    · omega
    · have h1 : s + y ≤ v := ih (s + y) v hv
      omega

private theorem scanl_pairwise : ∀ (ys : List Nat) (s : Nat),
    (ys.scanl (·+·) s).Pairwise (· ≤ ·) := by
  intro ys
  induction ys with
  | nil => intro s; simp [List.scanl]
  | cons y ys ih =>
    intro s
    rw [List.scanl_cons]
    constructor
    · intro v hv
      exact Nat.le_trans (by omega : s ≤ s + y) (scanl_bound ys (s + y) v hv)
    · exact ih (s + y)

private theorem ends_get (words : List Word) (i : Nat) (hi : i < words.length) :
    ((((words.map List.length).scanl (·+·) 0).drop 1)[i]?) =
      some ((words.take (i+1)).flatten.length) := by
  rw [List.getElem?_drop, Nat.add_comm 1 i]
  have hi' : i + 1 ≤ (words.map List.length).length := by
    rw [List.length_map]
    omega
  rw [scanl_prefix _ _ _ hi']
  simp only [Nat.zero_add]
  congr 1
  rw [List.length_flatten, List.map_take]

private theorem findIdx_spec : ∀ (l : List Nat) (g : Nat),
    (∀ j, j < l.findIdx (fun v => decide (g < v)) →
      match l[j]? with | some v => v ≤ g | none => True) ∧
    (l.findIdx (fun v => decide (g < v)) < l.length →
      g < l[l.findIdx (fun v => decide (g < v))]!) := by
  intro l
  induction l with
  | nil =>
    intro g
    simp [List.findIdx]
  | cons a t ih =>
    intro g
    by_cases hg : g < a
    · have hf : (a :: t).findIdx (fun v => decide (g < v)) = 0 := by
        simp [List.findIdx_cons, hg]
      constructor
      · intro j hj
        omega
      · intro hlen
        simpa [hf] using hg
    · have hga : a ≤ g := by omega
      have hf : (a :: t).findIdx (fun v => decide (g < v)) =
          t.findIdx (fun v => decide (g < v)) + 1 := by
        simp [List.findIdx_cons, hg]
      constructor
      · intro j hj
        cases j with
        | zero => simp [hga]
        | succ j =>
          have hj' : j < t.findIdx (fun v => decide (g < v)) := by omega
          have hs := (ih g).1 j hj'
          simpa [List.getElem?_cons_succ] using hs
      · intro hlen
        have hlt : t.findIdx (fun v => decide (g < v)) < t.length := by
          simp only [List.length_cons] at hlen
          omega
        have hs := (ih g).2 hlt
        simpa [hf, List.getElem_cons_succ] using hs

private theorem findIdx_mono : ∀ (l : List Nat), l.Pairwise (· ≤ ·) →
    ∀ {g g' : Nat}, g' ≤ g →
      l.findIdx (fun v => decide (g' < v)) ≤ l.findIdx (fun v => decide (g < v)) := by
  intro l hp
  induction l with
  | nil =>
    intro g g' hle
    change 0 ≤ 0
    omega
  | cons a t ih =>
    intro g g' hle
    have htail : t.Pairwise (· ≤ ·) := (List.pairwise_cons.mp hp).2
    by_cases hg' : g' < a
    · have hf : (a :: t).findIdx (fun v => decide (g' < v)) = 0 := by
        simp [List.findIdx_cons, hg']
      omega
    · have hga' : a ≤ g' := by omega
      have hga : a ≤ g := by omega
      have hgfalse : ¬ g < a := by omega
      have hf' : (a :: t).findIdx (fun v => decide (g' < v)) =
          t.findIdx (fun v => decide (g' < v)) + 1 := by
        simp [List.findIdx_cons, hg']
      have hf : (a :: t).findIdx (fun v => decide (g < v)) =
          t.findIdx (fun v => decide (g < v)) + 1 := by
        simp [List.findIdx_cons, hgfalse]
      rw [hf', hf]
      exact Nat.add_le_add_right (ih htail hle) 1

private theorem starts_mem (needle haystack : Word) (i : Nat) :
    i ∈ starts needle haystack ↔ i ≤ haystack.length ∧
      needle.isPrefixOf (haystack.drop i) = true := by
  unfold starts
  rw [List.mem_filter, List.mem_range]
  constructor
  · intro h
    exact ⟨by omega, h.2⟩
  · intro h
    exact ⟨by omega, h.2⟩

private theorem prefix_of_append {a b : Word} {needle : Word}
    (h : needle.isPrefixOf (a ++ b) = true) (hlen : needle.length ≤ a.length) :
    needle.isPrefixOf a = true := by
  rw [List.isPrefixOf_iff_prefix] at h ⊢
  rcases h with ⟨t, ht⟩
  have htake := congrArg (List.take needle.length) ht
  rw [List.take_append_of_le_length hlen] at htake
  rw [List.take_append_of_le_length (Nat.le_refl needle.length)] at htake
  rw [List.take_length] at htake
  have heq : needle = a.take needle.length := htake
  have htake_len : (a.take needle.length).length = needle.length := by
    rw [List.length_take, Nat.min_eq_left hlen]
  refine ⟨a.drop needle.length, ?_⟩
  rw [heq, htake_len, List.take_append_drop]

private theorem prefix_append {a b : Word} {needle : Word}
    (h : needle.isPrefixOf a = true) : needle.isPrefixOf (a ++ b) = true := by
  rw [List.isPrefixOf_iff_prefix] at h ⊢
  rcases h with ⟨t, ht⟩
  exact ⟨t ++ b, by rw [← ht, List.append_assoc]⟩

private theorem flatten_drop (words : List Word) (i j : Nat) (hi : i < words.length)
    (hj : j ≤ (words[i]?.getD []).length) :
    words.flatten.drop ((words.take i).flatten.length + j) =
      ((words[i]?.getD []).drop j) ++ (words.drop (i+1)).flatten := by
  induction words generalizing i j with
  | nil => simp at hi
  | cons w ws ih =>
    cases i with
    | zero =>
      simpa using (List.drop_append_of_le_length (l₁ := w) (l₂ := ws.flatten) hj)
    | succ i =>
      have hi' : i < ws.length := by simpa using hi
      have hj' : j ≤ (ws[i]?.getD []).length := by simpa using hj
      have hcall := ih i j hi' hj'
      simp
      rw [← List.drop_drop]
      simp
      simpa [List.length_flatten, List.map_take] using hcall

private theorem range_pairwise : ∀ n : Nat, (List.range n).Pairwise (· < ·) := by
  intro n
  induction n with
  | zero => exact List.Pairwise.nil
  | succ n ih =>
    rw [List.range_succ]
    rw [List.pairwise_append]
    refine ⟨ih, ?_, ?_⟩
    · simp [List.Pairwise]
    · intro a ha b hb
      simp at hb
      simpa [hb] using (List.mem_range.mp ha)

private theorem pairwise_filter_le {α : Type} (L : List α) (p : α → Bool) (f : α → Nat) :
    L.Pairwise (fun a b => f a ≤ f b) →
    ((L.filter p).map f).Pairwise (· ≤ ·) := by
  intro h
  exact (h.filter p).map f (by intro a b hab; exact hab)

private theorem pairwise_eraseDups' : ∀ (L : List Nat), L.Pairwise (· ≤ ·) →
    ∀ (p : Nat → Bool), (L.filter p).eraseDups.Pairwise (· < ·) := by
  intro L
  induction L with
  | nil =>
    intro h p
    simp [List.eraseDups]
  | cons a L ih =>
    intro h p
    have hp := List.pairwise_cons.mp h
    by_cases hpa : p a = true
    · have hfilter : (List.filter p (a :: L)) = a :: List.filter p L := by
        simp [hpa]
      rw [hfilter, List.eraseDups_cons]
      constructor
      · intro b hb
        rw [List.mem_eraseDups] at hb
        have hb' : b ∈ List.filter (fun x => !x == a) (List.filter p L) := hb
        have hbneq := (List.mem_filter.mp hb').2
        have hne : b ≠ a := by
          intro heq
          subst b
          simp at hbneq
        have hbinL : b ∈ L := (List.mem_filter.mp (List.mem_filter.mp hb').1).1
        have hab := hp.1 b hbinL
        omega
      · have heqf : List.filter (fun x => !x == a) (List.filter p L) =
            List.filter (fun x => p x && !x == a) L := by
          rw [List.filter_filter]
          induction L with
          | nil => rfl
          | cons x xs ih =>
            by_cases hx : p x = true <;> by_cases hxa : x == a <;>
              simp [hx, hxa, Bool.and_comm]
        rw [heqf]
        exact ih hp.2 (fun x => p x && !x == a)
    · have hfilter : List.filter p (a :: L) = List.filter p L := by
        simp [hpa]
      rw [hfilter]
      exact ih hp.2 p

private theorem pairwise_eraseDups {L : List Nat} (h : L.Pairwise (· ≤ ·)) :
    L.eraseDups.Pairwise (· < ·) := by
  have hres := pairwise_eraseDups' L h (fun _ => true)
  have hf : List.filter (fun _ : Nat => true) L = L := by simp
  rw [hf] at hres
  exact hres

private theorem pairwise_ext {L₁ L₂ : List Nat}
    (h₁ : L₁.Pairwise (· < ·)) (h₂ : L₂.Pairwise (· < ·))
    (hm : ∀ i, i ∈ L₁ ↔ i ∈ L₂) : L₁ = L₂ := by
  induction L₁ generalizing L₂ with
  | nil =>
    cases L₂ with
    | nil => rfl
    | cons b t =>
      have hb : b ∈ ([] : List Nat) := (hm b).2 (by simp)
      simp at hb
  | cons a t ih =>
    have ht : t.Pairwise (· < ·) := (List.pairwise_cons.mp h₁).2
    have hmin₁ : ∀ i ∈ t, a < i := (List.pairwise_cons.mp h₁).1
    have ha₂ : a ∈ L₂ := (hm a).1 (by simp)
    cases L₂ with
    | nil => simp at ha₂
    | cons b u =>
      have hbu : u.Pairwise (· < ·) := (List.pairwise_cons.mp h₂).2
      have hmin₂ : ∀ i ∈ u, b < i := (List.pairwise_cons.mp h₂).1
      by_cases hba : b = a
      · subst b
        have hm' : ∀ i, i ∈ t ↔ i ∈ u := by
          intro i
          constructor
          · intro hi
            have hia : i ≠ a := by
              intro heq
              subst i
              have hh := hmin₁ a hi
              omega
            have hmem := (hm i).1 (by simp [hi])
            simp [hia] at hmem
            exact hmem
          · intro hi
            have hib : i ≠ a := by
              intro heq
              subst i
              have hh := hmin₂ a hi
              omega
            have hmem := (hm i).2 (by simp [hi])
            simp [hib] at hmem
            exact hmem
        rw [ih ht hbu hm']
      · have hab : a ≠ b := by omega
        have hbint : b ∈ t := by
          have hmem := (hm b).2 (by simp)
          simp [hba] at hmem
          exact hmem
        have hainu : a ∈ u := by
          have hmem := (hm a).1 (by simp)
          simp [hab] at hmem
          exact hmem
        have hltb : b < a := hmin₂ a hainu
        have hlta : a < b := hmin₁ b hbint
        omega

private theorem starts_empty_nonempty (w : Word) : !(starts [] w).isEmpty = true := by
  unfold starts
  simp
  exact ⟨0, by omega⟩

private theorem empty_case_aux : ∀ (offset : Nat) (xs : List (Option Word)),
    (xs.zipIdx offset).filterMap (fun p => p.1.map (fun _ => p.2)) =
    (xs.zipIdx offset).filterMap (fun p =>
      if p.1.any (fun w => !(starts [] w).isEmpty) then some p.2 else none) := by
  intro offset xs
  induction xs generalizing offset with
  | nil => rfl
  | cons x xs ih =>
    cases x with
    | none => simp [ih (offset+1)]
    | some w =>
      have hap : (some w : Option Word).any (fun w => !(starts [] w).isEmpty) = true := by
        change !(starts [] w).isEmpty = true
        exact starts_empty_nonempty w
      simp [hap, ih (offset+1)]

private theorem flatten_take_succ (words : List Word) (i : Nat) (hi : i < words.length) :
    (words.take (i+1)).flatten.length = (words.take i).flatten.length +
      (words[i]?.getD []).length := by
  have htake : words.take (i+1) = words.take i ++ [words[i]?.getD []] := by
    rw [List.take_succ]
    have hget := List.getElem?_eq_getElem hi
    rw [hget]
    rfl
  rw [htake]
  simp

private theorem flatten_take_le : ∀ (words : List Word) (k i : Nat), k ≤ i →
    (words.take k).flatten.length ≤ (words.take i).flatten.length := by
  intro words
  induction words with
  | nil => intro k i h; simp
  | cons w ws ih =>
    intro k i h
    cases k with
    | zero => simp
    | succ k =>
      cases i with
      | zero => omega
      | succ i =>
        have hki : k ≤ i := by omega
        simpa using Nat.add_le_add_left (ih k i hki) w.length

private theorem findIdx_eq_of_bounds : ∀ (ends : List Nat) (g i : Nat),
    ends.Pairwise (· ≤ ·) →
    (∀ k, k < i → ∀ v', ends[k]? = some v' → v' ≤ g) →
    (∀ v', ends[i]? = some v' → g < v') →
    ends[i]? ≠ none →
    ends.findIdx (fun v => decide (g < v)) = i := by
  intro ends
  induction ends with
  | nil =>
    intro g i hp hprev hcur hne
    simp at hne
  | cons a t ih =>
    intro g i hp hprev hcur hne
    have hp' := List.pairwise_cons.mp hp
    cases i with
    | zero =>
      have hga : g < a := by simpa using hcur a
      simp [List.findIdx_cons, hga]
    | succ i =>
      have ha : a ≤ g := by
        exact hprev 0 (by omega) a (by simp)
      have hnlt : ¬ g < a := by omega
      have hf : (a :: t).findIdx (fun v => decide (g < v)) =
          t.findIdx (fun v => decide (g < v)) + 1 := by
        simp [List.findIdx_cons, hnlt]
      have hit : i < t.length := by
        by_cases hge : t.length ≤ i
        · have hnone : (a :: t)[i+1]? = none := by
            rw [List.getElem?_eq_none_iff]
            simp only [List.length_cons]
            omega
          exact False.elim (hne hnone)
        · omega
      have hprev' : ∀ k, k < i → ∀ v', t[k]? = some v' → v' ≤ g := by
        intro k hk v' hv
        have hv' : (a :: t)[k+1]? = some v' := by simpa [List.getElem?_cons_succ] using hv
        exact hprev (k+1) (by omega) v' hv'
      have hcur' : ∀ v', t[i]? = some v' → g < v' := by
        intro v' hv
        have hv' : (a :: t)[i+1]? = some v' := by simpa [List.getElem?_cons_succ] using hv
        exact hcur v' hv'
      have hne' : t[i]? ≠ none := by
        intro hnone
        have hv' : (a :: t)[i+1]? = none := by simpa [List.getElem?_cons_succ] using hnone
        exact hne hv'
      rw [hf]
      exact congrArg (fun n => n + 1) (ih g i hp'.2 hprev' hcur' hne')

private theorem zipIdx_mem_of_get {α : Type} : ∀ (xs : List α) (k i : Nat) (x : α),
    k ≤ i → i < k + xs.length → xs[i - k]? = some x → (x, i) ∈ xs.zipIdx k := by
  intro xs
  induction xs with
  | nil =>
    intro k i x hlo hhi hx
    simp at hhi
    omega
  | cons a as ih =>
    intro k i x hlo hhi hx
    simp only [List.length_cons] at hhi
    by_cases hik : i = k
    · subst i
      have hx' : x = a := by simpa using hx.symm
      subst x
      simp [List.zipIdx_cons]
    · have hidx : i - k = (i - (k + 1)) + 1 := by omega
      rw [hidx] at hx
      have hx' : as[i - (k + 1)]? = some x := by simpa [List.getElem?_cons_succ] using hx
      have hhi' : i < (k+1) + as.length := by omega
      have hmem := ih (k+1) i x (by omega) hhi' hx'
      exact List.mem_cons_of_mem (a, k) (by simpa [List.zipIdx_cons] using hmem)

private theorem mem_rhs : ∀ (xs : List (Option Word)) (needle : Word) (i : Nat),
    i ∈ xs.zipIdx.filterMap (fun p =>
      if p.1.any (fun w => !(starts needle w).isEmpty) then some p.2 else none) ↔
    ∃ x, (xs[i]? : Option (Option Word)) = some x ∧
      x.any (fun w => !(starts needle w).isEmpty) = true := by
  intro xs needle i
  constructor
  · intro hi
    rw [List.mem_filterMap] at hi
    rcases hi with ⟨p, hpz, hp⟩
    have hinfo := List.mem_zipIdx hpz
    have hlt : p.2 < xs.length := by simpa using hinfo.2.1
    have hget : xs[p.2]? = some p.1 := by
      rw [List.getElem?_eq_getElem hlt]
      simpa using congrArg some hinfo.2.2.symm
    by_cases hpany : p.1.any (fun w => !(starts needle w).isEmpty) = true
    · have hp2 : p.2 = i := by simpa [hpany] using hp
      subst i
      exact ⟨p.1, hget, hpany⟩
    · have hpany' : p.1.any (fun w => !(starts needle w).isEmpty) = false := by
        cases hbool : p.1.any (fun w => !(starts needle w).isEmpty)
        · rfl
        · contradiction
      simp [hpany'] at hp
  · intro h
    rcases h with ⟨x, hget, hany⟩
    rcases List.getElem?_eq_some_iff.mp hget with ⟨hilen, hxget⟩
    exact List.mem_filterMap.mpr ⟨(x, i), zipIdx_mem_of_get xs 0 i x (by omega)
      (by simpa using hilen) (by simpa using hget), by simp [hany]⟩

private theorem starts_pairwise (needle haystack : Word) :
    (starts needle haystack).Pairwise (· < ·) := by
  unfold starts
  exact (range_pairwise (haystack.length+1)).filter _

private theorem pairwise_zipIdx_snd : ∀ (xs : List (Option Word)) (k : Nat),
    (xs.zipIdx k).Pairwise (fun p q => p.2 < q.2) := by
  intro xs
  induction xs with
  | nil => intro k; simp [List.zipIdx]
  | cons x xs ih =>
    intro k
    rw [List.zipIdx_cons]
    constructor
    · intro p hp
      have hm := List.mem_zipIdx hp
      omega
    · exact ih (k+1)

private theorem pairwise_filterMap_snd : ∀ {α : Type} (L : List (α × Nat)) (P : α × Nat → Bool),
    L.Pairwise (fun p q => p.2 < q.2) →
    (L.filterMap (fun p => if P p then some p.2 else none)).Pairwise (· < ·) := by
  intro α L
  induction L with
  | nil =>
    intro P h
    simp [List.filterMap]
  | cons a L ih =>
    intro P h
    have hp := List.pairwise_cons.mp h
    by_cases hPa : P a = true
    · simp only [List.filterMap_cons, hPa, ↓reduceIte]
      constructor
      · intro c hc
        rw [List.mem_filterMap] at hc
        rcases hc with ⟨p, hpL, hpf⟩
        have hlt := hp.1 p hpL
        by_cases hPp : P p = true
        · have hcval : p.2 = c := by simpa [hPp] using hpf
          omega
        · simp [hPp] at hpf
      · exact ih P hp.2
    · have hfilt : (a :: L).filterMap (fun p => if P p then some p.2 else none) =
          L.filterMap (fun p => if P p then some p.2 else none) := by
        simp [hPa]
      rw [hfilt]
      exact ih P hp.2

private theorem pairwise_filterMap_le (L : List Nat) (F : Nat → Option Nat)
    (hL : L.Pairwise (· < ·))
    (hmono : ∀ a ∈ L, ∀ b ∈ L, a < b → ∀ i, F a = some i → ∀ j, F b = some j → i ≤ j) :
    (L.filterMap F).Pairwise (· ≤ ·) := by
  induction L with
  | nil => simp [List.filterMap]
  | cons a L ih =>
    have hp := List.pairwise_cons.mp hL
    rw [List.filterMap_cons]
    split
    · next hnone =>
      exact ih hp.2 (by
        intro x hx y hy hxy i hi j hj
        exact hmono x (by simp [hx]) y (by simp [hy]) hxy i hi j hj)
    · next i hiF =>
      constructor
      · intro c hc
        rw [List.mem_filterMap] at hc
        rcases hc with ⟨b, hbL, hbF⟩
        have hlt := hp.1 b hbL
        exact hmono a (by simp) b (by simp [hbL]) hlt i hiF c hbF
      · exact ih hp.2 (by
          intro x hx y hy hxy j hjx k hky
          exact hmono x (by simp [hx]) y (by simp [hy]) hxy j hjx k hky)

private theorem F_some_eq_findIdx (ends : List Nat) (len pos idx : Nat)
    (h : (let k := ends.findIdx (fun e => decide (pos < e))
          ends[k]?.bind (fun e => if pos + len ≤ e then some k else none)) = some idx) :
    ends.findIdx (fun e => decide (pos < e)) = idx := by
  let k := ends.findIdx (fun e => decide (pos < e))
  change (ends[k]?.bind (fun e => if pos + len ≤ e then some k else none)) = some idx at h
  rcases hget : ends[k]? with _ | e
  · rw [hget] at h
    simp at h
  · rw [hget] at h
    simp at h
    exact h.2

private theorem exists_mem_of_not_empty {α : Type} {l : List α} (h : !l.isEmpty = true) :
    ∃ x, x ∈ l := by
  have hne : l ≠ [] := by
    intro heq
    subst l
    simp at h
  exact List.exists_mem_of_ne_nil l hne

private theorem mem_left (xs : List (Option Word)) (needle : Word) (hne : needle ≠ []) (i : Nat) :
    (let words := xs.map (fun x => x.getD [])
     let ends := ((words.map List.length).scanl (·+·) 0).drop 1
     let F : Nat → Option Nat := fun pos => do
       let k := ends.findIdx (pos < ·)
       let e ← ends[k]?
       if pos + needle.length ≤ e then some k else none
     i ∈ (((starts needle words.flatten).filterMap F).eraseDups))
    ↔ ∃ x, (xs[i]? : Option (Option Word)) = some x ∧
      x.any (fun w => !(starts needle w).isEmpty) = true := by
  cases needle with
  | nil => contradiction
  | cons u t =>
    let words := xs.map (fun x => x.getD [])
    let ends := ((words.map List.length).scanl (·+·) 0).drop 1
    let F : Nat → Option Nat := fun pos => do
      let k := ends.findIdx (pos < ·)
      let e ← ends[k]?
      if pos + (u :: t).length ≤ e then some k else none
    change i ∈ (((starts (u :: t) words.flatten).filterMap F).eraseDups) ↔
      ∃ x, (xs[i]? : Option (Option Word)) = some x ∧
        x.any (fun w => !(starts (u :: t) w).isEmpty) = true
    have hpairEnds : ends.Pairwise (· ≤ ·) := by
      dsimp [ends]
      simpa [List.drop] using (scanl_pairwise (words.map List.length) 0).tail
    have hlenEnds : ends.length = xs.length := by
      dsimp [ends, words]
      simp
    constructor
    · intro hi
      rw [List.mem_eraseDups, List.mem_filterMap] at hi
      rcases hi with ⟨pos, hposStart, hFpos⟩
      have hstart := (starts_mem (u :: t) words.flatten pos).mp hposStart
      let k := ends.findIdx (fun e => decide (pos < e))
      have hFinfo : ∃ e, ends[k]? = some e ∧ pos + (u :: t).length ≤ e ∧ k = i := by
        dsimp [F] at hFpos
        rcases hget : ends[k]? with _ | e
        · rw [hget] at hFpos
          simp at hFpos
        · rw [hget] at hFpos
          simp at hFpos
          rcases hFpos with ⟨hfit, hki⟩
          exact ⟨e, rfl, hfit, by simpa [k] using hki⟩
      rcases hFinfo with ⟨e, he, hfit, hki⟩
      have he_i : ends[i]? = some e := by simpa [hki] using he
      have hilenEnds : i < ends.length := by
        by_cases hle : ends.length ≤ i
        · have hnone : ends[i]? = none := List.getElem?_eq_none_iff.mpr hle
          rw [hnone] at he_i
          simp at he_i
        · omega
      have hi_lt : i < xs.length := by omega
      have hi_words : i < words.length := by simpa [words] using hi_lt
      have he_i' : ends[i]? = some (words.take (i+1)).flatten.length := by
        simpa [ends] using ends_get words i hi_words
      have he_eq : e = (words.take (i+1)).flatten.length := by
        simpa [he_i'] using he_i.symm
      have hts := flatten_take_succ words i hi_words
      have hprevpos : (words.take i).flatten.length ≤ pos := by
        cases i with
        | zero => simp
        | succ i' =>
          have hi'lt : i' < words.length := by omega
          have hgetprev : ends[i']? = some (words.take (i'+1)).flatten.length := by
            simpa [ends] using ends_get words i' hi'lt
          have hidxeq : ends.findIdx (fun v => decide (pos < v)) = i' + 1 := by
            simpa [k] using hki
          have hb := (findIdx_spec ends pos).1 i' (by omega)
          simpa [hgetprev] using hb
      let j := pos - (words.take i).flatten.length
      have hjdef : (words.take i).flatten.length + j = pos := by
        dsimp [j]
        omega
      have hjle : j ≤ (words[i]?.getD []).length := by
        have hposlt : pos < e := by
          have hm : 0 < (u :: t).length := by simp
          omega
        omega
      have hdrop := flatten_drop words i j hi_words hjle
      have hdroppos : words.flatten.drop pos =
          ((words[i]?.getD []).drop j) ++ (words.drop (i+1)).flatten := by
        rw [← hdrop, hjdef]
      have hprefFlat := hstart.2
      rw [hdroppos] at hprefFlat
      have hlenlocal : (u :: t).length ≤ ((words[i]?.getD []).drop j).length := by
        rw [List.length_drop]
        omega
      have hprefLocal : (u :: t).isPrefixOf ((words[i]?.getD []).drop j) = true :=
        prefix_of_append hprefFlat hlenlocal
      have hjLocal : j ∈ starts (u :: t) (words[i]?.getD []) :=
        (starts_mem (u :: t) (words[i]?.getD []) j).mpr ⟨hjle, hprefLocal⟩
      have hxsget : xs[i]? = some xs[i] := List.getElem?_eq_getElem hi_lt
      let x := xs[i]
      have hwordeq : (x.getD [] : Word) = words[i]?.getD [] := by
        have hmap : words[i]? = xs[i]?.map (fun z => z.getD []) := by
          simp [words, List.getElem?_map]
        rw [hxsget] at hmap
        simp at hmap
        dsimp [x]
        rw [hmap]
        rfl
      have hlocalx : j ∈ starts (u :: t) (x.getD []) := by simpa [hwordeq] using hjLocal
      have hany : x.any (fun w => !(starts (u :: t) w).isEmpty) = true := by
        cases hx : x with
        | none =>
          have hbad := (starts_mem (u :: t) ([] : Word) j).mp (by simpa [hx] using hlocalx)
          have hm : 0 < (u :: t).length := by simp
          have hlen0 : ([] : Word).length = 0 := rfl
          simp [hlen0] at hbad
        | some w =>
          have heqfalse : (starts (u :: t) w).isEmpty = false := by
            cases hb : (starts (u :: t) w).isEmpty
            · rfl
            · have hnil : starts (u :: t) w = [] := List.isEmpty_iff.mp hb
              have hl : j ∈ starts (u :: t) w := by simpa [hx] using hlocalx
              rw [hnil] at hl
              simp at hl
          simp [hx, heqfalse]
      exact ⟨x, hxsget, hany⟩
    · rintro ⟨x, hxsget, hany⟩
      rcases List.getElem?_eq_some_iff.mp hxsget with ⟨hi_lt, hxget⟩
      rcases hx : x with _ | w
      · simp [hx] at hany
      · have hstartw : !(starts (u :: t) w).isEmpty = true := by simpa [hx] using hany
        rcases exists_mem_of_not_empty hstartw with ⟨j, hjLocal⟩
        have hjInfo := (starts_mem (u :: t) w j).mp hjLocal
        have hmpos : 0 < (u :: t).length := by simp
        have hjlt : j < w.length := by
          have hlen := (List.isPrefixOf_iff_prefix.mp hjInfo.2).length_le
          rw [List.length_drop] at hlen
          omega
        let words := xs.map (fun x => x.getD [])
        let ends := ((words.map List.length).scanl (·+·) 0).drop 1
        let p := (words.take i).flatten.length
        let pos := p + j
        have hi_words : i < words.length := by simpa [words] using hi_lt
        have hwordeq : (words[i]?.getD [] : Word) = w := by
          have hmap : words[i]? = xs[i]?.map (fun z => z.getD []) := by
            simp [words, List.getElem?_map]
          rw [hxsget] at hmap
          simp at hmap
          rw [hmap]
          simp [hx]
        have hlenw : (words[i]?.getD []).length = w.length := by rw [hwordeq]
        have hjle : j ≤ (words[i]?.getD []).length := by omega
        have he_i : ends[i]? = some (p + w.length) := by
          have hg := ends_get words i hi_words
          have ht := flatten_take_succ words i hi_words
          dsimp [ends, p]
          rw [hg]
          rw [ht, hwordeq]
        have hfit : pos + (u :: t).length ≤ p + w.length := by
          have hlen := (List.isPrefixOf_iff_prefix.mp hjInfo.2).length_le
          rw [List.length_drop] at hlen
          have hsub : j + (w.length - j) = w.length := Nat.add_sub_of_le hjInfo.1
          have hsum : j + (u :: t).length ≤ w.length := by
            have hs := Nat.add_le_add_left hlen j
            simpa [hsub] using hs
          dsimp [pos]
          simpa [Nat.add_assoc] using Nat.add_le_add_left hsum p
        have hprev : ∀ k, k < i → ∀ v', ends[k]? = some v' → v' ≤ pos := by
          intro k hk v' hv
          have hklt : k < words.length := by omega
          have hgk : ends[k]? = some (words.take (k+1)).flatten.length := by
            simpa [ends] using ends_get words k hklt
          rw [hgk] at hv
          have hv' : (words.take (k+1)).flatten.length = v' := Option.some.inj hv
          rw [← hv']
          have hmono := flatten_take_le words (k+1) i (by omega)
          dsimp [pos, p]
          omega
        have hcur : ∀ v', ends[i]? = some v' → pos < v' := by
          intro v' hv
          rw [he_i] at hv
          have hv' : p + w.length = v' := Option.some.inj hv
          rw [← hv']
          dsimp [pos]
          omega
        have hne_i : ends[i]? ≠ none := by rw [he_i]; simp
        have hfind : ends.findIdx (fun e => decide (pos < e)) = i :=
          findIdx_eq_of_bounds ends pos i hpairEnds hprev hcur hne_i
        have hdrop := flatten_drop words i j hi_words hjle
        have hdroppos : words.flatten.drop pos =
            w.drop j ++ (words.drop (i+1)).flatten := by
          dsimp [pos, p]
          have hdrop' := hdrop
          rw [hwordeq] at hdrop'
          exact hdrop'
        have hprefFlat : (u :: t).isPrefixOf (words.flatten.drop pos) = true := by
          rw [hdroppos]
          exact prefix_append hjInfo.2
        have hposle : pos ≤ words.flatten.length := by
          have ht : (words.take i).flatten.length + (words[i]?.getD []).length =
              (words.take (i+1)).flatten.length := by
            rw [flatten_take_succ words i hi_words]
          have hmono : (words.take (i+1)).flatten.length ≤ words.flatten.length := by
            have hm := flatten_take_le words (i+1) words.length (by omega)
            simpa using hm
          dsimp [pos, p]
          omega
        have hposStart : pos ∈ starts (u :: t) words.flatten :=
          (starts_mem (u :: t) words.flatten pos).mpr ⟨hposle, hprefFlat⟩
        have hFpos : F pos = some i := by
          dsimp [F]
          rw [hfind, he_i]
          have hfit' : pos + (t.length + 1) ≤ p + w.length := by simpa using hfit
          simp [hfit']
        exact List.mem_eraseDups.mpr (List.mem_filterMap.mpr ⟨pos, hposStart, hFpos⟩)
theorem sound : TinylambOpt.ResearchGoals.ClickstreamLayout.Execution.ConcatSearch.Goal := by
  intro xs needle
  by_cases hne : needle.isEmpty
  · have hnil : needle = [] := List.isEmpty_iff.mp hne
    subst needle
    change (xs.zipIdx.filterMap (fun p => p.1.map (fun _ => p.2))) =
      xs.zipIdx.filterMap (fun p =>
        match p with
        | (x, i) => if x.any (fun w => !(starts [] w).isEmpty) then some i else none)
    exact empty_case_aux 0 xs
  · have hnene : needle ≠ [] := by
      intro hnil
      subst needle
      simp at hne
    have hnot : ¬ needle.isEmpty = true := by simpa using hne
    unfold rowIDs
    rw [if_neg hnot]
    let words := xs.map (fun x => x.getD [])
    let ends := ((words.map List.length).scanl (·+·) 0).drop 1
    let F : Nat → Option Nat := fun pos => do
      let k := ends.findIdx (pos < ·)
      let e ← ends[k]?
      if pos + needle.length ≤ e then some k else none
    change (((starts needle words.flatten).filterMap F).eraseDups) =
      xs.zipIdx.filterMap (fun p =>
        if p.1.any (fun w => !(starts needle w).isEmpty) then some p.2 else none)
    have hpairEnds : ends.Pairwise (· ≤ ·) := by
      dsimp [ends]
      simpa [List.drop] using (scanl_pairwise (words.map List.length) 0).tail
    have hL0 : ((starts needle words.flatten).filterMap F).Pairwise (· ≤ ·) := by
      apply pairwise_filterMap_le
      · exact starts_pairwise needle words.flatten
      · intro a ha b hb hab ia hFa ib hFb
        have hka : ends.findIdx (fun e => decide (a < e)) = ia := by
          apply F_some_eq_findIdx ends needle.length a ia
          simpa [F] using hFa
        have hkb : ends.findIdx (fun e => decide (b < e)) = ib := by
          apply F_some_eq_findIdx ends needle.length b ib
          simpa [F] using hFb
        rw [← hka, ← hkb]
        exact findIdx_mono ends hpairEnds (by omega)
    have hL : (((starts needle words.flatten).filterMap F).eraseDups).Pairwise (· < ·) :=
      pairwise_eraseDups hL0
    have hR : (xs.zipIdx.filterMap (fun p =>
        if p.1.any (fun w => !(starts needle w).isEmpty) then some p.2 else none)).Pairwise (· < ·) := by
      exact pairwise_filterMap_snd (xs.zipIdx)
        (fun p => p.1.any (fun w => !(starts needle w).isEmpty))
        (pairwise_zipIdx_snd xs 0)
    exact pairwise_ext hL hR (by
      intro i
      constructor
      · intro hi
        have hex : ∃ x, (xs[i]? : Option (Option Word)) = some x ∧
            x.any (fun w => !(starts needle w).isEmpty) = true :=
          (mem_left xs needle hnene i).mp hi
        exact (mem_rhs xs needle i).mpr hex
      · intro hi
        have hex : ∃ x, (xs[i]? : Option (Option Word)) = some x ∧
            x.any (fun w => !(starts needle w).isEmpty) = true :=
          (mem_rhs xs needle i).mp hi
        exact (mem_left xs needle hnene i).mpr hex)
