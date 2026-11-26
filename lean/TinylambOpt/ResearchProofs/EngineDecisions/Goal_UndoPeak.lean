import TinylambOpt.ResearchGoals.EngineDecisions.MVCC

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.MVCC_UndoPeak

open TinylambOpt.ResearchGoals.EngineDecisions.MVCC.UndoPeak

private def addAt : List Int → Nat → Int → List Int
  | [], _, _ => []
  | x :: xs, 0, v => (x + v) :: xs
  | x :: xs, i + 1, v => x :: addAt xs i v

private theorem addAt_length (l : List Int) (i : Nat) (v : Int) : (addAt l i v).length = l.length := by
  induction l generalizing i with
  | nil => simp [addAt]
  | cons x xs ih => cases i <;> simp [addAt, ih]

private theorem addAt_sum (l : List Int) (i : Nat) (v : Int) (hi : i < l.length) :
    (addAt l i v).sum = l.sum + v := by
  induction l generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
      cases i with
      | zero => simp [addAt]; omega
      | succ i =>
          have hi' : i < xs.length := by simpa using hi
          simp [addAt, ih i hi']
          omega

private theorem addAt_get (l : List Int) (i : Nat) (v : Int) :
    (addAt l i v)[i]? = (l[i]?).map (fun x => x + v) := by
  induction l generalizing i with
  | nil => simp [addAt]
  | cons x xs ih =>
      cases i with
      | zero => simp [addAt]
      | succ i => simpa [addAt] using ih i

private theorem addAt_get_ne (l : List Int) (i j : Nat) (v : Int) (hij : i ≠ j) :
    (addAt l i v)[j]? = l[j]? := by
  induction l generalizing i j with
  | nil => simp [addAt]
  | cons x xs ih =>
      cases i with
      | zero =>
          cases j with
          | zero => contradiction
          | succ j => simp [addAt]
      | succ i =>
          cases j with
          | zero => simp [addAt]
          | succ j =>
              have hij' : i ≠ j := by omega
              simpa [addAt] using ih i j hij'

private theorem prefix_sum_le_peak (steps : List Int) (n : Nat) (hn : n ≤ steps.length) :
    (steps.take n).sum ≤ (peak steps : Int) := by
  unfold peak
  let L := steps.scanl (fun a b => a + b) 0
  have hLne : L ≠ [] := by simp [L]
  have hM : L.max? = some (L.max?.getD 0) := by
    cases hm : L.max? with
    | none => simp [List.max?_eq_none_iff] at hm; contradiction
    | some m => simp [hm]
  have hM0 : 0 ≤ L.max?.getD 0 := by
    have hmem : (0 : Int) ∈ L := by
      rw [List.mem_iff_getElem?]
      refine ⟨0, ?_⟩
      simp [L]
    have hrefl : L.max?.getD 0 ≤ L.max?.getD 0 := by omega
    have hle := (List.max?_le_iff hM).1 hrefl 0 hmem
    simpa using hle
  rw [Int.toNat_of_nonneg hM0]
  have hg : L[n]? = some ((steps.take n).sum) := by
    dsimp [L]
    rw [List.getElem?_scanl]
    simp [hn, List.sum_eq_foldl]
  have hmem2 : (steps.take n).sum ∈ L := by
    rw [List.mem_iff_getElem?]
    exact ⟨n, hg⟩
  have hrefl : L.max?.getD 0 ≤ L.max?.getD 0 := by omega
  have hle := (List.max?_le_iff hM).1 hrefl ((steps.take n).sum) hmem2
  simpa using hle

private def bucketsAux : List Int → List Event → List Int
  | b, [] => b
  | b, (i, s) :: es => bucketsAux (addAt b i s) es

private theorem bucketsAux_sum (b : List Int) (l : List Event) (n : Nat)
    (hlen : b.length = n) (hlegal : ∀ e ∈ l, e.1 < n) :
    (bucketsAux b l).sum = b.sum + (l.map Prod.snd).sum := by
  induction l generalizing b with
  | nil => simp [bucketsAux]
  | cons e es ih =>
      have he := hlegal e (by simp)
      have hlen' : (addAt b e.1 e.2).length = n := by simpa [addAt_length] using hlen
      have hlegal' : ∀ x ∈ es, x.1 < n := by intro x hx; exact hlegal x (by simp [hx])
      have hi : e.1 < (addAt b e.1 e.2).length := by omega
      have ih' := ih (addAt b e.1 e.2) hlen' hlegal'
      simp [bucketsAux] at ih' ⊢
      rw [ih', addAt_sum b e.1 e.2 (by simpa [hlen] using he)]
      omega

private theorem bucketsAux_get (b : List Int) (l : List Event) (i : Nat)
    (hlegal : ∀ e ∈ l, e.1 < b.length) :
    (bucketsAux b l)[i]? = (b[i]?).map (fun x => x + (((l.filter fun e => e.1 == i).map Prod.snd).sum)) := by
  induction l generalizing b with
  | nil => simp [bucketsAux]
  | cons e es ih =>
      rcases e with ⟨j, s⟩
      have hj : j < b.length := hlegal (j, s) (by simp)
      have hlenj : (addAt b j s).length = b.length := addAt_length b j s
      have hlegal' : ∀ x ∈ es, x.1 < (addAt b j s).length := by
        intro x hx
        have := hlegal x (by simp [hx])
        omega
      have ih' := ih (addAt b j s) hlegal'
      by_cases hji : j == i
      · have hji' : j = i := by simpa using hji
        subst j
        simp [bucketsAux] at ih' ⊢
        rw [ih', addAt_get]
        cases hbi : b[i]? with
        | none =>
            have hsome : b[i]? = some b[i] := List.getElem?_eq_some_iff.mpr ⟨hj, rfl⟩
            simp [hbi] at hsome
        | some x =>
            simp [hbi]
            omega
      · have hji' : j ≠ i := by intro h; apply hji; simpa [h]
        simp [hji, bucketsAux] at ih' ⊢
        rw [addAt_get_ne b j i s hji'] at ih'
        simpa using ih'

private theorem peak_le_of_bound (steps : List Int) (F : Nat)
    (h : ∀ m, m ≤ steps.length → (steps.take m).sum ≤ (F : Int)) : peak steps ≤ F := by
  unfold peak
  let L := steps.scanl (fun a b => a + b) 0
  have hM : L.max? = some (L.max?.getD 0) := by
    cases hm : L.max? with
    | none =>
        have hEmpty : L = [] := by simpa using hm
        have hne : L ≠ [] := by simp [L]
        exact False.elim (hne hEmpty)
    | some m => simp [hm]
  have hM0 : 0 ≤ L.max?.getD 0 := by
    have hmem : (0 : Int) ∈ L := by
      rw [List.mem_iff_getElem?]
      refine ⟨0, ?_⟩
      simp [L]
    have hrefl : L.max?.getD 0 ≤ L.max?.getD 0 := by omega
    have hle := (List.max?_le_iff hM).1 hrefl 0 hmem
    simpa using hle
  apply Int.toNat_le.mpr
  by_cases hL : L = []
  · have hne : L ≠ [] := by simp [L]
    exact False.elim (hne hL)
  · have hmemMax : L.max?.getD 0 ∈ L := List.max?_mem hM
    rcases List.mem_iff_getElem?.mp hmemMax with ⟨m, hm⟩
    have hmlen : m < L.length := (List.getElem?_eq_some_iff.mp hm).1
    have hLlen : L.length = steps.length + 1 := by simp [L, List.length_scanl]
    have hmle : m ≤ steps.length := by omega
    have hg : L[m]? = some ((steps.take m).sum) := by
      dsimp [L]
      rw [List.getElem?_scanl]
      simp [hmle, List.sum_eq_foldl]
    have hvaleq : L.max?.getD 0 = (steps.take m).sum := by
      have htmp := hm
      rw [hg] at htmp
      injection htmp with hEq
      exact hEq.symm
    rw [hvaleq]
    exact h m hmle


private theorem sum_le_sum_of_get {l p : List Int} (hlen : l.length = p.length)
    (h : ∀ i < p.length, l.getD i 0 ≤ p.getD i 0) : l.sum ≤ p.sum := by
  induction l generalizing p with
  | nil =>
      cases p with
      | nil => simp
      | cons q qs => simp at hlen
  | cons a as ih =>
      cases p with
      | nil => simp at hlen
      | cons b bs =>
          have hhead : a ≤ b := h 0 (by simp)
          have htail : ∀ i < bs.length, as.getD i 0 ≤ bs.getD i 0 := by
            intro i hi
            have hh := h (i + 1) (by simp [hi])
            simpa using hh
          have hlen' : as.length = bs.length := by simpa using hlen
          have ih' := ih hlen' htail
          simp
          omega

private theorem prefix_filter_sum_le (schedule : List Event) (owners : List (List Int))
    (m i : Nat) (hlegal : legal schedule owners) (hi : i < owners.length) :
    (((schedule.take m).filter fun e => e.1 == i).map Prod.snd).sum ≤
      (peak (owners[i]?.getD []) : Int) := by
  have hfull := hlegal.2 i (List.mem_range.mpr hi)
  have hpref : (((schedule.take m).filter fun e => e.1 == i).map Prod.snd) <+:
      (((schedule.filter fun e => e.1 == i).map Prod.snd)) :=
    List.IsPrefix.map Prod.snd ((List.take_prefix m schedule).filter (fun e => e.1 == i))
  rw [hfull] at hpref
  let L := (((schedule.take m).filter fun e => e.1 == i).map Prod.snd)
  have hlen : L.length ≤ (owners[i]?.getD []).length := hpref.length_le
  have htakeEq : L = (owners[i]?.getD []).take L.length := by
    have hp2 : L <+: (owners[i]?.getD []).take L.length :=
      List.prefix_take_iff.mpr ⟨hpref, by omega⟩
    exact hp2.eq_of_length_le (by
      have hm : ((owners[i]?.getD []).take L.length).length = L.length := by
        simp [Nat.min_eq_left hlen]
      omega)
  change L.sum ≤ (peak (owners[i]?.getD []) : Int)
  rw [htakeEq]
  exact prefix_sum_le_peak (owners[i]?.getD []) L.length hlen

private theorem bucketsAux_length (b : List Int) (l : List Event) :
    (bucketsAux b l).length = b.length := by
  induction l generalizing b with
  | nil => simp [bucketsAux]
  | cons e es ih =>
      simp [bucketsAux]
      exact (ih (addAt b e.1 e.2)).trans (addAt_length b e.1 e.2)

private theorem natPeaks_sum_cast (owners : List (List Int)) :
    ((owners.map peak).sum : Int) = (owners.map fun o => (peak o : Int)).sum := by
  induction owners with
  | nil => simp
  | cons o os ih => simp [ih]

private theorem exists_peak_prefix (o : List Int) :
    ∃ n ≤ o.length, (o.take n).sum = (peak o : Int) := by
  unfold peak
  let L := o.scanl (fun a b => a + b) 0
  have hM : L.max? = some (L.max?.getD 0) := by
    cases hm : L.max? with
    | none =>
        have hne : L ≠ [] := by simp [L]
        have hEmpty : L = [] := by simpa using hm
        exact False.elim (hne hEmpty)
    | some m => simp [hm]
  have hM0 : 0 ≤ L.max?.getD 0 := by
    have hmem : (0 : Int) ∈ L := by
      rw [List.mem_iff_getElem?]
      refine ⟨0, ?_⟩
      simp [L]
    have hrefl : L.max?.getD 0 ≤ L.max?.getD 0 := by omega
    have hle := (List.max?_le_iff hM).1 hrefl 0 hmem
    simpa using hle
  have hmemMax : L.max?.getD 0 ∈ L := List.max?_mem hM
  rcases List.mem_iff_getElem?.mp hmemMax with ⟨n, hn⟩
  have hnle : n ≤ o.length := by
    have hnlt : n < L.length := (List.getElem?_eq_some_iff.mp hn).1
    have hlenL : L.length = o.length + 1 := by simp [L, List.length_scanl]
    omega
  refine ⟨n, hnle, ?_⟩
  have hg : L[n]? = some ((o.take n).sum) := by
    dsimp [L]
    rw [List.getElem?_scanl]
    simp [hnle, List.sum_eq_foldl]
  have htmp := hn
  rw [hg] at htmp
  injection htmp with hEq
  rw [Int.toNat_of_nonneg hM0]
  exact hEq

private noncomputable def peakIdx (o : List Int) : Nat := Classical.choose (exists_peak_prefix o)
private theorem peakIdx_spec (o : List Int) :
    peakIdx o ≤ o.length ∧ (o.take (peakIdx o)).sum = (peak o : Int) :=
  Classical.choose_spec (exists_peak_prefix o)

private def tag (i : Nat) (l : List Int) : List Event := l.map fun s => (i, s)

private theorem filter_tag (i j : Nat) (l : List Int) :
    (tag i l).filter (fun e => e.1 == j) = if i == j then tag i l else [] := by
  unfold tag
  induction l with
  | nil => simp
  | cons s ss ih =>
      by_cases hij : i == j
      · simp [hij, ih]
      · simp [hij, ih]

private noncomputable def schedFrom (owners : List (List Int)) (takePart : Bool) (n : Nat) : List Event :=
  (List.range n).flatMap fun i =>
    let o := owners.getD i []
    tag i (if takePart then o.take (peakIdx o) else o.drop (peakIdx o))

private theorem schedFrom_index (owners : List (List Int)) (takePart : Bool) (n : Nat) :
    ∀ e ∈ schedFrom owners takePart n, e.1 < n := by
  induction n with
  | zero => simp [schedFrom]
  | succ n ih =>
      unfold schedFrom
      rw [List.range_succ, List.flatMap_append]
      intro e he
      simp only [List.mem_append] at he
      rcases he with he | he
      · rcases List.mem_flatMap.mp he with ⟨a, ha, heTag⟩
        have han : a < n := List.mem_range.mp ha
        rcases List.mem_map.mp heTag with ⟨s, hs, hEq⟩
        subst e
        have hmem : (a, s) ∈ schedFrom owners takePart n := by
          apply List.mem_flatMap.mpr
          exact ⟨a, ha, heTag⟩
        have := ih (a, s) hmem
        omega
      · have hmem : e ∈ tag n (let o := owners.getD n []; if takePart then o.take (peakIdx o) else o.drop (peakIdx o)) := by
          simpa using he
        rcases List.mem_map.mp hmem with ⟨s, hs, hEq⟩
        subst e
        simp

private theorem range_flatMap_filter (f : Nat → List Int) (n j : Nat) :
    ((List.range n).flatMap fun i => tag i (f i)).filter (fun e => e.1 == j) =
      if j < n then tag j (f j) else [] := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.flatMap_append, List.filter_append, ih]
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      rw [filter_tag]
      by_cases hjn1 : j < n + 1
      · by_cases hjn : j < n
        · have hjne : n ≠ j := by omega
          simp [hjn, hjne, hjn1]
        · have hjeq : j = n := by omega
          subst j
          simp
      · have hjn : ¬ j < n := by omega
        have hjne : n ≠ j := by omega
        simp [hjn, hjne, hjn1]

private theorem schedFrom_filter (owners : List (List Int)) (takePart : Bool) (n j : Nat) :
    (schedFrom owners takePart n).filter (fun e => e.1 == j) =
      if j < n then
        let o := owners.getD j []
        tag j (if takePart then o.take (peakIdx o) else o.drop (peakIdx o))
      else [] := by
  unfold schedFrom
  exact range_flatMap_filter (fun i => let o := owners.getD i []; if takePart then o.take (peakIdx o) else o.drop (peakIdx o)) n j

private theorem map_tag_sum (i : Nat) (l : List Int) : ((tag i l).map Prod.snd).sum = l.sum := by
  induction l with
  | nil => simp [tag]
  | cons s ss ih => simp [tag, ih, Function.comp_def]

private theorem schedFrom_sum (owners : List (List Int)) (takePart : Bool) (n : Nat) :
    ((schedFrom owners takePart n).map Prod.snd).sum =
      ((List.range n).map fun i => let o := owners.getD i [];
        if takePart then (o.take (peakIdx o)).sum else (o.drop (peakIdx o)).sum).sum := by
  unfold schedFrom
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.flatMap_append, List.map_append, List.sum_append, ih]
      simp [map_tag_sum]
      by_cases ht : takePart = true <;> simp [ht]

private theorem prefix_sum_mem_scanl (steps : List Int) (m : Nat) (hm : m ≤ steps.length) :
    (steps.take m).sum ∈ steps.scanl (fun a b => a + b) 0 := by
  rw [List.mem_iff_getElem?]
  refine ⟨m, ?_⟩
  rw [List.getElem?_scanl]
  simp [hm, List.sum_eq_foldl]

private theorem range_pre_sum (owners : List (List Int)) : ∀ n,
    ((List.range n).map fun i =>
      let o := owners.getD i []
      (o.take (peakIdx o)).sum).sum =
    ((owners.take n).map fun o => (peak o : Int)).sum := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      have hmapTakeSum (m : Nat) : (List.map (fun o : List Int => (peak o : Int)) (List.take m owners)).sum =
          (List.take m (List.map (fun o : List Int => (peak o : Int)) owners)).sum := by
        rw [List.map_take]
      by_cases hn : n < owners.length
      · have hget : owners.getD n [] = owners[n] := by
          simp [List.getD, List.getElem?_eq_getElem hn]
        rw [hget]
        have hspec := (peakIdx_spec owners[n]).2
        rw [hspec]
        have htake : List.take (n + 1) (List.map (fun o => (peak o : Int)) owners) =
            List.take n (List.map (fun o => (peak o : Int)) owners) ++ [(peak owners[n] : Int)] := by
          rw [List.take_succ]
          simp [List.getElem?_eq_getElem hn]
        rw [hmapTakeSum n, hmapTakeSum (n + 1), htake]
        simp
      · have hget : owners.getD n [] = [] := by
          simp [List.getD, hn, Nat.le_of_not_gt hn]
        rw [hget]
        simp [hn, Nat.le_of_not_gt hn]
        have htake : List.take (n + 1) (List.map (fun o => (peak o : Int)) owners) =
            List.take n (List.map (fun o => (peak o : Int)) owners) := by
          rw [List.take_succ]
          simp [hn]
        rw [htake]

theorem sound : Goal := by
  intro owners free
  constructor
  · intro hsum schedule hlegal
    have hbound (m : Nat) : ((schedule.take m).map Prod.snd).sum ≤ ((owners.map peak).sum : Int) := by
      let b : List Int := List.replicate owners.length 0
      have hhlen : b.length = owners.length := by simp [b]
      have hleg : ∀ e ∈ schedule.take m, e.1 < owners.length := by
        intro e he
        exact hlegal.1 e (List.mem_of_mem_take he)
      have hsumEq := bucketsAux_sum b (schedule.take m) owners.length hhlen hleg
      have hb0 : b.sum = 0 := by simp [b]
      simp [hb0] at hsumEq
      have hcast := natPeaks_sum_cast owners
      have hbuck' : (bucketsAux b (schedule.take m)).sum ≤ (owners.map fun o => (peak o : Int)).sum := by
        apply sum_le_sum_of_get
        · rw [bucketsAux_length]
          simpa using hhlen
        · intro i hi
          have hmaplen : (owners.map fun o => (peak o : Int)).length = owners.length := by simp
          have hiB : i < b.length := by omega
          have hget := bucketsAux_get b (schedule.take m) i (by
            intro e he
            have := hlegal.1 e (List.mem_of_mem_take he)
            simpa [hhlen] using this)
          rw [List.getElem?_eq_getElem hiB] at hget
          cases hbi : b[i]? with
          | none =>
              have hsome : b[i]? = some b[i] := List.getElem?_eq_some_iff.mpr ⟨hiB, rfl⟩
              simp [hbi] at hsome
          | some x =>
              simp [hbi] at hget
              have hlenX : i < (bucketsAux b (schedule.take m)).length := by
                rw [bucketsAux_length]
                omega
              have hbget : (bucketsAux b (schedule.take m)).getD i 0 =
                  (bucketsAux b (schedule.take m))[i] := by simp [List.getD, List.getElem?_eq_getElem hlenX]
              rw [List.getElem?_eq_getElem hlenX] at hget
              injection hget with hval
              rw [hbget, hval]
              have hiOwn : i < owners.length := by omega
              have hpf := prefix_filter_sum_le schedule owners m i hlegal hiOwn
              have hpeakGet : (owners.map fun o => (peak o : Int)).getD i 0 =
                  (owners.map fun o => (peak o : Int))[i] := by
                simp [List.getD, List.getElem?_eq_getElem hi]
              rw [hpeakGet]
              have hbzero : b[i] = 0 := by simp [b]
              rw [hbzero]; simp
              simpa [List.getD, List.getElem?_eq_getElem hiOwn] using hpf
      have hbuck : (bucketsAux b (schedule.take m)).sum ≤ ((owners.map peak).sum : Int) := by
        simpa [hcast] using hbuck'
      have hsumEq' : (bucketsAux b (schedule.take m)).sum = ((schedule.take m).map Prod.snd).sum := by
        simpa [List.map_take] using hsumEq
      rw [← hsumEq']
      exact hbuck
    have hpeak : peak (schedule.map Prod.snd) ≤ (owners.map peak).sum := by
      apply peak_le_of_bound
      intro m hm
      simpa [List.map_take] using hbound m
    omega
  · intro h
    let n := owners.length
    let pre := schedFrom owners true n
    let post := schedFrom owners false n
    let sch := pre ++ post
    have hlegal : legal sch owners := by
      constructor
      · intro e he
        simp [sch] at he
        rcases he with he | he
        · exact schedFrom_index owners true n e he
        · exact schedFrom_index owners false n e he
      · intro i hi
        have hi_lt : i < n := List.mem_range.mp hi
        dsimp [sch]
        rw [List.filter_append, schedFrom_filter, schedFrom_filter]
        simp [hi_lt]
        have htake : (owners.getD i []).take (peakIdx (owners.getD i [])) ++
              (owners.getD i []).drop (peakIdx (owners.getD i [])) = owners.getD i [] :=
          List.take_append_drop (peakIdx (owners.getD i [])) (owners.getD i [])
        simpa [tag, List.map_map, Function.comp_def, htake]
    have hupper : peak (sch.map Prod.snd) ≤ free := h sch hlegal
    have hpre_sum : ((pre.map Prod.snd).sum) = ((owners.map fun o => (peak o : Int)).sum) := by
      dsimp [pre]
      rw [schedFrom_sum]
      simpa using (range_pre_sum owners owners.length)
    have hlower : (owners.map peak).sum ≤ peak (sch.map Prod.snd) := by
      let steps := sch.map Prod.snd
      let L := steps.scanl (fun a b => a + b) 0
      have hM : L.max? = some (L.max?.getD 0) := by
        cases hm : L.max? with
        | none =>
            have hne : L ≠ [] := by simp [L]
            have hEmpty : L = [] := by simpa using hm
            exact False.elim (hne hEmpty)
        | some m => simp [hm]
      have hM0 : 0 ≤ L.max?.getD 0 := by
        have hmem : (0 : Int) ∈ L := by
          rw [List.mem_iff_getElem?]
          refine ⟨0, ?_⟩
          simp [L]
        have hrefl : L.max?.getD 0 ≤ L.max?.getD 0 := by omega
        have hle := (List.max?_le_iff hM).1 hrefl 0 hmem
        simpa using hle
      have hmle : pre.length ≤ steps.length := by
        dsimp [steps, sch]
        simp
      have htake : steps.take pre.length = pre.map Prod.snd := by
        dsimp [steps, sch]
        simp [List.map_append, List.map_take]
      have hmemsum : ((pre.map Prod.snd).sum) ∈ L := by
        simpa [tag, List.map_map, Function.comp_def, htake] using prefix_sum_mem_scanl steps pre.length hmle
      have hrefl : L.max?.getD 0 ≤ L.max?.getD 0 := by omega
      have hle := (List.max?_le_iff hM).1 hrefl ((pre.map Prod.snd).sum) hmemsum
      have hcast := natPeaks_sum_cast owners
      have hint : (((owners.map peak).sum : Nat) : Int) ≤ (peak (sch.map Prod.snd) : Int) := by
        dsimp [L] at hle
        rw [hpre_sum] at hle
        have hpeakEq : (peak (sch.map Prod.snd) : Int) = L.max?.getD 0 := by
          unfold peak
          dsimp [steps, L]
          rw [Int.toNat_of_nonneg]
          simpa [L] using hM0
        rw [hpeakEq]
        rw [hcast]
        exact hle
      omega
    omega

end TinylambOpt.ResearchProofs.EngineDecisions.MVCC_UndoPeak
