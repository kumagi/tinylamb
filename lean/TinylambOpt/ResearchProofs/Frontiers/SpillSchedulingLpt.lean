import TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

def makespan (l : List Nat) : Nat := l.max?.getD 0

theorem foldl_max_aux (l : List Nat) (a : Nat) : l.foldl max a = max a (l.foldl max 0) := by
  induction l generalizing a with
  | nil => simp
  | cons b l ih =>
      simp only [List.foldl_cons, Nat.zero_max]
      rw [ih (max a b), ih b, Nat.max_assoc]

theorem makespan_foldl (l : List Nat) : l.max?.getD 0 = l.foldl max 0 := by
  cases l with
  | nil => rfl
  | cons a l =>
      simp only [List.max?, Option.getD_some, List.foldl_cons, Nat.zero_max]

theorem ms_cons (a : Nat) (l : List Nat) : (a :: l).max?.getD 0 = max a (l.max?.getD 0) := by
  rw [makespan_foldl, makespan_foldl]
  simp only [List.foldl_cons, Nat.zero_max]
  exact foldl_max_aux l a

theorem makespan_le_iff (l : List Nat) (c : Nat) : l.max?.getD 0 ≤ c ↔ ∀ x ∈ l, x ≤ c := by
  induction l with
  | nil => simp
  | cons a l ih =>
      rw [ms_cons]
      constructor
      · intro h x hx
        simp only [List.mem_cons] at hx
        rcases hx with hxa | hx
        · subst x
          exact Nat.le_trans (Nat.le_max_left a (l.max?.getD 0)) h
        · exact (ih.mp (Nat.le_trans (Nat.le_max_right a (l.max?.getD 0)) h)) x hx
      · intro h
        exact (Nat.max_le).mpr ⟨h a List.mem_cons_self, ih.mpr (fun x hx => h x (List.mem_cons_of_mem a hx))⟩

theorem le_makespan (l : List Nat) {x : Nat} (hx : x ∈ l) : x ≤ l.max?.getD 0 := by
  induction l with
  | nil => simp at hx
  | cons a l ih =>
      rw [ms_cons]
      simp only [List.mem_cons] at hx
      rcases hx with hxa | hx
      · subst x
        exact Nat.le_max_left a (l.max?.getD 0)
      · exact Nat.le_trans (ih hx) (Nat.le_max_right a (l.max?.getD 0))

theorem makespan_perm {l₁ l₂ : List Nat} (h : List.Perm l₁ l₂) : l₁.max?.getD 0 = l₂.max?.getD 0 := by
  apply Nat.le_antisymm
  · exact (makespan_le_iff l₁ (l₂.max?.getD 0)).mpr (fun x hx => le_makespan l₂ (h.mem_iff.mp hx))
  · exact (makespan_le_iff l₂ (l₁.max?.getD 0)).mpr (fun x hx => le_makespan l₁ (h.mem_iff.mpr hx))

theorem sum_ge_card_mul_min (l : List Nat) (p : Nat) (h : ∀ x ∈ l, p ≤ x) : l.length * p ≤ l.sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.length_cons, List.sum_cons, Nat.succ_mul]
      have h1 : p ≤ a := h a List.mem_cons_self
      have h2 : l.length * p ≤ l.sum := ih (fun x hx => h x (List.mem_cons_of_mem a hx))
      omega

theorem sum_le_card_mul_max (l : List Nat) (c : Nat) (h : ∀ x ∈ l, x ≤ c) : l.sum ≤ l.length * c := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.length_cons, List.sum_cons, Nat.succ_mul]
      have h1 : a ≤ c := h a List.mem_cons_self
      have h2 : l.sum ≤ l.length * c := ih (fun x hx => h x (List.mem_cons_of_mem a hx))
      omega

theorem sum_ge_one_ge_rest (l : List Nat) (L p : Nat)
    (hL : ∃ x ∈ l, L ≤ x) (hall : ∀ x ∈ l, p ≤ x) :
    L + (l.length - 1) * p ≤ l.sum := by
  rcases hL with ⟨x, hx, hLx⟩
  have hp : List.Perm l (x :: l.erase x) := List.perm_cons_erase hx
  have hsum : l.sum = x + (l.erase x).sum := by
    rw [hp.sum_nat]
    simp
  have hlen : (l.erase x).length = l.length - 1 := by
    have := hp.length_eq
    simp at this
    omega
  rw [hsum]
  rw [← hlen]
  have hbase : (l.erase x).length * p ≤ (l.erase x).sum :=
    sum_ge_card_mul_min (l.erase x) p (fun y hy => hall y (List.mem_of_mem_erase hy))
  omega

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

def lptLeAsc (a b : Nat) : Bool := a ≤ b
def lptLeDesc (a b : Nat) : Bool := a ≥ b

theorem leAsc_trans : ∀ a b c : Nat, lptLeAsc a b = true → lptLeAsc b c = true → lptLeAsc a c = true := by
  intro a b c h₁ h₂
  simp [lptLeAsc] at h₁ h₂ ⊢
  omega

theorem leAsc_total : ∀ a b : Nat, (lptLeAsc a b || lptLeAsc b a) = true := by
  intro a b
  simp [lptLeAsc]
  omega

theorem leDesc_trans : ∀ a b c : Nat, lptLeDesc a b = true → lptLeDesc b c = true → lptLeDesc a c = true := by
  intro a b c h₁ h₂
  simp [lptLeDesc] at h₁ h₂ ⊢
  omega

theorem leDesc_total : ∀ a b : Nat, (lptLeDesc a b || lptLeDesc b a) = true := by
  intro a b
  simp [lptLeDesc]
  omega

def place (ls : List Nat) (j : Nat) : List Nat :=
  match ls.mergeSort lptLeAsc with
  | [] => []
  | l :: rest => (l + j) :: rest

def minload (ls : List Nat) : Nat :=
  match ls.mergeSort lptLeAsc with
  | [] => 0
  | l :: _ => l

theorem mergeSort_head_min {ls : List Nat} {l : Nat} {rest : List Nat}
    (h : ls.mergeSort lptLeAsc = l :: rest) : ∀ x ∈ ls, l ≤ x := by
  have hp := List.pairwise_mergeSort leAsc_trans leAsc_total ls
  rw [h] at hp
  have hrel := (List.pairwise_cons.mp hp).1
  intro x hx
  have hx' : x ∈ ls.mergeSort lptLeAsc := List.mem_mergeSort.mpr hx
  rw [h] at hx'
  simp only [List.mem_cons] at hx'
  rcases hx' with hx' | hx'
  · subst x
    exact Nat.le_refl l
  · have := hrel x hx'
    simpa [lptLeAsc] using this

theorem mergeSort_sum (ls : List Nat) : (ls.mergeSort lptLeAsc).sum = ls.sum :=
  (List.mergeSort_perm ls lptLeAsc).sum_nat

theorem mergeSort_makespan (ls : List Nat) : (ls.mergeSort lptLeAsc).max?.getD 0 = ls.max?.getD 0 :=
  makespan_perm (List.mergeSort_perm ls lptLeAsc)

theorem place_eq (ls : List Nat) (j : Nat) {l : Nat} {rest : List Nat}
    (h : ls.mergeSort lptLeAsc = l :: rest) : place ls j = (l + j) :: rest := by
  simp [place, h]

theorem place_sum (ls : List Nat) (j : Nat) (hne : ls ≠ []) : (place ls j).sum = ls.sum + j := by
  have hn : ls.mergeSort lptLeAsc ≠ [] := by
    intro hh
    have hh' : (ls.mergeSort lptLeAsc).length = 0 := by rw [hh]; simp
    have hls : ls.length = 0 := by simpa using hh'
    exact hne (List.eq_nil_of_length_eq_zero hls)
  obtain ⟨l, rest, h⟩ := List.exists_cons_of_ne_nil hn
  rw [place_eq ls j h]
  simp only [List.sum_cons]
  have hs : (ls.mergeSort lptLeAsc).sum = ls.sum := mergeSort_sum ls
  rw [h] at hs
  simp only [List.sum_cons] at hs
  omega

theorem place_length (ls : List Nat) (j : Nat) : (place ls j).length = ls.length := by
  unfold place
  split
  · rename_i h
    have hn : (ls.mergeSort lptLeAsc).length = 0 := by rw [h]; simp
    have hls : ls.length = 0 := by simpa using hn
    have hs : ls = [] := List.eq_nil_of_length_eq_zero hls
    subst ls
    simp
  · rename_i l rest h
    simp
    have hlen := List.length_mergeSort (le := lptLeAsc) ls
    rw [h] at hlen
    simp at hlen
    omega

theorem rest_min (ls : List Nat) {l : Nat} {rest : List Nat}
    (h : ls.mergeSort lptLeAsc = l :: rest) : ∀ x ∈ rest, l ≤ x := by
  have hp := List.pairwise_mergeSort leAsc_trans leAsc_total ls
  rw [h] at hp
  intro x hx
  have := (List.pairwise_cons.mp hp).1 x hx
  simpa [lptLeAsc] using this

theorem place_makespan (ls : List Nat) (j : Nat) {l : Nat} {rest : List Nat}
    (h : ls.mergeSort lptLeAsc = l :: rest) :
    (place ls j).max?.getD 0 = max (ls.max?.getD 0) (l + j) := by
  rw [place_eq ls j h, ms_cons]
  rw [← mergeSort_makespan ls]
  rw [h]
  simp only [ms_cons]
  let R := rest.max?.getD 0
  change max (l + j) R = max (max l R) (l + j)
  have hl : l ≤ l + j := Nat.le_add_right l j
  by_cases hc : l + j ≤ R
  · have hA : max (l + j) R = R := Nat.max_eq_right hc
    have hB : max l R = R := Nat.max_eq_right (Nat.le_trans hl hc)
    rw [hA, hB, Nat.max_eq_left hc]
  · have hR : R ≤ l + j := Nat.le_of_not_ge hc
    have hA : max (l + j) R = l + j := Nat.max_eq_left hR
    have hB : max l R ≤ l + j := (Nat.max_le).mpr ⟨hl, hR⟩
    rw [hA, Nat.max_eq_right hB]

theorem minload_le_all (ls : List Nat) : ∀ x ∈ ls, minload ls ≤ x := by
  unfold minload
  split
  · intro x hx
    exact Nat.zero_le x
  · rename_i l rest h
    exact mergeSort_head_min h

theorem minload_mem (ls : List Nat) (hne : ls ≠ []) : minload ls ∈ ls := by
  unfold minload
  split
  · rename_i h
    have hn : (ls.mergeSort lptLeAsc).length = 0 := by rw [h]; simp
    have hls : ls.length = 0 := by simpa using hn
    exact (hne (List.eq_nil_of_length_eq_zero hls)).elim
  · rename_i l rest h
    have hm : l ∈ ls.mergeSort lptLeAsc := by rw [h]; exact List.mem_cons_self
    exact (List.mem_mergeSort (l := ls) (le := lptLeAsc)).mp hm

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

abbrev PState := List (Nat × List Nat)

def dispatch (loads jobs : List Nat) : List Nat :=
  jobs.foldl (fun ls j => place ls j) loads

def placeS (ps : PState) (j : Nat) : PState :=
  match ps.mergeSort (fun p q : Nat × List Nat => p.1 ≤ q.1) with
  | [] => []
  | (l, js) :: rest => (l + j, js ++ [j]) :: rest

theorem placeS_loads (ps : PState) (j : Nat) : (placeS ps j).map Prod.fst = place (ps.map Prod.fst) j := by
  unfold placeS place
  generalize hsort : ps.mergeSort (fun p q : Nat × List Nat => p.1 ≤ q.1) = sp
  have hmap : sp.map Prod.fst = (ps.map Prod.fst).mergeSort lptLeAsc := by
    rw [← hsort]
    apply List.map_mergeSort
    intro a ha b hb
    rfl
  cases sp with
  | nil =>
      have hsorted : (ps.map Prod.fst).mergeSort lptLeAsc = [] := by simpa using hmap.symm
      rw [hsorted]
      simp
  | cons p rest =>
      cases p with
      | mk l js =>
          have hsorted : (ps.map Prod.fst).mergeSort lptLeAsc = l :: rest.map Prod.fst := by simpa using hmap.symm
          rw [hsorted]
          simp

theorem foldl_greedy_loads (jobs : List Nat) : ∀ ps : PState,
    (jobs.foldl placeS ps).map Prod.fst = dispatch (ps.map Prod.fst) jobs := by
  induction jobs with
  | nil => intro ps; simp [dispatch]
  | cons j js ih =>
      intro ps
      simp only [List.foldl_cons]
      rw [ih (placeS ps j)]
      rw [placeS_loads]
      rfl

def greedyS (m : Nat) (jobs : List Nat) : PState :=
  jobs.foldl placeS (List.replicate m (0, []))

theorem greedyS_loads (m : Nat) (jobs : List Nat) :
    (greedyS m jobs).map Prod.fst = dispatch (List.replicate m 0) jobs := by
  unfold greedyS
  rw [foldl_greedy_loads]
  simp only [List.map_replicate]

def GoodState (ps : PState) : Prop := ∀ p ∈ ps, p.1 = p.2.sum

theorem good_init (m : Nat) : GoodState (List.replicate m (0, [])) := by
  intro p hp
  simp at hp
  rcases hp with ⟨_, rfl⟩
  simp

theorem good_placeS (ps : PState) (j : Nat) : GoodState ps → GoodState (placeS ps j) := by
  intro hs
  unfold placeS
  split
  · intro p hp
    cases hp
  · rename_i l js rest h
    intro p hp
    simp only [List.mem_cons] at hp
    rcases hp with rfl | hp
    · simp only [List.sum_append, List.sum_singleton]
      have hmem : (l, js) ∈ ps := by
        have hperm := List.mergeSort_perm ps (fun p q : Nat × List Nat => p.1 ≤ q.1)
        apply hperm.mem_iff.mp
        rw [h]
        simp
      have := hs (l, js) hmem
      simp at this
      omega
    · have hmem : p ∈ ps := by
        have hperm := List.mergeSort_perm ps (fun p q : Nat × List Nat => p.1 ≤ q.1)
        apply hperm.mem_iff.mp
        rw [h]
        simp [hp]
      exact hs p hmem

theorem good_foldl (ps : PState) (jobs : List Nat) : GoodState ps → GoodState (jobs.foldl placeS ps) := by
  induction jobs generalizing ps with
  | nil => intro h; exact h
  | cons j js ih => intro h; exact ih (placeS ps j) (good_placeS ps j h)

theorem good_greedyS (m : Nat) (jobs : List Nat) : GoodState (greedyS m jobs) := by
  unfold greedyS
  exact good_foldl _ _ (good_init m)

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

def jobsOf (asg : List (List Nat)) : List Nat := asg.flatten.mergeSort lptLeDesc

theorem jobsOf_pairwise (asg : List (List Nat)) :
    (jobsOf asg).Pairwise (fun a b => lptLeDesc a b = true) :=
  List.pairwise_mergeSort leDesc_trans leDesc_total _

theorem jobsOf_perm (asg : List (List Nat)) : List.Perm (jobsOf asg) asg.flatten :=
  List.mergeSort_perm _ _

theorem dispatch_sum (loads jobs : List Nat) (hne : loads ≠ []) :
    (dispatch loads jobs).sum = loads.sum + jobs.sum := by
  induction jobs generalizing loads with
  | nil => simp [dispatch]
  | cons j js ih =>
      change (dispatch (place loads j) js).sum = loads.sum + (j + js.sum)
      rw [ih (place loads j)]
      · rw [place_sum loads j hne]
        omega
      · intro h
        apply hne
        have hlen0 : loads.length = 0 := by
          have hh := congrArg List.length h
          rw [place_length] at hh
          simpa using hh
        exact List.eq_nil_of_length_eq_zero hlen0

def gsAt (m : Nat) (jobs : List Nat) (t : Nat) : PState :=
  (jobs.take t).foldl placeS (List.replicate m (0, []))

def loadAt (m : Nat) (jobs : List Nat) (t : Nat) : List Nat := (gsAt m jobs t).map Prod.fst

def capAt (m : Nat) (jobs : List Nat) (t : Nat) : Nat := makespan (loadAt m jobs t)

theorem foldl_length (jobs : List Nat) : ∀ ps : PState,
    ((jobs.foldl placeS ps).map Prod.fst).length = (ps.map Prod.fst).length := by
  induction jobs with
  | nil => intro ps; rfl
  | cons j js ih =>
      intro ps
      simp only [List.foldl_cons]
      rw [ih (placeS ps j), placeS_loads, place_length]

theorem loadAt_length (m : Nat) (jobs : List Nat) (t : Nat) : (loadAt m jobs t).length = m := by
  unfold loadAt gsAt
  rw [foldl_length]
  simp

theorem loadAt_step (m : Nat) (jobs : List Nat) {t : Nat} (ht : t < jobs.length) :
    loadAt m jobs (t + 1) = place (loadAt m jobs t) (jobs[t]) := by
  unfold loadAt gsAt
  have htake : jobs.take (t+1) = jobs.take t ++ [jobs[t]] := by
    rw [List.take_succ, List.getElem?_eq_getElem ht]
    simp
  rw [htake, List.foldl_append]
  simp only [List.foldl_cons, List.foldl_nil]
  rw [placeS_loads]

theorem loadAt_sum (m : Nat) (jobs : List Nat) (t : Nat) (hm : 0 < m) :
    (loadAt m jobs t).sum = (jobs.take t).sum := by
  unfold loadAt gsAt
  rw [foldl_greedy_loads]
  rw [dispatch_sum]
  · simp
  · intro hnil
    have hnil' : List.replicate m 0 = [] := by simpa using hnil
    have hlen0 : (List.replicate m 0).length = 0 := by rw [hnil']; simp
    simp at hlen0
    omega

theorem makespan_replicate_zero (m : Nat) : makespan (List.replicate m 0) = 0 := by
  unfold makespan
  induction m with
  | zero => rfl
  | succ m ih =>
      simp only [List.replicate_succ]
      rw [ms_cons]
      simpa using ih

theorem capAt_zero (m : Nat) (jobs : List Nat) : capAt m jobs 0 = 0 := by
  unfold capAt loadAt gsAt
  rw [foldl_greedy_loads]
  simp only [List.map_replicate]
  rw [dispatch]
  exact makespan_replicate_zero m

theorem place_makespan' (ls : List Nat) (j : Nat) (hne : ls ≠ []) :
    (place ls j).max?.getD 0 = max (ls.max?.getD 0) (minload ls + j) := by
  have hlsne : ls.mergeSort lptLeAsc ≠ [] := by
    intro h
    have : (ls.mergeSort lptLeAsc).length = 0 := by rw [h]; simp
    have : ls.length = 0 := by simpa using this
    exact hne (List.eq_nil_of_length_eq_zero this)
  obtain ⟨l, rest, h⟩ := List.exists_cons_of_ne_nil hlsne
  rw [place_makespan ls j h]
  have hm : minload ls = l := by simp [minload, h]
  rw [hm]

theorem capAt_step (m : Nat) (jobs : List Nat) (hm : 0 < m) {t : Nat} (ht : t < jobs.length) :
    capAt m jobs (t+1) = max (capAt m jobs t) (minload (loadAt m jobs t) + jobs[t]) := by
  unfold capAt makespan
  rw [loadAt_step m jobs ht, place_makespan']
  · have hlen := loadAt_length m jobs t
    intro h
    have : (loadAt m jobs t).length = 0 := by rw [h]; simp
    omega

theorem capAt_mono (m : Nat) (jobs : List Nat) (hm : 0 < m) {t : Nat} (ht : t < jobs.length) :
    capAt m jobs t ≤ capAt m jobs (t+1) := by
  rw [capAt_step m jobs hm ht]
  exact Nat.le_max_left _ _

theorem capAt_mono_le (m : Nat) (jobs : List Nat) (hm : 0 < m) {a b : Nat} (hab : a ≤ b) (hb : b ≤ jobs.length) :
    capAt m jobs a ≤ capAt m jobs b := by
  induction b with
  | zero =>
      have : a = 0 := by omega
      subst a
      exact Nat.le_refl _
  | succ b ih =>
      by_cases h : a ≤ b
      · exact Nat.le_trans (ih h (by omega)) (capAt_mono m jobs hm (by omega))
      · have : a = b + 1 := by omega
        subst a
        exact Nat.le_refl _

theorem capAt_final (m : Nat) (jobs : List Nat) :
    capAt m jobs jobs.length = makespan (dispatch (List.replicate m 0) jobs) := by
  unfold capAt loadAt gsAt
  rw [List.take_length]
  rw [foldl_greedy_loads]
  simp only [List.map_replicate]

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

theorem range_getElem (n i : Nat) (hi : i < (List.range n).length) : (List.range n)[i] = i := by
  have h1 : (List.range n)[i]? = some (List.range n)[i] := List.getElem?_eq_getElem hi
  have h2 : (List.range n)[i]? = some i := List.getElem?_range (by simpa using hi)
  exact Option.some.inj (h1.symm.trans h2)

theorem critical_exists (m : Nat) (jobs : List Nat) (hm : 0 < m)
    (hC : 0 < capAt m jobs jobs.length) :
    ∃ ki, ki < jobs.length ∧
      capAt m jobs (ki + 1) = capAt m jobs jobs.length ∧
      minload (loadAt m jobs ki) + jobs.getD ki 0 = capAt m jobs jobs.length ∧
      (∀ t, t < ki + 1 → capAt m jobs t < capAt m jobs jobs.length) := by
  let n := jobs.length
  let C := capAt m jobs n
  let r := List.range (n + 1)
  let p : Nat → Bool := fun t => decide (C ≤ capAt m jobs t)
  have hex : ∃ x ∈ r, p x = true := by
    refine ⟨n, ?_, ?_⟩
    · simp [r]
    · simp [p, C]
  let k := r.findIdx p
  have hklt_len : k < r.length := List.findIdx_lt_length_of_exists hex
  have hklt : k < n + 1 := by simpa [r] using hklt_len
  have hkprop_bool : p r[k] = true := List.findIdx_getElem (xs := r) (p := p) (w := hklt_len)
  have hkr : r[k] = k := range_getElem (n + 1) k hklt_len
  have hkprop : C ≤ capAt m jobs k := by
    rw [hkr] at hkprop_bool
    simpa [p] using hkprop_bool
  have hkpos : 0 < k := by
    by_cases hk0 : k = 0
    · rw [hk0] at hkprop
      have hcap0 := capAt_zero m jobs
      rw [hcap0] at hkprop
      have hC' : 0 < C := by simpa [C, n] using hC
      omega
    · omega
  have hmin : ∀ t, t < k → capAt m jobs t < C := by
    intro t ht
    have ht_len : t < r.length := by simpa [r] using Nat.lt_trans ht hklt
    have hnot := List.not_of_lt_findIdx (xs := r) (p := p) (i := t) (by simpa [k] using ht)
    have htr : r[t] = t := range_getElem (n + 1) t ht_len
    have hnp : p t = false := by simpa [htr] using hnot
    have hnprop : ¬ C ≤ capAt m jobs t := by simpa [p] using hnp
    exact Nat.lt_of_not_ge hnprop
  have hkle : k ≤ n := by omega
  have hcapk_eq : capAt m jobs k = C := by
    apply Nat.le_antisymm
    · exact capAt_mono_le m jobs hm hkle (by omega)
    · exact hkprop
  let ki := k - 1
  have hki_succ : ki + 1 = k := by omega
  have hki_lt : ki < jobs.length := by omega
  have hki_small : capAt m jobs ki < C := hmin ki (by omega)
  have hstep := capAt_step m jobs hm (t := ki) hki_lt
  rw [hki_succ, hcapk_eq] at hstep
  have hb : minload (loadAt m jobs ki) + jobs.getD ki 0 = C := by
    have hget : jobs.getD ki 0 = jobs[ki] := by
      simp [List.getD, List.getElem?_eq_getElem hki_lt]
    rw [hget]
    by_cases hle : capAt m jobs ki ≤ minload (loadAt m jobs ki) + jobs[ki]
    · have hh := Nat.max_eq_right hle
      rw [hh] at hstep
      exact hstep.symm
    · have hle' : minload (loadAt m jobs ki) + jobs[ki] ≤ capAt m jobs ki := Nat.le_of_not_ge hle
      have hh := Nat.max_eq_left hle'
      rw [hh] at hstep
      omega
  refine ⟨ki, hki_lt, ?_, hb, ?_⟩
  · simpa [C, n, hki_succ] using hcapk_eq
  · intro t ht
    simpa [C, n] using hmin t (by omega)

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

abbrev Tagged := Nat × Nat × Nat  -- (value, global index, machine index)

def tagListsAux (mi off : Nat) : List (List Nat) → List Tagged
  | [] => []
  | L :: rest => (L.zipIdx off).map (fun p => (p.1, p.2, mi)) ++ tagListsAux (mi + 1) (off + L.length) rest

def tagAsg (asg : List (List Nat)) : List Tagged := tagListsAux 0 0 asg

theorem tagAux_pair (mi off : Nat) (asg : List (List Nat)) :
    (tagListsAux mi off asg).map (fun t => (t.1, t.2.1)) = asg.flatten.zipIdx off := by
  induction asg generalizing mi off with
  | nil => simp [tagListsAux]
  | cons L rest ih =>
      rw [tagListsAux, List.map_append, ih (mi := mi+1) (off := off + L.length)]
      simp [List.map_map, Function.comp_def, List.zipIdx_append]

theorem tagAsg_pair (asg : List (List Nat)) :
    (tagAsg asg).map (fun t => (t.1, t.2.1)) = asg.flatten.zipIdx := by
  exact tagAux_pair 0 0 asg

theorem tagAux_midx (mi off : Nat) (asg : List (List Nat)) :
    ∀ t ∈ tagListsAux mi off asg, mi ≤ t.2.2 ∧ t.2.2 < mi + asg.length := by
  induction asg generalizing mi off with
  | nil =>
      intro t ht
      simp [tagListsAux] at ht
  | cons L rest ih =>
      intro t ht
      simp only [tagListsAux, List.mem_append] at ht
      rcases ht with ht | ht
      · rcases List.mem_map.mp ht with ⟨p, hp, rfl⟩
        simp
      · have h := ih (mi := mi + 1) (off := off + L.length) t ht
        simp only [List.length_cons]
        omega

theorem tagAux_filter_sum (mi off q : Nat) (asg : List (List Nat)) :
    (((tagListsAux mi off asg).filter (fun t => t.2.2 = mi + q)).map (fun t => t.1)).sum =
      (asg.getD q []).sum := by
  induction asg generalizing mi off q with
  | nil => simp [tagListsAux]
  | cons L rest ih =>
      simp only [tagListsAux, List.filter_append, List.map_append, List.sum_append]
      by_cases hq : q = 0
      · subst q
        simp only [Nat.add_zero]
        have hnot : ∀ t ∈ tagListsAux (mi+1) (off+L.length) rest, t.2.2 ≠ mi := by
          intro t ht hh
          have hb := tagAux_midx (mi+1) (off+L.length) rest t ht
          omega
        have hf : (tagListsAux (mi+1) (off+L.length) rest).filter (fun t => t.2.2 = mi) = [] := by
          rw [List.filter_eq_nil_iff]
          intro t ht
          simp [hnot t ht]
        have hf2 : ((L.zipIdx off).map (fun p => (p.1, p.2, mi))).filter (fun t => t.2.2 = mi) =
            (L.zipIdx off).map (fun p => (p.1, p.2, mi)) := by
          rw [List.filter_eq_self]
          intro t ht
          rcases List.mem_map.mp ht with ⟨p, hp, rfl⟩
          simp
        rw [hf, hf2]
        simpa [List.map_map, Function.comp_def] using List.zipIdx_map_fst off L
      · have hfirst : ((L.zipIdx off).map (fun p => (p.1, p.2, mi))).filter (fun t => t.2.2 = mi + q) = [] := by
          rw [List.filter_eq_nil_iff]
          intro t ht
          rcases List.mem_map.mp ht with ⟨p, hp, rfl⟩
          simp
          omega
        rw [hfirst]
        have hq' : q - 1 + 1 = q := by omega
        have hqeq : 1 + (q - 1) = q - 1 + 1 := by omega
        rw [← hq']
        simpa [Nat.add_assoc, hqeq] using ih (mi := mi+1) (off := off + L.length) (q := q - 1)

theorem tagAsg_filter_sum (asg : List (List Nat)) (mi : Nat) :
    (((tagAsg asg).filter (fun t => t.2.2 = mi)).map (fun t => t.1)).sum =
      (asg.getD mi []).sum := by
  unfold tagAsg
  simpa using tagAux_filter_sum 0 0 mi asg

theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      omega

theorem sum_range_indicator (i m : Nat) :
    ((List.range m).map (fun j => if j = i then 1 else 0)).sum = if i < m then 1 else 0 := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      by_cases hi : i < m
      · have him1 : i < m + 1 := by omega
        have hmi : ¬ m = i := by omega
        simp [hi, him1, hmi]
      · by_cases heq : i = m
        · simp [heq]
        · have hgt : m < i := by omega
          have hlt : ¬ i < m + 1 := by omega
          have hmi : ¬ m = i := by omega
          simp [hi, hlt, hmi]

theorem replicate_append_zero (m : Nat) : List.replicate m 0 ++ [0] = List.replicate (m + 1) 0 := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [List.replicate_succ, List.replicate_succ]
      simp [ih]

theorem sum_cntIdx {α : Type} (m : Nat) (l : List α) (idOf : α → Nat)
    (h : ∀ x ∈ l, idOf x < m) :
    ((List.range m).map (fun i => (l.filter (fun x => idOf x = i)).length)).sum = l.length := by
  induction l with
  | nil =>
      simp only [List.filter_nil, List.length_nil]
      have hz : (List.range m).map (fun _ => 0) = List.replicate m 0 := by
        induction m with
        | zero => rfl
        | succ m hm =>
            rw [List.range_succ, List.map_append, hm (by intro x hx; cases hx)]
            exact replicate_append_zero m
      rw [hz]
      simp
  | cons x xs ih =>
      have h' : ∀ y ∈ xs, idOf y < m := fun y hy => h y (List.mem_cons_of_mem x hy)
      have hc : ∀ i : Nat,
          (List.filter (fun y => idOf y = i) (x :: xs)).length =
            (List.filter (fun y => idOf y = i) xs).length + (if idOf x = i then 1 else 0) := by
        intro i
        by_cases hi : idOf x = i
        · simp [hi]
        · simp [hi]
      have hmap : (List.range m).map (fun i => (List.filter (fun y => idOf y = i) (x :: xs)).length) =
          (List.range m).map (fun i => (List.filter (fun y => idOf y = i) xs).length + (if idOf x = i then 1 else 0)) := by
        apply List.map_congr_left
        intro i hi
        exact hc i
      rw [hmap, sum_map_add, ih h']
      have hxm : idOf x < m := h x List.mem_cons_self
      have hind : ((List.range m).map (fun i => if idOf x = i then 1 else 0)).sum = 1 := by
        have h0 := sum_range_indicator (idOf x) m
        have hmapeq : (List.range m).map (fun i => if idOf x = i then 1 else 0) =
            (List.range m).map (fun j => if j = idOf x then 1 else 0) := by
          apply List.map_congr_left
          intro j hj
          by_cases hjx : j = idOf x <;> simp [hjx, eq_comm]
        rw [hmapeq, h0]
        simp [hxm]
      rw [hind]
      simp

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt


namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

theorem sorted_ge_of_lt (l : List Nat) (hp : l.Pairwise (fun a b => lptLeDesc a b = true))
    {i j : Nat} (hij : i < j) (hj : j < l.length) : l[j] ≤ l[i] := by
  have hi : i < l.length := Nat.lt_trans hij hj
  have htlen : (l.take (i+1)).length = i+1 := by
    rw [List.length_take]
    exact Nat.min_eq_left (Nat.succ_le_of_lt hi)
  have hmemi : l[i] ∈ l.take (i+1) := by
    rw [List.mem_iff_getElem]
    refine ⟨i, ?_, ?_⟩
    · rw [htlen]
      exact Nat.lt_succ_self i
    · exact List.getElem_take
  have hdl : (l.drop (i+1)).length = l.length - (i+1) := List.length_drop
  have hmemj : l[j] ∈ l.drop (i+1) := by
    rw [List.mem_iff_getElem]
    refine ⟨j - (i+1), ?_, ?_⟩
    · rw [hdl]
      omega
    · rw [List.getElem_drop]
      congr 1
      omega
  have hrel := hp.rel_of_mem_take_of_mem_drop hmemi hmemj
  simpa [lptLeDesc] using hrel

theorem sorted_mem_take_ge_last (l : List Nat) (hp : l.Pairwise (fun a b => lptLeDesc a b = true))
    {k x : Nat} (hkpos : 0 < k) (hkle : k ≤ l.length) (hx : x ∈ l.take k) :
    l.getD (k-1) 0 ≤ x := by
  obtain ⟨i, hi, hxi⟩ := List.mem_iff_getElem.mp hx
  have hitakes : i < (l.take k).length := hi
  have hilen : (l.take k).length = k := by rw [List.length_take]; exact Nat.min_eq_left hkle
  rw [hilen] at hitakes
  have hi' : i < l.length := Nat.lt_of_lt_of_le hitakes hkle
  have hjk : k - 1 < l.length := by omega
  have hget : l.getD (k-1) 0 = l[k-1] := by
    simp [List.getD, List.getElem?_eq_getElem hjk]
  have hxiL : l[i] = x := by
    rw [← List.getElem_take (j := k) (i := i) (h := hi)]
    exact hxi
  by_cases hlt : i < k - 1
  · have hge := sorted_ge_of_lt l hp hlt hjk
    rw [hxiL] at hge
    rw [hget]
    exact hge
  · have hieq : i = k - 1 := by omega
    subst i
    rw [hget]
    exact hxiL ▸ Nat.le_refl _

theorem filter_filter_comm {α : Type} (l : List α) (p q : α → Bool) :
    (l.filter p).filter q = (l.filter q).filter p := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hp : p a = true <;> by_cases hq : q a = true <;> simp [hp, hq, ih]

theorem count_filter_le_length {α : Type} (l : List α) (p : α → Bool) :
    (l.filter p).length ≤ l.length := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true
      · simp [h, ih]
      · simp [h]
        omega

theorem sum_ge_two_mul_sub_count_one (l : List Nat) (s : Nat)
    (hall : ∀ x ∈ l, 1 ≤ x) (hs : s = (l.filter (fun x => x = 1)).length) :
    s + 2 * (l.length - s) ≤ l.sum := by
  subst s
  induction l with
  | nil => simp
  | cons a l ih =>
      have hall' : ∀ x ∈ l, 1 ≤ x := fun x hx => hall x (List.mem_cons_of_mem a hx)
      have hi := ih hall'
      have hle : (l.filter (fun x => x = 1)).length ≤ l.length := count_filter_le_length l _
      by_cases ha : a = 1
      · simp [ha]
        omega
      · have ha2 : 2 ≤ a := by
          have := hall a List.mem_cons_self
          omega
        simp [ha]
        omega

theorem Perm.sum_map {α : Type} {l₁ l₂ : List α} (h : List.Perm l₁ l₂) (f : α → Nat) :
    (l₁.map f).sum = (l₂.map f).sum := by
  induction h with
  | nil => simp
  | cons a h ih => simp [ih]
  | swap => simp [Nat.add_left_comm]
  | trans h₁ h₂ ih₁ ih₂ => rw [ih₁, ih₂]

theorem Perm.flatMap {α β : Type} {l₁ l₂ : List α} (h : List.Perm l₁ l₂) (f : α → List β) :
    List.Perm ((l₁.map f).flatten) ((l₂.map f).flatten) :=
  List.Perm.flatten (h.map f)

theorem Perm.countP {α : Type} {l₁ l₂ : List α} (p : α → Bool) (h : List.Perm l₁ l₂) :
    l₁.countP p = l₂.countP p := by
  induction h with
  | nil => simp
  | cons a h ih => simp [List.countP_cons, ih]
  | swap => simp [List.countP_cons, Nat.add_left_comm, Nat.add_assoc, Nat.add_comm]
  | trans h₁ h₂ ih₁ ih₂ => rw [ih₁, ih₂]

theorem singleton_append_perm (a : Nat) (l : List Nat) : List.Perm ([a] ++ l) (l ++ [a]) := by
  induction l with
  | nil => simp
  | cons b l ih =>
      exact (List.Perm.swap a b l).symm.trans (List.Perm.cons b ih)

def flatP (ps : PState) : List Nat := (ps.map (fun p => p.2)).flatten

theorem placeS_length (ps : PState) (j : Nat) : (placeS ps j).length = ps.length := by
  unfold placeS
  split
  · rename_i h
    have hn : (ps.mergeSort (fun p q : Nat × List Nat => p.1 ≤ q.1)).length = 0 := by rw [h]; simp
    have hps : ps.length = 0 := by simpa using hn
    have : ps = [] := List.eq_nil_of_length_eq_zero hps
    subst ps
    simp
  · rename_i l js rest h
    simp
    have hlen := List.length_mergeSort (le := fun p q : Nat × List Nat => p.1 ≤ q.1) ps
    rw [h] at hlen
    simp at hlen
    omega

theorem placeS_ne_nil (ps : PState) (j : Nat) (hne : ps ≠ []) : placeS ps j ≠ [] := by
  intro h
  have hl := placeS_length ps j
  rw [h] at hl
  simp at hl
  exact hne (List.eq_nil_of_length_eq_zero hl.symm)

theorem placeS_flatten_perm (ps : PState) (j : Nat) (hpsne : ps ≠ []) :
    List.Perm (flatP (placeS ps j)) (flatP ps ++ [j]) := by
  unfold placeS flatP
  split
  · rename_i h
    have hn : (ps.mergeSort (fun p q : Nat × List Nat => p.1 ≤ q.1)).length = 0 := by rw [h]; simp
    have hps : ps.length = 0 := by simpa using hn
    exact (hpsne (List.eq_nil_of_length_eq_zero hps)).elim
  · rename_i l js rest h
    have hperm0 := List.mergeSort_perm ps (fun p q : Nat × List Nat => p.1 ≤ q.1)
    rw [h] at hperm0
    have hb : List.Perm ((((l, js) :: rest).map (fun p => p.2)).flatten) ((ps.map (fun p => p.2)).flatten) :=
      Perm.flatMap hperm0 (fun p => p.2)
    let restflat := ((rest.map (fun p => p.2)).flatten)
    have h1 : List.Perm ((js ++ [j]) ++ restflat) ((js ++ restflat) ++ [j]) := by
      simpa [List.append_assoc] using List.Perm.append_left js (singleton_append_perm j restflat)
    have h2 : List.Perm ((((l, js) :: rest).map (fun p => p.2)).flatten) ((ps.map (fun p => p.2)).flatten) := hb
    have h3 : List.Perm ((((l, js) :: rest).map (fun p => p.2)).flatten ++ [j])
        (((ps.map (fun p => p.2)).flatten) ++ [j]) := h2.append_right [j]
    simpa [flatP, restflat, List.append_assoc] using h1.trans h3

theorem foldl_flatten_perm (jobs : List Nat) : ∀ ps : PState, ps ≠ [] →
    List.Perm (flatP (jobs.foldl placeS ps)) (flatP ps ++ jobs) := by
  induction jobs with
  | nil => intro ps hne; simp [flatP]
  | cons j js ih =>
      intro ps hne
      simp only [List.foldl_cons]
      have h1 := ih (placeS ps j) (placeS_ne_nil ps j hne)
      have h2 := (placeS_flatten_perm ps j hne).append_right js
      have h3 : List.Perm (flatP ps ++ [j] ++ js) (flatP ps ++ (j :: js)) := by
        apply List.Perm.of_eq
        simp [List.append_assoc]
      exact h1.trans (h2.trans h3)

theorem countP_flatP_eq (ps : PState) (q : Nat) :
    (flatP ps).countP (fun x => q ≤ x) =
      (ps.map (fun p => p.2.countP (fun x => q ≤ x))).sum := by
  induction ps with
  | nil => simp [flatP]
  | cons p ps ih =>
      simp [flatP, List.countP_append, List.map_map, Function.comp_def, ih]

theorem countP_flatP_le (ps : PState) (q : Nat) :
    (flatP ps).countP (fun x => q ≤ x) ≤
      (ps.map (fun p => (p.2.filter (fun x => q ≤ x)).length)).sum := by
  have hsumc : (ps.map (fun p => p.2.countP (fun x => q ≤ x))).sum =
      (ps.map (fun p => (p.2.filter (fun x => q ≤ x)).length)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro p hp
    exact List.countP_eq_length_filter
  rw [countP_flatP_eq, hsumc]
  exact Nat.le_refl _

theorem sum_map_le_sum_map {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.add_le_add (h a List.mem_cons_self) (ih (fun x hx => h x (List.mem_cons_of_mem a hx)))

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

theorem sum_take_le (l : List Nat) (n : Nat) : (l.take n).sum ≤ l.sum := by
  calc
    (l.take n).sum ≤ (l.take n).sum + (l.drop n).sum := Nat.le_add_right _ _
    _ = ((l.take n) ++ (l.drop n)).sum := by rw [List.sum_append]
    _ = l.sum := by rw [List.take_append_drop]

theorem foldl_length_ps (jobs : List Nat) : ∀ ps : PState, (jobs.foldl placeS ps).length = ps.length := by
  induction jobs with
  | nil => intro ps; rfl
  | cons j js ih =>
      intro ps
      simp only [List.foldl_cons]
      rw [ih (placeS ps j), placeS_length]

def singleGoodCount (ps : PState) (L : Nat) : Nat :=
  (ps.filter (fun p => p.2.length = 1 && L ≤ p.2.sum)).length

theorem singleGood_le_countP (ps : PState) (L : Nat) :
    singleGoodCount ps L ≤ (flatP ps).countP (fun x => L ≤ x) := by
  induction ps with
  | nil => simp [singleGoodCount, flatP]
  | cons p ps ih =>
      cases p with
      | mk load js =>
          rw [show flatP ((load, js) :: ps) = js ++ flatP ps by rfl, List.countP_append]
          by_cases hg : js.length = 1 && L ≤ js.sum
          · have hgP : js.length = 1 ∧ L ≤ js.sum := by simpa using hg
            have hj1 : js.length = 1 := hgP.1
            cases js with
            | nil => simp at hj1
            | cons x xs =>
                cases xs with
                | nil =>
                    have hLx : L ≤ x := by simpa using hgP.2
                    simp [singleGoodCount, hg, hLx]
                    simpa [singleGoodCount, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using Nat.add_le_add_left ih 1
                | cons y ys =>
                    simp at hj1
          · simp [singleGoodCount, hg]
            exact Nat.le_trans ih (by omega)

theorem mem_le_sum {l : List Nat} {x : Nat} (hx : x ∈ l) : x ≤ l.sum := by
  induction l with
  | nil => simp at hx
  | cons a l ih =>
      simp only [List.sum_cons]
      simp only [List.mem_cons] at hx
      rcases hx with rfl | hx
      · omega
      · exact Nat.le_trans (ih hx) (Nat.le_add_left _ _)

theorem job_le_T (asg : List (List Nat)) (hm : 0 < asg.length) {x : Nat} (hx : x ∈ asg.flatten) :
    x ≤ makespan (asg.map List.sum) := by
  rw [List.mem_flatten] at hx
  obtain ⟨L, hLasg, hxL⟩ := hx
  have hsum_mem : L.sum ∈ asg.map List.sum := by
    rw [List.mem_map]
    exact ⟨L, hLasg, rfl⟩
  have hLleT : L.sum ≤ makespan (asg.map List.sum) := le_makespan _ hsum_mem
  exact Nat.le_trans (mem_le_sum hxL) hLleT

theorem flatten_sum_eq_map_sum (asg : List (List Nat)) : asg.flatten.sum = (asg.map List.sum).sum := by
  induction asg with
  | nil => simp
  | cons L asg ih =>
      simp [List.flatten_cons, ih]

theorem sum_asg_le_T (asg : List (List Nat)) :
    (asg.map List.sum).sum ≤ asg.length * makespan (asg.map List.sum) := by
  simpa using sum_le_card_mul_max (asg.map List.sum) (makespan (asg.map List.sum))
    (fun x hx => le_makespan _ hx)

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt

theorem count_len_eq_singleGood (ps : PState) (L : Nat)
    (hL : ∀ p ∈ ps, p.2.length = 1 → L ≤ p.2.sum) :
    ((ps.map (fun p => p.2.length)).filter (fun n => n = 1)).length = singleGoodCount ps L := by
  induction ps with
  | nil => simp [singleGoodCount]
  | cons p ps ih =>
      have htail : ∀ q ∈ ps, q.2.length = 1 → L ≤ q.2.sum := fun q hq hq1 =>
        hL q (List.mem_cons_of_mem p hq) hq1
      by_cases hp1 : p.2.length = 1
      · have hsum : L ≤ p.2.sum := hL p List.mem_cons_self hp1
        simp [singleGoodCount, hp1, hsum, ih htail]
      · simp [singleGoodCount, hp1, ih htail]

theorem main (asg : List (List Nat)) (hm : 0 < asg.length) :
    let m := asg.length
    let jobs := asg.flatten.mergeSort (fun a b : Nat => a ≥ b)
    3 * m * makespan (dispatch (List.replicate m 0) jobs) ≤
      (4 * m - 1) * makespan (asg.map List.sum) := by
  let m := asg.length
  let jobs := jobsOf asg
  let T := makespan (asg.map List.sum)
  let C := makespan (dispatch (List.replicate m 0) jobs)
  change 3 * m * C ≤ (4 * m - 1) * T
  by_cases hC0 : C = 0
  · rw [hC0]
    exact Nat.zero_le _
  · have hcapfinal : capAt m jobs jobs.length = C := by
      simpa [C] using capAt_final m jobs
    have hCpos : 0 < capAt m jobs jobs.length := by
      simpa [hcapfinal] using Nat.pos_of_ne_zero hC0
    obtain ⟨ki, hki_lt, hcapk, hbcrit, hsmall⟩ := critical_exists m jobs hm hCpos
    let k := ki + 1
    let p := jobs.getD ki 0
    let L := minload (loadAt m jobs ki)
    have hgetp : jobs[ki] = p := by
      simp [p, List.getD, List.getElem?_eq_getElem hki_lt]
    have hCL : C = L + p := by
      simpa [L, p, hcapfinal] using hbcrit.symm
    have hp_mem : p ∈ asg.flatten := by
      rw [← hgetp]
      have hmjob : jobs[ki] ∈ jobs := List.getElem_mem hki_lt
      exact (List.mem_mergeSort (l := asg.flatten) (le := lptLeDesc)).mp (by
        simpa [jobs, jobsOf] using hmjob)
    have hp_le_T : p ≤ T := by
      simpa [T] using job_le_T asg hm hp_mem
    by_cases hsmallcase : 3 * p ≤ T
    · have htakek : jobs.take k = jobs.take ki ++ [p] := by
        rw [show k = ki + 1 by rfl, List.take_succ, List.getElem?_eq_getElem hki_lt, hgetp]
        simp
      have hsumtake : (jobs.take ki).sum + p = (jobs.take k).sum := by
        rw [htakek, List.sum_append]
        simp
      have hmL : m * L ≤ (jobs.take ki).sum := by
        have hsumload := loadAt_sum m jobs ki hm
        have hcard := sum_ge_card_mul_min (loadAt m jobs ki) L (minload_le_all (loadAt m jobs ki))
        have hlen := loadAt_length m jobs ki
        rw [hlen, hsumload] at hcard
        exact hcard
      have hsumtotal : (jobs.take k).sum ≤ (asg.map List.sum).sum := by
        apply Nat.le_trans (sum_take_le jobs k)
        have hp := (jobsOf_perm asg).sum_nat
        have hf := flatten_sum_eq_map_sum asg
        have hp' : jobs.sum = (asg.map List.sum).sum := by
          simpa [jobs, jobsOf, hf] using hp
        rw [hp']
        exact Nat.le_refl _
      have hsumT : (asg.map List.sum).sum ≤ m * T := by
        simpa [m, T] using sum_asg_le_T asg
      have hmain1 : m * L + p ≤ m * T := by omega
      rw [hCL]
      have hm1 : 1 ≤ m := hm
      have hA := Nat.mul_le_mul_left 3 hmain1
      have hB := Nat.mul_le_mul_left (m - 1) hsmallcase
      have hA' : 3 * m * L + 3 * p ≤ 3 * m * T := by
        simpa [Nat.mul_add, Nat.mul_assoc, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hA
      have hB' : 3 * ((m - 1) * p) ≤ (m - 1) * T := by
        simpa [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm] using hB
      have hpdecomp : m * p = p + (m - 1) * p := by
        have hm_eq : m = (m - 1) + 1 := by omega
        calc
          m * p = ((m - 1) + 1) * p := congrArg (fun z : Nat => z * p) hm_eq
          _ = (m - 1) * p + p := by rw [Nat.add_mul]; simp
          _ = p + (m - 1) * p := Nat.add_comm _ _
      have hgoal1 : 3 * m * (L + p) = (3 * m * L + 3 * p) + 3 * ((m - 1) * p) := by
        rw [Nat.mul_add]
        rw [Nat.mul_assoc 3 m p, hpdecomp]
        simp [Nat.mul_add, Nat.mul_assoc, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      calc
        3 * m * (L + p) = (3 * m * L + 3 * p) + 3 * ((m - 1) * p) := hgoal1
        _ ≤ 3 * m * T + (m - 1) * T := Nat.add_le_add hA' hB'
        _ = (4 * m - 1) * T := by
          have heq : 3 * m + (m - 1) = 4 * m - 1 := by omega
          rw [← Nat.add_mul, heq]
    · have hTlt3p : T < 3 * p := Nat.lt_of_not_ge hsmallcase
      have hCT : C ≤ T := by
        by_cases hleCT : C ≤ T
        · exact hleCT
        · have hTC : T < C := Nat.lt_of_not_ge hleCT
          let cmpT : Tagged → Tagged → Bool := fun x y =>
            List.zipIdxLE lptLeDesc (x.1, x.2.1) (y.1, y.2.1)
          let tagged := tagAsg asg
          let sortedT := tagged.mergeSort cmpT
          have hcmp_trans : ∀ a b c : Tagged, cmpT a b = true → cmpT b c = true → cmpT a c = true := by
            intro a b c h₁ h₂
            exact List.zipIdxLE_trans leDesc_trans (a.1, a.2.1) (b.1, b.2.1) (c.1, c.2.1) h₁ h₂
          have hcmp_total : ∀ a b : Tagged, (cmpT a b || cmpT b a) = true := by
            intro a b
            exact List.zipIdxLE_total leDesc_total (a.1, a.2.1) (b.1, b.2.1)
          have hpermT : List.Perm sortedT tagged := by
            exact List.mergeSort_perm tagged cmpT
          have hpairwiseT : sortedT.Pairwise (fun a b => cmpT a b = true) :=
            List.pairwise_mergeSort hcmp_trans hcmp_total tagged
          have hvals : sortedT.map (fun t => t.1) = jobs := by
            have hm1 := List.map_mergeSort (r := cmpT) (s := List.zipIdxLE lptLeDesc)
                (f := fun t : Tagged => (t.1, t.2.1)) (l := tagged) (by
                  intro a ha b hb
                  rfl)
            have hpair := tagAsg_pair asg
            rw [hpair] at hm1
            have hm2 := congrArg (List.map Prod.fst) hm1
            have hzip : ((asg.flatten.zipIdx).mergeSort (List.zipIdxLE lptLeDesc)).map Prod.fst = jobs := by
              simpa [jobs, jobsOf] using (List.mergeSort_zipIdx (l := asg.flatten) (le := lptLeDesc))
            simpa [List.map_map, Function.comp_def] using hm2.trans hzip
          let pfxT := sortedT.take k
          have hpfxvals : pfxT.map (fun t => t.1) = jobs.take k := by
            have hh := congrArg (List.take k) hvals
            simpa [pfxT, List.map_take] using hh
          have hsortedlen : sortedT.length = jobs.length := by
            have := congrArg List.length hvals
            simpa using this
          have hk_le_sorted : k ≤ sortedT.length := by
            simpa [hsortedlen] using (show k ≤ jobs.length by omega)
          have hpfxlen : pfxT.length = k := by
            rw [show pfxT.length = min k sortedT.length by simp [pfxT, List.length_take]]
            exact Nat.min_eq_left hk_le_sorted
          have hpfx_ge_p : ∀ t ∈ pfxT, p ≤ t.1 := by
            intro t ht
            have hvalmem : t.1 ∈ jobs.take k := by
              have : t.1 ∈ pfxT.map (fun x => x.1) := List.mem_map.mpr ⟨t, ht, rfl⟩
              simpa [hpfxvals] using this
            have hkle : k ≤ jobs.length := by simpa [hsortedlen] using hk_le_sorted
            have hp_eq_last : p = jobs.getD (k - 1) 0 := by simp [p, k]
            rw [hp_eq_last]
            exact sorted_mem_take_ge_last jobs (by simpa [jobs] using jobsOf_pairwise asg)
              (by omega) hkle hvalmem
          have hpfx_mid : ∀ t ∈ pfxT, t.2.2 < m := by
            intro t ht
            have htsorted : t ∈ sortedT := List.mem_of_mem_take ht
            have httagged : t ∈ tagged :=
              (List.mem_mergeSort (l := tagged) (le := cmpT)).mp (by simpa [sortedT] using htsorted)
            have hb := tagAux_midx 0 0 asg t (by simpa [tagged, tagAsg] using httagged)
            omega
          have hLpos : 0 < L := by
            by_cases hL0 : L = 0
            · have hCeqp : C = p := by omega
              rw [hCeqp] at hTC
              have hf : False := Nat.not_lt_of_ge hp_le_T hTC
              exact hf.elim
            · exact Nat.pos_of_ne_zero hL0
          have machine_sum_le_T : ∀ i, i < m →
              ((tagged.filter (fun t => t.2.2 = i)).map (fun t => t.1)).sum ≤ T := by
            intro i hi
            have htsum := tagAsg_filter_sum asg i
            have hget : asg.getD i [] = asg[i] := by
              simp [List.getD, List.getElem?_eq_getElem hi]
            have hsumle : (asg.getD i []).sum ≤ T := by
              rw [hget]
              exact le_makespan (asg.map List.sum) (by
                rw [List.mem_map]
                exact ⟨asg[i], List.getElem_mem hi, rfl⟩)
            rw [htsum]
            exact hsumle
          have pfx_machine_sum_le : ∀ i,
              ((pfxT.filter (fun t => t.2.2 = i)).map (fun t => t.1)).sum ≤
              ((tagged.filter (fun t => t.2.2 = i)).map (fun t => t.1)).sum := by
            intro i
            have hfsum := Perm.sum_map (List.Perm.filter (fun t => t.2.2 = i) hpermT) (fun t => t.1)
            have hsplit : sortedT = pfxT ++ sortedT.drop k := by
              change sortedT = sortedT.take k ++ sortedT.drop k
              exact (List.take_append_drop k sortedT).symm
            have hsumsplit : ((sortedT.filter (fun t => t.2.2 = i)).map (fun t => t.1)).sum =
                ((pfxT.filter (fun t => t.2.2 = i)).map (fun t => t.1)).sum +
                  (((sortedT.drop k).filter (fun t => t.2.2 = i)).map (fun t => t.1)).sum := by
              have hfiltereq : sortedT.filter (fun t => t.2.2 = i) =
                  pfxT.filter (fun t => t.2.2 = i) ++ (sortedT.drop k).filter (fun t => t.2.2 = i) := by
                calc
                  sortedT.filter (fun t => t.2.2 = i) =
                      (pfxT ++ sortedT.drop k).filter (fun t => t.2.2 = i) := by rw [← hsplit]
                  _ = pfxT.filter (fun t => t.2.2 = i) ++ (sortedT.drop k).filter (fun t => t.2.2 = i) :=
                    List.filter_append _ _
              rw [hfiltereq, List.map_append, List.sum_append]
            rw [hfsum] at hsumsplit
            omega
          have ci_le_two : ∀ i, i < m → (pfxT.filter (fun t => t.2.2 = i)).length ≤ 2 := by
            intro i hi
            let li := pfxT.filter (fun t => t.2.2 = i)
            by_cases hle2a : li.length ≤ 2
            · simpa [li] using hle2a
            · have hle2 : ¬ li.length ≤ 2 := hle2a
              have hlen3 : 3 ≤ li.length := by omega
              have hallp : ∀ x ∈ li.map (fun t => t.1), p ≤ x := by
                intro x hx
                rcases List.mem_map.mp hx with ⟨t, ht, rfl⟩
                exact hpfx_ge_p t (by simpa [li] using (List.mem_filter.mp ht).1)
              have hsumli := sum_ge_card_mul_min (li.map (fun t => t.1)) p hallp
              have hlenmap : (li.map (fun t => t.1)).length = li.length := by simp
              rw [hlenmap] at hsumli
              have h3p_le : 3 * p ≤ (li.map (fun t => t.1)).sum :=
                Nat.le_trans (Nat.mul_le_mul_right p hlen3) hsumli
              have hsumle := pfx_machine_sum_le i
              have hmle := machine_sum_le_T i hi
              have h3pT : 3 * p ≤ T := by
                exact Nat.le_trans h3p_le (Nat.le_trans (by simpa [li] using hsumle) hmle)
              omega
          let Bpred : Tagged → Bool := fun t => L ≤ t.1
          let Bs := pfxT.filter Bpred
          have ci_eq_one_of_B : ∀ i, (∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1) →
              (pfxT.filter (fun t => t.2.2 = i)).length = 1 := by
            intro i hB
            let li := pfxT.filter (fun t => t.2.2 = i)
            have hpos : 0 < li.length := by
              obtain ⟨t, htpfx, hti, hLt⟩ := hB
              rw [List.length_pos_iff]
              intro hnil
              have htmem : t ∈ ([] : List Tagged) := by simpa [hnil] using (show t ∈ li by simp [li, htpfx, hti])
              cases htmem
            by_cases hone : li.length = 1
            · exact hone
            · have hne1 : li.length ≠ 1 := hone
              have hlen2 : 2 ≤ li.length := by omega
              have hallp : ∀ x ∈ li.map (fun t => t.1), p ≤ x := by
                intro x hx
                rcases List.mem_map.mp hx with ⟨t, ht, rfl⟩
                exact hpfx_ge_p t (by simpa [li] using (List.mem_filter.mp ht).1)
              have hsomeL : ∃ x ∈ li.map (fun t => t.1), L ≤ x := by
                obtain ⟨t, htpfx, hti, hLt⟩ := hB
                refine ⟨t.1, ?_, hLt⟩
                rw [List.mem_map]
                exact ⟨t, by simp [li, htpfx, hti], rfl⟩
              have hs := sum_ge_one_ge_rest (li.map (fun t => t.1)) L p hsomeL hallp
              have hlenmap : (li.map (fun t => t.1)).length = li.length := by simp
              rw [hlenmap] at hs
              have hmul : p * 1 ≤ p * (li.length - 1) := Nat.mul_le_mul_left p (by omega)
              have hp_le : p ≤ (li.length - 1) * p := by simpa [Nat.mul_comm] using hmul
              have hLp_sum : L + p ≤ (li.map (fun t => t.1)).sum := by omega
              have him : i < m := by
                obtain ⟨t, htpfx, hti, hLt⟩ := hB
                have := hpfx_mid t htpfx
                omega
              have hsumle := pfx_machine_sum_le i
              have hmle := machine_sum_le_T i him
              have hLpT : L + p ≤ T := Nat.le_trans hLp_sum (Nat.le_trans (by simpa [li] using hsumle) hmle)
              have hTC' : T < L + p := by simpa [hCL] using hTC
              omega
          have hBs_cnt : ∀ i, i ∈ List.range m →
              (Bs.filter (fun t => t.2.2 = i)).length =
                if (∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1) then 1 else 0 := by
            intro i hi
            by_cases hB : ∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1
            · rw [if_pos hB]
              have hone := ci_eq_one_of_B i hB
              have hsub : (Bs.filter (fun t => t.2.2 = i)).length ≤
                  (pfxT.filter (fun t => t.2.2 = i)).length := by
                have heq : Bs.filter (fun t => t.2.2 = i) =
                    (pfxT.filter (fun t => t.2.2 = i)).filter (fun t => L ≤ t.1) := by
                  unfold Bs Bpred
                  exact filter_filter_comm pfxT (fun t => L ≤ t.1) (fun t => t.2.2 = i)
                rw [heq]
                exact count_filter_le_length _ _
              have hpos : 0 < (Bs.filter (fun t => t.2.2 = i)).length := by
                obtain ⟨t, htpfx, hti, hLt⟩ := hB
                rw [List.length_pos_iff]
                intro hnil
                have htmem : t ∈ ([] : List Tagged) := by
                  simpa [hnil] using (show t ∈ Bs.filter (fun t => t.2.2 = i) by simp [Bs, Bpred, htpfx, hti, hLt])
                cases htmem
              omega
            · rw [if_neg hB]
              by_cases hz : (Bs.filter (fun t => t.2.2 = i)).length = 0
              · exact hz
              · have hn : (Bs.filter (fun t => t.2.2 = i)).length ≠ 0 := hz
                have hpos : 0 < (Bs.filter (fun t => t.2.2 = i)).length := Nat.pos_of_ne_zero hn
                obtain ⟨t, ht⟩ := List.exists_mem_of_ne_nil _ (List.length_pos_iff.mp hpos)
                have htB : t ∈ Bs := (List.mem_filter.mp ht).1
                have hti : t.2.2 = i := by simpa using (List.mem_filter.mp ht).2
                have htpfx : t ∈ pfxT := (List.mem_filter.mp htB).1
                have hLt : L ≤ t.1 := by simpa [Bpred] using (List.mem_filter.mp htB).2
                exfalso
                exact hB ⟨t, htpfx, hti, hLt⟩
          have hBs_mid : ∀ t ∈ Bs, t.2.2 < m := by
            intro t ht
            exact hpfx_mid t (List.mem_filter.mp ht).1
          have hsumB := sum_cntIdx m Bs (fun t => t.2.2) hBs_mid
          have hmapeqB : (List.range m).map (fun i => (Bs.filter (fun t => t.2.2 = i)).length) =
              (List.range m).map (fun i => if (∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1) then 1 else 0) := by
            apply List.map_congr_left
            intro i hi
            exact hBs_cnt i hi
          rw [hmapeqB] at hsumB
          have hsumk := sum_cntIdx m pfxT (fun t => t.2.2) hpfx_mid
          have hsumk' : ((List.range m).map (fun i => (pfxT.filter (fun t => t.2.2 = i)).length)).sum = k := by
            simpa [hpfxlen] using hsumk
          have hsumUpper : k ≤ ((List.range m).map
              (fun i => if (∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1) then 1 else 2)).sum := by
            rw [← hsumk']
            apply sum_map_le_sum_map
            intro i hi
            by_cases hB : ∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1
            · rw [if_pos hB]
              have hh := ci_eq_one_of_B i hB
              omega
            · rw [if_neg hB]
              exact ci_le_two i (by simpa using hi)
          have hconst : ((List.range m).map (fun _ : Nat => 2)).sum = 2 * m := by
            induction m with
            | zero => simp
            | succ m ih =>
                simp [List.range_succ, ih, Nat.mul_succ, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
          have hsumPlus : ((List.range m).map
              (fun i => if (∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1) then 1 else 2)).sum + Bs.length = 2 * m := by
            have hadd := sum_map_add (List.range m)
              (fun i => if (∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1) then 1 else 2)
              (fun i => if (∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1) then 1 else 0)
            have hper : (List.range m).map (fun i =>
                (if (∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1) then 1 else 2) +
                (if (∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1) then 1 else 0)) =
                (List.range m).map (fun _ : Nat => 2) := by
              apply List.map_congr_left
              intro i hi
              by_cases hB : ∃ t ∈ pfxT, t.2.2 = i ∧ L ≤ t.1 <;> simp [hB]
            rw [hper, hconst, hsumB] at hadd
            omega
          have hupper : k + Bs.length ≤ 2 * m := by omega
          let initState : PState := List.replicate m (0, [])
          let gs := gsAt m jobs ki
          have hinitne : initState ≠ ([] : PState) := by
            intro hnil
            have hlen0 : initState.length = 0 := by rw [hnil]; simp
            have hinitlen : initState.length = m := by simp [initState]
            omega
          have hgs_ne : gs ≠ [] := by
            have hlen := foldl_length_ps (jobs.take ki) initState
            have hglen : gs.length = m := by
              change ((jobs.take ki).foldl placeS initState).length = m
              rw [hlen]
              simp [initState]
            intro hnil
            have hz : gs.length = 0 := by rw [hnil]; simp
            omega
          have hflatlen : (flatP gs).length = (gs.map (fun p => p.2.length)).sum := by
            induction gs with
            | nil => simp [flatP]
            | cons g gs ih =>
                simp [flatP, ih, List.map_map, Function.comp_def]
          have hsumlens : (gs.map (fun p => p.2.length)).sum = ki := by
            have hperm := foldl_flatten_perm (jobs.take ki) initState hinitne
            have hlenflat := hperm.length_eq
            have htake_len : (jobs.take ki).length = ki := by
              rw [List.length_take]
              exact Nat.min_eq_left (Nat.le_of_lt hki_lt)
            have hlenflat2 : (flatP gs).length = ki := by
              simpa [gs, gsAt, initState, flatP, htake_len] using hlenflat
            rw [hflatlen] at hlenflat2
            simpa [List.map_map, Function.comp_def] using hlenflat2
          let s := ((gs.map (fun p => p.2.length)).filter (fun n => n = 1)).length
          have hlenm : (gs.map (fun p => p.2.length)).length = m := by
            have hh := foldl_length_ps (jobs.take ki) initState
            simpa [gs, gsAt, initState] using hh
          have hsle_m : s ≤ m := by
            have hh := count_filter_le_length (gs.map (fun p => p.2.length)) (fun n => n = 1)
            simpa [s, hlenm] using hh
          have hgoodgs : GoodState gs := by
            simpa [gs, gsAt, initState] using good_foldl initState (jobs.take ki) (good_init m)
          have hlens_ge1 : ∀ x ∈ gs.map (fun p => p.2.length), 1 ≤ x := by
            intro x hx
            rcases List.mem_map.mp hx with ⟨g, hg, rfl⟩
            have hgmem : g.1 ∈ loadAt m jobs ki := by
              rw [loadAt]
              exact List.mem_map.mpr ⟨g, by simpa [gs, gsAt] using hg, rfl⟩
            have hge := minload_le_all (loadAt m jobs ki) g.1 hgmem
            have hsum := hgoodgs g hg
            have hne0 : g.2.length ≠ 0 := by
              intro h0
              have hnil : g.2 = [] := List.eq_nil_of_length_eq_zero h0
              rw [hnil] at hsum
              simp at hsum
              omega
            omega
          have hgreedy : 2 * m ≤ ki + s := by
            have hb := sum_ge_two_mul_sub_count_one (gs.map (fun p => p.2.length)) s hlens_ge1 rfl
            rw [hlenm, hsumlens] at hb
            omega
          have hLsingle : ∀ g ∈ gs, g.2.length = 1 → L ≤ g.2.sum := by
            intro g hg hg1
            have hgmem : g.1 ∈ loadAt m jobs ki := by
              rw [loadAt]
              exact List.mem_map.mpr ⟨g, by simpa [gs, gsAt] using hg, rfl⟩
            have hge := minload_le_all (loadAt m jobs ki) g.1 hgmem
            have hsum := hgoodgs g hg
            omega
          have hs_eq : s = singleGoodCount gs L := by
            simpa [s] using (count_len_eq_singleGood gs L hLsingle)
          have hs_le_flat : s ≤ (flatP gs).countP (fun x => L ≤ x) := by
            rw [hs_eq]
            exact singleGood_le_countP gs L
          have hpermflat := foldl_flatten_perm (jobs.take ki) initState hinitne
          have hcount_eq : (flatP gs).countP (fun x => L ≤ x) =
              (jobs.take ki).countP (fun x => L ≤ x) := by
            have hc := Perm.countP (fun x => L ≤ x) hpermflat
            simpa [gs, gsAt, initState, flatP] using hc
          have hs_le_take : s ≤ (jobs.take ki).countP (fun x => L ≤ x) := by
            simpa [hcount_eq] using hs_le_flat
          have hBs_eq_count : Bs.length = (jobs.take k).countP (fun x => L ≤ x) := by
            have hbflat : Bs.length = (pfxT.map (fun t => t.1)).countP (fun x => L ≤ x) := by
              unfold Bs Bpred
              rw [← List.countP_eq_length_filter]
              have hc2 : (pfxT.map (fun t => t.1)).countP (fun x => L ≤ x) =
                  pfxT.countP (fun t => L ≤ t.1) := by
                induction pfxT with
                | nil => simp
                | cons t pfxT ih =>
                    simp [List.countP_cons, ih]
              rw [hc2, List.countP_eq_length_filter]
            rw [hbflat, hpfxvals]
          have htakek : jobs.take k = jobs.take ki ++ [p] := by
            rw [show k = ki + 1 by rfl, List.take_succ, List.getElem?_eq_getElem hki_lt, hgetp]
            simp
          have hs_le_b : s ≤ Bs.length := by
            rw [hBs_eq_count, htakek, List.countP_append]
            simp
            omega
          have htwo : 2 * m ≤ ki + Bs.length := Nat.le_trans hgreedy (Nat.add_le_add_left hs_le_b ki)
          omega
      have h3m : 3 * m ≤ 4 * m - 1 := by omega
      exact Nat.le_trans (Nat.mul_le_mul_left (3 * m) hCT) (Nat.mul_le_mul_right T h3m)

theorem LargestFirst_sound :
    TinylambOpt.ResearchGoals.Frontiers.SpillScheduling.LargestFirst.Goal := by
  intro asg hm
  exact main asg hm

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingLpt



