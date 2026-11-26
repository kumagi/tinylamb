import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

/-! Shared graph lemmas for compiled-code reachability. They describe how the
mark closure grows along direct-call edges, how filtering the heap preserves
the edges of live objects, and how an injective relocation transports the
marked set. These facts let SQL execution reclaim code without changing the
targets reachable from active frames. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

theorem mem_grow (heap : List Obj) (live : List Key) (k : Key) :
    k ∈ grow heap live ↔ k ∈ live ∨ ∃ x ∈ live, k ∈ edges heap x := by
  simp [grow, List.mem_flatMap]

theorem grow_mono (heap : List Obj) {L M : List Key} (hsub : ∀ x, x ∈ L → x ∈ M) {k : Key}
    (hk : k ∈ grow heap L) : k ∈ grow heap M := by
  rw [mem_grow] at hk ⊢
  rcases hk with h | ⟨x, hx, hxk⟩
  · exact Or.inl (hsub k h)
  · exact Or.inr ⟨x, hsub x hx, hxk⟩

theorem grow_edges_mono (a b : List Obj) {L : List Key}
    (hedges : ∀ k, ∀ x ∈ edges b k, x ∈ edges a k) {k : Key}
    (hk : k ∈ grow b L) : k ∈ grow a L := by
  rw [mem_grow] at hk ⊢
  rcases hk with h | ⟨x, hx, hxk⟩
  · exact Or.inl h
  · exact Or.inr ⟨x, hx, hedges x k hxk⟩

theorem find_filter_eq_aux {α : Type} [BEq α] [LawfulBEq α] (p q : α → Bool)
    (h : ∀ a, q a = true → p a = true) : ∀ xs : List α,
    (xs.filter p).find? q = xs.find? q := by
  intro xs
  induction xs with
  | nil => simp
  | cons a as ih =>
      by_cases hqa : q a = true
      · have hpa : p a = true := h a hqa
        simp [hqa, hpa]
      · by_cases hpa : p a = true
        · simp [hqa, hpa, ih]
        · simp [hqa, hpa, ih]

theorem edges_retain (heap : List Obj) (live : List Key) (k : Key) (hk : k ∈ live) :
    edges (retain heap live) k = edges heap k := by
  unfold edges retain
  rw [find_filter_eq_aux]
  intro o ho
  have hok : o.key = k := by simpa using ho
  simp [List.contains_eq_mem, hok, hk]

theorem mark_mono_roots (heap : List Obj) {a b : List Key} (hsub : ∀ k, k ∈ a → k ∈ b) :
    ∀ n k, k ∈ mark heap a n → k ∈ mark heap b n := by
  intro n
  induction n with
  | zero =>
      intro k hk
      change k ∈ a.eraseDups at hk
      change k ∈ b.eraseDups
      simpa using hsub k (by simpa using hk)
  | succ n ih =>
      intro k hk
      change k ∈ grow heap (mark heap a n) at hk
      change k ∈ grow heap (mark heap b n)
      exact grow_mono heap (fun x hx => ih x hx) hk

theorem mark_mono_fuel (heap : List Obj) (roots : List Key) :
    ∀ n k, k ∈ mark heap roots n → k ∈ mark heap roots (n + 1) := by
  intro n k hk
  change k ∈ grow heap (mark heap roots n)
  rw [mem_grow]
  exact Or.inl hk

theorem mark_edges_mono (a b : List Obj) (roots : List Key)
    (hedges : ∀ k, ∀ x ∈ edges b k, x ∈ edges a k) :
    ∀ n k, k ∈ mark b roots n → k ∈ mark a roots n := by
  intro n
  induction n with
  | zero =>
      intro k hk
      change k ∈ roots.eraseDups at hk
      change k ∈ roots.eraseDups
      exact hk
  | succ n ih =>
      intro k hk
      change k ∈ grow b (mark b roots n) at hk
      change k ∈ grow a (mark a roots n)
      exact grow_mono a (fun x hx => ih x hx)
        (grow_edges_mono (L := mark b roots n) a b hedges hk)

theorem find?_map_injective (heap : List Obj) (f : Key → Key)
    (hf : ∀ a b, f a = f b → a = b) (k : Key) :
    (heap.map (fun o => { o with key := f o.key, calls := o.calls.map f })).find?
        (fun o => o.key == f k) =
      (heap.find? (fun o => o.key == k)).map (fun o => { o with key := f o.key, calls := o.calls.map f }) := by
  induction heap with
  | nil => simp
  | cons o os ih =>
      by_cases hok : o.key == k
      · have heq : o.key = k := by simpa using hok
        have hfk : f o.key == f k := by simp [heq]
        simp [heq]
      · have hneq : o.key ≠ k := by simpa using hok
        have hfk : (f o.key == f k) = false := by
          simp only [Bool.beq_eq_decide_eq, decide_eq_false_iff_not]
          intro h
          exact hneq (hf _ _ h)
        simp [hok, hfk, ih]

theorem edges_map_of_injective (heap : List Obj) (f : Key → Key)
    (hf : ∀ a b, f a = f b → a = b) (k : Key) :
    edges (heap.map (fun o => { o with key := f o.key, calls := o.calls.map f })) (f k) =
      (edges heap k).map f := by
  unfold edges
  rw [find?_map_injective heap f hf k]
  cases hfind : heap.find? (fun o => o.key == k) with
  | none => simp
  | some o => simp

theorem mark_map_image (heap : List Obj) (roots : List Key) (f : Key → Key)
    (hf : ∀ a b, f a = f b → a = b) :
    ∀ n x,
      x ∈ mark (heap.map (fun o => { o with key := f o.key, calls := o.calls.map f })) (roots.map f) n ↔
        ∃ y, f y = x ∧ y ∈ mark heap roots n := by
  let mapped := heap.map (fun o => { o with key := f o.key, calls := o.calls.map f })
  intro n
  induction n with
  | zero =>
      intro x
      change x ∈ (roots.map f).eraseDups ↔ ∃ y, f y = x ∧ y ∈ roots.eraseDups
      simp only [List.mem_eraseDups, List.mem_map]
      constructor
      · rintro ⟨y, hy, hfy⟩
        exact ⟨y, hfy, hy⟩
      · rintro ⟨y, hfy, hy⟩
        exact ⟨y, hy, hfy⟩
  | succ n ih =>
      intro x
      change x ∈ grow mapped (mark mapped (roots.map f) n) ↔
        ∃ y, f y = x ∧ y ∈ grow heap (mark heap roots n)
      constructor
      · intro hx
        simp only [mem_grow] at hx ⊢
        rcases hx with hx | ⟨z, hz, hxz⟩
        · rcases (ih x).mp hx with ⟨y, hfy, hy⟩
          exact ⟨y, hfy, Or.inl hy⟩
        · rcases (ih z).mp hz with ⟨y, hfy, hy⟩
          have hedge : edges mapped z = (edges heap y).map f := by
            rw [← hfy]
            exact edges_map_of_injective heap f hf y
          rw [hedge] at hxz
          rcases List.mem_map.mp hxz with ⟨w, hw, hfw⟩
          exact ⟨w, hfw, Or.inr ⟨y, hy, hw⟩⟩
      · intro hx
        simp only [mem_grow] at hx ⊢
        rcases hx with ⟨y, hfy, hy | ⟨z, hz, hyz⟩⟩
        · exact Or.inl ((ih x).mpr ⟨y, hfy, hy⟩)
        · refine Or.inr ⟨f z, (ih (f z)).mpr ⟨z, rfl, hz⟩, ?_⟩
          rw [← hfy]
          rw [edges_map_of_injective heap f hf z]
          exact List.mem_map.mpr ⟨y, hyz, rfl⟩

theorem mark_map_of_injective (heap : List Obj) (roots : List Key) (f : Key → Key)
    (hf : ∀ a b, f a = f b → a = b) :
    ∀ n k, f k ∈ mark (heap.map (fun o => { o with key := f o.key, calls := o.calls.map f }))
      (roots.map f) n ↔ k ∈ mark heap roots n := by
  intro n k
  rw [mark_map_image heap roots f hf]
  constructor
  · rintro ⟨y, hfy, hy⟩
    have hyk : y = k := hf y k hfy
    simpa [hyk] using hy
  · intro hk
    exact ⟨k, rfl, hk⟩

/-! Objects whose key is currently marked; its length counts the bodies that
can still be reached by a direct call. -/
def active (heap : List Obj) (L : List Key) : List Obj := heap.filter fun o => L.contains o.key

theorem mem_active (heap : List Obj) (L : List Key) (o : Obj) (ho : o ∈ heap)
    (hk : o.key ∈ L) : o ∈ active heap L := by
  simp [active, List.contains_eq_mem, ho, hk]

theorem not_mem_active (heap : List Obj) (L : List Key) (o : Obj) (hk : o.key ∉ L) :
    o ∉ active heap L := by
  intro ho
  have hc := (List.mem_filter.mp ho).2
  exact hk (by simpa [List.contains_eq_mem] using hc)

theorem active_sublist (heap : List Obj) {L M : List Key} (hsub : ∀ k, k ∈ L → k ∈ M) :
    (active heap L).Sublist (active heap M) := by
  induction heap with
  | nil => simp [active]
  | cons o os ih =>
      by_cases hL : o.key ∈ L
      · have hM : o.key ∈ M := hsub o.key hL
        simpa [active, List.contains_eq_mem, hL, hM] using ih
      · by_cases hM : o.key ∈ M
        · simpa [active, List.contains_eq_mem, hL, hM] using (List.Sublist.cons o ih)
        · simpa [active, List.contains_eq_mem, hL, hM] using ih

theorem active_strict_of_ne (heap : List Obj) {L M : List Key} (hsub : ∀ k, k ∈ L → k ∈ M)
    (hne : active heap L ≠ active heap M) : (active heap L).length < (active heap M).length := by
  have hs := active_sublist heap hsub
  have hle := hs.length_le
  apply Nat.lt_of_le_of_ne hle
  intro heq
  exact hne (hs.eq_of_length heq)

theorem edges_object (heap : List Obj) {x k : Key} (hk : k ∈ edges heap x) :
    ∃ o ∈ heap, o.key = x := by
  unfold edges at hk
  cases hfind : heap.find? (fun o => o.key == x) with
  | none => simp [hfind] at hk
  | some o =>
      refine ⟨o, List.mem_of_find?_eq_some hfind, ?_⟩
      have hp := List.find?_some hfind
      simpa using hp

theorem eraseDups_nodup (l : List Key) : l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · simp
      · exact eraseDups_nodup (as.filter fun b => !b == a)
termination_by l.length
decreasing_by
  simp only [List.length_cons]
  have hle := List.length_filter_le (fun b => !b == a) as
  omega

theorem eraseDups_of_nodup (l : List Key) (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hnot : a ∉ as := (List.nodup_cons.mp h).1
      have hnd : as.Nodup := (List.nodup_cons.mp h).2
      have hf : (as.filter fun b => !b == a) = as := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hba : b ≠ a := by
          intro heq
          exact hnot (by simpa [heq] using hb)
        simp [hba]
      rw [List.eraseDups_cons, hf, ih hnd]

/-! Appending keys that are already present to a deduplicated list leaves the
deduplicated list unchanged. -/
theorem eraseDups_append_of_mem (M xs : List Key) (hM : M.Nodup)
    (hmem : ∀ x ∈ xs, x ∈ M) : (M ++ xs).eraseDups = M := by
  induction M generalizing xs with
  | nil =>
      have hnil : xs = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro x hx
        simpa using hmem x hx
      simp [hnil]
  | cons a as ih =>
      change (a :: (as ++ xs)).eraseDups = a :: as
      rw [List.eraseDups_cons]
      have hnot : a ∉ as := (List.nodup_cons.mp hM).1
      have hnd : as.Nodup := (List.nodup_cons.mp hM).2
      have hf : (as.filter fun b => !b == a) = as := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hba : b ≠ a := by
          intro heq
          exact hnot (by simpa [heq] using hb)
        simp [hba]
      rw [List.filter_append, hf]
      have hmem' : ∀ x ∈ (xs.filter fun b => !b == a), x ∈ as := by
        intro x hx
        have hx' := (List.mem_filter.mp hx).1
        have hmemx := hmem x (by simp [hx'])
        simp at hmemx
        rcases hmemx with hxa | hxas
        · have hne : x ≠ a := by
            intro hxeq
            have hb := (List.mem_filter.mp hx).2
            simp [hxeq] at hb
          exact False.elim (hne hxa)
        · exact hxas
      rw [ih (xs.filter fun b => !b == a) hnd hmem']

theorem grow_nodup (heap : List Obj) (L : List Key) : (grow heap L).Nodup := by
  unfold grow
  exact eraseDups_nodup _

theorem grow_eq_self_of_active_eq (heap : List Obj) {L M : List Key}
    (hM : M = grow heap L) (hact : active heap L = active heap M) : grow heap M = M := by
  unfold grow
  apply eraseDups_append_of_mem
  · rw [hM]
    exact grow_nodup heap L
  · intro x hx
    rcases List.mem_flatMap.mp hx with ⟨z, hz, hxz⟩
    rcases edges_object heap hxz with ⟨o, ho, hoz⟩
    have hoM : o ∈ active heap M := mem_active heap M o ho (by simpa [hoz] using hz)
    have hoL : o ∈ active heap L := by simpa [hact] using hoM
    have hzL : z ∈ L := by
      have hc := (List.mem_filter.mp hoL).2
      simpa [List.contains_eq_mem, hoz] using hc
    rw [hM]
    rw [mem_grow]
    exact Or.inr ⟨z, hzL, hxz⟩

theorem grow_change_implies_active_growth (heap : List Obj) {L : List Key}
    (hchange : grow heap (grow heap L) ≠ grow heap L) :
    (active heap L).length < (active heap (grow heap L)).length := by
  have hsub : ∀ k, k ∈ L → k ∈ grow heap L := by
    intro k hk
    rw [mem_grow]
    exact Or.inl hk
  apply active_strict_of_ne heap hsub
  intro hact
  exact hchange (grow_eq_self_of_active_eq heap rfl hact)

theorem active_length_le (heap : List Obj) (L : List Key) : (active heap L).length ≤ heap.length := by
  have hs : (active heap L).Sublist heap := List.filter_sublist
  exact hs.length_le

theorem mem_mark_of_mem_roots (heap : List Obj) (roots : List Key) (k : Key)
    (hk : k ∈ roots) : ∀ n, k ∈ mark heap roots n := by
  intro n
  induction n with
  | zero =>
      change k ∈ roots.eraseDups
      simpa using hk
  | succ n ih => exact mark_mono_fuel heap roots n k ih

theorem retain_retain (heap : List Obj) (liveA liveB : List Key)
    (hsub : ∀ k ∈ liveB, k ∈ liveA) : retain (retain heap liveA) liveB = retain heap liveB := by
  unfold retain
  induction heap with
  | nil => simp
  | cons o os ih =>
      have ih' : List.filter (fun o => decide (o.key ∈ liveB))
            (List.filter (fun o => decide (o.key ∈ liveA)) os) =
          List.filter (fun o => decide (o.key ∈ liveB)) os := by
        simpa [List.contains_eq_mem] using ih
      by_cases hB : o.key ∈ liveB
      · have hA : o.key ∈ liveA := hsub o.key hB
        simpa [List.contains_eq_mem, hA, hB] using ih'
      · simpa [List.contains_eq_mem, hB] using ih'

theorem mark_closed_of_fixed (heap : List Obj) (roots : List Key)
    (hfix : mark heap roots (heap.length + 1) = mark heap roots heap.length) :
    ∀ k ∈ mark heap roots heap.length, ∀ x ∈ edges heap k, x ∈ mark heap roots heap.length := by
  intro k hk x hxk
  have hx' : x ∈ grow heap (mark heap roots heap.length) := by
    rw [mem_grow]
    exact Or.inr ⟨k, hk, hxk⟩
  change x ∈ mark heap roots (heap.length + 1) at hx'
  simpa [hfix] using hx'

theorem mark_minimal (heap : List Obj) (roots : List Key) (L : List Key)
    (hclosed : ∀ k ∈ L, ∀ x ∈ edges heap k, x ∈ L)
    (hroots : ∀ k ∈ roots, k ∈ L) :
    ∀ n k, k ∈ mark heap roots n → k ∈ L := by
  intro n
  induction n with
  | zero =>
      intro k hk
      change k ∈ roots.eraseDups at hk
      exact hroots k (by simpa using hk)
  | succ n ih =>
      intro k hk
      change k ∈ grow heap (mark heap roots n) at hk
      rw [mem_grow] at hk
      rcases hk with hk | ⟨x, hx, hxk⟩
      · exact ih k hk
      · exact hclosed x (ih x hx) k hxk

theorem retain_eq_of_mem_iff (heap : List Obj) (L₁ L₂ : List Key)
    (h : ∀ k, k ∈ L₁ ↔ k ∈ L₂) : retain heap L₁ = retain heap L₂ := by
  induction heap with
  | nil => simp [retain]
  | cons o os ih =>
      have ih' : List.filter (fun o => decide (o.key ∈ L₁)) os =
          List.filter (fun o => decide (o.key ∈ L₂)) os := by
        simpa [retain, List.contains_eq_mem] using ih
      by_cases hmem : o.key ∈ L₁
      · have hmem2 : o.key ∈ L₂ := (h o.key).mp hmem
        simpa [retain, List.contains_eq_mem, hmem, hmem2] using ih'
      · have hmem2 : o.key ∉ L₂ := fun h2 => hmem ((h o.key).mpr h2)
        simpa [retain, List.contains_eq_mem, hmem, hmem2] using ih'

end TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability


/-! One edge expansion per resident object is enough: every expansion that
changes the marked set activates a new object key, and there are only
`heap.length` objects. Hence the closure is reached after that many steps. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_BoundedCodeClosure

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability
open TinylambOpt.ResearchProofs.JitKernels.Common_CodeReachability

theorem stable_from (heap : List Obj) (roots : List Key) (j : Nat)
    (h : mark heap roots (j + 1) = mark heap roots j) :
    ∀ m, mark heap roots (j + m) = mark heap roots j := by
  intro m
  induction m with
  | zero => rfl
  | succ m ih =>
      change grow heap (mark heap roots (j + m)) = mark heap roots j
      rw [ih]
      change grow heap (mark heap roots j) = mark heap roots j at h
      exact h

theorem sound : BoundedCodeClosure.Goal := by
  intro heap roots
  cases heap with
  | nil =>
      change (roots.eraseDups ++ roots.eraseDups.flatMap (edges [])).eraseDups = roots.eraseDups
      have hedges : roots.eraseDups.flatMap (edges []) = [] := by
        simp [edges]
      rw [hedges, List.append_nil]
      exact eraseDups_of_nodup _ (eraseDups_nodup _)
  | cons o os =>
      let n := (o :: os).length
      have hnpos : 0 < n := by simp [n]
      by_cases heq : mark (o :: os) roots (n + 1) = mark (o :: os) roots n
      · exact heq
      · exfalso
        have hnonstable : ∀ j, j ≤ n → mark (o :: os) roots (j + 1) ≠ mark (o :: os) roots j := by
          intro j hj hstable
          have hs := stable_from (o :: os) roots j hstable (n - j)
          have hjn : j + (n - j) = n := Nat.add_sub_of_le hj
          rw [hjn] at hs
          have hgrow : grow (o :: os) (mark (o :: os) roots j) = mark (o :: os) roots j := by
            change mark (o :: os) roots (j + 1) = mark (o :: os) roots j at hstable
            exact hstable
          have hlast : grow (o :: os) (mark (o :: os) roots n) = mark (o :: os) roots n := by
            rw [hs]
            exact hgrow
          exact heq hlast
        have hgrowth : ∀ j, j < n →
            (active (o :: os) (mark (o :: os) roots j)).length <
              (active (o :: os) (mark (o :: os) roots (j + 1))).length := by
          intro j hj
          have hnext : mark (o :: os) roots (j + 2) ≠ mark (o :: os) roots (j + 1) :=
            hnonstable (j + 1) (by omega)
          have hmark : mark (o :: os) roots (j + 1) =
              grow (o :: os) (mark (o :: os) roots j) := by
            cases j <;> rfl
          rw [hmark] at hnext ⊢
          exact grow_change_implies_active_growth (o :: os) hnext
        have hcount : ∀ j, j ≤ n →
            (active (o :: os) (mark (o :: os) roots j)).length ≥
              (active (o :: os) (mark (o :: os) roots 0)).length + j := by
          intro j hj
          induction j with
          | zero => omega
          | succ j ih =>
              have hprev := ih (by omega)
              have hg := hgrowth j (by omega)
              omega
        have hfinal := hcount n (by omega)
        have hlen := active_length_le (o :: os) (mark (o :: os) roots n)
        have ha0 : (active (o :: os) (mark (o :: os) roots 0)).length = 0 := by
          simp [n] at hfinal hlen
          omega
        have hgrow0 : grow (o :: os) (mark (o :: os) roots 0) = mark (o :: os) roots 0 := by
          apply eraseDups_append_of_mem
          · apply eraseDups_nodup
          · intro x hx
            rcases List.mem_flatMap.mp hx with ⟨z, hz, hxz⟩
            rcases edges_object (o :: os) hxz with ⟨ob, hb, hzob⟩
            have hmem : ob ∈ active (o :: os) (mark (o :: os) roots 0) := by
              apply mem_active
              · exact hb
              · simpa [hzob] using hz
            have hnil : active (o :: os) (mark (o :: os) roots 0) = [] := by
              apply List.eq_nil_iff_forall_not_mem.mpr
              intro y hy
              have hycount : 0 < (active (o :: os) (mark (o :: os) roots 0)).length := by
                have := List.length_pos_iff_exists_mem.mpr ⟨y, hy⟩
                omega
              omega
            simp [hnil] at hmem
        have hnonstable0 := hnonstable 0 (by omega)
        apply hnonstable0
        change grow (o :: os) (mark (o :: os) roots 0) = mark (o :: os) roots 0
        exact hgrow0

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_BoundedCodeClosure
