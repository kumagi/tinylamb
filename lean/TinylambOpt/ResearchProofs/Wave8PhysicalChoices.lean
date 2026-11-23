import TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices

namespace TinylambOpt.ResearchProofs.Wave8PhysicalChoices

open TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices

private def chunks (cap : Nat) (vs : List Nat) : List (List Nat) :=
  (List.range ((vs.length + cap - 1) / cap)).map fun j => (vs.drop (j * cap)).take cap

private theorem take_self_of_le {α : Type} (xs : List α) {n : Nat} (h : xs.length ≤ n) :
    xs.take n = xs := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      cases n with
      | zero => simp at h
      | succ n =>
          simp only [List.length_cons, Nat.succ_le_succ_iff] at h
          simp [ih h]

private theorem drop_take_eq (vs pre suf : List Nat) (cap j : Nat)
    (hvs : vs = pre ++ suf) (hj : j * cap + cap ≤ pre.length) :
    (vs.drop (j * cap)).take cap = (pre.drop (j * cap)).take cap := by
  subst vs
  rw [List.drop_append_of_le_length (by omega : j * cap ≤ pre.length)]
  rw [List.take_append_of_le_length (by
    rw [List.length_drop]
    omega)]

private theorem chunks_aux_exact (cap q : Nat) (hc : 0 < cap) :
    ∀ vs : List Nat, vs.length = q * cap →
      ((List.range q).map fun j => (vs.drop (j * cap)).take cap).flatten = vs := by
  intro vs hlen
  induction q generalizing vs with
  | zero =>
      have hv : vs = [] := by cases vs <;> simp at hlen ⊢
      subst vs
      simp
  | succ q ih =>
      rw [Nat.succ_mul] at hlen
      let pre := vs.take (q * cap)
      let suf := vs.drop (q * cap)
      have hprelen : pre.length = q * cap := by
        dsimp [pre]
        rw [List.length_take, Nat.min_eq_left (by omega)]
      have hsuflen : suf.length = cap := by
        dsimp [suf]
        rw [List.length_drop, hlen]
        omega
      have hvs : vs = pre ++ suf := by
        dsimp [pre, suf]
        rw [List.take_append_drop]
      have hlast : (vs.drop (q * cap)).take cap = suf := by
        dsimp [suf]
        have hdl : (List.drop (q * cap) vs).length = cap := by simpa [suf] using hsuflen
        exact take_self_of_le _ (Nat.le_of_eq hdl)
      have hfirst :
          (List.range q).map (fun j => (vs.drop (j * cap)).take cap) =
            (List.range q).map (fun j => (pre.drop (j * cap)).take cap) := by
        apply List.map_congr_left
        intro j hj
        have hjlt : j < q := List.mem_range.mp hj
        have hj1 : (j + 1) * cap ≤ q * cap := Nat.mul_le_mul_right cap (Nat.succ_le_of_lt hjlt)
        rw [Nat.succ_mul] at hj1
        exact drop_take_eq vs pre suf cap j hvs (by rw [hprelen]; omega)
      rw [List.range_succ, List.map_append, List.flatten_append, hfirst, ih pre hprelen]
      simp [hlast, ← hvs]

private theorem chunks_aux (cap q : Nat) (hc : 0 < cap) :
    ∀ vs : List Nat, vs.length ≤ q * cap → q * cap ≤ vs.length + cap - 1 →
      ((List.range q).map fun j => (vs.drop (j * cap)).take cap).flatten = vs := by
  intro vs hle hge
  cases q with
  | zero =>
      have hv : vs = [] := by cases vs <;> simp at hle ⊢
      subst vs
      simp
  | succ q =>
      rw [Nat.succ_mul] at hle hge
      let pre := vs.take (q * cap)
      let suf := vs.drop (q * cap)
      have hq_lt : q * cap < vs.length := by omega
      have hprelen : pre.length = q * cap := by
        dsimp [pre]
        rw [List.length_take, Nat.min_eq_left (by omega)]
      have hsuflen_pos : 0 < suf.length := by
        dsimp [suf]
        rw [List.length_drop]
        omega
      have hsuflen_le : suf.length ≤ cap := by
        dsimp [suf]
        rw [List.length_drop]
        omega
      have hlast : (vs.drop (q * cap)).take cap = suf := take_self_of_le _ hsuflen_le
      have hfirst :
          (List.range q).map (fun j => (vs.drop (j * cap)).take cap) =
            (List.range q).map (fun j => (pre.drop (j * cap)).take cap) := by
        apply List.map_congr_left
        intro j hj
        have hjlt : j < q := List.mem_range.mp hj
        have hj1 : (j + 1) * cap ≤ q * cap := Nat.mul_le_mul_right cap (Nat.succ_le_of_lt hjlt)
        rw [Nat.succ_mul] at hj1
        exact drop_take_eq vs pre suf cap j (by
          dsimp [pre, suf]
          rw [List.take_append_drop]) (by rw [hprelen]; omega)
      rw [List.range_succ, List.map_append, List.flatten_append, hfirst,
        chunks_aux_exact cap q hc pre hprelen]
      simp [hlast, pre, suf, List.take_append_drop]

private theorem chunks_flatten (cap : Nat) (hc : 0 < cap) (vs : List Nat) :
    (chunks cap vs).flatten = vs := by
  unfold chunks
  let q := (vs.length + cap - 1) / cap
  apply chunks_aux cap q hc vs
  · have hmod : (vs.length + cap - 1) % cap < cap := Nat.mod_lt _ hc
    have hdecomp : q * cap + (vs.length + cap - 1) % cap = vs.length + cap - 1 := by
      dsimp [q]
      rw [Nat.mul_comm]
      exact Nat.div_add_mod _ _
    by_cases h0 : vs.length = 0
    · omega
    · have hposlen : 0 < vs.length := Nat.pos_of_ne_zero h0
      omega
  · dsimp [q]
    exact Nat.div_mul_le_self _ _

private theorem chunk_len_bounds {cap : Nat} (hc : 0 < cap) {vs : List Nat} {j : Nat}
    (hj : j < (vs.length + cap - 1) / cap) :
    0 < ((vs.drop (j * cap)).take cap).length ∧ ((vs.drop (j * cap)).take cap).length ≤ cap := by
  let q := (vs.length + cap - 1) / cap
  have hjq : j < q := by simpa [q] using hj
  have hj1 : (j + 1) * cap ≤ q * cap := Nat.mul_le_mul_right cap (Nat.succ_le_of_lt hjq)
  rw [Nat.succ_mul] at hj1
  have hdiv : q * cap ≤ vs.length + cap - 1 := by
    dsimp [q]
    exact Nat.div_mul_le_self _ _
  have hjlt : j * cap < vs.length := by omega
  have hd : (vs.drop (j * cap)).length = vs.length - j * cap := by rw [List.length_drop]
  have hposd : 0 < (vs.drop (j * cap)).length := by rw [hd]; omega
  rw [List.length_take]
  constructor
  · by_cases hle : cap ≤ (vs.drop (j * cap)).length
    · rwa [Nat.min_eq_left hle]
    · rw [Nat.min_eq_right (Nat.le_of_lt (Nat.lt_of_not_ge hle))]
      exact hposd
  · exact Nat.min_le_left _ _

private theorem chunk_mem_bounds {cap : Nat} (hc : 0 < cap) {vs ns : List Nat}
    (h : ns ∈ chunks cap vs) : 0 < ns.length ∧ ns.length ≤ cap := by
  unfold chunks at h
  rcases List.mem_map.mp h with ⟨j, hj, rfl⟩
  exact chunk_len_bounds hc (List.mem_range.mp hj)

private theorem chunks_length_bound (cap : Nat) (hc : 0 < cap) (vs : List Nat) :
    (chunks cap vs).length * cap ≤ vs.length + (cap - 1) * (if vs.isEmpty then 0 else 1) := by
  unfold chunks
  simp only [List.length_map]
  by_cases he : vs.isEmpty
  · have hv : vs = [] := by cases vs <;> simp at he ⊢
    subst vs
    simp [Nat.div_eq_of_lt (by omega : cap - 1 < cap)]
  · have hpos : 0 < vs.length := by
      cases vs with
      | nil => simp at he
      | cons x xs => simp
    have hdiv : ((vs.length + cap - 1) / cap) * cap ≤ vs.length + cap - 1 :=
      Nat.div_mul_le_self _ _
    simp [he]
    omega

private theorem expand_map_chunks (key : Nat) (cs : List (List Nat)) :
    ((cs.map fun ns => (key, ns)).flatMap fun p : Nat × List Nat => p.2.map (p.1, ·)) =
      cs.flatten.map (key, ·) := by
  induction cs with
  | nil => rfl
  | cons ns cs ih =>
      simp [ih, List.map_append]

theorem EdgeMorsels_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices.EdgeMorsels.Goal := by
  intro adj cap hcap
  induction adj with
  | nil => simp [slices]
  | cons a adj ih =>
      rcases a with ⟨key, vs⟩
      let cs := chunks cap vs
      have hfirst : (List.range ((vs.length + cap - 1) / cap)).map (fun j =>
          (key, (vs.drop (j * cap)).take cap)) = cs.map (fun ns => (key, ns)) := by
        simp [cs, chunks]
      have head :
          ((cs.map fun ns => (key, ns)).flatMap fun p : Nat × List Nat => p.2.map (p.1, ·)) =
            vs.map (key, ·) := by
        rw [expand_map_chunks, chunks_flatten cap hcap vs]
      simp only [slices, List.flatMap_cons]
      rw [List.flatMap_append, hfirst, head]
      constructor
      · exact congrArg (fun xs => vs.map (key, ·) ++ xs) ih.1
      constructor
      · intro j hj
        simp only [List.mem_append] at hj
        rcases hj with hj | hj
        · rcases List.mem_map.mp hj with ⟨ns, hns, hEq⟩
          subst j
          simpa [cs] using chunk_mem_bounds hcap (by simpa [cs] using hns)
        · exact ih.2.1 j (by simpa [slices] using hj)
      · -- length bound
        have hb := chunks_length_bound cap hcap vs
        have hib := ih.2.2
        have hcnt : (List.countP (fun a : Nat × List Nat => !a.2.isEmpty) ((key, vs) :: adj)) =
            (if vs.isEmpty then 0 else 1) + List.countP (fun a : Nat × List Nat => !a.2.isEmpty) adj := by
          by_cases he : vs.isEmpty <;> simp [he] <;> omega
        rw [hcnt]
        -- normalize the goal to bare Nat expressions via `simp`
        simp only [List.length_append, List.length_map, cs, Nat.add_mul] at hb ⊢
        -- identify `slices cap adj` length with the flattened chunk length
        have hsl : (slices cap adj).length =
            (List.flatMap (fun p : Nat × List Nat =>
              (List.range ((p.2.length + cap - 1) / cap)).map (fun j =>
                (p.1, (p.2.drop (j * cap)).take cap))) adj).length := rfl
        have hrest : (List.flatMap
            (fun p : Nat × List Nat =>
              (List.range ((p.2.length + cap - 1) / cap)).map (fun j =>
                (p.1, (p.2.drop (j * cap)).take cap))) adj).length =
            (slices cap adj).length := by
          unfold slices
          rfl
        -- the two halves of the IH bound
        have htail :
            (List.flatMap (fun p : Nat × List Nat =>
              (List.range ((p.2.length + cap - 1) / cap)).map (fun j =>
                (p.1, (p.2.drop (j * cap)).take cap))) adj).length * cap ≤
            (List.flatMap (fun x : Nat × List Nat =>
              match x with | (k, ns) => ns.map (k, ·)) adj).length +
              (cap - 1) * List.countP (fun a : Nat × List Nat => !a.2.isEmpty) adj := by
          simpa [slices] using hib
        have hEqM : (cap - 1) * ((if vs.isEmpty then 0 else 1) +
            List.countP (fun a : Nat × List Nat => !a.2.isEmpty) adj) =
            (cap - 1) * (if vs.isEmpty then 0 else 1) +
              (cap - 1) * List.countP (fun a : Nat × List Nat => !a.2.isEmpty) adj := by
          exact Nat.mul_add _ _ _
        rw [hEqM]
        have hI : (List.flatMap
            (fun p : Nat × List Nat =>
              (List.range ((p.2.length + cap - 1) / cap)).map (fun j =>
                (p.1, (p.2.drop (j * cap)).take cap))) adj).length =
            (List.flatMap
              (fun p : Nat × List Nat =>
                (List.range ((p.snd.length + cap - 1) / cap)).map (fun j =>
                  (p.fst, (p.snd.drop (j * cap)).take cap))) adj).length := by
          rfl
        rw [hI]
        clear hib hsl hrest hI hEqM hcnt
        have hsli : (slices cap adj).length =
            (List.flatMap (fun p : Nat × List Nat =>
              (List.range ((p.2.length + cap - 1) / cap)).map (fun j =>
                (p.1, (p.2.drop (j * cap)).take cap))) adj).length := by
          unfold slices
          rfl
        have hsli' : (slices cap adj).length * cap =
            (List.flatMap (fun p : Nat × List Nat =>
              (List.range ((p.2.length + cap - 1) / cap)).map (fun j =>
                (p.1, (p.2.drop (j * cap)).take cap))) adj).length * cap := by
          rw [hsli]
        rw [hsli'] at *
        have hEqHK : (List.flatMap (fun x : Nat × List Nat =>
              match x with | (k, ns) => ns.map (k, ·)) adj).length =
            (List.flatMap (fun x : Nat × List Nat => x.2.map (x.1, ·)) adj).length := by
          induction adj with
          | nil => rfl
          | cons a adj ih =>
              rcases a with ⟨k, vs⟩
              simp [ih]
        rw [hEqHK] at *
        omega

-- CachedProbeOrder

private def lookupVia (cache : Std.HashMap Nat Nat) (f : Nat → Nat) (k : Nat) :
    Std.HashMap Nat Nat × Nat × Nat :=
  match cache[k]? with
  | some n => (cache, n, 0)
  | none => (cache.insert k (f k), f k, 1)

private theorem lookupVia_eq (cache : Std.HashMap Nat Nat) (f : Nat → Nat) (k : Nat) :
    lookupVia cache f k = lookup cache f k := by
  rfl

private theorem hm_insert_some {α β : Type} [BEq α] [Hashable α] [LawfulBEq α]
    [LawfulHashable α] {m : Std.HashMap α β} {k j : α} {v v' : β} :
    (m.insert k v)[j]? = some v' → v' = v ∨ m[j]? = some v' := by
  intro h
  rw [Std.HashMap.getElem?_insert] at h
  by_cases hkj : k == j
  · simp [hkj] at h
    exact Or.inl h.symm
  · simp [hkj] at h
    exact Or.inr h

private theorem hm_insert_present {α β : Type} [BEq α] [Hashable α] [LawfulBEq α]
    [LawfulHashable α] {m : Std.HashMap α β} {k j : α} {v : β} :
    (m.insert k v)[j]?.isSome ↔ (k == j) = true ∨ m[j]?.isSome := by
  rw [Std.HashMap.getElem?_insert]
  by_cases h : k == j <;> simp [h]

private theorem removeAll_singleton_eq_self {α : Type} [BEq α] [LawfulBEq α]
    (as : List α) (x : α) (hx : x ∉ as) :
    [x].removeAll as = [x] := by
  induction as with
  | nil => simp
  | cons y ys ih =>
      by_cases hxy : x == y
      · exfalso
        apply hx
        have heq : x = y := LawfulBEq.eq_of_beq hxy
        simp [heq]
      · rw [List.removeAll_cons]
        have hfilt : (List.filter (fun z => !z == y) [x]) = [x] := by simp [hxy]
        rw [hfilt, ih (by intro hmem; exact hx (by simp [hmem]))]

private theorem eraseDups_append_new {α : Type} [BEq α] [LawfulBEq α]
    (as : List α) (x : α) (hx : x ∉ as) :
    (as ++ [x]).eraseDups = as.eraseDups ++ [x] := by
  rw [List.eraseDups_append, removeAll_singleton_eq_self as x hx]
  simp [List.eraseDups_cons]

private theorem eraseDups_append_new_length {α : Type} [BEq α] [LawfulBEq α]
    (as : List α) (x : α) (hx : x ∉ as) :
    (as ++ [x]).eraseDups.length = as.eraseDups.length + 1 := by
  rw [eraseDups_append_new as x hx]
  simp

private theorem removeAll_singleton_eq_nil {α : Type} [BEq α] [LawfulBEq α]
    (as : List α) (x : α) (hx : x ∈ as) :
    [x].removeAll as = [] := by
  induction as with
  | nil => simp at hx
  | cons y ys ih =>
      by_cases hxy : x == y
      · simp [List.removeAll_cons, hxy]
      · have hxne : x ≠ y := by intro heq; exact hxy (by simp [heq])
        have hxys : x ∈ ys := by simpa [hxne] using hx
        rw [List.removeAll_cons]
        have hfilt : (List.filter (fun z => !z == y) [x]) = [x] := by simp [hxy]
        rw [hfilt, ih hxys]

private theorem eraseDups_append_old {α : Type} [BEq α] [LawfulBEq α]
    (as : List α) (x : α) (hx : x ∈ as) :
    (as ++ [x]).eraseDups = as.eraseDups := by
  rw [List.eraseDups_append, removeAll_singleton_eq_nil as x hx]
  simp

private theorem mem_map_fst_append (done : List (Nat × Nat)) (x y : Nat) :
    ((done ++ [(x, y)]).map Prod.fst) = done.map Prod.fst ++ [x] := by simp

private theorem eraseDups_append_old_length (as : List Nat) (x : Nat) (hx : x ∈ as) :
    (as ++ [x]).eraseDups.length = as.eraseDups.length := by
  rw [List.eraseDups_append]
  have hremove : [x].removeAll as = [] := by
    induction as with
    | nil => simp at hx
    | cons y ys ih =>
        by_cases hxy : x == y
        · simp [List.removeAll_cons, hxy]
        · have hxne : x ≠ y := by intro heq; exact hxy (by simp [heq])
          have hxys : x ∈ ys := by simpa [hxne] using hx
          rw [List.removeAll_cons]
          have hfilt : (List.filter (fun z => !z == y) [x]) = [x] := by simp [hxy]
          rw [hfilt, ih hxys]
  rw [hremove]
  simp

private theorem filter_append_row (f : Nat → Nat) (done : List (Nat × Nat)) (x y : Nat) :
    ((done ++ [(x, y)]).filter (fun r => f r.1 != 0)) =
      done.filter (fun r => f r.1 != 0) ++ (if f x != 0 then [(x, y)] else []) := by
  by_cases h : f x != 0 <;> simp [List.filter_append, h]

private abbrev PState := Std.HashMap Nat Nat × Std.HashMap Nat Nat × List Nat × Nat × Nat

private abbrev PInv (a b : Nat → Nat) (done : List (Nat × Nat)) (st : PState) : Prop :=
  (∀ k v, st.1[k]? = some v → v = a k) ∧
  (∀ k, (st.1[k]?).isSome ↔ k ∈ done.map Prod.fst) ∧
  st.2.2.2.1 = (done.map Prod.fst).eraseDups.length ∧
  (∀ k v, st.2.1[k]? = some v → v = b k) ∧
  (∀ k, (st.2.1[k]?).isSome ↔ k ∈ (done.filter (fun r => a r.1 != 0)).map Prod.snd) ∧
  st.2.2.2.2 = ((done.filter (fun r => a r.1 != 0)).map Prod.snd).eraseDups.length ∧
  st.2.2.1.reverse = done.map (fun (p, q) => a p * b q)

private theorem cache_update (f : Nat → Nat) (done : List Nat) (m : Std.HashMap Nat Nat) (n : Nat)
    (hval : ∀ k v, m[k]? = some v → v = f k)
    (hmem : ∀ k, (m[k]?).isSome ↔ k ∈ done)
    (hn : n = done.eraseDups.length) (k : Nat) :
    let (m', w, i) := lookupVia m f k
    (∀ j v, m'[j]? = some v → v = f j) ∧
    (∀ j, (m'[j]?).isSome ↔ j ∈ done ++ [k]) ∧
    n + i = (done ++ [k]).eraseDups.length ∧
    w = f k ∧ (i = 0 ↔ (m[k]?).isSome) := by
  unfold lookupVia
  cases hm : m[k]? with
  | none =>
      have hknot : k ∉ done := by
        intro hk
        have : (m[k]?).isSome := by rw [hmem]; exact hk
        simp [hm] at this
      simp only [hm]
      constructor
      · intro j v hj
        rw [Std.HashMap.getElem?_insert] at hj
        by_cases hkj : k == j
        · simp [hkj] at hj
          have heq : k = j := LawfulBEq.eq_of_beq hkj
          subst v
          subst j
          rfl
        · simp [hkj] at hj
          exact hval j v hj
      constructor
      · intro j
        rw [hm_insert_present]
        rw [hmem]
        constructor
        · intro h
          rcases h with hkj | hj
          · have heq : j = k := (LawfulBEq.eq_of_beq hkj).symm
            subst j
            simp
          · simp [hj]
        · intro h
          simp at h
          rcases h with hj | hjk
          · exact Or.inr hj
          · exact Or.inl (by simpa [hjk])
      constructor
      · rw [hn]
        exact (eraseDups_append_new_length done k hknot).symm
      · simp [hm]
  | some v =>
      have hv : v = f k := hval k v hm
      have hk : k ∈ done := by rw [← hmem]; simp [hm]
      simp only [hm]
      constructor
      · exact hval
      constructor
      · intro j
        rw [hmem]
        constructor
        · intro hj; simp [hj]
        · intro h
          simp at h
          rcases h with hj | hjk
          · exact hj
          · subst j; exact hk
      constructor
      · rw [hn, eraseDups_append_old_length done k hk]
        simp
      · exact ⟨hv, by simp [hm]⟩

private def PStep (a b : Nat → Nat) (st : PState) (xy : Nat × Nat) : PState :=
  let (x, y) := xy
  let (ma, w, i) := lookupVia st.1 a x
  if w == 0 then (ma, st.2.1, 0 :: st.2.2.1, st.2.2.2.1 + i, st.2.2.2.2)
  else
    let (mb, v, j) := lookupVia st.2.1 b y
    (ma, mb, (w * v) :: st.2.2.1, st.2.2.2.1 + i, st.2.2.2.2 + j)

private theorem PStep_preserves (a b : Nat → Nat) (done : List (Nat × Nat))
    (st : PState) (h : PInv a b done st) (xy : Nat × Nat) :
    PInv a b (done ++ [xy]) (PStep a b st xy) := by
  rcases xy with ⟨x, y⟩
  dsimp [PStep]
  rcases h with ⟨hvalA, hmemA, hna, hvalB, hmemB, hnb, hrev⟩
  have hA := cache_update a (done.map Prod.fst) st.1 st.2.2.2.1 hvalA hmemA hna x
  generalize hAdef : lookupVia st.1 a x = qa at hA ⊢
  rcases qa with ⟨ma, w, i⟩
  rcases hA with ⟨hvalA', hmemA', hna', hwval, hi0⟩
  by_cases hw : w == 0
  · have hax0 : a x = 0 := by
      rw [← hwval]
      exact LawfulBEq.eq_of_beq hw
    simp [hw]
    refine ⟨hvalA', ?_, ?_, hvalB, ?_, ?_, ?_⟩
    · intro k
      rw [hmemA']
      simp
    · simpa [mem_map_fst_append] using hna'
    · intro k
      rw [hmemB]
      simp [filter_append_row, hax0]
    · simpa [filter_append_row, hax0] using hnb
    · rw [List.reverse_cons, hrev]
      simp [hax0]
  · have hax : a x != 0 := by
      rw [← hwval]
      simpa using hw
    have hB := cache_update b ((done.filter (fun r => a r.1 != 0)).map Prod.snd)
      st.2.1 st.2.2.2.2 hvalB hmemB hnb y
    generalize hBdef : lookupVia st.2.1 b y = qb at hB ⊢
    rcases qb with ⟨mb, v, j⟩
    rcases hB with ⟨hvalB', hmemB', hnb', hvval, hj0⟩
    have hwv : w * v = a x * b y := by rw [hwval, hvval]
    simp [hw]
    refine ⟨hvalA', ?_, ?_, hvalB', ?_, ?_, ?_⟩
    · intro k
      rw [hmemA']
      simp
    · simpa [mem_map_fst_append] using hna'
    · intro k
      rw [hmemB']
      simp [filter_append_row, hax]
    · simpa [filter_append_row, hax] using hnb'
    · rw [List.reverse_cons, hrev]
      simp [hwv]

private theorem P_foldl (a b : Nat → Nat) :
    ∀ (done : List (Nat × Nat)) (st : PState), PInv a b done st →
    ∀ rs : List (Nat × Nat), PInv a b (done ++ rs) (rs.foldl (PStep a b) st) := by
  intro done st h rs
  induction rs generalizing done st with
  | nil => simpa using h
  | cons xy rs ih =>
      have hs := PStep_preserves a b done st h xy
      have hdone : done ++ [xy] ++ rs = done ++ (xy :: rs) := by simp [List.append_assoc]
      rw [← hdone]
      exact ih (done ++ [xy]) (PStep a b st xy) hs

private theorem probe_order_eq (a b : Nat → Nat) (rs : List (Nat × Nat)) :
    probeOrder rs a b =
      let (ma, mb, rev, na, nb) := rs.foldl (PStep a b) ({}, {}, [], 0, 0)
      ⟨rev.reverse, na, nb⟩ := by
  unfold probeOrder PStep lookup
  rfl

theorem CachedProbeOrder_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices.CachedProbeOrder.Goal := by
  intro rs a b c1 c2
  dsimp only
  let swapped := rs.map Prod.swap
  let st := rs.foldl (PStep a b) ({}, {}, [], 0, 0)
  have hinit : PInv a b [] ({}, {}, [], 0, 0) := by
    simp [PInv, Std.HashMap.getElem?_empty]
  have hfinal := P_foldl a b [] ({}, {}, [], 0, 0) hinit rs
  have hab : probeOrder rs a b = ⟨rs.map (fun (p, q) => a p * b q),
      (rs.map Prod.fst).eraseDups.length,
      ((rs.filter (fun r => a r.1 != 0)).map Prod.snd).eraseDups.length⟩ := by
    rw [probe_order_eq]
    dsimp [st] at hfinal
    rcases hfinal with ⟨_, hmemA, hna, _, hmemB, hnb, hrev⟩
    simp at hmemA hna hmemB hnb hrev
    simp [hrev, hna, hnb]
  let stb := swapped.foldl (PStep b a) ({}, {}, [], 0, 0)
  have hinitb : PInv b a [] ({}, {}, [], 0, 0) := by
    simp [PInv, Std.HashMap.getElem?_empty]
  have hfinalb := P_foldl b a [] ({}, {}, [], 0, 0) hinitb swapped
  have hba : probeOrder swapped b a = ⟨rs.map (fun (p, q) => a p * b q),
      (swapped.map Prod.fst).eraseDups.length,
      ((swapped.filter (fun r => b r.1 != 0)).map Prod.snd).eraseDups.length⟩ := by
    rw [probe_order_eq]
    dsimp [stb] at hfinalb
    rcases hfinalb with ⟨_, hmemA, hna, _, hmemB, hnb, hrev⟩
    simp at hmemA hna hmemB hnb hrev
    have hw : swapped.map (fun (p, q) => b p * a q) = rs.map (fun (p, q) => a p * b q) := by
      simp [swapped, Nat.mul_comm]
    simp [hrev, hw, hna, hnb]
  rw [hab, hba]
  unfold orderCost
  simp [swapped]


-- TODO(RefineFanout): histogram/bucket interval proof not completed in this pass.
-- TODO(RepeatEnvelope): repeated-summary envelope proof not completed in this pass.
-- TODO(FieldBytes): cached DataRow field encoding proof not completed in this pass.

end TinylambOpt.ResearchProofs.Wave8PhysicalChoices
