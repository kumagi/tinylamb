import TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices
import TinylambOpt.ResearchProofs.CheckedAggregateTree

/-!
These models describe plan alternatives for analytical SQL, with row multiplicity and
aggregate state made explicit.

Proved

- `CachedProbeOrder` — `∀ rs a b c1 c2, let swapped := rs.map Prod.swap let ab :=
  probeOrder rs a b let ba := probeOrder swapped b a ab.weights = rs.map (fun (p,q)
  => a p * b q) ∧ ba.weights = ab.weights ∧ c1*ab.firstLoads + c2*ab.secondLoads =
  orderCost rs a c1 c2 ∧ c2*ba.firstLoads + c1*ba.secondLoads = orderCost swapped b
  c2 c1`
- `EdgeMorsels` — `∀ adj cap, 0 < cap → let jobs := slices cap adj let expand := fun (x
  : List (Nat × List Nat)) => x.flatMap (fun (k,ns) => ns.map (k, ·)) expand jobs =
  expand adj ∧ (∀ j ∈ jobs, 0 < j.2.length ∧ j.2.length ≤ cap) ∧ jobs.length * cap ≤
  (expand adj).length + (cap-1) * adj.countP (fun a => !a.2.isEmpty)`
- `FieldBytes` — `∀ rows hot, let ints := rows.flatten.filterMap (fun c => match c with
  | .intVal v => v | _ => none) cachedWire hot rows = (refRowEncode rows,
  (ints.filter hot).eraseDups.length + ints.countP (fun i => !hot i))`
- `RefineFanout` — `∀ l r heavy bucket k, let (lo,hi) := joinBounds l r heavy bucket
  let (lo',hi') := joinBounds l r (k::heavy) bucket let pairs := (l.map r.count).sum
  lo ≤ lo' ∧ lo' ≤ pairs ∧ pairs ≤ hi' ∧ hi' ≤ hi`
- `RepeatEnvelope` — `∀ (rs : List Row) (n : Nat) (acc : Option Int), InRange (acc.getD
  0) → let h := repeated (prepare (.chunk rs)).summary n let value := if h.count == 0
  then acc else some (acc.getD 0 + h.total) (InRange (acc.getD 0 + h.low) ∧ InRange
  (acc.getD 0 + h.high) ↔ sequential acc (List.replicate n rs).flatten = .ok value)`
-/

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.PhysicalChoices

open TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices
open TinylambOpt.ResearchGoals.AnalyticsWorkloads (histogram)
open TinylambOpt.ResearchGoals.CheckedAggregateTree
open TinylambOpt.CheckedEagerSum (InRange)
open TinylambOpt.ResearchGoals.ColumnarToPgWireChunkedFramingEquiv
  (Cell encodeCellBytes be16 be32 refEncodeDataRow refRowEncode)

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




private theorem hist_fold_count (xs : List Nat) (m : Std.HashMap Nat Nat) (k : Nat) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD k 0 = m.getD k 0 + xs.count k := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons]
      rw [ih]
      rw [Std.HashMap.getD_insert]
      by_cases h : x = k <;> simp [h, Nat.add_assoc, Nat.add_comm]

private theorem hist_getD (xs : List Nat) (k : Nat) :
    (histogram xs).getD k 0 = xs.count k := by
  unfold histogram
  rw [hist_fold_count]
  simp

private theorem filt_eq (m : Std.HashMap Nat Nat) (k : Nat) :
    m.toList.filter (fun kv => !(k == kv.1)) =
    m.toList.filter (fun x => decide ¬(k == x.fst) = true) := by
  apply List.filter_congr
  intro kv hmem
  by_cases h : k == kv.1 <;> simp [h]

private theorem toList_insert_sum (m : Std.HashMap Nat Nat) (k v : Nat) (f : Nat → Nat) :
    ((m.insert k v).toList.map (fun kv : Nat × Nat => kv.2 * f kv.1)).sum =
      v * f k + ((m.toList.filter (fun kv => !(k == kv.1))).map (fun kv : Nat × Nat => kv.2 * f kv.1)).sum := by
  have hp := Std.HashMap.toList_insert_perm (m := m) (k := k) (v := v)
  have hpm := hp.map (fun kv : Nat × Nat => kv.2 * f kv.1)
  rw [hpm.sum_nat]
  rw [filt_eq]
  simp

private theorem hsum_filter_add (L : List (Nat × Nat))
    (hd : L.Pairwise (fun a b => (a.1 == b.1) = false)) (f : Nat → Nat) (k : Nat) :
    ((L.filter fun kv => !(k == kv.1)).map fun kv => kv.2 * f kv.1).sum +
      (match L.find? (fun kv => kv.1 == k) with | some kv => kv.2 | none => 0) * f k =
    (L.map fun kv => kv.2 * f kv.1).sum := by
  induction L with
  | nil => simp
  | cons kv L ih =>
      have hno : ∀ v ∈ L, (kv.1 == v.1) = false := fun v hv => List.rel_of_pairwise_cons hd hv
      have hd' : L.Pairwise (fun a b => (a.1 == b.1) = false) := List.Pairwise.of_cons hd
      by_cases h : kv.1 = k
      · have hkfalse : ∀ v ∈ L, (k == v.1) = false := by
          intro v hv
          cases hkv : k == v.1 with
          | false => rfl
          | true =>
              have heq : k = v.1 := LawfulBEq.eq_of_beq hkv
              have heq2 : (kv.1 == v.1) = true := by simp [h, heq]
              have hnv := hno v hv
              rw [heq2] at hnv
              simp at hnv
        have hbeq : (k == kv.1) = true := by simp [h]
        have hf : (kv :: L).filter (fun v => !(k == v.1)) = L := by
          simp [hbeq]
          intro v w hv heq
          have hb : (k == v) = true := by simpa [heq]
          have hv0 := hkfalse (v, w) hv
          rw [hb] at hv0
          contradiction
        have hfind : (kv :: L).find? (fun v => v.1 == k) = some kv := by
          have : (kv.1 == k) = true := by simp [h]
          simp [this]
        rw [hf, hfind]
        simp [h, Nat.add_comm]
      · have hf : (kv :: L).filter (fun v => !(k == v.1)) =
            kv :: L.filter (fun v => !(k == v.1)) := by
          have hkne : k ≠ kv.1 := by omega
          have : (k == kv.1) = false := by simpa [beq_eq_false_iff_ne] using hkne
          simp [this]
        have hfind : (kv :: L).find? (fun v => v.1 == k) =
            L.find? (fun v => v.1 == k) := by
          have : (kv.1 == k) = false := by simpa [beq_eq_false_iff_ne] using h
          simp [this]
        rw [hf, hfind]
        simp only [List.map_cons, List.sum_cons]
        have hi := ih hd'
        omega

private theorem find_val_getD (m : Std.HashMap Nat Nat) (k : Nat) :
    (match m.toList.find? (fun kv => kv.1 == k) with | some kv => kv.2 | none => 0) = m.getD k 0 := by
  by_cases hmem : k ∈ m
  · have hgk : m.getKey? k = some k := Std.HashMap.getKey?_eq_some hmem
    have hgv : m[k]? = some (m.getD k 0) := Std.HashMap.getElem?_eq_some_getD hmem
    have hf : m.toList.find? (fun a => a.1 == k) = some (k, m.getD k 0) := by
      rw [Std.HashMap.find?_toList_eq_some_iff_getKey?_eq_some_and_getElem?_eq_some]
      exact ⟨hgk, hgv⟩
    rw [hf]
  · have hf : m.toList.find? (fun a => a.1 == k) = none := by
      rw [Std.HashMap.find?_toList_eq_none_iff_not_mem]
      exact hmem
    rw [hf]
    simp [Std.HashMap.getD_eq_fallback hmem]

private def hsum (m : Std.HashMap Nat Nat) (f : Nat → Nat) : Nat :=
  (m.toList.map fun kv : Nat × Nat => kv.2 * f kv.1).sum

private theorem hsum_insert_incr (m : Std.HashMap Nat Nat) (k : Nat) (f : Nat → Nat) :
    hsum (m.insert k (m.getD k 0 + 1)) f = hsum m f + f k := by
  unfold hsum
  rw [toList_insert_sum]
  rw [← hsum_filter_add m.toList (Std.HashMap.distinct_keys_toList (m := m)) f k]
  rw [find_val_getD]
  simp [Nat.add_mul, Nat.add_assoc, Nat.add_comm]

private theorem hist_fold_sum (ys : List Nat) (m : Std.HashMap Nat Nat) (f : Nat → Nat) :
    hsum (ys.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m) f = hsum m f + (ys.map f).sum := by
  induction ys generalizing m with
  | nil => simp
  | cons y ys ih =>
      rw [List.foldl_cons]
      rw [ih]
      rw [hsum_insert_incr]
      simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem hist_sum (ys : List Nat) (f : Nat → Nat) :
    hsum (histogram ys) f = (ys.map f).sum := by
  unfold histogram
  rw [hist_fold_sum]
  simp [hsum]

-- ===== cross helpers (scratch3 + scratch4) =====
private def cross (xs ys : List Nat) : Nat := (xs.map fun x => ys.count x).sum

private theorem count_le_map_count (rf : List Nat) (x : Nat) (g : Nat → Nat) :
    rf.count x ≤ (rf.map g).count (g x) := by
  induction rf with
  | nil => simp
  | cons y ys ih =>
      by_cases hyx : y = x
      · subst y
        simp [ih]
      · by_cases hb : g y = g x
        · simp [hyx, hb]
          omega
        · simp [hyx, hb, ih]

private theorem filter_sublist_filter (r : List Nat) (p : Nat → Bool) (k : Nat) :
    List.Sublist (r.filter (fun x => p x && !(x == k))) (r.filter p) := by
  induction r with
  | nil => simp
  | cons y ys ih =>
      by_cases hyk : y = k
      · subst y
        by_cases hp : p k = true <;> simp [hp, ih]
      · by_cases hp : p y = true <;> simp [hyk, hp, ih]

private theorem cross_filter_key_ge_map (l r : List Nat) (p : Nat → Bool) (k : Nat)
    (hpk : p k = true) (g : Nat → Nat) :
    cross ((l.filter p).map g) ((r.filter p).map g) ≥
      cross ((l.filter (fun x => p x && !(x == k))).map g)
            ((r.filter (fun x => p x && !(x == k))).map g) +
        (l.filter p).count k * (r.filter p).count k := by
  induction l with
  | nil => simp [cross]
  | cons x xs ih =>
      by_cases hpx : p x = true
      · by_cases hxk : x = k
        · subst x
          have hhead : ((r.filter p).map g).count (g k) ≥ (r.filter p).count k := count_le_map_count (r.filter p) k g
          simp [hpk] at hhead
          simp [cross, hpk, Nat.add_mul] at ih ⊢
          omega
        · have hmono : ((r.filter (fun y => p y && !(y == k))).map g).count (g x) ≤
              ((r.filter p).map g).count (g x) :=
            List.Sublist.count_le (g x) ((filter_sublist_filter r p k).map g)
          simp [cross, hpx, hxk, Nat.add_mul] at ih ⊢
          omega
      · simp [cross, hpx] at ih ⊢
        exact ih

private theorem count_filter_ne_key' (l : List Nat) (x k : Nat) (hxk : x ≠ k) :
    l.count x = (l.filter (fun y => y != k)).count x := by
  symm
  apply List.count_filter
  simpa [beq_eq_false_iff_ne] using hxk

private theorem sum_count_split (l : List Nat) (r : List Nat) (k : Nat) :
    (l.map fun x => r.count x).sum = l.count k * r.count k +
      ((l.filter fun x => x != k).map fun x => r.count x).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hxk : x = k
      · subst x
        simp [ih, Nat.add_mul, Nat.add_assoc, Nat.add_comm]
      · simp [hxk, ih, Nat.add_assoc, Nat.add_left_comm]

private theorem cross_split_key (l r : List Nat) (k : Nat) :
    cross l r = l.count k * r.count k +
      cross (l.filter (fun x => x != k)) (r.filter (fun x => x != k)) := by
  unfold cross
  have hcnt : ((l.filter fun x => x != k).map fun x => r.count x).sum =
      ((l.filter fun x => x != k).map fun x => (r.filter fun y => y != k).count x).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hxmem
    have hx : x ≠ k := by
      have hb := (List.mem_filter.mp hxmem).2
      simpa [beq_eq_false_iff_ne] using hb
    exact count_filter_ne_key' r x k hx
  rw [sum_count_split, hcnt]

private theorem filter_ne_then_notContains (l : List Nat) (k : Nat) (ks : List Nat) :
    l.filter (fun x => !(k :: ks).contains x) =
      (l.filter (fun x => x != k)).filter (fun x => !ks.contains x) := by
  have hp : (fun x : Nat => !(k :: ks).contains x) =
      (fun x : Nat => (!ks.contains x) && (x != k)) := by
    funext x
    by_cases hxk : x = k <;> by_cases hxks : ks.contains x <;> simp [hxk]
  rw [hp, List.filter_filter]

private theorem filter_notContains_nil (l : List Nat) :
    l.filter (fun x => !([].contains x)) = l := by
  induction l with
  | nil => rfl
  | cons y ys ih => simp

private theorem cross_split_keys (l r ks : List Nat) (hnd : ks.Nodup) :
    cross l r = (ks.map fun k => l.count k * r.count k).sum +
      cross (l.filter fun x => !ks.contains x) (r.filter fun x => !ks.contains x) := by
  induction ks generalizing l r with
  | nil =>
      rw [filter_notContains_nil l, filter_notContains_nil r]
      simp [cross]
  | cons k ks ih =>
      have hknot : k ∉ ks := by
        intro h
        exact (List.rel_of_pairwise_cons hnd h) rfl
      have hksnd : ks.Nodup := List.Pairwise.of_cons hnd
      rw [cross_split_key l r k]
      have hi := ih (l.filter (fun x => x != k)) (r.filter (fun x => x != k)) hksnd
      rw [hi]
      have hcount :
          (ks.map fun k' => (l.filter fun x => x != k).count k' *
            (r.filter fun x => x != k).count k').sum =
          (ks.map fun k' => l.count k' * r.count k').sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro k' hk'mem
        have hk'ne : k' ≠ k := by
          intro heq
          subst k'
          exact hknot hk'mem
        rw [count_filter_ne_key' l k' k hk'ne, count_filter_ne_key' r k' k hk'ne]
      rw [hcount]
      rw [filter_ne_then_notContains l k ks,
        filter_ne_then_notContains r k ks]
      simp [Nat.add_assoc]

-- ===== nodup / perm helpers =====
private theorem nodup_eraseDups : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      constructor
      · intro a' ha'
        rw [List.mem_eraseDups] at ha'
        have hb := (List.mem_filter.mp ha').2
        have hne : a' ≠ a := by simpa [beq_eq_false_iff_ne] using hb
        exact fun h => hne h.symm
      · exact nodup_eraseDups (as.filter fun b => !b == a)
termination_by l => l.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b : Nat => !b == a) as)

private theorem perm_contains_eq {l1 l2 : List Nat} (p : List.Perm l1 l2) (x : Nat) :
    l1.contains x = l2.contains x := by
  by_cases h1 : x ∈ l1
  · have h2 : x ∈ l2 := by
      rw [← p.mem_iff]
      exact h1
    have a : l1.contains x = true := by simpa using h1
    have b : l2.contains x = true := by simpa using h2
    rw [a, b]
  · have h2 : x ∉ l2 := by
      intro h
      exact h1 (by rwa [p.mem_iff])
    have a : l1.contains x = false := by simpa using h1
    have b : l2.contains x = false := by simpa using h2
    rw [a, b]

private theorem nodup_perm_of_mem {l1 l2 : List Nat} (h1 : l1.Nodup) (h2 : l2.Nodup)
    (hm : ∀ x, x ∈ l1 ↔ x ∈ l2) : List.Perm l1 l2 := by
  revert l2 h1 h2 hm
  induction l1 with
  | nil =>
      intro l2 h1 h2 hm
      have h2nil : l2 = [] := by
        cases l2 with
        | nil => rfl
        | cons y ys =>
            have : y ∈ ([] : List Nat) := (hm y).2 (by simp)
            simp at this
      subst l2
      exact List.Perm.nil
  | cons x xs ih =>
      intro l2 h1 h2 hm
      have hx2 : x ∈ l2 := (hm x).1 (by simp)
      have hp2 := List.perm_cons_erase hx2
      have h2tail : (l2.erase x).Nodup := h2.erase x
      have hxnotxs : x ∉ xs := by
        intro h
        exact (List.rel_of_pairwise_cons h1 h) rfl
      have hm' : ∀ y, y ∈ xs ↔ y ∈ l2.erase x := by
        intro y
        constructor
        · intro hy
          have hy2 : y ∈ l2 := (hm y).1 (by simp [hy])
          have hyne : y ≠ x := by
            intro heq
            subst y
            exact hxnotxs hy
          rw [List.Nodup.mem_erase_iff h2]
          exact ⟨hyne, hy2⟩
        · intro hy
          have hy2 : y ∈ l2 := List.mem_of_mem_erase hy
          have hy1 : y ∈ x :: xs := (hm y).2 hy2
          rcases List.mem_cons.mp hy1 with hxy | hys
          · subst y
            exact False.elim (h2.not_mem_erase hy)
          · exact hys
      have h1tail : xs.Nodup := List.Pairwise.of_cons h1
      have hp1 := ih h1tail (l2 := l2.erase x) h2tail hm'
      exact (hp1.cons x).trans hp2.symm

private theorem contains_eraseDups (heavy : List Nat) (x : Nat) :
    heavy.contains x = heavy.eraseDups.contains x := by
  by_cases h : x ∈ heavy
  · have h1 : heavy.contains x = true := by simpa using h
    have h2 : heavy.eraseDups.contains x = true := by simpa using h
    rw [h1, h2]
  · have h1 : heavy.contains x = false := by simpa using h
    have h2 : heavy.eraseDups.contains x = false := by simpa using h
    rw [h1, h2]

-- ===== joinBounds in cross form =====
private theorem joinBounds_eq_cross (l r : List Nat) (heavy : List Nat) (bucket : Nat → Nat) :
    joinBounds l r heavy bucket =
      let H := heavy.eraseDups
      let nf := fun x : Nat => !(H.contains x)
      let E := (H.map fun j => l.count j * r.count j).sum
      (E, E + cross ((l.filter nf).map bucket) ((r.filter nf).map bucket)) := by
  unfold joinBounds
  have hnf : (fun x : Nat => !heavy.contains x) = fun x : Nat => !(heavy.eraseDups.contains x) := by
    funext x
    rw [contains_eraseDups]
  rw [hnf]
  simp only [hist_getD]
  have htail :
      (((histogram ((l.filter fun x : Nat => !(heavy.eraseDups.contains x)).map bucket)).toList.map
          (fun (b, n) => n * ((r.filter fun x : Nat => !(heavy.eraseDups.contains x)).map bucket).count b)).sum) =
        cross (((l.filter fun x : Nat => !(heavy.eraseDups.contains x)).map bucket))
              (((r.filter fun x : Nat => !(heavy.eraseDups.contains x)).map bucket)) := by
    change hsum (histogram ((l.filter fun x : Nat => !(heavy.eraseDups.contains x)).map bucket))
      (fun b => ((r.filter fun x : Nat => !(heavy.eraseDups.contains x)).map bucket).count b) = _
    rw [hist_sum]
    unfold cross
    rfl
  rw [htail]

private theorem cross_le_map_cross (lf rf : List Nat) (g : Nat → Nat) :
    cross lf rf ≤ cross (lf.map g) (rf.map g) := by
  unfold cross
  induction lf with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have h1 := count_le_map_count rf x g
      omega

private theorem refine_fanout_ineq (l r heavy : List Nat) (bucket : Nat → Nat) (k : Nat) :
    let H := heavy.eraseDups
    let H' := (k :: heavy).eraseDups
    let nf := fun x : Nat => !(H.contains x)
    let nf' := fun x : Nat => !(H'.contains x)
    let f := fun j : Nat => l.count j * r.count j
    let E := (H.map f).sum
    let E' := (H'.map f).sum
    let L := l.filter nf
    let R := r.filter nf
    let L' := l.filter nf'
    let R' := r.filter nf'
    let T := cross (L.map bucket) (R.map bucket)
    let T' := cross (L'.map bucket) (R'.map bucket)
    let P := cross l r
    E ≤ E' ∧ E' ≤ P ∧ P ≤ E' + T' ∧ E' + T' ≤ E + T := by
  dsimp only
  let H := heavy.eraseDups
  let H' := (k :: heavy).eraseDups
  let nf : Nat → Bool := fun x => !(H.contains x)
  let nf' : Nat → Bool := fun x => !(H'.contains x)
  let f : Nat → Nat := fun j => l.count j * r.count j
  let E := (H.map f).sum
  let E' := (H'.map f).sum
  let L := l.filter nf
  let R := r.filter nf
  let L' := l.filter nf'
  let R' := r.filter nf'
  let T := cross (L.map bucket) (R.map bucket)
  let T' := cross (L'.map bucket) (R'.map bucket)
  let P := cross l r
  change E ≤ E' ∧ E' ≤ P ∧ P ≤ E' + T' ∧ E' + T' ≤ E + T
  have hHnod : H.Nodup := by simpa [H] using nodup_eraseDups heavy
  have hH'nod : H'.Nodup := by simpa [H'] using nodup_eraseDups (k :: heavy)
  have hsplitH : P = E + cross L R := by
    dsimp [P, E, L, R, H, nf, f]
    exact cross_split_keys l r heavy.eraseDups (nodup_eraseDups heavy)
  have hsplitH' : P = E' + cross L' R' := by
    dsimp [P, E', L', R', H', nf', f]
    exact cross_split_keys l r (k :: heavy).eraseDups (nodup_eraseDups (k :: heavy))
  have htail_le : cross L' R' ≤ T' := by
    dsimp [T', L', R']
    exact cross_le_map_cross (l.filter nf') (r.filter nf') bucket
  have h2 : E' ≤ P := by
    rw [hsplitH']
    omega
  have h3 : P ≤ E' + T' := by
    rw [hsplitH']
    omega
  by_cases hk : k ∈ heavy
  · have hmEq : ∀ x : Nat, x ∈ H' ↔ x ∈ H := by
      intro x
      constructor
      · intro hx
        have hx' : x ∈ k :: heavy := by simpa [H'] using hx
        rcases List.mem_cons.mp hx' with hxk | hxr
        · subst x
          simpa [H] using hk
        · simpa [H] using hxr
      · intro hx
        have hx' : x ∈ heavy := by simpa [H] using hx
        have : x ∈ k :: heavy := List.mem_cons_of_mem k hx'
        simpa [H'] using this
    have hperm : List.Perm H' H := nodup_perm_of_mem hH'nod hHnod hmEq
    have hEeq : E' = E := by
      have hp := (hperm.map f).sum_nat
      dsimp [E, E'] at *
      exact hp
    have hconteq : ∀ x : Nat, (k :: heavy).contains x = heavy.contains x := by
      intro x
      have hmem : x ∈ k :: heavy ↔ x ∈ heavy := by
        constructor
        · intro hx
          rcases List.mem_cons.mp hx with hxk | hxr
          · subst x
            exact hk
          · exact hxr
        · intro hxr
          exact List.mem_cons_of_mem k hxr
      by_cases h1 : x ∈ k :: heavy
      · have h2 : x ∈ heavy := hmem.mp h1
        simp [h1, h2]
      · have h2 : x ∉ heavy := by
          intro h
          exact h1 (hmem.mpr h)
        simp [h1, h2]
    have hmemEq : ∀ x : Nat, H'.contains x = H.contains x := by
      intro x
      rw [← contains_eraseDups (k :: heavy) x, ← contains_eraseDups heavy x]
      exact hconteq x
    have hnfeq : nf' = nf := by
      funext x
      dsimp [nf, nf']
      rw [hmemEq x]
    have h1 : E ≤ E' := by omega
    have h4 : E' + T' ≤ E + T := by
      have hTeq : T' = T := by
        dsimp [T, T', L, R, L', R']
        rw [hnfeq]
      rw [hEeq, hTeq]
      omega
    exact ⟨h1, h2, h3, h4⟩
  · have hkH : k ∉ H := by simpa [H] using hk
    have hnodkH : (k :: H).Nodup := by
      constructor
      · intro a ha heq
        exact hkH (by simpa [heq] using ha)
      · exact hHnod
    have hm : ∀ x : Nat, x ∈ H' ↔ x ∈ k :: H := by
      intro x
      simp [H, H']
    have hperm : List.Perm H' (k :: H) := nodup_perm_of_mem hH'nod hnodkH hm
    have hEeq : E' = E + l.count k * r.count k := by
      have hp := (hperm.map f).sum_nat
      dsimp [E, E', f] at hp ⊢
      simpa [Nat.add_comm] using hp
    have hmemH' : ∀ x : Nat, x ∈ H' ↔ x = k ∨ x ∈ H := by
      intro x
      simp [H, H']
    have hnfeq' : nf' = fun x : Nat => nf x && !(x == k) := by
      funext x
      by_cases hx : x = k
      · subst x
        have hkH' : k ∈ H' := (hmemH' k).2 (Or.inl rfl)
        simp [nf, nf', hkH, hkH']
      · by_cases hxH : x ∈ H
        · have hxH' : x ∈ H' := (hmemH' x).2 (Or.inr hxH)
          simp [nf, nf', hxH, hxH', hx]
        · have hxH' : x ∉ H' := by
            intro hmem
            have hd := (hmemH' x).1 hmem
            rcases hd with hd | hd
            · exact hx hd
            · exact hxH hd
          simp [nf, nf', hxH, hxH', hx]
    have hnfk : nf k = true := by
      simp [nf, hkH]
    have hTge :
        cross ((l.filter nf).map bucket) ((r.filter nf).map bucket) ≥
          cross ((l.filter nf').map bucket) ((r.filter nf').map bucket) +
            l.count k * r.count k := by
      rw [hnfeq']
      have hb := cross_filter_key_ge_map l r nf k hnfk bucket
      have hcntL : (l.filter nf).count k = l.count k := by rw [List.count_filter hnfk]
      have hcntR : (r.filter nf).count k = r.count k := by rw [List.count_filter hnfk]
      rw [hcntL, hcntR] at hb
      exact hb
    have h1 : E ≤ E' := by
      rw [hEeq]
      omega
    have h4 : E' + T' ≤ E + T := by
      rw [hEeq]
      dsimp [T, T', L, R, L', R'] at hTge ⊢
      omega
    exact ⟨h1, h2, h3, h4⟩

theorem RefineFanout_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices.RefineFanout.Goal := by
  intro l r heavy bucket k
  rw [joinBounds_eq_cross, joinBounds_eq_cross]
  dsimp only
  exact refine_fanout_ineq l r heavy bucket k

private def rVal (r : Row) : Int := (r.value.map Int64.toInt).getD 0
private def rSum (xs : List Row) : Int := (xs.map rVal).sum
private def rCount (xs : List Row) : Nat := xs.countP fun r => r.value.isSome
private def initS : Summary := ⟨0,0,0,0⟩
private def stepS (s : Summary) (r : Row) := combine s (rowSummary r)
private def sumRows (xs : List Row) : Summary := xs.foldl stepS initS

private theorem rSum_append (xs ys : List Row) : rSum (xs ++ ys) = rSum xs + rSum ys := by
  simp [rSum, List.map_append, List.sum_append]

private theorem rCount_append (xs ys : List Row) : rCount (xs ++ ys) = rCount xs + rCount ys := by
  simp [rCount, List.countP_append]

private theorem combine_assoc (a b c : Summary) : combine (combine a b) c = combine a (combine b c) := by
  cases a; cases b; cases c
  simp [combine]
  omega

private theorem combine_init_right (a : Summary) (hlo : a.low ≤ a.total) (hhi : a.total ≤ a.high) :
    combine a initS = a := by
  cases a with
  | mk total low high count =>
      simp [combine, initS] at hlo hhi ⊢
      omega

private theorem combine_init_left (a : Summary) (hlo : a.low ≤ 0) (hhi : 0 ≤ a.high) :
    combine initS a = a := by
  cases a with
  | mk total low high count =>
      simp [combine, initS] at hlo hhi ⊢
      omega

private theorem row_low_nonpos (r : Row) : (rowSummary r).low ≤ 0 := by
  rcases r with ⟨_, v⟩
  cases v <;> simp [rowSummary] <;> omega
private theorem row_high_nonneg (r : Row) : 0 ≤ (rowSummary r).high := by
  rcases r with ⟨_, v⟩
  cases v <;> simp [rowSummary] <;> omega
private theorem row_low_le_total (r : Row) : (rowSummary r).low ≤ (rowSummary r).total := by
  rcases r with ⟨_, v⟩
  cases v <;> simp [rowSummary] <;> omega
private theorem row_total_le_high (r : Row) : (rowSummary r).total ≤ (rowSummary r).high := by
  rcases r with ⟨_, v⟩
  cases v <;> simp [rowSummary] <;> omega
private theorem row_total (r : Row) : (rowSummary r).total = rVal r := by
  rcases r with ⟨_, v⟩
  cases v <;> rfl
private theorem row_count (r : Row) : (rowSummary r).count = if r.value.isSome then 1 else 0 := by
  rcases r with ⟨_, v⟩
  cases v <;> rfl

private theorem foldl_combine_eq (s a : Summary) (rows : List Row) :
    rows.foldl stepS (combine s a) = combine s (rows.foldl stepS a) := by
  induction rows generalizing a with
  | nil => rfl
  | cons r rs ih =>
      simp only [List.foldl_cons]
      have h : stepS (combine s a) r = combine s (stepS a r) := by
        simp [stepS, combine_assoc]
      rw [h]
      exact ih (stepS a r)

private theorem sumRows_cons (r : Row) (rs : List Row) :
    sumRows (r :: rs) = combine (rowSummary r) (sumRows rs) := by
  unfold sumRows
  simp only [List.foldl_cons, stepS]
  calc
    rs.foldl stepS (combine initS (rowSummary r))
        = rs.foldl stepS (rowSummary r) := by
            rw [combine_init_left _ (row_low_nonpos r) (row_high_nonneg r)]
    _ = rs.foldl stepS (combine (rowSummary r) initS) := by
            rw [combine_init_right _ (row_low_le_total r) (row_total_le_high r)]
    _ = combine (rowSummary r) (rs.foldl stepS initS) :=
            foldl_combine_eq (rowSummary r) initS rs

private theorem sumRows_low_nonpos (xs : List Row) : (sumRows xs).low ≤ 0 := by
  induction xs with
  | nil => simp [sumRows, initS]
  | cons r rs ih =>
      rw [sumRows_cons]
      have h1 := row_low_nonpos r
      simp [combine] at h1 ih ⊢
      omega
private theorem sumRows_high_nonneg (xs : List Row) : 0 ≤ (sumRows xs).high := by
  induction xs with
  | nil => simp [sumRows, initS]
  | cons r rs ih =>
      rw [sumRows_cons]
      have h1 := row_high_nonneg r
      simp [combine] at h1 ih ⊢
      omega
private theorem sumRows_low_le_total (xs : List Row) : (sumRows xs).low ≤ (sumRows xs).total := by
  induction xs with
  | nil => simp [sumRows, initS]
  | cons r rs ih =>
      rw [sumRows_cons]
      have h1 := row_low_le_total r
      simp [combine] at h1 ih ⊢
      omega
private theorem sumRows_total_le_high (xs : List Row) : (sumRows xs).total ≤ (sumRows xs).high := by
  induction xs with
  | nil => simp [sumRows, initS]
  | cons r rs ih =>
      rw [sumRows_cons]
      have h1 := row_total_le_high r
      simp [combine] at h1 ih ⊢
      omega

private theorem sumRows_append (xs ys : List Row) : sumRows (xs ++ ys) = combine (sumRows xs) (sumRows ys) := by
  unfold sumRows
  rw [List.foldl_append]
  have hx := combine_init_right (xs.foldl stepS initS) (sumRows_low_le_total xs) (sumRows_total_le_high xs)
  calc
    List.foldl stepS (List.foldl stepS initS xs) ys =
        List.foldl stepS (combine (List.foldl stepS initS xs) initS) ys := by rw [hx]
    _ = combine (List.foldl stepS initS xs) (List.foldl stepS initS ys) :=
        foldl_combine_eq (List.foldl stepS initS xs) initS ys

private theorem summary_ext {a b : Summary} (ht : a.total = b.total) (hl : a.low = b.low)
    (hh : a.high = b.high) (hc : a.count = b.count) : a = b := by
  cases a; cases b; simp_all

private theorem min_add (a b c : Int) : min (a + b) (a + c) = a + min b c := by omega
private theorem max_add (a b c : Int) : max (a + b) (a + c) = a + max b c := by omega

private theorem combine_repeat (s : Summary) (hlo : s.low ≤ s.total) (hhi : s.total ≤ s.high) (n : Nat) :
    combine s (repeated s n) = repeated s (n+1) := by
  rcases s with ⟨tot, lo, hi, cnt⟩
  simp at hlo hhi
  unfold repeated
  by_cases hn : n = 0
  · subst n
    simp [combine, initS]
    omega
  · simp [hn]
    let d : Int := (n - 1 : Nat) * tot
    have hnd : (n : Int) = (n - 1 : Nat) + 1 := by omega
    apply summary_ext
    · simp [combine]
      rw [Int.add_mul, Int.one_mul]
      omega
    · -- low
      simp only [combine]
      have hrewrite : min lo (tot + (lo + min 0 d)) = lo + min 0 (tot + min 0 d) := by
        have heq : tot + (lo + min 0 d) = lo + (tot + min 0 d) := by omega
        rw [heq]
        simpa using (min_add lo 0 (tot + min 0 d))
      rw [hrewrite]
      have hmin : min 0 (tot + min 0 d) = min 0 ((n : Int) * tot) := by
        rw [hnd, Int.add_mul, Int.one_mul]
        by_cases ht : tot ≤ 0
        · have hd0 : ((n - 1 : Nat) : Int) * tot ≤ 0 :=
            Int.mul_nonpos_of_nonneg_of_nonpos (by omega) ht
          simp [Int.min_def, ht, hd0]
          omega
        · have ht0 : 0 ≤ tot := by omega
          have hd0 : 0 ≤ ((n - 1 : Nat) : Int) * tot := Int.mul_nonneg (by omega) ht0
          simp [Int.min_def, ht0, hd0]
          omega
      rw [hmin]
    · -- high
      simp only [combine]
      have hrewrite : max hi (tot + (hi + max 0 d)) = hi + max 0 (tot + max 0 d) := by
        have heq : tot + (hi + max 0 d) = hi + (tot + max 0 d) := by omega
        rw [heq]
        simpa using (max_add hi 0 (tot + max 0 d))
      rw [hrewrite]
      have hmax : max 0 (tot + max 0 d) = max 0 ((n : Int) * tot) := by
        rw [hnd, Int.add_mul, Int.one_mul]
        by_cases ht : 0 ≤ tot
        · have hd0 : 0 ≤ ((n - 1 : Nat) : Int) * tot := Int.mul_nonneg (by omega) ht
          simp [Int.max_def, ht, hd0]
          omega
        · have ht0 : tot ≤ 0 := by omega
          have hd0 : ((n - 1 : Nat) : Int) * tot ≤ 0 :=
            Int.mul_nonpos_of_nonneg_of_nonpos (by omega) ht0
          simp [Int.max_def, ht0, hd0]
          omega
      rw [hmax]
    · simp [combine]
      rw [Nat.add_mul, Nat.one_mul]
      omega

private theorem sumRows_replicate (rs : List Row) (n : Nat) :
    sumRows (List.replicate n rs).flatten = repeated (sumRows rs) n := by
  induction n with
  | zero => simp [sumRows, initS, repeated]
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons]
      rw [sumRows_append, ih]
      exact combine_repeat (sumRows rs) (sumRows_low_le_total rs) (sumRows_total_le_high rs) n

private theorem sequential_ok_prefix (acc : Option Int) (xs : List Row)
    (hacc : InRange (acc.getD 0)) {b : Option Int}
    (hres : sequential acc xs = .ok b) :
    ∀ m ≤ xs.length, InRange (((xs.take m).foldl (fun a row => add a row.value) acc).getD 0) := by
  induction xs generalizing acc with
  | nil =>
      intro m hm
      have hm0 : m = 0 := by simpa using hm
      subst m
      exact hacc
  | cons r rs ih =>
      intro m hm
      cases m with
      | zero => exact hacc
      | succ m =>
          have hnext : InRange ((add acc r.value).getD 0) := by
            by_cases h : InRange ((add acc r.value).getD 0)
            · exact h
            · simp [sequential, h] at hres
          have hres' : sequential (add acc r.value) rs = .ok b := by
            simpa [sequential, hnext] using hres
          have hm' : m ≤ rs.length := by simpa using hm
          have hh := ih (add acc r.value) hnext hres' m hm'
          rw [List.take_succ_cons]
          simpa using hh

theorem RepeatEnvelope_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices.RepeatEnvelope.Goal := by
  intro rs n acc hacc
  dsimp only
  let flat := (List.replicate n rs).flatten
  let s := (prepare (.chunk rs)).summary
  let h := repeated s n
  have hprep : (prepare (.chunk flat)).summary = h := by
    dsimp [flat, h, s]
    change sumRows (List.replicate n rs).flatten = repeated (sumRows rs) n
    exact sumRows_replicate rs n
  have hsnd := (TinylambOpt.ResearchProofs.CheckedAggregateTree.sound (.chunk flat) acc hacc)
  let a := acc.getD 0
  let value := if h.count == 0 then acc else some (a + h.total)
  change (InRange (a + h.low) ∧ InRange (a + h.high) ↔ sequential acc flat = .ok value)
  have hrun :
      run acc (prepare (.chunk flat)) =
        if InRange (a + h.low) ∧ InRange (a + h.high) then
          (.ok value, [.skip])
        else
          (sequential acc flat, [.openChunk]) := by
    unfold run
    rw [hprep]
    simp [prepare, value, a]
  constructor
  · intro hg
    have hr1 : (run acc (prepare (.chunk flat))).1 = sequential acc flat := hsnd.1
    have hg' : InRange (a + h.low) ∧ InRange (a + h.high) := hg
    have hr2 : (run acc (prepare (.chunk flat))).2 = [.skip] := by
      rw [hrun]
      simp [hg']
    have hall : ∀ m ≤ flat.length,
        InRange (((flat.take m).foldl (fun a row => add a row.value) acc).getD 0) :=
      hsnd.2.mp hr2
    rw [hrun] at hr1
    simp [hg'] at hr1
    exact hr1.symm
  · intro hseq
    have hall : ∀ m ≤ flat.length,
        InRange (((flat.take m).foldl (fun a row => add a row.value) acc).getD 0) :=
      sequential_ok_prefix acc flat hacc hseq
    have hr2 : (run acc (prepare (.chunk flat))).2 = [.skip] := hsnd.2.mpr hall
    have hg' : InRange (a + h.low) ∧ InRange (a + h.high) := by
      by_cases hgcase : InRange (a + h.low) ∧ InRange (a + h.high)
      · exact hgcase
      · have hop : (run acc (prepare (.chunk flat))).2 = [.openChunk] := by
          rw [hrun]
          simp [hgcase]
        rw [hop] at hr2
        simp at hr2
    exact hg'

private def intOfCell : Cell → Option Int
  | .intVal v => v
  | _ => none

private def intsOfRows (rows : List (List Cell)) : List Int :=
  rows.flatten.filterMap intOfCell

private theorem intsOfRows_append (done : List (List Cell)) (row : List Cell) :
    intsOfRows (done ++ [row]) = intsOfRows done ++ row.filterMap intOfCell := by
  simp [intsOfRows]

private theorem fb_hm_insert_some {α β : Type} [BEq α] [Hashable α] [LawfulBEq α]
    [LawfulHashable α] {m : Std.HashMap α β} {k j : α} {v v' : β} :
    (m.insert k v)[j]? = some v' → ((k == j) = true ∧ v' = v) ∨ m[j]? = some v' := by
  intro h
  rw [Std.HashMap.getElem?_insert] at h
  by_cases hkj : k == j
  · simp [hkj] at h
    exact Or.inl ⟨hkj, h.symm⟩
  · simp [hkj] at h
    exact Or.inr h

private theorem fb_hm_insert_present {α β : Type} [BEq α] [Hashable α] [LawfulBEq α]
    [LawfulHashable α] {m : Std.HashMap α β} {k j : α} {v : β} :
    (m.insert k v)[j]?.isSome ↔ (k == j) = true ∨ m[j]?.isSome := by
  rw [Std.HashMap.getElem?_insert]
  by_cases h : k == j <;> simp [h]

private theorem fb_removeAll_singleton_eq_self {α : Type} [BEq α] [LawfulBEq α]
    (as : List α) (x : α) (hx : x ∉ as) : [x].removeAll as = [x] := by
  induction as with
  | nil => simp
  | cons y ys ih =>
      by_cases hxy : x == y
      · exfalso; apply hx
        have heq : x = y := LawfulBEq.eq_of_beq hxy
        simp [heq]
      · rw [List.removeAll_cons]
        have hfilt : (List.filter (fun z => !z == y) [x]) = [x] := by simp [hxy]
        rw [hfilt, ih (by intro hmem; exact hx (by simp [hmem]))]

private theorem fb_removeAll_singleton_eq_nil {α : Type} [BEq α] [LawfulBEq α]
    (as : List α) (x : α) (hx : x ∈ as) : [x].removeAll as = [] := by
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

private theorem fb_eraseDups_append_new_length {α : Type} [BEq α] [LawfulBEq α]
    (as : List α) (x : α) (hx : x ∉ as) :
    (as ++ [x]).eraseDups.length = as.eraseDups.length + 1 := by
  rw [List.eraseDups_append, fb_removeAll_singleton_eq_self as x hx]
  simp [List.eraseDups_cons]

private theorem fb_eraseDups_append_old_length {α : Type} [BEq α] [LawfulBEq α]
    (as : List α) (x : α) (hx : x ∈ as) :
    (as ++ [x]).eraseDups.length = as.eraseDups.length := by
  rw [List.eraseDups_append, fb_removeAll_singleton_eq_nil as x hx]
  simp

private theorem fmap_append_none (done : List Cell) :
    (done ++ [Cell.intVal (none : Option Int)]).filterMap intOfCell = done.filterMap intOfCell := by
  rw [List.filterMap_append]
  simp [intOfCell]

private theorem fmap_append_some (done : List Cell) (v : Int) :
    (done ++ [Cell.intVal (some v)]).filterMap intOfCell = done.filterMap intOfCell ++ [v] := by
  rw [List.filterMap_append]
  simp [intOfCell]

private theorem fmap_append_str (done : List Cell) (v : Option (List Nat)) :
    (done ++ [Cell.strVal v]).filterMap intOfCell = done.filterMap intOfCell := by
  rw [List.filterMap_append]
  simp [intOfCell]


private abbrev FState := FieldCache × List (List Nat) × Nat

private def RowInv (hot : Int → Bool) (seen : List Int) (done : List Cell) (st : FState) : Prop :=
  (∀ k bytes, st.1[k]? = some bytes → bytes = encodeCellBytes (.intVal (some k))) ∧
  st.2.1.reverse = done.map encodeCellBytes ∧
  (∀ k, (st.1[k]?).isSome ↔ k ∈ seen ∨ k ∈ (done.filterMap intOfCell).filter hot) ∧
  st.2.2 = ((done.filterMap intOfCell).filter (fun k => !decide (k ∈ seen) && hot k)).eraseDups.length +
    (done.filterMap intOfCell).countP (fun k => !hot k)

private theorem step_none (hot : Int → Bool) (seen : List Int) (done : List Cell) (st : FState)
    (h : RowInv hot seen done st) :
    RowInv hot seen (done ++ [Cell.intVal none]) (st.1, encodeCellBytes (.intVal none) :: st.2.1, st.2.2) := by
  rcases h with ⟨hval, hparts, hmem, hmiss⟩
  rcases st with ⟨m, rev, miss⟩
  dsimp [RowInv] at hval hparts hmem hmiss ⊢
  rw [fmap_append_none]
  refine ⟨hval, ?_, ?_, ?_⟩
  · simp [List.reverse_cons, hparts, encodeCellBytes]
  · intro k
    simpa using hmem k
  · simpa using hmiss

private theorem step_str (hot : Int → Bool) (seen : List Int) (done : List Cell) (v : Option (List Nat)) (st : FState)
    (h : RowInv hot seen done st) :
    RowInv hot seen (done ++ [Cell.strVal v]) (st.1, encodeCellBytes (.strVal v) :: st.2.1, st.2.2) := by
  rcases h with ⟨hval, hparts, hmem, hmiss⟩
  rcases st with ⟨m, rev, miss⟩
  dsimp [RowInv] at hval hparts hmem hmiss ⊢
  rw [fmap_append_str]
  refine ⟨hval, ?_, ?_, ?_⟩
  · simp [List.reverse_cons, hparts, encodeCellBytes]
  · intro k
    simpa using hmem k
  · simpa using hmiss

private theorem step_cold (hot : Int → Bool) (seen : List Int) (done : List Cell) (v : Int) (st : FState)
    (h : RowInv hot seen done st) (hh : hot v = false) :
    RowInv hot seen (done ++ [Cell.intVal (some v)]) (st.1, encodeCellBytes (.intVal (some v)) :: st.2.1, st.2.2 + 1) := by
  rcases h with ⟨hval, hparts, hmem, hmiss⟩
  rcases st with ⟨m, rev, miss⟩
  dsimp [RowInv] at hval hparts hmem hmiss ⊢
  rw [fmap_append_some]
  have hvnot : v ∉ (done.filterMap intOfCell).filter hot := by
    intro hv
    have ht : hot v = true := (List.mem_filter.mp hv).2
    simp [hh] at ht
  refine ⟨hval, ?_, ?_, ?_⟩
  · simp [List.reverse_cons, hparts, encodeCellBytes]
  · intro k
    by_cases hkv : k = v
    · subst k
      have hhmem := hmem v
      simpa [hh, hvnot] using hhmem
    · simpa [hkv] using hmem k
  · simp [hh, hmiss, Nat.add_assoc]

private theorem step_hit (hot : Int → Bool) (seen : List Int) (done : List Cell) (v : Int) (st : FState)
    (h : RowInv hot seen done st) (hh : hot v = true)
    (hm : st.1[v]? = some (encodeCellBytes (.intVal (some v)))) :
    RowInv hot seen (done ++ [Cell.intVal (some v)]) (st.1, encodeCellBytes (.intVal (some v)) :: st.2.1, st.2.2) := by
  rcases h with ⟨hval, hparts, hmem, hmiss⟩
  rcases st with ⟨m, rev, miss⟩
  dsimp [RowInv] at hval hparts hmem hmiss ⊢
  rw [fmap_append_some]
  have hvor : v ∈ seen ∨ v ∈ (done.filterMap intOfCell).filter hot := (hmem v).1 (by simp [hm])
  refine ⟨hval, ?_, ?_, ?_⟩
  · simp [List.reverse_cons, hparts, encodeCellBytes]
  · intro k
    by_cases hkv : k = v
    · subst k
      constructor
      · intro _
        simpa [hh] using hvor
      · intro _
        simp [hm]
    · simpa [hkv] using hmem k
  · simp [hh] at hmiss ⊢
    by_cases hvseen : v ∈ seen
    · simp [hvseen] at hmiss ⊢
      exact hmiss
    · have hvdone : v ∈ (done.filterMap intOfCell).filter hot := by
        rcases hvor with hs | hd
        · exact False.elim (hvseen hs)
        · exact hd
      have hve : v ∈ done.filterMap intOfCell := (List.mem_filter.mp hvdone).1
      have hvin : v ∈ (done.filterMap intOfCell).filter (fun k => !decide (k ∈ seen) && hot k) := by
        simp [hve, hvseen, hh]
      have hvlist : ([v] : List Int).filter (fun k => !decide (k ∈ seen) && hot k) = [v] := by
        simp [hvseen, hh]
      rw [hvlist, fb_eraseDups_append_old_length _ v hvin]
      exact hmiss

private theorem step_miss (hot : Int → Bool) (seen : List Int) (done : List Cell) (v : Int) (st : FState)
    (h : RowInv hot seen done st) (hh : hot v = true) (hm : st.1[v]? = none) :
    RowInv hot seen (done ++ [Cell.intVal (some v)])
      (st.1.insert v (encodeCellBytes (.intVal (some v))),
       encodeCellBytes (.intVal (some v)) :: st.2.1, st.2.2 + 1) := by
  rcases h with ⟨hval, hparts, hmem, hmiss⟩
  rcases st with ⟨m, rev, miss⟩
  dsimp [RowInv] at hval hparts hmem hmiss ⊢
  rw [fmap_append_some]
  have hvnotDone : v ∉ (done.filterMap intOfCell).filter hot := by
    intro hv
    have hs : (m[v]?).isSome = true := (hmem v).2 (Or.inr hv)
    simp [hm] at hs
  have hvnotSeen : v ∉ seen := by
    intro hv
    have hs : (m[v]?).isSome = true := (hmem v).2 (Or.inl hv)
    simp [hm] at hs
  have hvnot : v ∉ (done.filterMap intOfCell).filter (fun k => !decide (k ∈ seen) && hot k) := by
    intro hv
    have hv' : v ∈ (done.filterMap intOfCell).filter hot := by
      simpa [hvnotSeen] using hv
    exact hvnotDone hv'
  refine ⟨?_, ?_, ?_, ?_⟩
  · intro k b hk
    rcases fb_hm_insert_some hk with ⟨hkv, hkb⟩ | hkm
    · have hvk : v = k := LawfulBEq.eq_of_beq hkv
      subst b
      subst k
      rfl
    · exact hval k b hkm
  · simp [List.reverse_cons, hparts, encodeCellBytes]
  · intro k
    rw [fb_hm_insert_present]
    by_cases hkv : k = v
    · subst k
      simp [hh]
    · have hvk : v ≠ k := by omega
      have hhmem := hmem k
      simp [hkv, hvk] at hhmem ⊢
      exact hhmem
  · simp [hh] at hmiss ⊢
    have hvlist : ([v] : List Int).filter (fun k => !decide (k ∈ seen) && hot k) = [v] := by
      simp [hvnotSeen, hh]
    rw [hvlist, fb_eraseDups_append_new_length _ v hvnot]
    omega

private def fieldsStep (hot : Int → Bool) (st : FState) (c : Cell) : FState :=
  match st with
  | (h, rev, miss) =>
    match c with
    | .intVal (some v) =>
      if hot v then match h[v]? with
        | some bytes => (h, bytes :: rev, miss)
        | none => let bytes := encodeCellBytes c; (h.insert v bytes, bytes :: rev, miss + 1)
      else (h, encodeCellBytes c :: rev, miss + 1)
    | _ => (h, encodeCellBytes c :: rev, miss)

private abbrev fieldStep := fieldsStep

private theorem fieldStep_spec (hot : Int → Bool) (seen : List Int) (done : List Cell) (st : FState)
    (h : RowInv hot seen done st) (c : Cell) : RowInv hot seen (done ++ [c]) (fieldStep hot st c) := by
  rcases c with v | v
  · cases v with
    | none => simpa [fieldStep, fieldsStep] using step_none hot seen done st h
    | some v =>
        by_cases hh : hot v
        · cases hm : st.1[v]? with
          | some bytes =>
              have hb : bytes = encodeCellBytes (.intVal (some v)) := h.1 v bytes hm
              subst bytes
              simpa [fieldStep, fieldsStep, hh, hm] using step_hit hot seen done v st h hh hm
          | none =>
              simpa [fieldStep, fieldsStep, hh, hm] using step_miss hot seen done v st h hh hm
        · have hhfalse : hot v = false := by
            cases h : hot v <;> simp [h] at hh ⊢
          simpa [fieldStep, fieldsStep, hh] using step_cold hot seen done v st h hhfalse
  · simpa [fieldStep, fieldsStep] using step_str hot seen done v st h

private theorem rowFold (hot : Int → Bool) (seen : List Int) (done0 : List Cell) (row : List Cell) (st : FState)
    (h : RowInv hot seen done0 st) :
    RowInv hot seen (done0 ++ row) (row.foldl (fieldStep hot) st) := by
  induction row generalizing done0 st with
  | nil => simpa using h
  | cons c cs ih =>
      rw [List.foldl_cons]
      have h1 := fieldStep_spec hot seen done0 st h c
      have h2 := ih (done0 ++ [c]) (fieldStep hot st c) h1
      simpa [List.append_assoc] using h2

private theorem filter_true_eq_self (bs : List Int) : bs.filter (fun _ => true) = bs := by
  induction bs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem removeAll_eq_filter_notMem (bs as : List Int) :
    bs.removeAll as = bs.filter (fun x => x ∉ as) := by
  induction as generalizing bs with
  | nil =>
      simp [filter_true_eq_self]
  | cons a as ih =>
      rw [List.removeAll_cons, ih, List.filter_filter]
      apply List.filter_congr
      intro x hx
      by_cases hxa : x = a
      · subst x
        simp
      · have hbeq : (x == a) = false := by simpa [beq_eq_false_iff_ne] using hxa
        simp [hbeq, hxa]

private theorem eraseDups_append_filter (as bs : List Int) :
    (as ++ bs).eraseDups.length = as.eraseDups.length + (bs.filter (fun x => x ∉ as)).eraseDups.length := by
  rw [List.eraseDups_append, removeAll_eq_filter_notMem]
  simp

private theorem fieldStep_eq_fieldsStep (hot : Int → Bool) (st : FState) (c : Cell) :
    fieldStep hot st c = fieldsStep hot st c := by
  cases c with
  | intVal v =>
      cases v with
      | none => simp [fieldStep, fieldsStep]
      | some v =>
          by_cases hh : hot v
          · cases hm : st.1[v]? <;> simp [fieldStep, fieldsStep, hh, hm]
          · simp [fieldStep, fieldsStep, hh]
  | strVal v => simp [fieldStep, fieldsStep]

private theorem fields_eq_fold (hot : Int → Bool) (cache : FieldCache) (row : List Cell) :
    fields hot cache row = row.foldl (fieldStep hot) (cache, [], 0) := by
  unfold fields
  rfl

private theorem frame_eq (row : List Cell) (payload : List Nat)
    (hp : payload = (row.map encodeCellBytes).flatten) :
    68 :: (be32 (6 + payload.length) ++ be16 row.length ++ payload) = refEncodeDataRow row := by
  subst payload
  simp [refEncodeDataRow]

private abbrev GState := FieldCache × List (List Nat) × Nat

private def GlobalInv (hot : Int → Bool) (doneRows : List (List Cell)) (st : GState) : Prop :=
  (∀ k bytes, st.1[k]? = some bytes → bytes = encodeCellBytes (.intVal (some k))) ∧
  (∀ k, (st.1[k]?).isSome ↔ k ∈ (intsOfRows doneRows).filter hot) ∧
  st.2.1.reverse = doneRows.map refEncodeDataRow ∧
  st.2.2 = ((intsOfRows doneRows).filter hot).eraseDups.length +
    (intsOfRows doneRows).countP (fun k => !hot k)

private theorem globalStep (hot : Int → Bool) (doneRows : List (List Cell)) (st : GState)
    (h : GlobalInv hot doneRows st) (row : List Cell) :
    let (c, parts, calls) := fields hot st.1 row
    let payload := parts.reverse.flatten
    let frame := 68 :: (be32 (6 + payload.length) ++ be16 row.length ++ payload)
    GlobalInv hot (doneRows ++ [row]) (c, frame :: st.2.1, st.2.2 + calls) := by
  rcases h with ⟨hval, hmem, hframes, hcount⟩
  rcases st with ⟨m, rev, total⟩
  dsimp only
  let seen := (intsOfRows doneRows).filter hot
  have hrow0 : RowInv hot seen [] (m, [], 0) := by
    refine ⟨hval, ?_, ?_, ?_⟩
    · simp
    · intro k
      simpa [seen] using hmem k
    · simp [seen]
  have hrow := rowFold hot seen [] row (m, [], 0) hrow0
  rw [fields_eq_fold hot m row]
  rcases hr : row.foldl (fieldStep hot) (m, [], 0) with ⟨m', parts, calls⟩
  rw [hr] at hrow
  dsimp [RowInv] at hrow
  rcases hrow with ⟨hval', hparts', hmem', hcount'⟩
  have hpayload : parts.reverse.flatten = (row.map encodeCellBytes).flatten := by
    rw [hparts']
  refine ⟨hval', ?_, ?_, ?_⟩
  · intro k
    have hk := hmem' k
    simpa [intsOfRows_append, seen] using hk
  · rw [List.reverse_cons, hframes]
    rw [frame_eq row _ hpayload]
    simp
  · rw [intsOfRows_append]
    dsimp [seen] at hcount' ⊢
    have hnewhot :
        (row.filterMap intOfCell).filter (fun k => !decide (k ∈ seen) && hot k) =
          ((row.filterMap intOfCell).filter hot).filter (fun k => k ∉ seen) := by
      rw [List.filter_filter]
      apply List.filter_congr
      intro k hk
      by_cases hks : k ∈ seen <;> by_cases hht : hot k <;> simp [hks, hht]
    rw [hnewhot] at hcount'
    have hfilt :
        (intsOfRows doneRows ++ row.filterMap intOfCell).filter hot =
          seen ++ (row.filterMap intOfCell).filter hot := by
      simp [seen, List.filter_append]
    rw [hfilt, eraseDups_append_filter]
    simp at hcount
    rw [hcount', hcount]
    simp [seen, List.countP_append, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private def wireStep (hot : Int → Bool) (st : GState) (row : List Cell) : GState :=
  let (c, parts, calls) := fields hot st.1 row
  let payload := parts.reverse.flatten
  let frame := 68 :: (be32 (6 + payload.length) ++ be16 row.length ++ payload)
  (c, frame :: st.2.1, st.2.2 + calls)

private theorem globalFold (hot : Int → Bool) (doneRows rows : List (List Cell)) (st : GState)
    (h : GlobalInv hot doneRows st) :
    GlobalInv hot (doneRows ++ rows) (rows.foldl (wireStep hot) st) := by
  induction rows generalizing doneRows st with
  | nil => simpa using h
  | cons row rows ih =>
      rw [List.foldl_cons]
      have h1 : GlobalInv hot (doneRows ++ [row]) (wireStep hot st row) := by
        simpa [wireStep] using globalStep hot doneRows st h row
      have h2 := ih (doneRows ++ [row]) (wireStep hot st row) h1
      simpa [List.append_assoc] using h2

theorem FieldBytes_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices.FieldBytes.Goal := by
  intro rows hot
  dsimp only
  change cachedWire hot rows = (refRowEncode rows,
    ((rows.flatten.filterMap intOfCell).filter hot).eraseDups.length +
      (rows.flatten.filterMap intOfCell).countP (fun i => !hot i))
  let ints := rows.flatten.filterMap intOfCell
  have hinit : GlobalInv hot [] ({}, [], 0) := by
    refine ⟨?_, ?_, ?_, ?_⟩
    · intro k b h
      simp at h
    · intro k
      simp [intsOfRows]
    · simp
    · simp [intsOfRows]
  have hglob : GlobalInv hot rows (rows.foldl (wireStep hot) ({}, [], 0)) := by
    simpa using globalFold hot [] rows ({}, [], 0) hinit
  rcases hglob with ⟨_, _, hframes, hcount⟩
  unfold cachedWire
  have hfold :
      (List.foldl (fun (h, rev, total) row =>
        let (next, parts, calls) := fields hot h row
        let payload := parts.reverse.flatten
        let frame := 68 :: (be32 (6 + payload.length) ++ be16 row.length ++ payload)
        (next, frame :: rev, total + calls)) ({}, [], 0) rows) =
      rows.foldl (wireStep hot) ({}, [], 0) := by
    rfl
  rw [hfold]
  have hbytes :
      (rows.foldl (wireStep hot) ({}, [], 0)).2.1.reverse.flatten = refRowEncode rows := by
    rw [hframes]
    rfl
  have hcnt :
      (rows.foldl (wireStep hot) ({}, [], 0)).2.2 =
        ((rows.flatten.filterMap intOfCell).filter hot).eraseDups.length +
          (rows.flatten.filterMap intOfCell).countP (fun i => !hot i) := by
    simpa [intsOfRows] using hcount
  exact Prod.ext hbytes hcnt

end TinylambOpt.ResearchProofs.AnalyticsWorkloads.PhysicalChoices

-- C++ references to this model or its proof.
-- server/postgres_protocol.cpp
