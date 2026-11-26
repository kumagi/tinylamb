import TinylambOpt.ResearchGoals.EngineDecisions.Optimizer
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.Opt_NullableGroupKey

open TinylambOpt.ResearchGoals.EngineDecisions.Optimizer.NullableGroupKey

theorem compact_beq_eq_fst_beq (r : Row) (n : Nat) :
    (compact r == (some n, none)) = (r.1 == some n) := by
  rcases r with ⟨f, s⟩
  cases f with
  | none => simp [compact]
  | some m =>
    have hpEq : (((some m, (none : Option Nat)) == (some n, none)) = true) ↔ m = n := by
      constructor
      · intro h
        have hpair : (some m, (none : Option Nat)) = (some n, none) := beq_iff_eq.mp h
        cases hpair
        rfl
      · intro h
        subst n
        simp
    by_cases hmn : m = n
    · subst n
      simp [compact]
    · have hpf : ((some m, (none : Option Nat)) == (some n, none)) = false := by
        cases hp : ((some m, (none : Option Nat)) == (some n, none))
        · rfl
        · exact False.elim (hmn (hpEq.mp hp))
      have hqf : (m == n) = false := by
        cases hq : (m == n)
        · rfl
        · exact False.elim (hmn (beq_iff_eq.mp hq))
      simp [compact, hpf, hqf]

theorem mem_groups {key : Row → Row} {xs : List Row} {r : Row} {c : Nat} :
    (r, c) ∈ groups xs key ↔
      ∃ k ∈ (xs.map key).eraseDups,
        xs.find? (fun x => (key x == k)) = some r ∧
        xs.countP (fun x => (key x == k)) = c := by
  unfold groups
  rw [List.mem_filterMap]
  constructor
  · rintro ⟨k, hk, h⟩
    dsimp at h
    cases hf : xs.find? (fun x => (key x == k)) with
    | none => simp [hf] at h
    | some rep =>
      simp [hf] at h
      rcases h with ⟨hrepr, hcnt⟩
      subst r
      exact ⟨k, hk, hf, hcnt⟩
  · rintro ⟨k, hk, hf, hc⟩
    refine ⟨k, hk, ?_⟩
    dsimp
    simp [hf, hc]

theorem id_group_count {xs : List Row} {r c} (h : (r, c) ∈ groups xs id) :
    c = xs.countP (fun x => x == r) := by
  rcases (mem_groups.mp h) with ⟨k, hk, hf, hc⟩
  have hkr : r = k := by
    have ht := List.find?_some hf
    exact beq_iff_eq.mp ht
  subst r
  exact hc.symm

theorem id_group_rep_mem {xs : List Row} {r c} (h : (r, c) ∈ groups xs id) : r ∈ xs := by
  rcases (mem_groups.mp h) with ⟨k, hk, hf, hc⟩
  have hkr : r = k := by
    have ht := List.find?_some hf
    exact beq_iff_eq.mp ht
  subst r
  exact List.mem_of_find?_eq_some hf

theorem countP_le_of_subset {α} (p q : α → Bool) (l : List α)
    (hsub : ∀ x ∈ l, p x = true → q x = true) :
    l.countP p ≤ l.countP q := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    simp only [List.countP_cons]
    have hsub' : ∀ y ∈ xs, p y = true → q y = true := by
      intro y hy hpy
      exact hsub y (by simp [hy]) hpy
    have hi := ih hsub'
    by_cases hpx : p x = true
    · have hqx : q x = true := hsub x (by simp) hpx
      simp [hpx, hqx]
      omega
    · by_cases hqx : q x = true
      · simp [hpx, hqx]
        omega
      · simp [hpx, hqx]
        exact hi

theorem countP_lt_of_subset {α} (p q : α → Bool) (l : List α)
    (hsub : ∀ x ∈ l, p x = true → q x = true)
    (hstrict : ∃ x ∈ l, q x = true ∧ p x = false) :
    l.countP p < l.countP q := by
  induction l with
  | nil => simp at hstrict
  | cons x xs ih =>
    simp only [List.countP_cons]
    have hsub' : ∀ y ∈ xs, p y = true → q y = true := by
      intro y hy hpy
      exact hsub y (by simp [hy]) hpy
    have hle : xs.countP p ≤ xs.countP q := countP_le_of_subset p q xs hsub'
    rcases hstrict with ⟨y, hy, hqy, hpy⟩
    by_cases hyx : y = x
    · subst y
      have hpx' : p x = false := hpy
      have hqx' : q x = true := hqy
      simp [hpx', hqx']
      omega
    · have hyxs : y ∈ xs := by simpa [hyx] using hy
      have hi := ih hsub' ⟨y, hyxs, hqy, hpy⟩
      by_cases hpx : p x = true
      · have hqx : q x = true := hsub x (by simp) hpx
        simp [hpx, hqx]
        omega
      · by_cases hqx : q x = true
        · simp [hpx, hqx]
          omega
        · simp [hpx, hqx]
          exact hi

theorem find_compact_some_mem {xs : List Row} {n : Nat}
    (hf : xs.find? (fun x => (compact x == (some n, none))) = some r) :
    r.1 = some n ∧ r ∈ xs := by
  constructor
  · have ht : (compact r == (some n, none)) = true :=
      List.find?_some (p := fun x : Row => (compact x == (some n, none))) (a := r) (l := xs) hf
    have ht' : (r.1 == some n) = true := by simpa [compact_beq_eq_fst_beq] using ht
    exact beq_iff_eq.mp ht'
  · exact List.mem_of_find?_eq_some hf

theorem countP_compact_some (xs : List Row) (n : Nat) :
    xs.countP (fun x => (compact x == (some n, none))) = xs.countP (fun x => (x.1 == some n)) := by
  apply List.countP_congr
  intro x hx
  simp [compact_beq_eq_fst_beq]


def resolve (xs : List Row) (k : Row) : Row :=
  (xs.find? (fun x => (compact x == k))).getD k

def fCompact (xs : List Row) (k : Row) : Option (Row × Nat) :=
  do let r ← xs.find? (fun x => (compact x == k))
     return (r, xs.countP (fun x => (compact x == k)))

def fId (xs : List Row) (r : Row) : Option (Row × Nat) :=
  do let rep ← xs.find? (fun x => (x == r))
     return (rep, xs.countP (fun x => (x == r)))

theorem eraseDups_nodup_general {α} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
  induction h : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
    cases l with
    | nil => simp
    | cons x xs =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · simp [List.mem_eraseDups]
      · apply ih (List.filter (fun b => (!b == x)) xs).length
        · simp only [List.length_cons] at h
          have hle : (List.filter (fun b => (!b == x)) xs).length ≤ xs.length := List.length_filter_le _ _
          omega
        · rfl

theorem nodup_map_of_inj {α β} (l : List α) (f : α → β) (hnd : l.Nodup)
    (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    simp only [List.map_cons, List.nodup_cons]
    constructor
    · intro hxmap
      rcases List.mem_map.mp hxmap with ⟨y, hy, hyf⟩
      have hxy : y = x := hinj y (by simp [hy]) x (by simp) hyf
      subst y
      exact (List.nodup_cons.mp hnd).1 hy
    · apply ih (List.nodup_cons.mp hnd).2
      intro a ha b hb hab
      exact hinj a (by simp [ha]) b (by simp [hb]) hab

theorem find_compact_eq_some {xs : List Row} {k : Row}
    (hk : k ∈ (xs.map compact).eraseDups) :
    ∃ r ∈ xs, compact r = k ∧ xs.find? (fun x => (compact x == k)) = some r := by
  have hkmap : k ∈ xs.map compact := List.mem_eraseDups.mp hk
  rcases List.mem_map.mp hkmap with ⟨x, hx, hxk⟩
  cases hf : xs.find? (fun y => (compact y == k)) with
  | none =>
    have hn := (List.find?_eq_none).mp hf x hx
    have ht : (compact x == k) = true := by rw [hxk]; simp
    exact False.elim (hn ht)
  | some r =>
    refine ⟨r, List.mem_of_find?_eq_some hf, ?_, ?_⟩
    · have ht : (compact r == k) = true :=
        List.find?_some (p := fun y : Row => (compact y == k)) (a := r) (l := xs) hf
      exact beq_iff_eq.mp ht
    · rfl

theorem resolve_of_mem {xs : List Row} {k : Row}
    (hk : k ∈ (xs.map compact).eraseDups) :
    resolve xs k ∈ xs ∧ compact (resolve xs k) = k ∧
      xs.find? (fun x => (compact x == k)) = some (resolve xs k) := by
  rcases find_compact_eq_some hk with ⟨r, hrs, hcr, hf⟩
  have hres : resolve xs k = r := by simp [resolve, hf]
  subst hres
  exact ⟨hrs, hcr, hf⟩

theorem resolve_inj_on (xs : List Row) {k1 k2 : Row}
    (hk1 : k1 ∈ (xs.map compact).eraseDups) (hk2 : k2 ∈ (xs.map compact).eraseDups)
    (h : resolve xs k1 = resolve xs k2) : k1 = k2 := by
  have h1 := resolve_of_mem hk1
  have h2 := resolve_of_mem hk2
  have hc1 : compact (resolve xs k1) = k1 := h1.2.1
  have hc2 : compact (resolve xs k2) = k2 := h2.2.1
  rw [← hc1, ← hc2, h]

theorem find_self_eq_some {xs : List Row} {r : Row} (hr : r ∈ xs) :
    xs.find? (fun x => (x == r)) = some r := by
  cases hf : xs.find? (fun x => (x == r)) with
  | none =>
    have hn := (List.find?_eq_none).mp hf r hr
    exact False.elim (hn (by simp))
  | some x =>
    have ht : (x == r) = true :=
      List.find?_some (p := fun y : Row => (y == r)) (a := x) (l := xs) hf
    have hxr : x = r := beq_iff_eq.mp ht
    subst x
    rfl

theorem compact_eq_compact_imp_eq {xs : List Row} (hdep :
    ∀ a ∈ xs, ∀ b ∈ xs, a.1.isSome → a.1 = b.1 → a.2 = b.2)
    {r s : Row} (hr : r ∈ xs) (hs : s ∈ xs) (hc : compact r = compact s) : r = s := by
  rcases r with ⟨rf, rs⟩
  rcases s with ⟨sf, ss⟩
  cases rf with
  | none =>
    cases sf with
    | none =>
      simp [compact] at hc
      simp [hc]
    | some n =>
      simp [compact] at hc
  | some n =>
    cases sf with
    | none =>
      simp [compact] at hc
    | some m =>
      simp [compact] at hc
      subst m
      have his : (some n : Option Nat).isSome = true := by simp
      have h2 : rs = ss := hdep (some n, rs) (by simpa using hr) (some n, ss) (by simpa using hs) his rfl
      simp [h2]
theorem resolve_compact_self {xs : List Row} (hdep :
    ∀ a ∈ xs, ∀ b ∈ xs, a.1.isSome → a.1 = b.1 → a.2 = b.2)
    {r : Row} (hr : r ∈ xs) : resolve xs (compact r) = r := by
  have hk : compact r ∈ (xs.map compact).eraseDups := by
    rw [List.mem_eraseDups]
    exact List.mem_map.mpr ⟨r, hr, rfl⟩
  have h1 := resolve_of_mem hk
  have hrep : resolve xs (compact r) ∈ xs := h1.1
  have hceq : compact (resolve xs (compact r)) = compact r := h1.2.1
  exact compact_eq_compact_imp_eq hdep hrep hr hceq

theorem filterMap_congr {α β} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.filterMap_cons]
    rw [h x (by simp), ih (by intro y hy; exact h y (by simp [hy]))]

theorem entries_eq {xs : List Row} (hdep :
    ∀ a ∈ xs, ∀ b ∈ xs, a.1.isSome → a.1 = b.1 → a.2 = b.2)
    {k : Row} (hk : k ∈ (xs.map compact).eraseDups) :
    fCompact xs k = fId xs (resolve xs k) := by
  have h1 := resolve_of_mem hk
  let r := resolve xs k
  have hrs : r ∈ xs := h1.1
  have hcr : compact r = k := h1.2.1
  have hf : xs.find? (fun x => (compact x == k)) = some r := h1.2.2
  have hcnt : xs.countP (fun x => (compact x == k)) = xs.countP (fun x => (x == r)) := by
    apply List.countP_congr
    intro x hx
    constructor
    · intro hxtrue
      have hxek : compact x = k := beq_iff_eq.mp hxtrue
      have hxr : x = r := compact_eq_compact_imp_eq hdep hx hrs (by rw [hxek, hcr])
      rw [hxr]
      simp
    · intro hxtrue
      have hxr : x = r := beq_iff_eq.mp hxtrue
      rw [hxr, hcr]
      simp
  have hself : xs.find? (fun x => (x == r)) = some r := find_self_eq_some hrs
  dsimp [fCompact, fId]
  rw [hf, hself, hcnt]

theorem key_list_perm {xs : List Row} (hdep :
    ∀ a ∈ xs, ∀ b ∈ xs, a.1.isSome → a.1 = b.1 → a.2 = b.2) :
    ((xs.map compact).eraseDups.map (resolve xs)).Perm xs.eraseDups := by
  let kc := (xs.map compact).eraseDups
  have hndc : kc.Nodup := eraseDups_nodup_general (xs.map compact)
  have hndmap : (kc.map (resolve xs)).Nodup :=
    nodup_map_of_inj kc (resolve xs) hndc (by
      intro a ha b hb hab
      exact resolve_inj_on xs ha hb hab)
  have hndi : xs.eraseDups.Nodup := eraseDups_nodup_general xs
  rw [List.perm_iff_count]
  intro r
  rw [List.nodup_iff_count_eq_ite.mp hndmap r, List.nodup_iff_count_eq_ite.mp hndi r]
  have hmem : r ∈ kc.map (resolve xs) ↔ r ∈ xs.eraseDups := by
    constructor
    · intro hrmap
      rcases List.mem_map.mp hrmap with ⟨k, hk, hkr⟩
      have hrs : resolve xs k ∈ xs := (resolve_of_mem hk).1
      rw [hkr] at hrs
      exact List.mem_eraseDups.mpr hrs
    · intro hri
      have hrs : r ∈ xs := List.mem_eraseDups.mp hri
      have hk : compact r ∈ kc := by
        dsimp [kc]
        rw [List.mem_eraseDups]
        exact List.mem_map.mpr ⟨r, hrs, rfl⟩
      refine List.mem_map.mpr ⟨compact r, hk, ?_⟩
      exact resolve_compact_self hdep hrs
  simp [hmem]

theorem sound : Goal := by
  intro xs
  constructor
  · intro hperm
    intro a ha b hb hasome hfst
    by_cases hneq : a.2 = b.2
    · exact hneq
    · cases ha1 : a.1 with
    | none => simp [ha1] at hasome
    | some n =>
      have hb1 : b.1 = some n := by rw [← hfst, ha1]
      have hk : (some n, none) ∈ (xs.map compact).eraseDups := by
        rw [List.mem_eraseDups]
        refine List.mem_map.mpr ⟨a, ha, ?_⟩
        simp [compact, ha1]
      have hex : ∃ r, xs.find? (fun x => (compact x == (some n, none))) = some r := by
        cases hf : xs.find? (fun x => (compact x == (some n, none))) with
        | none =>
          have hn := (List.find?_eq_none).mp hf a ha
          have ht : (compact a == (some n, none)) = true := by simp [compact, ha1]
          exact False.elim (hn ht)
        | some r => exact ⟨r, rfl⟩
      rcases hex with ⟨r, hfr⟩
      have hr1 : r.1 = some n := (find_compact_some_mem hfr).1
      have hrxs : r ∈ xs := (find_compact_some_mem hfr).2
      have hcEnt : (r, xs.countP (fun x : Row => (x.1 == some n))) ∈ groups xs compact := by
        rw [mem_groups]
        refine ⟨(some n, none), hk, hfr, ?_⟩
        exact countP_compact_some xs n
      have hcId : (r, xs.countP (fun x : Row => (x.1 == some n))) ∈ groups xs id :=
        hperm.mem_iff.mp hcEnt
      have hcEq := id_group_count hcId
      have hd : ∃ d ∈ xs, d.1 = some n ∧ d ≠ r := by
        by_cases har : a = r
        · refine ⟨b, hb, hb1, ?_⟩
          intro hbr
          apply hneq
          calc
            a.2 = r.2 := by rw [har]
            _ = b.2 := by rw [hbr]
        · exact ⟨a, ha, ha1, har⟩
      rcases hd with ⟨d, hdxs, hd1, hdr⟩
      have hlt := countP_lt_of_subset
        (fun x : Row => (x == r)) (fun x : Row => (x.1 == some n)) xs
        (by
          intro x hx hxr
          have hxeq : x = r := beq_iff_eq.mp hxr
          rw [hxeq, hr1]
          simp)
        (by
          refine ⟨d, hdxs, ?_, ?_⟩
          · rw [hd1]
            simp
          · cases hbeq : (d == r)
            · rfl
            · exact False.elim (hdr (beq_iff_eq.mp hbeq)))
      omega
  · intro hdep
    let kc := (xs.map compact).eraseDups
    let ki := xs.eraseDups
    have hfEnt : ∀ k ∈ kc, fCompact xs k = fId xs (resolve xs k) := by
      intro k hk
      exact entries_eq hdep hk
    have hkc_eq : kc.filterMap (fCompact xs) =
        kc.filterMap (fun k => fId xs (resolve xs k)) :=
      filterMap_congr hfEnt
    have hperm_map : (kc.filterMap (fun k => fId xs (resolve xs k))).Perm
        (ki.filterMap (fId xs)) := by
      have hp := (key_list_perm hdep).filterMap (fId xs)
      simpa [kc, ki, List.filterMap_map, Function.comp_def] using hp
    have hgroupsC : groups xs compact = kc.filterMap (fCompact xs) := by
      rfl
    have hgroupsI : groups xs id = ki.filterMap (fId xs) := by
      dsimp [groups, fId, ki]
      rw [List.map_id]
      rfl
    rw [hgroupsC, hgroupsI, hkc_eq]
    exact hperm_map

end TinylambOpt.ResearchProofs.EngineDecisions.Opt_NullableGroupKey
