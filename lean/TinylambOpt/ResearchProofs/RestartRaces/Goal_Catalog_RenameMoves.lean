import TinylambOpt.ResearchGoals.RestartRaces.Catalog

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.

Proved

- `RenameMoves` — A batch requires exactly one move per changed name plus one extra per
  nontrivial permutation cycle. Can every permutation attain that lower bound with
  just one temporary name, including multiple independent rename cycles?
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Catalog_RenameMoves
open TinylambOpt.ResearchGoals.RestartRaces.Catalog.RenameMoves


private def iter (f : Nat → Nat) : Nat → Nat → Nat
  | 0, x => x
  | k+1, x => iter f k (f x)

private theorem iter_succ' (f : Nat → Nat) : ∀ k x, iter f (k+1) x = f (iter f k x)
  | 0, x => by simp [iter]
  | k+1, x => by simpa [iter] using iter_succ' f k (f x)

private theorem iter_add (f : Nat → Nat) : ∀ a b x, iter f (a+b) x = iter f a (iter f b x)
  | 0, b, x => by simp [iter]
  | a+1, b, x => by
      rw [show a+1+b = (a+b)+1 by omega]
      change iter f (a+b) (f x) = iter f a (f (iter f b x))
      rw [iter_add f a b (f x)]
      rw [← iter_succ' f b x]
      rfl

private theorem not_nodup_pair {l : List Nat} (h : ¬ l.Nodup) :
    ∃ i j, i < j ∧ j < l.length ∧ l[i]? = l[j]? := by
  induction l with
  | nil => simp at h
  | cons a as ih =>
      by_cases ha : a ∈ as
      · let k := as.idxOf a
        have hklt : k < as.length := List.idxOf_lt_length_of_mem ha
        have hget : as[k]? = some a := by rw [List.getElem?_eq_getElem hklt, List.getElem_idxOf hklt]
        refine ⟨0, k+1, by omega, by simpa using Nat.succ_lt_succ hklt, ?_⟩
        simp [List.getElem?_cons_succ, hget]
      · have htail : ¬ as.Nodup := by intro hn; apply h; simpa [ha] using hn
        obtain ⟨i, j, hij, hj, heq⟩ := ih htail
        refine ⟨i+1, j+1, by omega, by simpa using Nat.succ_lt_succ hj, ?_⟩
        simpa [List.getElem?_cons_succ] using heq

private theorem nodup_length_le_of_lt {l : List Nat} {n : Nat}
    (hnod : l.Nodup) (hall : ∀ x ∈ l, x < n) : l.length ≤ n := by
  have hsub : l ⊆ List.range n := by intro x hx; simp [hall x hx]
  simpa using (List.Nodup.length_le_of_subset (l₁ := l) (l₂ := List.range n) hnod hsub)

private theorem cancel_iter (f : Nat → Nat) {n i : Nat}
    (hinj : ∀ {x y}, x < n → y < n → f x = f y → x = y)
    (hmem : ∀ k, iter f k i < n) :
    ∀ a b, a ≤ b → iter f a i = iter f b i → iter f (b - a) i = i := by
  intro a
  induction a with
  | zero =>
      intro b hle heq
      simpa [iter] using heq.symm
  | succ a ih =>
      intro b hle heq
      have hbpos : 0 < b := by omega
      have hb : b = (b - 1) + 1 := by omega
      have heq1 : iter f (a+1) i = iter f ((b-1)+1) i := by
        have hh := heq
        rw [hb] at hh
        exact hh
      have heq' : f (iter f a i) = f (iter f (b-1) i) := by
        simpa [iter_succ'] using heq1
      have hia : iter f a i < n := hmem a
      have hib : iter f (b-1) i < n := hmem (b-1)
      have heq'' : iter f a i = iter f (b-1) i := hinj hia hib heq'
      have hres := ih (b-1) (by omega) heq''
      have hsub : b - (a+1) = (b-1) - a := by omega
      rw [hsub]
      exact hres

private theorem exists_period (f : Nat → Nat) {n i : Nat}
    (hinj : ∀ {x y}, x < n → y < n → f x = f y → x = y)
    (hmem : ∀ k, iter f k i < n) :
    ∃ L, 0 < L ∧ L ≤ n ∧ iter f L i = i := by
  let vals := (List.range (n+1)).map (fun k => iter f k i)
  have hlen : vals.length = n+1 := by simp [vals]
  have hall : ∀ x ∈ vals, x < n := by
    intro x hx
    rcases List.mem_map.mp hx with ⟨k, hk, rfl⟩
    exact hmem k
  have hnot : ¬ vals.Nodup := by
    intro hnod
    have hle := nodup_length_le_of_lt hnod hall
    omega
  obtain ⟨a, b, hab, hb, heq⟩ := not_nodup_pair hnot
  have ha : a < n+1 := by omega
  have hb' : b < n+1 := by simpa [hlen] using hb
  have hga : vals[a]? = some (iter f a i) := by
    rw [List.getElem?_eq_getElem (by omega)]
    dsimp [vals]
    rw [List.getElem_map]
    rw [List.getElem_range (by simpa using ha)]
  have hgb : vals[b]? = some (iter f b i) := by
    rw [List.getElem?_eq_getElem (by omega)]
    dsimp [vals]
    rw [List.getElem_map]
    rw [List.getElem_range (by simpa using hb')]
  have habEq : iter f a i = iter f b i := by
    have hs : some (iter f a i) = some (iter f b i) := hga.symm.trans (heq.trans hgb)
    simpa using hs
  refine ⟨b-a, by omega, by omega, ?_⟩
  exact cancel_iter f hinj hmem a b (by omega) habEq

private theorem iter_period (f : Nat → Nat) {L i : Nat} (hL : iter f L i = i) :
    ∀ a, iter f (a + L) i = iter f a i := by
  intro a
  rw [iter_add, hL]

private theorem iter_mod (f : Nat → Nat) {L i : Nat} (hLpos : 0 < L) (hL : iter f L i = i) :
    ∀ a, iter f a i = iter f (a % L) i := by
  intro a
  induction a using Nat.strongRecOn with
  | ind a ih =>
      by_cases ha : a < L
      · rw [Nat.mod_eq_of_lt ha]
      · have hge : L ≤ a := by omega
        have hsub : a - L < a := by omega
        have hrec := ih (a-L) hsub
        have hp := iter_period f hL (a-L)
        have heq : a = (a-L) + L := by omega
        rw [heq, hp]
        have hm : ((a-L)+L) % L = (a-L) % L := by
          rw [Nat.add_mod, Nat.mod_self, Nat.add_zero, Nat.mod_eq_of_lt (Nat.mod_lt _ hLpos)]
        rw [hm]
        exact hrec

private theorem orbit_length (p : List Nat) : ∀ m i, (orbit p m i).length = m
  | 0, i => rfl
  | m+1, i => by simp [orbit, orbit_length p m]

private def step (p : List Nat) (i : Nat) : Nat := p[i]?.getD i

private theorem step_eq_of_lt (p : List Nat) {i : Nat} (hi : i < p.length) : step p i = p[i] := by
  simp [step, List.getElem?_eq_getElem hi]

private theorem p_lt_of_perm {p : List Nat} (hp : p.Perm (List.range p.length)) {i : Nat}
    (hi : i < p.length) : p[i] < p.length := by
  have hm : p[i] ∈ p := List.getElem_mem hi
  have hm' : p[i] ∈ List.range p.length := hp.subset hm
  simpa using hm'

private theorem step_lt {p : List Nat} (hp : p.Perm (List.range p.length)) {i : Nat}
    (hi : i < p.length) : step p i < p.length := by
  rw [step_eq_of_lt p hi]
  exact p_lt_of_perm hp hi

private theorem step_inj {p : List Nat} (hp : p.Perm (List.range p.length)) {a b : Nat}
    (ha : a < p.length) (hb : b < p.length) (h : step p a = step p b) : a = b := by
  have hn : p.Nodup := (List.nodup_range (n := p.length)).perm hp.symm
  rw [step_eq_of_lt p ha, step_eq_of_lt p hb] at h
  exact (hn.getElem_inj.mp h)

private theorem iter_lt {p : List Nat} (hp : p.Perm (List.range p.length)) {i : Nat}
    (hi : i < p.length) : ∀ k, iter (step p) k i < p.length := by
  intro k
  induction k with
  | zero => simpa [iter] using hi
  | succ k ih =>
      rw [iter_succ']
      exact step_lt hp ih

private theorem orbit_get (p : List Nat) : ∀ (m i k : Nat), k < m →
    (orbit p m i)[k]? = some (iter (step p) k i)
  | 0, i, k, hk => by omega
  | m+1, i, 0, hk => by simp [orbit, iter]
  | m+1, i, k+1, hk => by
      change (i :: orbit p m (step p i))[k+1]? = some (iter (step p) (k+1) i)
      rw [List.getElem?_cons_succ]
      simpa [iter] using orbit_get p m (step p i) k (by omega)

private theorem mem_orbit_iff_iter {p : List Nat} (hp : p.Perm (List.range p.length)) {i j : Nat}
    (hi : i < p.length) : j ∈ orbit p p.length i ↔ ∃ k < p.length, iter (step p) k i = j := by
  constructor
  · intro hj
    rcases List.mem_iff_get.mp hj with ⟨k, hk⟩
    let idx := k.val
    have hlen : (orbit p p.length i).length = p.length := orbit_length p _ _
    have hklt : idx < p.length := by
      dsimp [idx]
      omega
    have hget : (orbit p p.length i)[idx]? = some j := by
      change (orbit p p.length i)[k.val]? = some j
      rw [List.getElem?_eq_getElem (by omega)]
      simpa using hk
    rw [orbit_get p p.length i idx hklt] at hget
    injection hget with hEq
    exact ⟨idx, hklt, hEq⟩
  · rintro ⟨k, hk, hEq⟩
    rw [← hEq]
    have hget := orbit_get p p.length i k (by simpa using hk)
    rcases List.getElem?_eq_some_iff.mp hget with ⟨hvalid, hval⟩
    rw [← hval]
    exact List.getElem_mem hvalid

private theorem iter_iter (f : Nat → Nat) (a b x : Nat) : iter f a (iter f b x) = iter f (a+b) x :=
  (iter_add f a b x).symm

private theorem orbit_min_eq_of_mem {p : List Nat} (hp : p.Perm (List.range p.length)) {i j : Nat}
    (hi : i < p.length) (hj : j ∈ orbit p p.length i) :
    (orbit p p.length i).min? = (orbit p p.length j).min? := by
  let f := step p
  rcases mem_orbit_iff_iter hp hi |>.1 hj with ⟨k, hk, hkEq⟩
  have hjlt : j < p.length := by
    rw [← hkEq]
    exact iter_lt hp hi k
  obtain ⟨L, hLpos, hLle, hLper⟩ := exists_period f
    (fun {a b} ha hb h => step_inj hp ha hb h) (iter_lt hp hi)
  have hmod : ∀ a, iter f a i = iter f (a % L) i := iter_mod f hLpos hLper
  have hnonempty : orbit p p.length i ≠ [] := by
    intro he
    have hlen0 := congrArg List.length he
    have hp0 : p.length = 0 := by simpa [orbit_length] using hlen0
    omega
  obtain ⟨m, hminI⟩ : ∃ m, (orbit p p.length i).min? = some m := by
    cases hmn : (orbit p p.length i).min? with
    | none =>
        have : orbit p p.length i = [] := by
          simpa using hmn
        exact False.elim (hnonempty this)
    | some m => exact ⟨m, rfl⟩
  have hmMin := List.min?_eq_some_iff.mp hminI
  rcases mem_orbit_iff_iter hp hi |>.1 hmMin.1 with ⟨t, ht, htEq⟩
  let kmod := k % L
  let tmod := t % L
  let s := (tmod + L - kmod) % L
  have hkmodlt : kmod < L := by exact Nat.mod_lt k hLpos
  have htmodlt : tmod < L := by exact Nat.mod_lt t hLpos
  have hsL : s < L := by exact Nat.mod_lt _ hLpos
  have hsn : s < p.length := by omega
  have hAadd : (tmod + L - kmod) + kmod = tmod + L := by omega
  have hAmod0 : ((tmod + L - kmod) % L + kmod) % L = tmod := by
    have hh : ((tmod + L - kmod) + kmod) % L = tmod := by
      rw [hAadd, Nat.add_mod, Nat.mod_self, Nat.add_zero]
      simp [Nat.mod_eq_of_lt htmodlt]
    rw [Nat.add_mod] at hh
    simpa [Nat.mod_eq_of_lt hkmodlt] using hh
  have hAmod : (s + kmod) % L = tmod := by
    simpa [s] using hAmod0
  have hsjk : iter f s j = m := by
    rw [← hkEq]
    rw [iter_iter]
    rw [hmod (s + k)]
    rw [hmod t] at htEq
    have hres : (s + k) % L = tmod := by
      rw [Nat.add_mod, Nat.mod_eq_of_lt hsL]
      simpa [kmod] using hAmod
    rw [hres]
    simpa [tmod] using htEq
  have hle_of_mem : ∀ x ∈ orbit p p.length j, m ≤ x := by
    intro x hx
    rcases mem_orbit_iff_iter hp hjlt |>.1 hx with ⟨u, hu, huEq⟩
    have hxmem : x ∈ orbit p p.length i := by
      apply mem_orbit_iff_iter hp hi |>.2
      refine ⟨(u + k) % L, ?_, ?_⟩
      · have hlt : (u + k) % L < L := Nat.mod_lt _ hLpos
        omega
      · rw [← huEq]
        rw [← hkEq]
        rw [iter_iter]
        rw [hmod (u + k)]
    exact hmMin.2 x hxmem
  have hminJ : (orbit p p.length j).min? = some m := by
    apply List.min?_eq_some_iff.mpr
    exact ⟨by rw [← hsjk]; apply mem_orbit_iff_iter hp hjlt |>.2; exact ⟨s, hsn, rfl⟩, hle_of_mem⟩
  rw [hminI, hminJ]



private theorem move_spec (s : List (Option Nat)) (m : Move) (s' : List (Option Nat))
    (h : move s m = some s') :
    ∃ id, s[m.1]? = some (some id) ∧ s[m.2]? = some none ∧
      s' = (s.set m.1 none).set m.2 (some id) := by
  unfold move at h
  cases hsrc : s[m.1]? with
  | none =>
      rw [hsrc] at h
      simp [guard, Alternative.failure] at h
  | some o =>
      cases o with
      | none =>
          rw [hsrc] at h
          simp [guard, Alternative.failure] at h
      | some id =>
          rw [hsrc] at h
          by_cases hg : s[m.2]? = some none
          · simp [guard, hg] at h
            exact ⟨id, by simpa using hsrc, hg, h.symm⟩
          · simp [guard, hg, Alternative.failure] at h

private def oneBlank (z : Nat) (s : List (Option Nat)) : Prop :=
  s[z]? = some none ∧ ∀ i, i < s.length → s[i]? = some none → i = z

private theorem oneBlank_start (n : Nat) : oneBlank n ((List.range n).map some ++ [none]) := by
  constructor
  · simp [List.getElem?_append]
  · intro i hi hnone
    by_cases hil : i < n
    · have hget : ((List.range n).map some ++ [none])[i]? = some (some i) := by
        rw [List.getElem?_append_left (by simpa using hil)]
        rw [List.getElem?_map, List.getElem?_eq_getElem (by simpa using hil)]
        rw [List.getElem_range (by simpa using hil)]
        rfl
      simp [hget] at hnone
    · have hlen : ((List.range n).map some ++ [none]).length = n+1 := by simp
      omega

private theorem set_get_other {α : Type} (l : List α) (i j : Nat) (a : α) (h : i ≠ j) :
    (l.set i a)[j]? = l[j]? := by
  rw [List.getElem?_set]
  simp [h]

private theorem set_get_self {α : Type} (l : List α) (i : Nat) (a : α) (h : i < l.length) :
    (l.set i a)[i]? = some a := by
  rw [List.getElem?_set]
  simp [h]

private theorem move_preserves_blank {s : List (Option Nat)} {z : Nat} {m : Move}
    {s' : List (Option Nat)} (hz : oneBlank z s) (h : move s m = some s') :
    m.2 = z ∧ oneBlank m.1 s' := by
  obtain ⟨id, hsrc, htgt, hs'⟩ := move_spec s m s' h
  have hm2lt : m.2 < s.length := (List.getElem?_eq_some_iff.mp htgt).1
  have hz' : m.2 = z := hz.2 m.2 hm2lt htgt
  subst z
  have hm1lt : m.1 < s.length := (List.getElem?_eq_some_iff.mp hsrc).1
  have hne : m.1 ≠ m.2 := by
    intro he
    rw [← he] at htgt
    simp [hsrc] at htgt
  constructor
  · rfl
  · subst s'
    have hlen : ((s.set m.1 none).set m.2 (some id)).length = s.length := by simp
    constructor
    · rw [set_get_other _ m.2 m.1 (some id) hne.symm]
      rw [set_get_self s m.1 none hm1lt]
    · intro i hi hni
      by_cases him1 : i = m.1
      · exact him1
      · by_cases him2 : i = m.2
        · subst i
          rw [set_get_self (s.set m.1 none) m.2 (some id) (by simpa using hm2lt)] at hni
          simp at hni
        · rw [set_get_other (s.set m.1 none) m.2 i (some id) (fun h => him2 h.symm)] at hni
          rw [set_get_other s m.1 i none (fun h => him1 h.symm)] at hni
          have hi' : i < s.length := by omega
          have hzm : i = m.2 := hz.2 i hi' hni
          exact False.elim (him2 hzm)

private def run (s : List (Option Nat)) : List Move → Option (List (Option Nat))
  | [] => some s
  | m :: ms => match move s m with
      | none => none
      | some s' => run s' ms

private theorem foldlM_eq_run (s : List (Option Nat)) (ms : List Move) :
    ms.foldlM move s = run s ms := by
  induction ms generalizing s with
  | nil => rfl
  | cons m ms ih =>
      cases hm : move s m with
      | none => simp [List.foldlM, run, hm]
      | some s' => simpa [List.foldlM, run, hm] using ih s'

private theorem run_append (s : List (Option Nat)) (ms₁ ms₂ : List Move) :
    run s (ms₁ ++ ms₂) = (run s ms₁).bind (fun s' => run s' ms₂) := by
  induction ms₁ generalizing s with
  | nil => rfl
  | cons m ms ih =>
      cases hm : move s m with
      | none => simp [run, hm]
      | some s' => simpa [run, hm] using ih s'

private theorem run_of_run_append {s sF : List (Option Nat)} {ms₁ ms₂ : List Move}
    (h : run s (ms₁ ++ ms₂) = some sF) :
    ∃ sM, run s ms₁ = some sM ∧ run sM ms₂ = some sF := by
  rw [run_append] at h
  cases hrun : run s ms₁ with
  | none => simp [hrun] at h
  | some sM => exact ⟨sM, rfl, by simpa [hrun] using h⟩

private theorem targets_of_run {s0 sF : List (Option Nat)} {z0 : Nat} {ms : List Move}
    (hz : oneBlank z0 s0) (h : run s0 ms = some sF) :
    ∀ i, i < ms.length →
      (z0 :: ms.map Prod.fst)[i]? = (ms[i]?).map Prod.snd := by
  induction ms generalizing s0 z0 with
  | nil => intro i hi; simp at hi
  | cons m ms ih =>
      cases hm : move s0 m with
      | none => simp [run, hm] at h
      | some s1 =>
          have hblank := move_preserves_blank hz hm
          have hm2 : m.2 = z0 := hblank.1
          have hz1 : oneBlank m.1 s1 := hblank.2
          have htail : run s1 ms = some sF := by simpa [run, hm] using h
          intro i hi
          cases i with
          | zero => simp [hm2]
          | succ i =>
              have hi' : i < ms.length := by simpa using hi
              have hrec := ih (s0 := s1) (z0 := m.1) hz1 htail i hi'
              simpa using hrec

private theorem length_run {s sF : List (Option Nat)} {ms : List Move}
    (h : run s ms = some sF) : sF.length = s.length := by
  induction ms generalizing s with
  | nil =>
      have hs : s = sF := by simpa [run] using h
      subst sF
      rfl
  | cons m ms ih =>
      cases hm : move s m with
      | none => simp [run, hm] at h
      | some s1 =>
          have h1 : s1.length = s.length := by
            obtain ⟨id, hsrc, htgt, hs1⟩ := move_spec s m s1 hm
            subst s1
            simp
          have h2 := ih (s := s1) (by simpa [run, hm] using h)
          omega

private theorem move_untouched {s : List (Option Nat)} {m : Move} {s' : List (Option Nat)} {j : Nat}
    (h : move s m = some s') (h1 : m.1 ≠ j) (h2 : m.2 ≠ j) : s'[j]? = s[j]? := by
  obtain ⟨id, hsrc, htgt, hs'⟩ := move_spec s m s' h
  subst s'
  rw [set_get_other _ m.2 j (some id) h2]
  rw [set_get_other _ m.1 j none h1]

private theorem run_untouched {s sF : List (Option Nat)} {ms : List Move} {j : Nat}
    (h : run s ms = some sF) (hno : ∀ m ∈ ms, m.1 ≠ j ∧ m.2 ≠ j) : sF[j]? = s[j]? := by
  induction ms generalizing s with
  | nil =>
      have hs : s = sF := by simpa [run] using h
      subst sF
      rfl
  | cons m ms ih =>
      cases hm : move s m with
      | none => simp [run, hm] at h
      | some s1 =>
          have hmno := hno m (by simp)
          have ht := move_untouched hm hmno.1 hmno.2
          have htail := ih (s := s1) (by simpa [run, hm] using h)
            (fun q hq => hno q (by simp [hq]))
          rw [← ht, htail]

private def touch (j : Nat) (m : Move) : Prop := m.1 = j ∨ m.2 = j

private theorem split_last_touch (ms : List Move) (j : Nat) (hex : ∃ m ∈ ms, touch j m) :
    ∃ pre m post, ms = pre ++ m :: post ∧ touch j m ∧ ∀ q ∈ post, ¬ touch j q := by
  induction ms with
  | nil => simp at hex
  | cons a as ih =>
      by_cases hpost : ∃ q ∈ as, touch j q
      · obtain ⟨pre, m, post, hms, ht, hno⟩ := ih hpost
        exact ⟨a :: pre, m, post, by simp [hms], ht, hno⟩
      · have ht : touch j a := by
          rcases hex with ⟨m, hm, htm⟩
          simp only [List.mem_cons] at hm
          rcases hm with hEq | hmem
          · subst m; exact htm
          · exact False.elim (hpost ⟨m, hmem, htm⟩)
        exact ⟨[], a, as, rfl, ht, by intro q hq htq; exact hpost ⟨q, hq, htq⟩⟩

private theorem split_first_source (ms : List Move) (S : Nat → Prop) [DecidablePred S]
    (hex : ∃ m ∈ ms, S m.1) :
    ∃ pre m post, ms = pre ++ m :: post ∧ S m.1 ∧ ∀ q ∈ pre, ¬ S q.1 := by
  induction ms with
  | nil => simp at hex
  | cons a as ih =>
      by_cases hhead : S a.1
      · exact ⟨[], a, as, rfl, hhead, by simp⟩
      · have htail : ∃ q ∈ as, S q.1 := by
          rcases hex with ⟨m, hm, hS⟩
          simp only [List.mem_cons] at hm
          rcases hm with hEq | hmem
          · subst m; exact False.elim (hhead hS)
          · exact ⟨m, hmem, hS⟩
        obtain ⟨pre, m, post, hms, hS, hno⟩ := ih htail
        exact ⟨a :: pre, m, post, by simp [hms], hS, by
          intro q hq
          simp only [List.mem_cons] at hq
          rcases hq with hEq | hmem
          · subst q; exact hhead
          · exact hno q hmem⟩

private theorem blank_out_after {s0 sF : List (Option Nat)} {z0 : Nat} {ms : List Move}
    {S : Nat → Prop} [DecidablePred S]
    (hz : oneBlank z0 s0) (hout : ¬ S z0) (h : run s0 ms = some sF)
    (hno : ∀ m ∈ ms, ¬ S m.1) : ∃ z, oneBlank z sF ∧ ¬ S z := by
  induction ms generalizing s0 z0 with
  | nil =>
      have hs : s0 = sF := by simpa [run] using h
      subst sF
      exact ⟨z0, hz, hout⟩
  | cons m ms ih =>
      cases hm : move s0 m with
      | none => simp [run, hm] at h
      | some s1 =>
          have hb := move_preserves_blank hz hm
          exact ih hb.2 (hno m (by simp)) (by simpa [run, hm] using h)
            (fun q hq => hno q (by simp [hq]))

private theorem no_source_no_target {s0 sF : List (Option Nat)} {z0 m : Nat} {ms : List Move}
    (hz : oneBlank z0 s0) (hzne : z0 ≠ m) (h : run s0 ms = some sF)
    (hno : ∀ q ∈ ms, q.1 ≠ m) : ∀ q ∈ ms, q.2 ≠ m := by
  induction ms generalizing s0 z0 with
  | nil => simp
  | cons q qs ih =>
      cases hmq : move s0 q with
      | none => simp [run, hmq] at h
      | some s1 =>
          have hb := move_preserves_blank hz hmq
          have hq2 : q.2 ≠ m := by rw [hb.1]; exact hzne
          intro r hr
          simp only [List.mem_cons] at hr
          rcases hr with heq | hmem
          · subst r; exact hq2
          · exact ih hb.2 (hno q (by simp)) (by simpa [run, hmq] using h)
              (fun t ht => hno t (by simp [ht])) r hmem

private theorem no_target_pred {s0 sF : List (Option Nat)} {z0 : Nat} {ms : List Move}
    {S : Nat → Prop} [DecidablePred S]
    (hz : oneBlank z0 s0) (hzout : ¬ S z0) (h : run s0 ms = some sF)
    (hno : ∀ q ∈ ms, ¬ S q.1) : ∀ q ∈ ms, ¬ S q.2 := by
  induction ms generalizing s0 z0 with
  | nil => simp
  | cons q qs ih =>
      cases hmq : move s0 q with
      | none => simp [run, hmq] at h
      | some s1 =>
          have hb := move_preserves_blank hz hmq
          have hq2 : ¬ S q.2 := by rw [hb.1]; exact hzout
          intro r hr
          simp only [List.mem_cons] at hr
          rcases hr with heq | hmem
          · subst r; exact hq2
          · exact ih hb.2 (hno q (by simp)) (by simpa [run, hmq] using h)
              (fun t ht => hno t (by simp [ht])) r hmem

private theorem run_untouched_pred {s0 sF : List (Option Nat)} {ms : List Move} {j : Nat}
    {S : Nat → Prop} [DecidablePred S] (hSj : S j)
    (h : run s0 ms = some sF) (hno : ∀ q ∈ ms, ¬ S q.1 ∧ ¬ S q.2) : sF[j]? = s0[j]? := by
  apply run_untouched h
  intro q hq
  have h1 := (hno q hq).1
  have h2 := (hno q hq).2
  constructor
  · intro he; exact h1 (he ▸ hSj)
  · intro he; exact h2 (he ▸ hSj)

private theorem start_slot (n j : Nat) (hj : j < n) :
    (((List.range n).map some ++ [none])[j]? : Option (Option Nat)) = some (some j) := by
  rw [List.getElem?_append_left (by simp [hj])]
  rw [List.getElem?_map, List.getElem?_eq_getElem (by simp [hj])]
  rw [List.getElem_range (by simp [hj])]
  rfl

private theorem finish_slot (p : List Nat) (j : Nat) (hj : j < p.length) :
    ((p.map some ++ [none])[j]? : Option (Option Nat)) = some (some (p.get ⟨j, hj⟩)) := by
  rw [List.getElem?_append_left (by simpa using hj)]
  rw [List.getElem?_map, List.getElem?_eq_getElem (by simpa using hj)]
  rfl

private theorem touch_exists_of_changed_from {j v : Nat} (hv : v ≠ j)
    {s0 sF : List (Option Nat)} {ms : List Move}
    (hs0 : s0[j]? = some (some j)) (hsF : sF[j]? = some (some v))
    (h : run s0 ms = some sF) : ∃ m ∈ ms, touch j m := by
  induction ms generalizing s0 sF with
  | nil =>
      have hsEq : s0 = sF := by simpa [run] using h
      rw [hsEq] at hs0
      rw [hs0] at hsF
      injection hsF with heq
      exact False.elim (hv (by simpa using heq.symm))
  | cons m ms ih =>
      by_cases hm : touch j m
      · exact ⟨m, by simp, hm⟩
      · cases hmv : move s0 m with
        | none => simp [run, hmv] at h
        | some s1 =>
            have hno : m.1 ≠ j ∧ m.2 ≠ j := by
              constructor
              · intro he; exact hm (Or.inl he)
              · intro he; exact hm (Or.inr he)
            have ht := move_untouched hmv hno.1 hno.2
            have hs1 : s1[j]? = some (some j) := by rw [ht, hs0]
            rcases ih hs1 hsF (by simpa [run, hmv] using h) with ⟨q, hq, htq⟩
            exact ⟨q, by simp [hq], htq⟩

private theorem touch_exists_of_changed {p : List Nat} {j : Nat}
    (hjlt : j < p.length) (hjne : p.get ⟨j, hjlt⟩ ≠ j) {ms : List Move}
    (h : run ((List.range p.length).map some ++ [none]) ms = some (p.map some ++ [none])) :
    ∃ m ∈ ms, touch j m := by
  apply touch_exists_of_changed_from (j := j) (v := p.get ⟨j, hjlt⟩) hjne
  · exact start_slot p.length j hjlt
  · rw [finish_slot p j hjlt]
  · exact h

private theorem good_move_spec {p : List Nat} {j : Nat} (hjlt : j < p.length)
    {pre : List Move} {m : Move} {post : List Move}
    {sB sA sF : List (Option Nat)}
    (ht : touch j m) (hno : ∀ q ∈ post, ¬ touch j q)
    (hpre : run ((List.range p.length).map some ++ [none]) pre = some sB)
    (hm : move sB m = some sA)
    (hpost : run sA post = some sF)
    (hsF : sF[j]? = some (some (p.get ⟨j, hjlt⟩))) :
    m.2 = j ∧ m.1 ≠ j ∧ ∃ id, sB[m.1]? = some (some id) ∧ id = p.get ⟨j, hjlt⟩ := by
  have hsA : sA[j]? = sF[j]? := by
    apply (run_untouched hpost ?_).symm
    intro q hq
    constructor
    · intro he; exact hno q hq (Or.inl he)
    · intro he; exact hno q hq (Or.inr he)
  obtain ⟨id, hsrc, htgt, hsAeq⟩ := move_spec sB m sA hm
  have hsBlen : sB.length = p.length + 1 := by
    have h0 : ((List.range p.length).map some ++ [none]).length = p.length + 1 := by simp
    have hlen := length_run hpre
    omega
  have hm1lt : m.1 < sB.length := (List.getElem?_eq_some_iff.mp hsrc).1
  have hm2lt : m.2 < sB.length := (List.getElem?_eq_some_iff.mp htgt).1
  have hne : m.1 ≠ m.2 := by
    intro heq
    rw [← heq] at htgt
    simp [hsrc] at htgt
  have hm2eq : m.2 = j := by
    rcases ht with h1 | h2
    · have heq : m.1 = j := h1
      have hAj : sA[j]? = some none := by
        rw [← heq]
        rw [hsAeq]
        rw [set_get_other _ m.2 m.1 (some id) hne.symm]
        rw [set_get_self sB m.1 none (by simpa using hm1lt)]
      rw [hAj] at hsA
      rw [hsF] at hsA
      simp at hsA
    · exact h2
  have hm1ne : m.1 ≠ j := by
    intro heq
    exact hne (heq.trans hm2eq.symm)
  have hAj : sA[j]? = some (some id) := by
    rw [← hm2eq]
    rw [hsAeq]
    rw [set_get_self (sB.set m.1 none) m.2 (some id) (by simpa using hm2lt)]
  rw [hAj] at hsA
  rw [hsF] at hsA
  injection hsA with hidEq
  injection hidEq with hidEq2
  exact ⟨hm2eq, hm1ne, id, hsrc, by simpa using hidEq2⟩

private theorem mem_orbit_self {p : List Nat} (hp : p.Perm (List.range p.length)) {i : Nat}
    (hi : i < p.length) : i ∈ orbit p p.length i := by
  apply mem_orbit_iff_iter hp hi |>.2
  refine ⟨0, by omega, ?_⟩
  rfl

private theorem mem_orbit_lt {p : List Nat} (hp : p.Perm (List.range p.length)) {i j : Nat}
    (hi : i < p.length) (hj : j ∈ orbit p p.length i) : j < p.length := by
  rcases mem_orbit_iff_iter hp hi |>.1 hj with ⟨k, hk, hEq⟩
  rw [← hEq]
  exact iter_lt hp hi k

private theorem mem_orbit_symm {p : List Nat} (hp : p.Perm (List.range p.length)) {i j : Nat}
    (hi : i < p.length) (hj : j ∈ orbit p p.length i) : i ∈ orbit p p.length j := by
  rcases mem_orbit_iff_iter hp hi |>.1 hj with ⟨k, hk, hkEq⟩
  have hjlt := mem_orbit_lt hp hi hj
  obtain ⟨L, hLpos, hLle, hLper⟩ := exists_period (step p)
    (fun {a b} ha hb h => step_inj hp ha hb h) (iter_lt hp hi)
  have hmod := iter_mod (step p) hLpos hLper
  let kmod := k % L
  let s := (L - kmod) % L
  have hkmodlt : kmod < L := Nat.mod_lt k hLpos
  have hsL : s < L := Nat.mod_lt _ hLpos
  have hsn : s < p.length := by omega
  apply mem_orbit_iff_iter hp hjlt |>.2
  refine ⟨s, hsn, ?_⟩
  rw [← hkEq]
  rw [iter_iter]
  rw [hmod (s + k)]
  have hres : (s + k) % L = 0 := by
    rw [Nat.add_mod, Nat.mod_eq_of_lt hsL]
    have hh : (L - kmod) + kmod = L := by omega
    have hhm : ((L - kmod) % L + kmod % L) % L = 0 := by
      rw [← Nat.add_mod, hh, Nat.mod_self]
    simpa [s, kmod, Nat.mod_eq_of_lt hkmodlt] using hhm
  rw [hres]
  simpa [iter] using hmod 0

private theorem orbit_not_contain_n {p : List Nat} (hp : p.Perm (List.range p.length)) {m : Nat}
    (hm : m < p.length) : p.length ∉ orbit p p.length m := by
  intro h
  have hlt := mem_orbit_lt hp hm h
  omega

private def cycleMin (p : List Nat) (m : Nat) : Prop :=
  m < p.length ∧ step p m ≠ m ∧ (orbit p p.length m).min? = some m

private theorem oneBlank_unique {s : List (Option Nat)} {z w : Nat}
    (hz : oneBlank z s) (hw : oneBlank w s) : z = w := hw.2 z (by
      have hlt := List.getElem?_eq_some_iff.mp hz.1
      exact hlt.1) hz.1

private theorem bad_move_exists {p : List Nat} (hp : p.Perm (List.range p.length)) {m : Nat}
    (hm : cycleMin p m) {ms : List Move}
    (h : run ((List.range p.length).map some ++ [none]) ms = some (p.map some ++ [none])) :
    ∃ q ∈ ms, m ∈ orbit p p.length m ∧ q.1 ∈ orbit p p.length m ∧ q.2 ∉ orbit p p.length m ∧
      (∃ sB, ∃ pre, ms = pre ++ q :: (List.drop (pre.length + 1) ms) ∧
        run ((List.range p.length).map some ++ [none]) pre = some sB ∧ sB[q.1]? = some (some q.1)) := by
  classical
  have hlt : m < p.length := hm.1
  have hsrcExists : ∃ q ∈ ms, q.1 = m := by
    by_cases hcases : ∃ q ∈ ms, q.1 = m
    · exact hcases
    · have hnoall : ¬ ∃ q ∈ ms, q.1 = m := hcases
      exfalso
      have hnoall' : ∀ q ∈ ms, q.1 ≠ m := by
        intro q hq he
        exact hnoall ⟨q, hq, he⟩
      have hzout : p.length ≠ m := by intro hh; omega
      have hno2 := no_source_no_target (oneBlank_start p.length) hzout h hnoall'
      have heq := run_untouched h (fun q hq => ⟨hnoall' q hq, hno2 q hq⟩)
      have hs := start_slot p.length m hlt
      have hf := finish_slot p m hlt
      rw [hs] at heq
      rw [hf] at heq
      injection heq with heq1
      injection heq1 with heq2
      have hsEq : step p m = p.get ⟨m, hlt⟩ := step_eq_of_lt p hlt
      exact hm.2.1 (by rw [hsEq]; exact heq2)
  have hSexists : ∃ q ∈ ms, q.1 ∈ orbit p p.length m := by
    rcases hsrcExists with ⟨q, hq, hq1⟩
    refine ⟨q, hq, ?_⟩
    rw [hq1]
    exact mem_orbit_self hp hlt
  let S : Nat → Prop := fun a => a ∈ orbit p p.length m
  obtain ⟨pre, q, post, hms, hSq, hnoPre⟩ := split_first_source ms S hSexists
  have hh : run ((List.range p.length).map some ++ [none]) (pre ++ q :: post) =
      some (p.map some ++ [none]) := by
    rw [← hms]
    exact h
  obtain ⟨sB, hpre, hrest⟩ := run_of_run_append hh
  cases hmq : move sB q with
  | none => simp [run, hmq] at hrest
  | some sA =>
      obtain ⟨z, hzB, hzout⟩ := blank_out_after (oneBlank_start p.length)
        (orbit_not_contain_n hp hlt) hpre hnoPre
      have hblank := move_preserves_blank hzB hmq
      have hq2 : q.2 = z := hblank.1
      have hnoPre2 := no_target_pred (oneBlank_start p.length) (orbit_not_contain_n hp hlt) hpre hnoPre
      have hsBq : sB[q.1]? = some (some q.1) := by
        have heq := run_untouched_pred (S := S) hSq hpre
          (fun r hr => ⟨hnoPre r hr, hnoPre2 r hr⟩)
        rw [heq]
        exact start_slot p.length q.1 (mem_orbit_lt hp hlt hSq)
      refine ⟨q, by rw [hms]; simp, mem_orbit_self hp hlt, hSq, ?_, ?_⟩
      · simpa [hq2] using hzout
      · refine ⟨sB, pre, ?_, hpre, hsBq⟩
        simp [hms]

private theorem good_exists_idx {p : List Nat} {j : Nat}
    (hjlt : j < p.length) (hjne : p.get ⟨j, hjlt⟩ ≠ j) {ms : List Move}
    (h : run ((List.range p.length).map some ++ [none]) ms = some (p.map some ++ [none])) :
    ∃ i, i < ms.length ∧ ∃ q, ms[i]? = some q ∧ q.2 = j ∧ q.1 ≠ j ∧
      (∃ sB, run ((List.range p.length).map some ++ [none]) (ms.take i) = some sB ∧
        sB[q.1]? = some (some (p.get ⟨j, hjlt⟩))) := by
  classical
  obtain ⟨q0, hq0mem, hq0touch⟩ := touch_exists_of_changed hjlt hjne h
  obtain ⟨pre, q, post, hms, htq, hnoPost⟩ := split_last_touch ms j ⟨q0, hq0mem, hq0touch⟩
  have hh : run ((List.range p.length).map some ++ [none]) (pre ++ q :: post) =
      some (p.map some ++ [none]) := by rw [← hms]; exact h
  obtain ⟨sB, hpre, hrest⟩ := run_of_run_append hh
  cases hmq : move sB q with
  | none => simp [run, hmq] at hrest
  | some sA =>
      have htail : run sA post = some (p.map some ++ [none]) := by simpa [run, hmq] using hrest
      have hspec := good_move_spec hjlt htq hnoPost hpre hmq htail (finish_slot p j hjlt)
      refine ⟨pre.length, ?_, q, ?_, hspec.1, hspec.2.1, ?_⟩
      · rw [hms]; simp
      · rw [hms, List.getElem?_append_right]
        · simp
        · simp
      · refine ⟨sB, ?_, ?_⟩
        · rw [hms]
          simp
          exact hpre
        · obtain ⟨id, hsrc, hidEq⟩ := hspec.2.2
          simpa [hidEq] using hsrc

private theorem bad_exists_idx {p : List Nat} (hp : p.Perm (List.range p.length)) {m : Nat}
    (hm : cycleMin p m) {ms : List Move}
    (h : run ((List.range p.length).map some ++ [none]) ms = some (p.map some ++ [none])) :
    ∃ i, i < ms.length ∧ ∃ q, ms[i]? = some q ∧ q.1 ∈ orbit p p.length m ∧
      q.2 ∉ orbit p p.length m ∧
      (∃ sB, run ((List.range p.length).map some ++ [none]) (ms.take i) = some sB ∧
        sB[q.1]? = some (some q.1)) := by
  classical
  have hlt : m < p.length := hm.1
  have hsrcExists : ∃ q ∈ ms, q.1 = m := by
    by_cases hcases : ∃ q ∈ ms, q.1 = m
    · exact hcases
    · have hnoall : ¬ ∃ q ∈ ms, q.1 = m := hcases
      exfalso
      have hnoall' : ∀ q ∈ ms, q.1 ≠ m := by
        intro q hq he
        exact hnoall ⟨q, hq, he⟩
      have hzout : p.length ≠ m := by intro hh; omega
      have hno2 := no_source_no_target (oneBlank_start p.length) hzout h hnoall'
      have heq := run_untouched h (fun q hq => ⟨hnoall' q hq, hno2 q hq⟩)
      have hs := start_slot p.length m hlt
      have hf := finish_slot p m hlt
      rw [hs] at heq
      rw [hf] at heq
      injection heq with heq1
      injection heq1 with heq2
      have hsEq : step p m = p.get ⟨m, hlt⟩ := step_eq_of_lt p hlt
      exact hm.2.1 (by rw [hsEq]; exact heq2)
  have hSexists : ∃ q ∈ ms, q.1 ∈ orbit p p.length m := by
    rcases hsrcExists with ⟨q, hq, hq1⟩
    refine ⟨q, hq, ?_⟩
    rw [hq1]
    exact mem_orbit_self hp hlt
  let S : Nat → Prop := fun a => a ∈ orbit p p.length m
  obtain ⟨pre, q, post, hms, hSq, hnoPre⟩ := split_first_source ms S hSexists
  have hh : run ((List.range p.length).map some ++ [none]) (pre ++ q :: post) =
      some (p.map some ++ [none]) := by rw [← hms]; exact h
  obtain ⟨sB, hpre, hrest⟩ := run_of_run_append hh
  cases hmq : move sB q with
  | none => simp [run, hmq] at hrest
  | some sA =>
      obtain ⟨z, hzB, hzout⟩ := blank_out_after (oneBlank_start p.length)
        (orbit_not_contain_n hp hlt) hpre hnoPre
      have hblank := move_preserves_blank hzB hmq
      have hq2 : q.2 = z := hblank.1
      have hnoPre2 := no_target_pred (oneBlank_start p.length) (orbit_not_contain_n hp hlt) hpre hnoPre
      have hsBq : sB[q.1]? = some (some q.1) := by
        have heq := run_untouched_pred (S := S) hSq hpre
          (fun r hr => ⟨hnoPre r hr, hnoPre2 r hr⟩)
        rw [heq]
        exact start_slot p.length q.1 (mem_orbit_lt hp hlt hSq)
      refine ⟨pre.length, ?_, q, ?_, hSq, ?_, ?_⟩
      · rw [hms]; simp
      · rw [hms, List.getElem?_append_right] <;> simp
      · simpa [hq2] using hzout
      · refine ⟨sB, ?_, hsBq⟩
        rw [hms]
        simp
        exact hpre

/-- First return time of the permutation step from m, as the minimum of the
positive return times in range (n+1). -/
private def returnList (p : List Nat) (hp : p.Perm (List.range p.length)) (m : Nat) : List Nat :=
  (List.range (p.length + 1)).filter (fun k => 0 < k && iter (step p) k m == m)

private def per (p : List Nat) (hp : p.Perm (List.range p.length)) (m : Nat) : Nat :=
  (returnList p hp m).min?.getD 0

private theorem returnList_nonempty {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hm : m < p.length) : ∃ k, k ∈ returnList p hp m := by
  obtain ⟨L, hpos, hle, hper⟩ := exists_period (step p)
    (fun {a b} ha hb h => step_inj hp ha hb h) (iter_lt hp hm)
  refine ⟨L, ?_⟩
  simp [returnList, hpos, hper]
  omega

private theorem per_min_eq {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hm : m < p.length) : (returnList p hp m).min? = some (per p hp m) := by
  have hne := returnList_nonempty hp hm
  rcases hne with ⟨k, hk⟩
  cases hmin : (returnList p hp m).min? with
  | none =>
      have : returnList p hp m = [] := by simpa using hmin
      simp [this] at hk
  | some k' =>
      simp [per, hmin]

private theorem per_spec {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hm : m < p.length) :
    per p hp m ∈ returnList p hp m ∧
      ∀ k ∈ returnList p hp m, per p hp m ≤ k := by
  have hmin := per_min_eq hp hm
  have hs := (List.min?_eq_some_iff.mp hmin)
  constructor
  · exact hs.1
  · exact hs.2

private theorem per_mem_range {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hm : m < p.length) : per p hp m < p.length + 1 := by
  have hs := (per_spec hp hm).1
  simp [returnList] at hs
  exact hs.1

private theorem per_pos {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hm : m < p.length) : 0 < per p hp m := by
  have hs := (per_spec hp hm).1
  simp [returnList] at hs
  exact hs.2.1

private theorem per_le {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hm : m < p.length) : per p hp m ≤ p.length := by
  have hlt := per_mem_range hp hm
  omega

private theorem per_iter {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hm : m < p.length) : iter (step p) (per p hp m) m = m := by
  have hs := (per_spec hp hm).1
  simp [returnList] at hs
  exact hs.2.2

private theorem per_le_of_return {p : List Nat} (hp : p.Perm (List.range p.length))
    {m k : Nat} (hm : m < p.length) (hkpos : 0 < k) (hklt : k ≤ p.length)
    (hiter : iter (step p) k m = m) : per p hp m ≤ k := by
  apply (per_spec hp hm).2 k
  simp [returnList, hkpos, hiter]
  omega


private theorem iter_inj_on_per {p : List Nat} (hp : p.Perm (List.range p.length))
    {m a b : Nat} (hm : m < p.length) (ha : a < per p hp m) (hb : b < per p hp m)
    (hEq : iter (step p) a m = iter (step p) b m) : a = b := by
  by_cases hab : a = b
  · exact hab
  have hlt_or_gt : a < b ∨ b < a := by omega
  rcases hlt_or_gt with hlt | hgt
  · have hcan := cancel_iter (step p)
      (fun {x y} hx hy h => step_inj hp hx hy h) (iter_lt hp hm) a b (Nat.le_of_lt hlt) hEq
    have hdiffpos : 0 < b - a := by omega
    have hdiffle : b - a ≤ p.length := by
      have hple := per_le hp hm
      omega
    have hperle := per_le_of_return hp hm hdiffpos hdiffle hcan
    have hdiff_lt : b - a < per p hp m := by omega
    omega
  · have hcan := cancel_iter (step p)
      (fun {x y} hx hy h => step_inj hp hx hy h) (iter_lt hp hm) b a (Nat.le_of_lt hgt) hEq.symm
    have hdiffpos : 0 < a - b := by omega
    have hdiffle : a - b ≤ p.length := by
      have hple := per_le hp hm
      omega
    have hperle := per_le_of_return hp hm hdiffpos hdiffle hcan
    have hdiff_lt : a - b < per p hp m := by omega
    omega

private theorem mem_orbit_iff_iter_per {p : List Nat} (hp : p.Perm (List.range p.length))
    {m j : Nat} (hm : m < p.length) :
    j ∈ orbit p p.length m ↔ ∃ k, k < per p hp m ∧ iter (step p) k m = j := by
  constructor
  · intro hj
    rcases mem_orbit_iff_iter hp hm |>.1 hj with ⟨t, ht, hEq⟩
    have hpos := per_pos hp hm
    refine ⟨t % per p hp m, Nat.mod_lt _ hpos, ?_⟩
    rw [← hEq]
    exact (iter_mod (step p) hpos (per_iter hp hm) t).symm
  · rintro ⟨k, hk, hEq⟩
    apply mem_orbit_iff_iter hp hm |>.2
    refine ⟨k, ?_, hEq⟩
    have hle := per_le hp hm
    omega

private theorem cycle_min_per_ge_two {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hmc : cycleMin p m) : 2 ≤ per p hp m := by
  have hpos := per_pos hp hmc.1
  have hiter := per_iter hp hmc.1
  by_cases hle : 2 ≤ per p hp m
  · exact hle
  · have hper1 : per p hp m = 1 := by omega
    rw [hper1] at hiter
    have hstep : step p m = m := by simpa [iter] using hiter
    exact False.elim (hmc.2.1 hstep)

private def rotPre (p : List Nat) (hp : p.Perm (List.range p.length)) (m k : Nat) : List Move :=
  (m, p.length) :: (List.range k).map
    (fun t => (iter (step p) (t+1) m, iter (step p) t m))

private theorem rotPre_succ (p : List Nat) (hp : p.Perm (List.range p.length)) (m k : Nat) :
    rotPre p hp m (k+1) = rotPre p hp m k ++
      [(iter (step p) (k+1) m, iter (step p) k m)] := by
  simp [rotPre, List.range_succ]

private theorem rotPre_run {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hmc : cycleMin p m) {s sF : List (Option Nat)} {k : Nat}
    (hz : oneBlank p.length s) (hlen : s.length = p.length + 1)
    (horig : ∀ j, j ∈ orbit p p.length m → s[j]? = some (some j))
    (hk : k ≤ per p hp m - 1) (h : run s (rotPre p hp m k) = some sF) :
    oneBlank (iter (step p) k m) sF ∧
      sF[p.length]? = some (some m) ∧
      (∀ t, t < k → sF[iter (step p) t m]? = some (some (iter (step p) (t+1) m))) ∧
      (∀ j, j < p.length → j ∉ orbit p p.length m → sF[j]? = s[j]?) := by
  induction k generalizing s sF with
  | zero =>
      cases hm : move s (m, p.length) with
      | none => simp [rotPre, run, hm] at h
      | some s1 =>
      have hs1F : s1 = sF := by simpa [rotPre, run, hm] using h
      subst sF
      have hmv : move s (m, p.length) = some s1 := hm
      obtain ⟨id, hsrc, htgt, hsF⟩ := move_spec s (m, p.length) s1 hmv
      have hmMem : m ∈ orbit p p.length m := mem_orbit_self hp hmc.1
      have hsrcm := horig m hmMem
      have hid : id = m := by simpa [hsrc] using hsrcm
      subst id
      have hb := move_preserves_blank hz hmv
      constructor
      · simpa [iter] using hb.2
      constructor
      · rw [hsF]
        apply set_get_self
        have hnlt : p.length < (s.set m none).length := by
          simpa using (List.getElem?_eq_some_iff.mp hz.1).1
        exact hnlt
      constructor
      · intro t ht
        omega
      · intro j hj hjnot
        have hmnej : m ≠ j := by
          intro heq
          subst j
          exact hjnot hmMem
        have hnnej : p.length ≠ j := by omega
        exact move_untouched hmv hmnej hnnej
  | succ k ih =>
      have hleL : k ≤ per p hp m - 1 := by omega
      have hkLm1 : k + 1 ≤ per p hp m - 1 := hk
      have hkLt : k + 1 < per p hp m := by
        have hpos := per_pos hp hmc.1
        omega
      have hstepEq : rotPre p hp m (k+1) = rotPre p hp m k ++
          [(iter (step p) (k+1) m, iter (step p) k m)] := rotPre_succ p hp m k
      rw [hstepEq] at h
      obtain ⟨sM, hrunM, hrest⟩ := run_of_run_append h
      have hM := ih hz hlen horig hleL hrunM
      have hblankM := hM.1
      have htempM := hM.2.1
      let a := iter (step p) (k+1) m
      let b := iter (step p) k m
      have halt : a < p.length := by
        dsimp [a]
        exact iter_lt hp hmc.1 (k+1)
      have hblt : b < p.length := by
        dsimp [b]
        exact iter_lt hp hmc.1 k
      have haOrbit : a ∈ orbit p p.length m := by
        apply mem_orbit_iff_iter_per hp hmc.1 |>.2
        exact ⟨k+1, hkLt, rfl⟩
      have haNoTouch : ∀ q ∈ rotPre p hp m k, q.1 ≠ a ∧ q.2 ≠ a := by
        intro q hq
        simp [rotPre, List.mem_map] at hq
        rcases hq with hq | ⟨t, ht, rfl⟩
        · subst q
          constructor
          · intro heq
            have hEq := iter_inj_on_per hp hmc.1 (per_pos hp hmc.1) hkLt heq
            omega
          · intro heq
            omega
        · have htL : t < per p hp m := by omega
          have ht1L : t + 1 < per p hp m := by omega
          constructor
          · intro heq
            have hEq := iter_inj_on_per hp hmc.1 ht1L hkLt heq
            omega
          · intro heq
            have hEq := iter_inj_on_per hp hmc.1 htL hkLt heq
            omega
      have hsMa : sM[a]? = s[a]? := run_untouched hrunM haNoTouch
      have hsSrc : sM[a]? = some (some a) := by
        rw [hsMa]
        exact horig a haOrbit
      have hmv' : move sM (a, b) = some sF := by
        cases hmcv : move sM (a, b) with
        | none =>
            have hcontra : (none : Option (List (Option Nat))) = some sF := by
              simpa [a, b, run, hmcv] using hrest
            cases hcontra
        | some s1 =>
            have hs1F : s1 = sF := by simpa [a, b, run, hmcv] using hrest
            exact congrArg some hs1F
      have hbM : sM[b]? = some none := by
        simpa [b] using hblankM.1
      obtain ⟨id, hsrcId, htgtId, hsF⟩ := move_spec sM (a, b) sF hmv'
      have hid : id = a := by simpa [hsSrc] using hsrcId.symm
      subst id
      have hb := move_preserves_blank (by simpa [b] using hblankM) hmv'
      constructor
      · simpa [a] using hb.2
      constructor
      · rw [move_untouched hmv' (j := p.length) (by dsimp; omega) (by dsimp; omega)]
        exact htempM
      constructor
      · intro t ht
        by_cases htold : t < k
        · have hane : a ≠ iter (step p) t m := by
            dsimp [a]
            intro heq
            have hh := iter_inj_on_per hp hmc.1 hkLt (by omega) heq
            omega
          have hbne : b ≠ iter (step p) t m := by
            dsimp [b]
            intro heq
            have hh := iter_inj_on_per hp hmc.1 (by omega) (by omega) heq
            omega
          rw [move_untouched hmv' (j := iter (step p) t m) (by dsimp; exact hane) (by dsimp; exact hbne)]
          exact hM.2.2.1 t htold
        · have hteq : t = k := by omega
          subst t
          rw [hsF]
          have hnSM : p.length < sM.length := (List.getElem?_eq_some_iff.mp htempM).1
          have hbltSM : b < (sM.set a none).length := by simp; omega
          rw [set_get_self (sM.set a none) b (some a) hbltSM]
      · intro j hj hjnot
        have haj : a ≠ j := by
          dsimp [a]
          intro heq
          exact hjnot (by simpa [a, heq] using haOrbit)
        have hbj : b ≠ j := by
          dsimp [b]
          intro heq
          exact hjnot (mem_orbit_iff_iter_per hp hmc.1 |>.2 ⟨k, by omega, heq⟩)
        rw [move_untouched hmv' haj hbj]
        exact hM.2.2.2 j hj hjnot

private def rot (p : List Nat) (hp : p.Perm (List.range p.length)) (m : Nat) : List Move :=
  rotPre p hp m (per p hp m - 1) ++
    [(p.length, iter (step p) (per p hp m - 1) m)]

private theorem rot_len {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hmc : cycleMin p m) : (rot p hp m).length = per p hp m + 1 := by
  have hge := cycle_min_per_ge_two hp hmc
  simp [rot, rotPre]
  omega

private theorem rot_run {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hmc : cycleMin p m) {s sF : List (Option Nat)}
    (hz : oneBlank p.length s) (hlen : s.length = p.length + 1)
    (horig : ∀ j, j ∈ orbit p p.length m → s[j]? = some (some j))
    (h : run s (rot p hp m) = some sF) :
    ∃ s', run s (rot p hp m) = some s' ∧ oneBlank p.length s' ∧
      (∀ j (hj : j < p.length), j ∈ orbit p p.length m →
        s'[j]? = some (some (p.get ⟨j, hj⟩))) ∧
      (∀ j, j < p.length → j ∉ orbit p p.length m → s'[j]? = s[j]?) := by
  classical
  let L := per p hp m
  have h0 : run s (rot p hp m) = some sF := h
  have hge : 2 ≤ L := cycle_min_per_ge_two hp hmc
  have hsplit : rot p hp m = rotPre p hp m (L - 1) ++
      [(p.length, iter (step p) (L - 1) m)] := by rfl
  have hrun : run s (rot p hp m) =
      (run s (rotPre p hp m (L - 1))).bind
        (fun sM => run sM [(p.length, iter (step p) (L - 1) m)]) := by
    rw [hsplit]
    exact run_append s (rotPre p hp m (L - 1)) [(p.length, iter (step p) (L - 1) m)]
  rw [hrun] at h
  cases hpre : run s (rotPre p hp m (L - 1)) with
  | none => simp [hpre] at h
  | some sM =>
      have hrest : run sM [(p.length, iter (step p) (L - 1) m)] = some sF := by
        simpa [hpre] using h
      have hM := rotPre_run hp hmc hz hlen horig (by omega) hpre
      have hblankM := hM.1
      have htempM := hM.2.1
      have hmv : move sM (p.length, iter (step p) (L - 1) m) = some sF := by
        cases hmcv : move sM (p.length, iter (step p) (L - 1) m) with
        | none =>
            have hcontra : (none : Option (List (Option Nat))) = some sF := by
              simpa [run, hmcv] using hrest
            cases hcontra
        | some s1 =>
            have hs1F : s1 = sF := by simpa [run, hmcv] using hrest
            exact congrArg some hs1F
      refine ⟨sF, ?_⟩
      constructor
      · exact h0
      constructor
      · have hb := move_preserves_blank hblankM hmv
        simpa using hb.2
      constructor
      · intro j hj hjmem
        rcases mem_orbit_iff_iter_per hp hmc.1 |>.1 hjmem with ⟨t, ht, htEq⟩
        by_cases htlast : t = L - 1
        · subst t
          have hstepEq : step p (iter (step p) (L - 1) m) = m := by
            have hiter := per_iter hp hmc.1
            change iter (step p) L m = m at hiter
            have hsum : (L - 1) + 1 = L := by omega
            have hs : iter (step p) ((L - 1) + 1) m = m := by simpa [hsum] using hiter
            rw [iter_succ'] at hs
            exact hs
          have hpLast : p.get ⟨iter (step p) (L - 1) m, iter_lt hp hmc.1 (L - 1)⟩ = m := by
            have hget : step p (iter (step p) (L - 1) m) =
                p.get ⟨iter (step p) (L - 1) m, iter_lt hp hmc.1 (L - 1)⟩ := by
              exact step_eq_of_lt p (iter_lt hp hmc.1 (L - 1))
            rw [← hget]
            exact hstepEq
          have hgetj : p[j]? = some (p.get ⟨j, hj⟩) := List.getElem?_eq_getElem hj
          have hgetlast : p[iter (step p) (L - 1) m]? =
              some (p.get ⟨iter (step p) (L - 1) m, iter_lt hp hmc.1 (L - 1)⟩) :=
            List.getElem?_eq_getElem (iter_lt hp hmc.1 (L - 1))
          have hgetEq : p[j]? = p[iter (step p) (L - 1) m]? := by rw [htEq]
          have hpEq : p.get ⟨j, hj⟩ = m := by
            have hEqIdx : p.get ⟨j, hj⟩ =
                p.get ⟨iter (step p) (L - 1) m, iter_lt hp hmc.1 (L - 1)⟩ := by
              injection (hgetj.symm.trans (hgetEq.trans hgetlast))
            rw [hEqIdx]
            exact hpLast
          rw [hpEq]
          obtain ⟨id, hsrc, htgt, hsEq⟩ := move_spec sM (p.length, iter (step p) (L - 1) m) sF hmv
          have hsEq' : some (some id) = some (some m) := hsrc.symm.trans htempM
          injection hsEq' with h1
          injection h1 with hid
          subst id
          rw [← htEq]
          change sF = (sM.set p.length none).set (iter (step p) (L - 1) m) (some m) at hsEq
          rw [hsEq]
          have hnSM : p.length < sM.length := (List.getElem?_eq_some_iff.mp htempM).1
          have hlastltSM : iter (step p) (L - 1) m < (sM.set p.length none).length := by
            simp
            omega
          rw [set_get_self (sM.set p.length none) (iter (step p) (L - 1) m) (some m) hlastltSM]
        · have htold : t < L - 1 := by omega
          have hnJ : p.length ≠ j := by omega
          have hlastJ : iter (step p) (L - 1) m ≠ j := by
            intro heq
            have heq' : iter (step p) (L - 1) m = iter (step p) t m := heq.trans htEq.symm
            have hh := iter_inj_on_per hp hmc.1 (by omega) ht heq'
            omega
          rw [move_untouched hmv hnJ hlastJ]
          have hsMj : sM[j]? = some (some (iter (step p) (t+1) m)) := by
            simpa [htEq] using hM.2.2.1 t htold
          have hpEq2 : p.get ⟨j, hj⟩ = iter (step p) (t+1) m := by
            have hstepj : step p j = iter (step p) (t+1) m := by
              rw [← htEq]
              exact (iter_succ' (step p) t m).symm
            have hget : step p j = p.get ⟨j, hj⟩ := by
              exact step_eq_of_lt p hj
            rw [← hget]
            exact hstepj
          rw [hsMj, hpEq2]
      · intro j hj hjnot
        have hane : p.length ≠ j := by omega
        have hbne : iter (step p) (L - 1) m ≠ j := by
          intro heq
          apply hjnot
          apply mem_orbit_iff_iter_per hp hmc.1 |>.2
          exact ⟨L - 1, by omega, heq⟩
        rw [move_untouched hmv hane hbne]
        exact hM.2.2.2 j hj hjnot

private noncomputable def cms (p : List Nat) (hp : p.Perm (List.range p.length)) : List Nat := by
  classical
  exact (List.range p.length).filter (fun m => decide (cycleMin p m))

private def changedList (p : List Nat) : List Nat :=
  (List.range p.length).filter (fun j => (p[j]? != some j) == true)

private theorem cms_nodup {p : List Nat} (hp : p.Perm (List.range p.length)) : (cms p hp).Nodup := by
  classical
  exact (List.filter_sublist (p := fun m => decide (cycleMin p m)) (l := List.range p.length)).nodup List.nodup_range

private theorem cms_mem {p : List Nat} (hp : p.Perm (List.range p.length)) {m : Nat}
    (hm : m ∈ cms p hp) : m < p.length ∧ cycleMin p m := by
  classical
  simpa [cms] using hm

private theorem changedList_nodup (p : List Nat) : (changedList p).Nodup := by
  exact (List.filter_sublist (p := fun j => (p[j]? != some j) == true) (l := List.range p.length)).nodup List.nodup_range

private theorem changedList_mem {p : List Nat} {j : Nat} (hj : j ∈ changedList p) :
    j < p.length ∧ p[j]? ≠ some j := by
  simpa [changedList] using hj

private theorem changed_exists_cms {p : List Nat} (hp : p.Perm (List.range p.length))
    {j : Nat} (hj : j < p.length) (hchange : p[j]? ≠ some j) :
    ∃ m ∈ cms p hp, j ∈ orbit p p.length m := by
  classical
  have hlen : (orbit p p.length j).length = p.length := orbit_length p _ _
  have hnpos : 0 < p.length := by omega
  obtain ⟨m, hmMin⟩ : ∃ m, (orbit p p.length j).min? = some m := by
    cases hmn : (orbit p p.length j).min? with
    | none =>
        have hempty : orbit p p.length j = [] := by simpa using hmn
        have h0 : (orbit p p.length j).length = 0 := by simp [hempty]
        omega
    | some m => exact ⟨m, rfl⟩
  have hmSpec := List.min?_eq_some_iff.mp hmMin
  have hmInJ : m ∈ orbit p p.length j := hmSpec.1
  have hmlt : m < p.length := mem_orbit_lt hp hj hmInJ
  have hstepNe : step p m ≠ m := by
    intro hfix
    have hiterFixed : ∀ k, iter (step p) k m = m := by
      intro k
      induction k with
      | zero => rfl
      | succ k ih => simpa [iter, hfix] using ih
    have hjInM : j ∈ orbit p p.length m := mem_orbit_symm hp hj hmInJ
    rcases mem_orbit_iff_iter hp hmlt |>.1 hjInM with ⟨k, hk, hkEq⟩
    have hjm : j = m := by
      rw [hiterFixed k] at hkEq
      exact hkEq.symm
    have hmget : p[m]? = some m := by
      have hget : p[m]? = some (p.get ⟨m, hmlt⟩) := List.getElem?_eq_getElem hmlt
      have hgetval : p.get ⟨m, hmlt⟩ = m := by
        have hst : step p m = p.get ⟨m, hmlt⟩ := step_eq_of_lt p hmlt
        rw [← hst]
        exact hfix
      rw [hget, hgetval]
    exact hchange (by rw [hjm]; exact hmget)
  have hminM : (orbit p p.length m).min? = some m := by
    have hEq := orbit_min_eq_of_mem hp hj hmInJ
    rw [← hEq]
    exact hmMin
  have hmc : cycleMin p m := ⟨hmlt, hstepNe, hminM⟩
  refine ⟨m, ?_, ?_⟩
  · simp [cms, hmlt, hmc]
  · exact mem_orbit_symm hp hj hmInJ

private theorem cms_disjoint {p : List Nat} (hp : p.Perm (List.range p.length))
    {m1 m2 j : Nat} (hm1 : m1 ∈ cms p hp) (hm2 : m2 ∈ cms p hp)
    (hj1 : j ∈ orbit p p.length m1) (hj2 : j ∈ orbit p p.length m2) : m1 = m2 := by
  have hmc1 := (cms_mem hp hm1).2
  have hmc2 := (cms_mem hp hm2).2
  have hjlt : j < p.length := mem_orbit_lt hp hmc1.1 hj1
  have hminJ1 : (orbit p p.length j).min? = some m1 := by
    have hEq := orbit_min_eq_of_mem hp hmc1.1 hj1
    rw [← hEq]
    exact hmc1.2.2
  have hminJ2 : (orbit p p.length j).min? = some m2 := by
    have hEq := orbit_min_eq_of_mem hp hmc2.1 hj2
    rw [← hEq]
    exact hmc2.2.2
  rw [hminJ1] at hminJ2
  injection hminJ2

private theorem changed_of_cms_orbit {p : List Nat} (hp : p.Perm (List.range p.length))
    {m j : Nat} (hm : m ∈ cms p hp) (hj : j ∈ orbit p p.length m) :
    (p[j]? != some j) = true := by
  classical
  have hmc := (cms_mem hp hm).2
  by_cases hfix : p[j]? = some j
  · exfalso
    have hjlt : j < p.length := mem_orbit_lt hp hmc.1 hj
    have hstep : step p j = j := by simp [step, hfix]
    have hiterFixed : ∀ k, iter (step p) k j = j := by
      intro k
      induction k with
      | zero => rfl
      | succ k ih => simpa [iter, hstep] using ih
    have hmInJ : m ∈ orbit p p.length j := mem_orbit_symm hp hmc.1 hj
    rcases mem_orbit_iff_iter hp hjlt |>.1 hmInJ with ⟨k, hk, hkEq⟩
    have hmj : m = j := by
      rw [hiterFixed k] at hkEq
      exact hkEq.symm
    exact hmc.2.1 (by rw [hmj]; exact hstep)
  · simp [hfix]

private theorem not_mem_erase_self {α : Type} [DecidableEq α] (a : α) {l : List α}
    (hnod : l.Nodup) : a ∉ l.erase a := by
  induction l with
  | nil => simp [List.erase]
  | cons x xs ih =>
      have htailNod : xs.Nodup := (List.nodup_cons.mp hnod).2
      have hheadNot : x ∉ xs := (List.nodup_cons.mp hnod).1
      by_cases hx : (x == a) = true
      · have hxa : x = a := beq_iff_eq.mp hx
        rw [List.erase_cons]
        simp [hx]
        intro hmem
        exact hheadNot (by simpa [hxa] using hmem)
      · rw [List.erase_cons]
        simp [hx]
        constructor
        · intro heq
          apply hx
          simpa [heq]
        · exact ih htailNod

private theorem perm_of_nodup_mem {α : Type} [DecidableEq α] {l₁ l₂ : List α}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hmem : ∀ a, a ∈ l₁ ↔ a ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      cases l₂ with
      | nil => exact List.Perm.nil
      | cons a as =>
          have ha : a ∈ ([] : List α) := (hmem a).2 (by simp)
          simp at ha
  | cons a as ih =>
      have ha2 : a ∈ l₂ := (hmem a).1 (by simp)
      have htailNod : as.Nodup := (List.nodup_cons.mp h₁).2
      have haTail : a ∉ as := (List.nodup_cons.mp h₁).1
      have heraseNod : (l₂.erase a).Nodup := List.Nodup.erase a h₂
      have htailPerm : as.Perm (l₂.erase a) := by
        apply ih htailNod heraseNod
        intro x
        by_cases hxa : x = a
        · subst x
          constructor
          · intro hmemAs
            exact (haTail hmemAs).elim
          · intro hmemErase
            exact (not_mem_erase_self a h₂ hmemErase).elim
        · rw [List.mem_erase_of_ne hxa]
          simpa [hxa] using hmem x
      exact (List.Perm.cons a htailPerm).trans (List.perm_cons_erase ha2).symm

private theorem range_map_iter_nodup {p : List Nat} (hp : p.Perm (List.range p.length))
    {m k : Nat} (hm : m < p.length) (hk : k ≤ per p hp m) :
    ((List.range k).map (fun t => iter (step p) t m)).Nodup := by
  induction k with
  | zero => simp
  | succ k ih =>
      have hkL : k ≤ per p hp m := by omega
      rw [List.range_succ, List.map_append, List.map_singleton]
      rw [List.nodup_append]
      constructor
      · exact ih hkL
      constructor
      · simp
      · intro a ha b hb hne
        simp [List.mem_map] at ha
        rcases ha with ⟨t, ht, hEq⟩
        have htL : t < per p hp m := by omega
        have hklt : k < per p hp m := by omega
        have haSingleton : a ∈ [iter (step p) k m] := by simpa [hne] using hb
        have haEq : a = iter (step p) k m := by simpa using haSingleton
        have hEq' : iter (step p) t m = iter (step p) k m := hEq.trans haEq
        have hh := iter_inj_on_per hp hm htL hklt hEq'
        omega

private def cycleElems (p : List Nat) (hp : p.Perm (List.range p.length)) (m : Nat) : List Nat :=
  (List.range (per p hp m)).map (fun t => iter (step p) t m)

private theorem cycleElems_nodup {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hm : m < p.length) : (cycleElems p hp m).Nodup := by
  simpa [cycleElems] using range_map_iter_nodup hp hm (Nat.le_refl (per p hp m))

private theorem cycleElems_len {p : List Nat} (hp : p.Perm (List.range p.length)) (m : Nat) :
    (cycleElems p hp m).length = per p hp m := by
  simp [cycleElems]

private theorem cycleElems_mem_orbit {p : List Nat} (hp : p.Perm (List.range p.length))
    {m x : Nat} (hm : m < p.length) : x ∈ cycleElems p hp m ↔ x ∈ orbit p p.length m := by
  simp [cycleElems, List.mem_map, List.mem_range, mem_orbit_iff_iter_per hp hm]

private theorem changed_filter_orbit_perm {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hm : m ∈ cms p hp) :
    ((changedList p).filter (fun j => j ∈ orbit p p.length m)).Perm (cycleElems p hp m) := by
  classical
  have hmc := (cms_mem hp hm).2
  apply perm_of_nodup_mem
  · exact (List.filter_sublist (p := fun j => j ∈ orbit p p.length m) (l := changedList p)).nodup
      (changedList_nodup p)
  · exact cycleElems_nodup hp hmc.1
  · intro x
    constructor
    · intro hx
      have hxorb : x ∈ orbit p p.length m := by simpa using (List.mem_filter.mp hx).2
      exact (cycleElems_mem_orbit hp hmc.1).2 hxorb
    · intro hx
      have hxorb : x ∈ orbit p p.length m := (cycleElems_mem_orbit hp hmc.1).1 hx
      have hxlt : x < p.length := mem_orbit_lt hp hmc.1 hxorb
      have hb := changed_of_cms_orbit hp hm hxorb
      have hxch : p[x]? ≠ some x := by simpa using hb
      exact List.mem_filter.mpr ⟨by simpa [changedList] using And.intro hxlt hxch, by simp [hxorb]⟩

private theorem changed_filter_orbit_len {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hm : m ∈ cms p hp) :
    ((changedList p).filter (fun j => j ∈ orbit p p.length m)).length = per p hp m := by
  rw [(changed_filter_orbit_perm hp hm).length_eq]
  exact cycleElems_len hp m

private theorem flatMap_changed_filter_nodup {p : List Nat} (hp : p.Perm (List.range p.length))
    {ms : List Nat} (hms : ms.Nodup) (hmem : ∀ m ∈ ms, m ∈ cms p hp) :
    (ms.flatMap (fun m => (changedList p).filter (fun j => j ∈ orbit p p.length m))).Nodup := by
  induction ms with
  | nil => simp
  | cons m ms ih =>
      have hheadNot : m ∉ ms := (List.nodup_cons.mp hms).1
      have htailNod : ms.Nodup := (List.nodup_cons.mp hms).2
      have hmcm : m ∈ cms p hp := hmem m (by simp)
      have hmemTail : ∀ n ∈ ms, n ∈ cms p hp := by
        intro n hn
        exact hmem n (by simp [hn])
      rw [List.flatMap_cons, List.nodup_append]
      constructor
      · exact (List.filter_sublist (p := fun j => j ∈ orbit p p.length m) (l := changedList p)).nodup
          (changedList_nodup p)
      constructor
      · exact ih htailNod hmemTail
      · intro a ha b hb hne
        rcases List.mem_flatMap.mp hb with ⟨n, hn, hbfilter⟩
        have hborb : b ∈ orbit p p.length n := by simpa using (List.mem_filter.mp hbfilter).2
        have haorb : a ∈ orbit p p.length m := by simpa using (List.mem_filter.mp ha).2
        have hborb' : a ∈ orbit p p.length n := by simpa [hne] using hborb
        have hmn : m = n := cms_disjoint hp hmcm (hmemTail n hn) haorb hborb'
        exact hheadNot (by simpa [hmn] using hn)

private theorem changed_partition_perm {p : List Nat} (hp : p.Perm (List.range p.length)) :
    (changedList p).Perm ((cms p hp).flatMap
      (fun m => (changedList p).filter (fun j => j ∈ orbit p p.length m))) := by
  classical
  apply perm_of_nodup_mem (changedList_nodup p)
    (flatMap_changed_filter_nodup hp (cms_nodup hp) (fun m hm => hm))
  intro x
  constructor
  · intro hx
    obtain ⟨hxlt, hxne⟩ := changedList_mem hx
    obtain ⟨m, hm, hxorb⟩ := changed_exists_cms hp hxlt hxne
    exact List.mem_flatMap.mpr ⟨m, hm, List.mem_filter.mpr ⟨hx, by simp [hxorb]⟩⟩
  · intro hx
    rcases List.mem_flatMap.mp hx with ⟨m, hm, hxfilter⟩
    exact (List.mem_filter.mp hxfilter).1

private theorem changed_length_eq_sum_per {p : List Nat} (hp : p.Perm (List.range p.length)) :
    (changedList p).length = ((cms p hp).map (fun m => per p hp m)).sum := by
  rw [(changed_partition_perm hp).length_eq, List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro m hm
  exact changed_filter_orbit_len hp hm

private theorem isMin_filter_cms_perm {p : List Nat} (hp : p.Perm (List.range p.length)) :
    ((changedList p).filter (fun i => (orbit p p.length i).min? == some i)).Perm (cms p hp) := by
  classical
  apply perm_of_nodup_mem
  · exact (List.filter_sublist (p := fun i => (orbit p p.length i).min? == some i)
      (l := changedList p)).nodup (changedList_nodup p)
  · exact cms_nodup hp
  · intro x
    constructor
    · intro hx
      have hxch := changedList_mem (List.mem_filter.mp hx).1
      have hxmin : (orbit p p.length x).min? = some x := by simpa using (List.mem_filter.mp hx).2
      have hstepNe : step p x ≠ x := by
        intro hfix
        have hget : p[x]? = some x := by
          have hg : p[x]? = some (p.get ⟨x, hxch.1⟩) := List.getElem?_eq_getElem hxch.1
          have hgv : p.get ⟨x, hxch.1⟩ = x := by
            have hst : step p x = p.get ⟨x, hxch.1⟩ := step_eq_of_lt p hxch.1
            rw [← hst]
            exact hfix
          rw [hg, hgv]
        exact hxch.2 hget
      have hcmc : cycleMin p x := ⟨hxch.1, hstepNe, hxmin⟩
      exact by simpa [cms] using (show x < p.length ∧ cycleMin p x from ⟨hcmc.1, hcmc⟩)
    · intro hx
      have hcmc := (cms_mem hp hx).2
      have hb := changed_of_cms_orbit hp hx (mem_orbit_self hp hcmc.1)
      have hxch : p[x]? ≠ some x := by simpa using hb
      exact List.mem_filter.mpr ⟨by simpa [changedList] using And.intro hcmc.1 hxch, by simp [hcmc.2.2]⟩

private theorem budget_unfold (p : List Nat) :
    budget p = (changedList p).length +
      (changedList p).countP (fun i => (orbit p p.length i).min? == some i) := by
  unfold budget changedList
  simp

private theorem budget_eq {p : List Nat} (hp : p.Perm (List.range p.length)) :
    budget p = (changedList p).length + (cms p hp).length := by
  rw [budget_unfold, List.countP_eq_length_filter, (isMin_filter_cms_perm hp).length_eq]

private theorem rotPre_run_some {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hmc : cycleMin p m) {s : List (Option Nat)}
    (hz : oneBlank p.length s) (hlen : s.length = p.length + 1)
    (horig : ∀ j, j ∈ orbit p p.length m → s[j]? = some (some j))
    {k : Nat} (hk : k ≤ per p hp m - 1) :
    ∃ sF, run s (rotPre p hp m k) = some sF := by
  induction k generalizing s with
  | zero =>
      have hsrcm := horig m (mem_orbit_self hp hmc.1)
      have hmv : move s (m, p.length) = some ((s.set m none).set p.length (some m)) := by
        unfold move
        rw [hsrcm, hz.1]
        simp [guard]
      refine ⟨(s.set m none).set p.length (some m), ?_⟩
      simpa [rotPre, run, hmv]
  | succ k ih =>
      have hleL : k ≤ per p hp m - 1 := by omega
      obtain ⟨sM, hpre⟩ := ih hz hlen horig hleL
      have hM := rotPre_run hp hmc hz hlen horig hleL hpre
      have hblankM := hM.1
      let a := iter (step p) (k + 1) m
      let b := iter (step p) k m
      have halt : a < p.length := by dsimp [a]; exact iter_lt hp hmc.1 (k + 1)
      have hblt : b < p.length := by dsimp [b]; exact iter_lt hp hmc.1 k
      have hkLt : k + 1 < per p hp m := by
        have hpos := per_pos hp hmc.1
        omega
      have haOrbit : a ∈ orbit p p.length m := by
        apply mem_orbit_iff_iter_per hp hmc.1 |>.2
        exact ⟨k + 1, hkLt, rfl⟩
      have haNoTouch : ∀ q ∈ rotPre p hp m k, q.1 ≠ a ∧ q.2 ≠ a := by
        intro q hq
        simp [rotPre, List.mem_map] at hq
        rcases hq with hq | ⟨t, ht, rfl⟩
        · subst q
          constructor
          · intro heq
            have hEq := iter_inj_on_per hp hmc.1 (per_pos hp hmc.1) hkLt heq
            omega
          · intro heq
            omega
        · have htL : t < per p hp m := by omega
          have ht1L : t + 1 < per p hp m := by omega
          constructor
          · intro heq
            have hEq := iter_inj_on_per hp hmc.1 ht1L hkLt heq
            omega
          · intro heq
            have hEq := iter_inj_on_per hp hmc.1 htL hkLt heq
            omega
      have hsMa : sM[a]? = s[a]? := run_untouched hpre haNoTouch
      have hsSrc : sM[a]? = some (some a) := by rw [hsMa]; exact horig a haOrbit
      have hbM : sM[b]? = some none := by simpa [b] using hblankM.1
      have hmv : move sM (a, b) = some ((sM.set a none).set b (some a)) := by
        unfold move
        rw [hsSrc, hbM]
        simp [guard]
      refine ⟨(sM.set a none).set b (some a), ?_⟩
      rw [rotPre_succ, run_append, hpre]
      simpa [a, b, run, hmv]

private theorem rot_run_some {p : List Nat} (hp : p.Perm (List.range p.length))
    {m : Nat} (hmc : cycleMin p m) {s : List (Option Nat)}
    (hz : oneBlank p.length s) (hlen : s.length = p.length + 1)
    (horig : ∀ j, j ∈ orbit p p.length m → s[j]? = some (some j)) :
    ∃ sF, run s (rot p hp m) = some sF := by
  classical
  let L := per p hp m
  have hge : 2 ≤ L := cycle_min_per_ge_two hp hmc
  obtain ⟨sM, hpre⟩ := rotPre_run_some hp hmc hz hlen horig (by omega : L - 1 ≤ per p hp m - 1)
  have hM := rotPre_run hp hmc hz hlen horig (by omega : L - 1 ≤ per p hp m - 1) hpre
  have hsrcN := hM.2.1
  have hbLast := hM.1.1
  have hmv : move sM (p.length, iter (step p) (L - 1) m) =
      some ((sM.set p.length none).set (iter (step p) (L - 1) m) (some m)) := by
    unfold move
    rw [hsrcN, hbLast]
    simp [guard]
  refine ⟨(sM.set p.length none).set (iter (step p) (L - 1) m) (some m), ?_⟩
  change run s (rotPre p hp m (L - 1) ++ [(p.length, iter (step p) (L - 1) m)]) = _
  rw [run_append, hpre]
  simpa [run, hmv]

private noncomputable def allMoves (p : List Nat) (hp : p.Perm (List.range p.length)) : List Move :=
  (cms p hp).reverse.flatMap (fun m => rot p hp m)

private theorem run_rot_prefix {p : List Nat} (hp : p.Perm (List.range p.length))
    {ms : List Nat} (hmsNod : ms.Nodup) (hmsMem : ∀ m ∈ ms, m ∈ cms p hp) :
    ∃ sF, run ((List.range p.length).map some ++ [none])
        (ms.reverse.flatMap (fun m => rot p hp m)) = some sF ∧ oneBlank p.length sF ∧
      (∀ j (hj : j < p.length), (∃ m ∈ ms, j ∈ orbit p p.length m) →
        sF[j]? = some (some (p.get ⟨j, hj⟩))) ∧
      (∀ j (hj : j < p.length), ¬ (∃ m ∈ ms, j ∈ orbit p p.length m) → sF[j]? = some j) := by
  classical
  induction ms with
  | nil =>
      refine ⟨(List.range p.length).map some ++ [none], by simp [run], oneBlank_start p.length, ?_, ?_⟩
      · intro j hj h; rcases h with ⟨m, hm, _⟩; simp at hm
      · intro j hj hnot
        exact start_slot p.length j hj
  | cons m ms ih =>
      have hheadNot : m ∉ ms := (List.nodup_cons.mp hmsNod).1
      have htailNod : ms.Nodup := (List.nodup_cons.mp hmsNod).2
      have htailMem : ∀ n ∈ ms, n ∈ cms p hp := by
        intro n hn
        exact hmsMem n (by simp [hn])
      obtain ⟨sM, hrunM, hblankM, hfinM, hunchM⟩ := ih htailNod htailMem
      have hmcm : m ∈ cms p hp := hmsMem m (by simp)
      have hmc := (cms_mem hp hmcm).2
      have hmNotProc : ¬ ∃ n ∈ ms, m ∈ orbit p p.length n := by
        rintro ⟨n, hn, hmn⟩
        have hmnEq : m = n := cms_disjoint hp hmcm (htailMem n hn)
          (mem_orbit_self hp hmc.1) hmn
        exact hheadNot (by simpa [hmnEq] using hn)
      have horigM : ∀ j, j ∈ orbit p p.length m → sM[j]? = some (some j) := by
        intro j hjorb
        have hjnot : ¬ ∃ n ∈ ms, j ∈ orbit p p.length n := by
          rintro ⟨n, hn, hjn⟩
          have hmnEq : m = n := cms_disjoint hp hmcm (htailMem n hn) hjorb hjn
          exact hheadNot (by simpa [hmnEq] using hn)
        exact hunchM j (mem_orbit_lt hp hmc.1 hjorb) hjnot
      have hlenM : sM.length = p.length + 1 := by
        have h := length_run hrunM
        simpa using h
      obtain ⟨sF0, hrotRun0⟩ := rot_run_some hp hmc hblankM hlenM horigM
      obtain ⟨sF, hrotRun, hblankF, hfinF, hunchF⟩ := rot_run hp hmc hblankM hlenM horigM hrotRun0
      have hsFF : sF = sF0 := by
        exact Option.some.inj (hrotRun.symm.trans hrotRun0)
      subst sF
      have hrunAll : run ((List.range p.length).map some ++ [none])
          ((m :: ms).reverse.flatMap (fun n => rot p hp n)) = some sF0 := by
        simp only [List.reverse_cons, List.flatMap_append, List.flatMap_cons, List.flatMap_nil,
          List.append_nil]
        rw [run_append, hrunM]
        simpa using hrotRun0
      refine ⟨sF0, hrunAll, hblankF, ?_, ?_⟩
      · intro j hj hjmem
        by_cases hjorb : j ∈ orbit p p.length m
        · exact hfinF j hj hjorb
        · have hjTail : ∃ n ∈ ms, j ∈ orbit p p.length n := by
            rcases hjmem with ⟨n, hn, hjn⟩
            by_cases hnm : n = m
            · subst n
              exact False.elim (hjorb hjn)
            · have hnTail : n ∈ ms := by simpa [hnm] using hn
              exact ⟨n, hnTail, hjn⟩
          rw [hunchF j hj hjorb]
          exact hfinM j hj hjTail
      · intro j hj hjnot
        have hjorb : j ∉ orbit p p.length m := by intro hmjor; exact hjnot ⟨m, by simp, hmjor⟩
        have hjTail : ¬ ∃ n ∈ ms, j ∈ orbit p p.length n := by
          rintro ⟨n, hn, hjn⟩
          exact hjnot ⟨n, by simp [hn], hjn⟩
        rw [hunchF j hj hjorb]
        exact hunchM j hj hjTail

private theorem sum_one_add_per {p : List Nat} (hp : p.Perm (List.range p.length)) {ms : List Nat} :
    ((ms.map (fun m => 1 + per p hp m)).sum) = ms.length + (ms.map (fun m => per p hp m)).sum := by
  induction ms with
  | nil => simp
  | cons m ms ih =>
      simp [List.sum_cons, ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem allMoves_len {p : List Nat} (hp : p.Perm (List.range p.length)) :
    (allMoves p hp).length = budget p := by
  classical
  have hlen : (allMoves p hp).length = ((cms p hp).map (fun m => 1 + per p hp m)).sum := by
    unfold allMoves
    rw [List.length_flatMap, List.map_reverse, List.sum_reverse]
    congr 1
    apply List.map_congr_left
    intro m hm
    simpa [Nat.add_comm] using rot_len hp (cms_mem hp hm |>.2)
  rw [hlen, sum_one_add_per, budget_eq hp, changed_length_eq_sum_per hp]
  omega

private theorem upper_exists {p : List Nat} (hp : p.Perm (List.range p.length)) :
    ∃ ms, run ((List.range p.length).map some ++ [none]) ms =
        some (p.map some ++ [none]) ∧ ms.length = budget p := by
  classical
  obtain ⟨sF, hrun, hblank, hfin, hunch⟩ :=
    run_rot_prefix hp (cms_nodup hp) (fun m hm => hm)
  refine ⟨allMoves p hp, ?_, allMoves_len hp⟩
  have hlenF : sF.length = p.length + 1 := by
    have h := length_run hrun
    simpa using h
  have hEq : sF = p.map some ++ [none] := by
    apply List.ext_getElem?
    intro j
    by_cases hj : j < p.length
    · by_cases hmem : ∃ m ∈ cms p hp, j ∈ orbit p p.length m
      · have hsf := hfin j hj hmem
        have hf : (p.map some ++ [none])[j]? = some (some (p.get ⟨j, hj⟩)) := by
          rw [List.getElem?_append_left (by simpa using hj)]
          rw [List.getElem?_map, List.getElem?_eq_getElem hj]
          rfl
        rw [hsf, hf]
      · have hsf := hunch j hj hmem
        have hpj : p[j]? = some j := by
          by_cases hne : p[j]? = some j
          · exact hne
          · obtain ⟨m, hm, hjorbit⟩ := changed_exists_cms hp hj hne
            exact False.elim (hmem ⟨m, hm, hjorbit⟩)
        have hpjVal : p[j] = j := by
          have hget : p[j]? = some p[j] := List.getElem?_eq_getElem hj
          rw [hget] at hpj
          injection hpj
        have hf : (p.map some ++ [none])[j]? = some (some j) := by
          rw [List.getElem?_append_left (by simpa using hj)]
          rw [List.getElem?_map, List.getElem?_eq_getElem hj]
          rw [hpjVal]
          rfl
        have hsFj : sF[j]? = some (some j) := by simpa using hsf
        rw [hsFj, hf]
    · by_cases hjEq : j = p.length
      · subst j
        rw [hblank.1]
        simp
      · have hjgt : p.length + 1 ≤ j := by omega
        rw [List.getElem?_eq_none (by simpa [hlenF] using hjgt)]
        rw [List.getElem?_eq_none]
        simp
        omega
  simpa [allMoves, hEq] using hrun

private theorem get_ne_of_changed {p : List Nat} {j : Nat} (hjlt : j < p.length)
    (hne : p[j]? ≠ some j) : p.get ⟨j, hjlt⟩ ≠ j := by
  intro hv
  have hget : p[j]? = some (p.get ⟨j, hjlt⟩) := List.getElem?_eq_getElem hjlt
  apply hne
  rw [hget, hv]

private theorem nodup_map_of_injective {α β : Type} [DecidableEq α] {l : List α} {f : α → β}
    (hnod : l.Nodup) (hinj : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hheadNot : a ∉ as := (List.nodup_cons.mp hnod).1
      have htailNod : as.Nodup := (List.nodup_cons.mp hnod).2
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        simp [List.mem_map] at hmem
        rcases hmem with ⟨b, hb, hEq⟩
        have hab : a = b := hinj a (by simp) b (by simp [hb]) hEq.symm
        exact hheadNot (by simpa [hab] using hb)
      · apply ih htailNod
        intro x hx y hy hxy
        exact hinj x (by simp [hx]) y (by simp [hy]) hxy

private noncomputable def goodIdxOf (p : List Nat) {ms : List Move}
    (h : run ((List.range p.length).map some ++ [none]) ms = some (p.map some ++ [none]))
    (j : Nat) : Nat := by
  classical
  by_cases hj : j ∈ changedList p
  · exact Classical.choose (good_exists_idx (p := p) (j := j) (changedList_mem hj).1
      (get_ne_of_changed (changedList_mem hj).1 (changedList_mem hj).2) h)
  · exact 0

private theorem goodIdxOf_spec {p : List Nat} {ms : List Move}
    (h : run ((List.range p.length).map some ++ [none]) ms = some (p.map some ++ [none]))
    {j : Nat} (hj : j ∈ changedList p) :
    goodIdxOf p h j < ms.length ∧ ∃ q, ms[goodIdxOf p h j]? = some q ∧ q.2 = j ∧ q.1 ≠ j ∧
      (∃ sB, run ((List.range p.length).map some ++ [none]) (ms.take (goodIdxOf p h j)) = some sB ∧
        sB[q.1]? = some (some (p.get ⟨j, (changedList_mem hj).1⟩))) := by
  classical
  unfold goodIdxOf
  rw [dif_pos hj]
  exact Classical.choose_spec (good_exists_idx (p := p) (j := j) (changedList_mem hj).1
    (get_ne_of_changed (changedList_mem hj).1 (changedList_mem hj).2) h)

private noncomputable def badIdxOf (p : List Nat) (hp : p.Perm (List.range p.length)) {ms : List Move}
    (h : run ((List.range p.length).map some ++ [none]) ms = some (p.map some ++ [none]))
    (m : Nat) : Nat := by
  classical
  by_cases hm : m ∈ cms p hp
  · exact Classical.choose (bad_exists_idx hp (cms_mem hp hm).2 h)
  · exact 0

private theorem badIdxOf_spec {p : List Nat} (hp : p.Perm (List.range p.length)) {ms : List Move}
    (h : run ((List.range p.length).map some ++ [none]) ms = some (p.map some ++ [none]))
    {m : Nat} (hm : m ∈ cms p hp) :
    badIdxOf p hp h m < ms.length ∧ ∃ q, ms[badIdxOf p hp h m]? = some q ∧
      q.1 ∈ orbit p p.length m ∧ q.2 ∉ orbit p p.length m ∧
      (∃ sB, run ((List.range p.length).map some ++ [none]) (ms.take (badIdxOf p hp h m)) = some sB ∧
        sB[q.1]? = some (some q.1)) := by
  classical
  unfold badIdxOf
  rw [dif_pos hm]
  exact Classical.choose_spec (bad_exists_idx hp (cms_mem hp hm).2 h)

private theorem step_mem_orbit {p : List Nat} (hp : p.Perm (List.range p.length))
    {m j : Nat} (hm : m < p.length) (hj : j ∈ orbit p p.length m) :
    step p j ∈ orbit p p.length m := by
  rcases mem_orbit_iff_iter_per hp hm |>.1 hj with ⟨t, ht, htEq⟩
  by_cases htlast : t + 1 < per p hp m
  · apply mem_orbit_iff_iter_per hp hm |>.2
    refine ⟨t + 1, htlast, ?_⟩
    rw [← htEq]
    exact iter_succ' (step p) t m
  · have hperEq : per p hp m = t + 1 := by omega
    have hstepEq : step p j = m := by
      rw [← htEq]
      have hs : iter (step p) (t + 1) m = m := by
        simpa [hperEq] using per_iter hp hm
      simpa [iter_succ'] using hs
    rw [hstepEq]
    exact mem_orbit_self hp hm

private theorem lower_bound {p : List Nat} (hp : p.Perm (List.range p.length)) {ms : List Move}
    (h : run ((List.range p.length).map some ++ [none]) ms = some (p.map some ++ [none])) :
    budget p ≤ ms.length := by
  classical
  let gi := goodIdxOf p h
  let bi := badIdxOf p hp h
  have hgoodInj : ∀ j ∈ changedList p, ∀ k ∈ changedList p, gi j = gi k → j = k := by
    intro j hj k hk hidx
    rcases goodIdxOf_spec h hj with ⟨_, qj, hgetj, hqj2, _, _⟩
    rcases goodIdxOf_spec h hk with ⟨_, qk, hgetk, hqk2, _, _⟩
    have hgetEq : ms[goodIdxOf p h j]? = ms[goodIdxOf p h k]? := by
      simpa [gi] using congrArg (fun i => ms[i]?) hidx
    have hqEq : qj = qk := by
      rw [hgetj, hgetk] at hgetEq
      exact Option.some.inj hgetEq
    exact by simpa [hqj2, hqk2] using congrArg Prod.snd hqEq
  have hbadInj : ∀ m ∈ cms p hp, ∀ n ∈ cms p hp, bi m = bi n → m = n := by
    intro m hm n hn hidx
    rcases badIdxOf_spec hp h hm with ⟨_, qm, hgetm, hqm1, _, _⟩
    rcases badIdxOf_spec hp h hn with ⟨_, qn, hgetn, hqn1, _, _⟩
    have hgetEq : ms[badIdxOf p hp h m]? = ms[badIdxOf p hp h n]? := by
      simpa [bi] using congrArg (fun i => ms[i]?) hidx
    have hqEq : qm = qn := by
      rw [hgetm, hgetn] at hgetEq
      exact Option.some.inj hgetEq
    have hq1 : qm.1 = qn.1 := congrArg Prod.fst hqEq
    exact cms_disjoint hp hm hn hqm1 (by simpa [hq1] using hqn1)
  have hcross : ∀ a ∈ (changedList p).map gi, ∀ b ∈ (cms p hp).map bi, a ≠ b := by
    intro a ha b hb hidxAB
    rcases List.mem_map.mp ha with ⟨j, hj, haj⟩
    rcases List.mem_map.mp hb with ⟨m, hm, hbm⟩
    have hidxEq : gi j = bi m := haj.trans (hidxAB.trans hbm.symm)
    rcases goodIdxOf_spec h hj with ⟨_, qg, hgetg, hqg2, _, sBg, hpreG, hvalG⟩
    rcases badIdxOf_spec hp h hm with ⟨_, qb, hgetb, hqb1, hqb2, sBb, hpreB, hvalB⟩
    have hgetEq : ms[goodIdxOf p h j]? = ms[badIdxOf p hp h m]? := by
      simpa [gi, bi] using congrArg (fun i => ms[i]?) hidxEq
    have hqEq : qg = qb := by
      rw [hgetg, hgetb] at hgetEq
      exact Option.some.inj hgetEq
    have hpreB' : run ((List.range p.length).map some ++ [none])
        (ms.take (goodIdxOf p h j)) = some sBb := by
      simpa [gi, bi, hidxEq] using hpreB
    have hprefEq : sBg = sBb := Option.some.inj (hpreG.symm.trans hpreB')
    have hvalG' : sBb[qb.1]? = some (some (p.get ⟨j, (changedList_mem hj).1⟩)) := by
      simpa [← hprefEq, hqEq] using hvalG
    have hvalEq : p.get ⟨j, (changedList_mem hj).1⟩ = qb.1 := by
      have hs : some (some (p.get ⟨j, (changedList_mem hj).1⟩)) = some (some qb.1) :=
        hvalG'.symm.trans hvalB
      exact Option.some.inj (Option.some.inj hs)
    obtain ⟨mj, hmj, hjorbitMj⟩ := changed_exists_cms hp (changedList_mem hj).1 (changedList_mem hj).2
    have hpjOrbitMj : p.get ⟨j, (changedList_mem hj).1⟩ ∈ orbit p p.length mj := by
      have hs := step_mem_orbit hp (cms_mem hp hmj |>.1) hjorbitMj
      simpa [step, List.getElem?_eq_getElem (changedList_mem hj).1] using hs
    have hpjOrbitM : p.get ⟨j, (changedList_mem hj).1⟩ ∈ orbit p p.length m := by
      rw [hvalEq]
      exact hqb1
    have hmMjEq : mj = m := cms_disjoint hp hmj hm hpjOrbitMj hpjOrbitM
    have hjOrbitM : j ∈ orbit p p.length m := by simpa [hmMjEq] using hjorbitMj
    have hqb2j : qb.2 = j := by simpa [hqEq] using hqg2
    exact hqb2 (by simpa [hqb2j] using hjOrbitM)
  let idxs := (changedList p).map gi ++ (cms p hp).map bi
  have hnodG : ((changedList p).map gi).Nodup :=
    nodup_map_of_injective (changedList_nodup p) hgoodInj
  have hnodB : ((cms p hp).map bi).Nodup :=
    nodup_map_of_injective (cms_nodup hp) hbadInj
  have hnod : idxs.Nodup := by
    change ((changedList p).map gi ++ (cms p hp).map bi).Nodup
    exact List.nodup_append.mpr ⟨hnodG, hnodB, hcross⟩
  have hall : ∀ x ∈ idxs, x < ms.length := by
    intro x hx
    change x ∈ (changedList p).map gi ++ (cms p hp).map bi at hx
    rw [List.mem_append] at hx
    rcases hx with hx | hx
    · rcases List.mem_map.mp hx with ⟨j, hj, rfl⟩
      exact (goodIdxOf_spec h hj).1
    · rcases List.mem_map.mp hx with ⟨m, hm, rfl⟩
      exact (badIdxOf_spec hp h hm).1
  have hle : idxs.length ≤ ms.length := nodup_length_le_of_lt hnod hall
  have hidxLen : idxs.length = (changedList p).length + (cms p hp).length := by
    simp [idxs]
  rw [budget_eq hp]
  omega

theorem sound : TinylambOpt.ResearchGoals.RestartRaces.Catalog.RenameMoves.Goal := by
  intro p hp
  dsimp
  constructor
  · simpa [foldlM_eq_run] using (upper_exists hp)
  · intro ms hworks
    have hrun : run ((List.range p.length).map some ++ [none]) ms =
        some (p.map some ++ [none]) := by
      simpa [foldlM_eq_run] using hworks
    exact lower_bound hp hrun
