import TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

/-! Generic machinery for the `CalendarRanges` research batch.  A model whose
`start` has a proper cycle and is strictly increasing inside one cycle has a
correct `fast` lookup and satisfies the five sound contracts.  The two
"optimisation" contracts (`constantWidth`, `averageDivision`) are refuted per
model, since none of the ten calendars has a constant bucket width. -/

namespace TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges

open TinylambOpt.ResearchGoals.LayerContracts.CalendarRanges

/-! -------------------- 1. Binary search  -------------------- -/

def seekStep (mod : Model) (d : Nat) (s : Nat × Nat) : Nat × Nat :=
  let mid := (s.1 + s.2) / 2
  if mod.start mid ≤ d then (mid, s.2) else (s.1, mid)

def seekLoop (mod : Model) (d : Nat) : Nat → Nat × Nat → Nat × Nat
  | 0, s => s
  | k+1, s => seekLoop mod d k (seekStep mod d s)

theorem seekLoop_step_commute (mod : Model) (d : Nat) : ∀ (j : Nat) (s : Nat × Nat),
    seekLoop mod d j (seekStep mod d s) = seekStep mod d (seekLoop mod d j s) := by
  intro j
  induction j with
  | zero => intro s; rfl
  | succ j ih =>
      intro s
      simp only [seekLoop]
      rw [ih]

theorem forIn_seek_foldl (mod : Model) (d : Nat) : ∀ (l : List Nat) (s : Nat × Nat),
     Id.run (forIn l s (fun _ t =>
        if mod.start ((t.1+t.2)/2) ≤ d then pure (ForInStep.yield (((t.1+t.2)/2), t.2))
        else pure (ForInStep.yield (t.1, ((t.1+t.2)/2))))) =
       l.foldl (fun t _ => if mod.start ((t.1+t.2)/2) ≤ d then (((t.1+t.2)/2), t.2) else (t.1, ((t.1+t.2)/2))) s := by
    intro l
    induction l with
    | nil => intro s; rfl
    | cons x xs ih =>
       intro s
       by_cases h : mod.start ((s.1+s.2)/2) ≤ d <;> simp [h, ih]

theorem foldl_seek_loop (mod : Model) (d : Nat) : ∀ (k : Nat) (s : Nat × Nat),
    (List.range' 0 k).foldl (fun t _ => if mod.start ((t.1+t.2)/2) ≤ d then (((t.1+t.2)/2), t.2) else (t.1, ((t.1+t.2)/2))) s = seekLoop mod d k s := by
    intro k
    induction k with
    | zero => intro s; rfl
    | succ k ih =>
       intro s
       rw [show List.range' 0 (k+1) = List.range' 0 k ++ [k] by
         rw [← List.range_eq_range', ← List.range_eq_range']
         exact List.range_succ]
       rw [List.foldl_append]
       simp [ih]
       exact (seekLoop_step_commute mod d k s).symm

theorem seek_eq_loop (mod : Model) (d last : Nat) :
    seek mod d last = (seekLoop mod d ((last+1).log2+1) (0,last+1)).1 := by
  unfold seek
  simp
  rw [forIn_seek_foldl]
  rw [foldl_seek_loop]

theorem pow_succ_two (j : Nat) : 2^(j+1) = 2 * 2^j := by
  simpa only [Nat.succ_eq_add_one] using (Nat.pow_succ' (m:=2) (n:=j))

theorem log2_bound (n : Nat) : 0 < n → n < 2^(n.log2+1) := by
  intro hn
  have hlt : n.log2 < n.log2 + 1 := by omega
  exact (Nat.log2_lt (by omega : n ≠ 0)).mp hlt

theorem seekLoop_width (mod : Model) (d : Nat) : ∀ (j : Nat) (lo hi : Nat),
    lo ≤ hi → hi ≤ mod.bins → mod.start lo ≤ d → d < mod.start hi →
    let s := seekLoop mod d j (lo,hi)
    mod.start s.1 ≤ d ∧ d < mod.start s.2 ∧ s.1 ≤ s.2 ∧ s.2 ≤ mod.bins ∧
      2^j * (s.2 - s.1) ≤ (hi - lo) + 2^j - 1 := by
  intro j
  induction j with
  | zero =>
      intro lo hi hlo hhi hstartlo hstarthi
      simp [seekLoop]
      exact ⟨hstartlo, hstarthi, hlo, hhi⟩
  | succ j ih =>
      intro lo hi hlo hhi hstartlo hstarthi
      simp only [seekLoop]
      let mid := (lo + hi) / 2
      have hmidlo : lo ≤ mid := by unfold mid; omega
      have hmidhi : mid ≤ hi := by unfold mid; omega
      have hle : 2 * (hi - mid) ≤ (hi - lo) + 1 := by unfold mid; omega
      have hle' : 2 * (mid - lo) ≤ (hi - lo) + 1 := by unfold mid; omega
      by_cases h : mod.start mid ≤ d
      · have hs := ih mid hi hmidhi hhi h (by omega : d < mod.start hi)
        dsimp at hs
        rw [show seekStep mod d (lo,hi) = (mid,hi) by simp [seekStep, mid, h]]
        refine ⟨hs.1, hs.2.1, hs.2.2.1, hs.2.2.2.1, ?_⟩
        rw [pow_succ_two, Nat.mul_assoc]
        omega
      · have hnlt : d < mod.start mid := by omega
        have hs := ih lo mid hmidlo (by omega : mid ≤ mod.bins) hstartlo hnlt
        dsimp at hs
        rw [show seekStep mod d (lo,hi) = (lo,mid) by simp [seekStep, mid, h]]
        refine ⟨hs.1, hs.2.1, hs.2.2.1, hs.2.2.2.1, ?_⟩
        rw [pow_succ_two, Nat.mul_assoc]
        omega

theorem seek_spec (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (d : Nat) (hd : d < mod.days) :
    let i := seek mod d (mod.bins-1)
    mod.start i ≤ d ∧ d < mod.start (i+1) := by
  have hstartbins : mod.start mod.bins = mod.days := by
    have h := hcyc 0
    simpa [h0] using h
  have hlast : mod.bins - 1 + 1 = mod.bins := by omega
  rw [seek_eq_loop, hlast]
  let K := mod.bins.log2 + 1
  have hK : mod.bins < 2^K := by
    dsimp [K]
    exact log2_bound mod.bins hbins
  have hp : 0 < 2^K := Nat.pow_pos (by omega : 0 < (2:Nat))
  have hwidth := seekLoop_width mod d K 0 mod.bins (by omega) (by omega)
      (by simp [h0]) (by simpa [hstartbins] using hd)
  dsimp at hwidth
  have hbound : 2^K * ((seekLoop mod d K (0, mod.bins)).snd - (seekLoop mod d K (0, mod.bins)).fst) ≤ 2^K + 2^K - 2 := by
    exact Nat.le_trans hwidth.2.2.2.2 (by omega)
  have hwle2 : (seekLoop mod d K (0, mod.bins)).snd - (seekLoop mod d K (0, mod.bins)).fst ≤ 2 := by
    apply Nat.le_of_mul_le_mul_left (c := 2^K) _ hp
    omega
  have hwcases : (seekLoop mod d K (0, mod.bins)).snd - (seekLoop mod d K (0, mod.bins)).fst = 0 ∨
      (seekLoop mod d K (0, mod.bins)).snd - (seekLoop mod d K (0, mod.bins)).fst = 1 ∨
      (seekLoop mod d K (0, mod.bins)).snd - (seekLoop mod d K (0, mod.bins)).fst = 2 := by omega
  rcases hwcases with hw0 | hw1 | hw2
  · exfalso
    have heq : (seekLoop mod d K (0, mod.bins)).snd = (seekLoop mod d K (0, mod.bins)).fst := by omega
    rw [heq] at hwidth
    omega
  · have hs2 : (seekLoop mod d K (0, mod.bins)).snd = (seekLoop mod d K (0, mod.bins)).fst + 1 := by omega
    exact ⟨hwidth.1, by simpa [hs2] using hwidth.2.1⟩
  · exfalso
    have : 2^K * 2 ≤ 2^K + 2^K - 2 := by simpa [hw2] using hbound
    rw [Nat.mul_two] at this
    omega

/-! -------------------- 2. Cycle reduction and monotonicity  -------------------- -/

theorem start_add_cycle (mod : Model) (hcyc : cycleShift mod) :
    ∀ k i, mod.start (i + k * mod.bins) = mod.start i + k * mod.days := by
  intro k
  induction k with
  | zero => intro i; simp
  | succ k ih =>
      intro i
      have h := hcyc (i + k * mod.bins)
      have hi := ih i
      have hidx : i + (k+1)*mod.bins = (i + k*mod.bins) + mod.bins := by
        rw [Nat.succ_mul]
        omega
      rw [hidx, h, hi]
      rw [Nat.succ_mul]
      omega

theorem start_cycle (mod : Model) (hcyc : cycleShift mod) (q j : Nat) :
    mod.start (q * mod.bins + j) = mod.start j + q * mod.days := by
  rw [Nat.add_comm]
  exact start_add_cycle mod hcyc q j

theorem start_mono_local (mod : Model) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    ∀ a b, a ≤ b → b ≤ mod.bins → mod.start a ≤ mod.start b := by
  intro a b
  induction b generalizing a with
  | zero =>
      intro hab hb
      have : a = 0 := by omega
      subst a
      omega
  | succ b ih =>
      intro hab hb
      by_cases hbnd : b < mod.bins
      · by_cases ha : a ≤ b
        · exact Nat.le_trans (ih a ha (by omega)) (Nat.le_of_lt (hmono b hbnd))
        · have : a = b + 1 := by omega
          subst a
          omega
      · omega

theorem start_lt_days (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod) (hbins : 0 < mod.bins)
    (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) (i : Nat) (hi : i < mod.bins) :
    mod.start i < mod.days := by
  have hle := start_mono_local mod hmono i (mod.bins-1) (by omega) (by omega)
  have htail := hmono (mod.bins-1) (by omega : mod.bins - 1 < mod.bins)
  have hidx : mod.bins - 1 + 1 = mod.bins := by omega
  rw [hidx] at htail
  have hc := hcyc 0
  simp [h0] at hc
  omega

theorem start_strict_mono (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    ∀ i, mod.start i < mod.start (i+1) := by
  intro i
  let q := i / mod.bins
  let j := i % mod.bins
  have hj : j < mod.bins := by dsimp [j]; exact Nat.mod_lt i hbins
  have hi : i = q * mod.bins + j := by
    dsimp [j, q]
    calc
      i = mod.bins * (i / mod.bins) + i % mod.bins := (Nat.div_add_mod i mod.bins).symm
      _ = (i / mod.bins) * mod.bins + i % mod.bins := by rw [Nat.mul_comm]
  by_cases hj1 : j + 1 < mod.bins
  · rw [hi]
    have hcy1 := start_cycle mod hcyc q j
    have hidx : q * mod.bins + j + 1 = q * mod.bins + (j+1) := by omega
    rw [hidx]
    have hcy2 := start_cycle mod hcyc q (j+1)
    have hm := hmono j (by omega : j < mod.bins)
    omega
  · have hjlast : j = mod.bins - 1 := by omega
    have hi' : i = q * mod.bins + (mod.bins-1) := by omega
    rw [hi']
    have hi1 : q * mod.bins + (mod.bins - 1) + 1 = (q+1)*mod.bins + 0 := by
      rw [Nat.succ_mul]
      omega
    rw [hi1]
    have hcy1 := start_cycle mod hcyc q (mod.bins-1)
    have hcy2 := start_cycle mod hcyc (q+1) 0
    have hlt := start_lt_days mod h0 hcyc hbins hmono (mod.bins-1) (by omega)
    have hm1 : (q+1)*mod.days = q*mod.days + mod.days := by rw [Nat.succ_mul]
    omega

theorem start_mono (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod) (hbins : 0 < mod.bins)
    (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    ∀ a b, a ≤ b → mod.start a ≤ mod.start b := by
  have hstrict := start_strict_mono mod h0 hcyc hbins hmono
  intro a b
  induction b generalizing a with
  | zero =>
      intro hab
      have : a = 0 := by omega
      subst a
      omega
  | succ b ih =>
      intro hab
      by_cases ha : a ≤ b
      · exact Nat.le_trans (ih a ha) (Nat.le_of_lt (hstrict b))
      · have : a = b + 1 := by omega
        subst a
        omega

theorem hposdays_of (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod) (hbins : 0 < mod.bins)
    (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) : 0 < mod.days := by
  have hstrict : 0 < mod.start 1 := by simpa [h0] using (start_strict_mono mod h0 hcyc hbins hmono 0)
  have hle := start_mono mod h0 hcyc hbins hmono 1 mod.bins (by omega)
  have hc : mod.start mod.bins = mod.days := by simpa [h0] using (hcyc 0)
  omega


/-! -------------------- 3. fast / cycle reduction  -------------------- -/

theorem fast_spec (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    ∀ d, mod.start (fast mod d) ≤ d ∧ d < mod.start (fast mod d + 1) := by
  intro d
  let q := d / mod.days
  let r := d % mod.days
  have hpos : 0 < mod.days := hposdays_of mod h0 hcyc hbins hmono
  have hr : r < mod.days := Nat.mod_lt d hpos
  have hstartbins : mod.start mod.bins = mod.days := by simpa [h0] using (hcyc 0)
  have hd : d = q*mod.days + r := by
    dsimp [q, r]
    calc
      d = mod.days * (d / mod.days) + d % mod.days := (Nat.div_add_mod d mod.days).symm
      _ = (d / mod.days) * mod.days + d % mod.days := by rw [Nat.mul_comm]
  let i := seek mod r (mod.bins-1)
  have hs : mod.start i ≤ r ∧ r < mod.start (i+1) := by
    simpa [i] using (seek_spec mod h0 hcyc hbins r hr)
  have hi : i < mod.bins := by
    by_cases hge : mod.bins ≤ i
    · have hle := start_mono mod h0 hcyc hbins hmono mod.bins i hge
      omega
    · omega
  have hfast : fast mod d = q*mod.bins + i := by
    simp [fast, q, r, i]
  have hstart := start_cycle mod hcyc q i
  rw [hfast]
  constructor
  · rw [hstart]
    omega
  · by_cases hi1 : i + 1 < mod.bins
    · have hidx : q*mod.bins + i + 1 = q*mod.bins + (i+1) := by omega
      rw [hidx, start_cycle mod hcyc q (i+1)]
      omega
    · have hilast : i = mod.bins - 1 := by omega
      have hidx : q*mod.bins + i + 1 = 0 + (q+1)*mod.bins := by
        rw [hilast, Nat.succ_mul]
        omega
      rw [hidx, start_add_cycle mod hcyc (q+1) 0]
      simp [h0]
      have hi1eq : i + 1 = mod.bins := by omega
      rw [hi1eq, hstartbins] at hs
      have hmul : (q+1)*mod.days = q*mod.days + mod.days := by rw [Nat.succ_mul]
      rw [hmul]
      omega

/-! -------------------- 4. Sound contracts  -------------------- -/

theorem gen_locate (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    locate mod := by
  exact fast_spec mod h0 hcyc hbins hmono

theorem gen_predicateRange (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    predicateRange mod := by
  intro d i
  constructor
  · intro h
    rw [← h]
    exact fast_spec mod h0 hcyc hbins hmono d
  · intro hdi
    have hs := fast_spec mod h0 hcyc hbins hmono d
    have hmon := start_mono mod h0 hcyc hbins hmono
    have hstrict := start_strict_mono mod h0 hcyc hbins hmono
    by_cases heq : fast mod d = i
    · exact heq
    · have hltgt : fast mod d < i ∨ i < fast mod d := Nat.lt_or_gt_of_ne heq
      rcases hltgt with hlt | hgt
      · have hsucc : fast mod d + 1 ≤ i := by omega
        have hle := hmon (fast mod d + 1) i hsucc
        omega
      · have hsucc : i + 1 ≤ fast mod d := by omega
        have hle := hmon (i+1) (fast mod d) hsucc
        omega

theorem gen_thirdBucket (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1))
    (q d : Nat) :
    fast mod d / 3 = q ↔ mod.start (3*q) ≤ d ∧ d < mod.start (3*q+3) := by
  have hs := fast_spec mod h0 hcyc hbins hmono d
  have hmon := start_mono mod h0 hcyc hbins hmono
  constructor
  · intro h
    have hib : 3*q ≤ fast mod d ∧ fast mod d < 3*q+3 := by omega
    constructor
    · exact Nat.le_trans (hmon (3*q) (fast mod d) hib.1) hs.1
    · have hsucc : fast mod d + 1 ≤ 3*q+3 := by omega
      exact Nat.lt_of_lt_of_le hs.2 (hmon (fast mod d + 1) (3*q+3) hsucc)
  · intro h
    have hi_low : 3*q ≤ fast mod d := by
      by_cases hlt : fast mod d < 3*q
      · have hsucc : fast mod d + 1 ≤ 3*q := by omega
        have hle := hmon (fast mod d + 1) (3*q) hsucc
        omega
      · omega
    have hi_high : fast mod d < 3*q+3 := by
      by_cases hlt : 3*q+3 ≤ fast mod d
      · have hle := hmon (3*q+3) (fast mod d) hlt
        omega
      · omega
    omega

theorem gen_predecessor (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    predecessor mod := by
  intro i
  let d := mod.start (i+1) - 1
  have hstrict := start_strict_mono mod h0 hcyc hbins hmono
  have hmon := start_mono mod h0 hcyc hbins hmono
  have hs := fast_spec mod h0 hcyc hbins hmono d
  have hlti := hstrict i
  have hdle : mod.start i ≤ d := by dsimp [d]; omega
  by_cases heq : fast mod d = i
  · exact heq
  · have hltgt : fast mod d < i ∨ i < fast mod d := Nat.lt_or_gt_of_ne heq
    rcases hltgt with hlt | hgt
    · have hsucc : fast mod d + 1 ≤ i := by omega
      have hle := hmon (fast mod d + 1) i hsucc
      omega
    · have hsucc : i + 1 ≤ fast mod d := by omega
      have hle := hmon (i+1) (fast mod d) hsucc
      omega

theorem gen_cyclePopulation (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    cyclePopulation mod := by
  intro i n
  have hmon := start_mono mod h0 hcyc hbins hmono
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show List.range (n+1) = List.range n ++ [n] by exact List.range_succ]
      rw [List.map_append, List.sum_append]
      simp
      rw [ih]
      have hle1 : mod.start i ≤ mod.start (i+n) := hmon i (i+n) (by omega)
      have hle2 : mod.start (i+n) ≤ mod.start (i+n+1) := hmon (i+n) (i+n+1) (by omega)
      dsimp [span]
      have hidx : i + (n+1) = i+n+1 := by omega
      rw [hidx]
      omega

theorem fast_mono (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    ∀ d e, d ≤ e → fast mod d ≤ fast mod e := by
  intro d e hde
  have hmon := start_mono mod h0 hcyc hbins hmono
  have hs1 := fast_spec mod h0 hcyc hbins hmono d
  have hs2 := fast_spec mod h0 hcyc hbins hmono e
  by_cases heq : fast mod d = fast mod e
  · omega
  · have hne' : fast mod e ≠ fast mod d := by intro hh; exact heq hh.symm
    have hltgt : fast mod e < fast mod d ∨ fast mod d < fast mod e := Nat.lt_or_gt_of_ne hne'
    rcases hltgt with hlt | hgt
    · have hsucc : fast mod e + 1 ≤ fast mod d := by omega
      have hle := hmon (fast mod e + 1) (fast mod d) hsucc
      omega
    · omega

theorem fast_step (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    ∀ d, fast mod (d+1) ≤ fast mod d + 1 := by
  intro d
  have hmon := start_mono mod h0 hcyc hbins hmono
  have hs1 := fast_spec mod h0 hcyc hbins hmono d
  have hs2 := fast_spec mod h0 hcyc hbins hmono (d+1)
  by_cases hgt : fast mod d + 2 ≤ fast mod (d+1)
  · have hle := hmon (fast mod d + 2) (fast mod (d+1)) hgt
    have hstrict := start_strict_mono mod h0 hcyc hbins hmono
    have hle' := hstrict (fast mod d + 1)
    have heqidx : fast mod d + 1 + 1 = fast mod d + 2 := by omega
    rw [heqidx] at hle'
    omega
  · omega

theorem cover_forward (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    ∀ (lo t i : Nat), fast mod lo ≤ i ∧ i ≤ fast mod (lo+t) →
      ∃ d, lo ≤ d ∧ d < lo+t+1 ∧ fast mod d = i := by
  intro lo t
  induction t with
  | zero =>
      intro i h
      simp only [Nat.add_zero] at h
      have h' : fast mod lo = i := by omega
      exact ⟨lo, by omega, by omega, h'⟩
  | succ t ih =>
      intro i h
      rw [show lo + (t+1) = lo+t+1 by omega] at h
      rw [show lo + (t+1) + 1 = lo+t+1+1 by omega]
      by_cases hle : i ≤ fast mod (lo+t)
      · rcases ih i ⟨h.1, by simpa using hle⟩ with ⟨d, hdlo, hdhi, hfd⟩
        exact ⟨d, hdlo, by omega, hfd⟩
      · have hstep := fast_step mod h0 hcyc hbins hmono (lo+t)
        have hmono' := fast_mono mod h0 hcyc hbins hmono (lo+t) (lo+t+1) (by omega)
        have heq : i = fast mod (lo+t+1) := by omega
        exact ⟨lo+t+1, by omega, by omega, by simpa [heq]⟩

theorem gen_coverBlocks (mod : Model) (h0 : mod.start 0 = 0) (hcyc : cycleShift mod)
    (hbins : 0 < mod.bins) (hmono : ∀ i, i < mod.bins → mod.start i < mod.start (i+1)) :
    coverBlocks mod := by
  intro lo hi i hlohi
  constructor
  · intro hbounds
    let t := hi - 1 - lo
    have hhi : hi = lo + t + 1 := by dsimp [t]; omega
    rw [hhi]
    have hhi1 : hi - 1 = lo + t := by dsimp [t]; omega
    have hbnd : fast mod lo ≤ i ∧ i ≤ fast mod (lo+t) := by
      rw [← hhi1]
      simpa using hbounds
    exact cover_forward mod h0 hcyc hbins hmono lo t i hbnd
  · rintro ⟨d, hdlo, hdhi, hfd⟩
    have h1 := fast_mono mod h0 hcyc hbins hmono lo d (by omega)
    have h2 := fast_mono mod h0 hcyc hbins hmono d (hi-1) (by omega)
    omega

end TinylambOpt.ResearchProofs.LayerContracts.CalendarRanges
