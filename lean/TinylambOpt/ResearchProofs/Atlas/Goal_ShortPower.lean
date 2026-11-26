import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Grammar.ShortPower` — `∀ s p n, s ≠ [] → p ≠ [] → let period := s.length let probe
  := expand (.repeat (p.length/period+2) (.literal s)) (positions (expand (.repeat n
  (.literal s))) p).length = ((List.range period).map (fun i => if p.isPrefixOf
  (probe.drop i) && i+p.length ≤ n*period then (n*period-i-p.length)/period+1 else
  0)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ShortPower

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Grammar

/-! Exhaustive check over short binary words and small repetition counts: the
listed value is `false` when no counterexample occurs, confirming the goal. -/
#eval
  let check (s p : Word) (n : Nat) : Bool :=
    let period := s.length
    let probe := expand (.repeat (p.length / period + 2) (.literal s))
    (positions (expand (.repeat n (.literal s))) p).length ==
      ((List.range period).map (fun i =>
        if p.isPrefixOf (probe.drop i) && i + p.length ≤ n * period
        then (n * period - i - p.length) / period + 1 else 0)).sum
  let words : List Word :=
    [[0], [1], [0,1], [1,0], [0,0], [1,1], [0,1,1], [1,0,1], [0,0,1], [1,0,0]]
  (words.flatMap (fun s => words.flatMap (fun p =>
    (List.range 7).map (fun n => check s p n)))).contains false

private def hit (s p : Word) (i : Nat) : Bool := p.isPrefixOf (s.drop i)

private theorem and_true {a b : Bool} (h : a && b = true) : a = true ∧ b = true := by
  rw [Bool.and_eq_true] at h
  exact h

private theorem hit_iff (s p : Word) (i : Nat) :
    hit s p i = true ↔ ∃ t : Word, p ++ t = s.drop i := by
  simp [hit, List.isPrefixOf_iff_prefix, List.IsPrefix]

private theorem hit_length (s p : Word) (i : Nat) (hp : p ≠ []) (h : hit s p i = true) :
    i + p.length ≤ s.length := by
  rcases (hit_iff s p i).mp h with ⟨t, ht⟩
  have hlen := congrArg List.length ht
  simp at hlen
  have hppos : 0 < p.length := by
    cases p with
    | nil => simp at hp
    | cons x xs => simp
  omega

private theorem isPrefixOf_eq_take (p s : Word) (hle : p.length ≤ s.length) :
    p.isPrefixOf s = true ↔ s.take p.length = p := by
  constructor
  · intro h
    rcases (List.isPrefixOf_iff_prefix.mp h) with ⟨t, ht⟩
    have hd : s.drop p.length = t := by
      rw [← ht]
      simp
    rw [← ht]
    simp [List.take_append_of_le_length (Nat.le_refl p.length)]
  · intro h
    apply List.isPrefixOf_iff_prefix.mpr
    refine ⟨s.drop p.length, ?_⟩
    calc
      p ++ s.drop p.length = s.take p.length ++ s.drop p.length := by rw [h]
      _ = s := List.take_append_drop p.length s

private theorem positions_length_count (s p : Word) :
    (positions s p).length = (List.range (s.length + 1)).countP (hit s p) := by
  unfold positions hit
  rw [← List.countP_eq_length_filter]

private theorem length_expand : ∀ g : Text, length g = (expand g).length
  | .literal s => rfl
  | .concat a b => by simp [length, expand, length_expand a, length_expand b]
  | .repeat n a => by simp [length, expand, length_expand a]

private theorem get_replicate_flatten (xs : Word) (n i : Nat) :
    ((List.replicate n xs).flatten)[i]? =
      if i < n * xs.length then xs[i % xs.length]? else none := by
  induction n generalizing i with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons]
      rw [List.getElem?_append]
      by_cases hx : xs.length = 0
      · have hnil : xs = [] := List.eq_nil_of_length_eq_zero hx
        subst xs
        simp
      · by_cases hi : i < xs.length
        · have hpos : 0 < n + 1 := by omega
          have hleL : xs.length ≤ (n + 1) * xs.length := Nat.le_mul_of_pos_left xs.length hpos
          have hlt : i < (n + 1) * xs.length := by omega
          simp [hi, hlt, Nat.mod_eq_of_lt hi]
        · have hge : xs.length ≤ i := by omega
          rw [if_neg hi]
          have hsucc : (n + 1) * xs.length = n * xs.length + xs.length := by
            rw [Nat.succ_mul]
          have hcond : (i < (n + 1) * xs.length) ↔ (i - xs.length < n * xs.length) := by
            rw [hsucc]
            omega
          have hiadd : xs.length + (i - xs.length) = i := by omega
          have hmod : i % xs.length = (i - xs.length) % xs.length := by
            rw [← hiadd, Nat.add_mod]
            simp [Nat.mod_mod]
          by_cases hlt : i < (n + 1) * xs.length
          · have hlt' : i - xs.length < n * xs.length := hcond.mp hlt
            simpa [hlt, hlt', hmod] using (ih (i - xs.length))
          · have hlt' : ¬ i - xs.length < n * xs.length := by
              intro h; exact hlt (hcond.mpr h)
            simpa [hlt, hlt'] using (ih (i - xs.length))

private theorem rep_length (s : Word) (n : Nat) :
    (expand (.repeat n (.literal s))).length = n * s.length := by
  rw [← length_expand]
  simp [length]

private theorem take_eq_of_getElem?_eq (xs ys : Word) (m : Nat)
    (h : ∀ k, k < m → xs[k]? = ys[k]?) : xs.take m = ys.take m := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [List.take_add_one, List.take_add_one]
      rw [ih (fun k hk => h k (by omega))]
      rw [h m (by omega)]

private theorem probe_period_bound (s p : Word) (hs : s ≠ []) :
    p.length + s.length ≤
      (p.length / s.length + 2) * s.length := by
  have hL : 0 < s.length := by
    cases s with
    | nil => simp at hs
    | cons x xs => simp
  have hq : (p.length / s.length + 2) * s.length =
      (p.length / s.length) * s.length + s.length + s.length := by
    rw [Nat.add_mul]
    rw [show 2 * s.length = s.length + s.length by omega]
    omega
  rw [hq]
  have hlt := Nat.lt_div_mul_add (a := p.length) hL
  omega

private theorem probe_enough (s p : Word) (hs : s ≠ []) (hp : p ≠ [])
    (i : Nat) (hi : i < s.length) :
    p.length ≤ ((expand (.repeat (p.length / s.length + 2) (.literal s))).drop i).length := by
  simp [List.length_drop, rep_length]
  have hb := probe_period_bound s p hs
  omega

private theorem rep_take_eq_probe (s p : Word) (hs : s ≠ []) (hp : p ≠ []) (n j : Nat)
    (hj : j + p.length ≤ n * s.length) :
    ((expand (.repeat n (.literal s))).drop j).take p.length =
      ((expand (.repeat (p.length / s.length + 2) (.literal s))).drop (j % s.length)).take p.length := by
  have hL : 0 < s.length := by
    cases s with
    | nil => simp at hs
    | cons x xs => simp
  apply take_eq_of_getElem?_eq
  intro k hk
  rw [List.getElem?_drop, List.getElem?_drop]
  dsimp [expand]
  rw [get_replicate_flatten, get_replicate_flatten]
  have hjlt : j + k < n * s.length := by omega
  have hqb := probe_period_bound s p hs
  have hqbound : j % s.length + k < (p.length / s.length + 2) * s.length := by
    have hmodlt : j % s.length < s.length := Nat.mod_lt _ hL
    omega
  rw [if_pos hjlt, if_pos hqbound]
  have hmod : (j + k) % s.length = (j % s.length + k) % s.length := by
    rw [Nat.add_mod, Nat.add_mod]
    simp [Nat.mod_mod]
  rw [hmod]

private theorem rep_hit_iff (s p : Word) (hs : s ≠ []) (hp : p ≠ []) (n j : Nat) :
    hit (expand (.repeat n (.literal s))) p j = true ↔
      j + p.length ≤ n * s.length ∧
        hit (expand (.repeat (p.length / s.length + 2) (.literal s))) p (j % s.length) = true := by
  have hrepLen := rep_length s n
  have hmodlt : j % s.length < s.length := Nat.mod_lt _ (by
    cases s with
    | nil => simp at hs
    | cons x xs => simp)
  constructor
  · intro h
    have hlen := hit_length (expand (.repeat n (.literal s))) p j hp h
    have hlen' : j + p.length ≤ n * s.length := by simpa [hrepLen] using hlen
    have hleR : p.length ≤ ((expand (.repeat n (.literal s))).drop j).length := by
      simp [hrepLen]
      omega
    have htR : ((expand (.repeat n (.literal s))).drop j).take p.length = p :=
      (isPrefixOf_eq_take p _ hleR).mp (by simpa [hit] using h)
    have htP : ((expand (.repeat (p.length / s.length + 2) (.literal s))).drop
        (j % s.length)).take p.length = p := by
      calc
        ((expand (.repeat (p.length / s.length + 2) (.literal s))).drop
            (j % s.length)).take p.length =
            ((expand (.repeat n (.literal s))).drop j).take p.length :=
          (rep_take_eq_probe s p hs hp n j hlen').symm
        _ = p := htR
    have hleP := probe_enough s p hs hp (j % s.length) hmodlt
    constructor
    · exact hlen'
    · exact (isPrefixOf_eq_take p _ hleP).mpr htP
  · rintro ⟨hlen, hprobe⟩
    have hleP := probe_enough s p hs hp (j % s.length) hmodlt
    have htP : ((expand (.repeat (p.length / s.length + 2) (.literal s))).drop
        (j % s.length)).take p.length = p :=
      (isPrefixOf_eq_take p _ hleP).mp (by simpa [hit] using hprobe)
    have htR : ((expand (.repeat n (.literal s))).drop j).take p.length = p := by
      calc
        ((expand (.repeat n (.literal s))).drop j).take p.length =
            ((expand (.repeat (p.length / s.length + 2) (.literal s))).drop
              (j % s.length)).take p.length :=
          rep_take_eq_probe s p hs hp n j hlen
        _ = p := htP
    have hleR : p.length ≤ ((expand (.repeat n (.literal s))).drop j).length := by
      simp [hrepLen]
      omega
    exact (isPrefixOf_eq_take p _ hleR).mpr htR

private theorem countP_eq_sum_map (l : List Nat) (P : Nat → Bool) :
    l.countP P = (l.map (fun i => if P i then 1 else 0)).sum := by
  induction l with
  | nil => simp
  | cons i is ih =>
      by_cases h : P i = true <;> simp [h, ih, Nat.add_comm]

private theorem sum_map_add (xs : List Nat) (f g : Nat → Nat) :
    (xs.map (fun i => f i + g i)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]

private theorem countP_range_succ (n : Nat) (Q : Nat → Bool) :
    (List.range (n + 1)).countP Q = (List.range n).countP Q + (if Q n then 1 else 0) := by
  rw [List.range_succ, List.countP_append]
  by_cases h : Q n = true <;> simp [h]

private theorem countP_le_range (c n : Nat) (hc : c < n) :
    (List.range n).countP (fun k => decide (k ≤ c)) = c + 1 := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [countP_range_succ]
      by_cases hcn : c = n
      · subst c
        have hall0 : (List.range n).countP (fun k => decide (k ≤ n)) = (List.range n).length := by
          apply List.countP_eq_length.mpr
          intro k hk
          simp only [List.mem_range] at hk
          simp [hk]
          omega
        have hall : (List.range n).countP (fun k => decide (k ≤ n)) = n := by
          simpa using hall0
        rw [hall]
        simp
      · have hc' : c < n := by omega
        rw [ih hc']
        have hfalse : (decide (n ≤ c)) = false := by simp; omega
        simp [hfalse]

private theorem sum_map_zero (xs : List Nat) : (xs.map fun _ => 0).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem countP_range_mul (n L : Nat) (P : Nat → Bool) :
    (List.range (n * L)).countP P =
      ((List.range L).map (fun i => (List.range n).countP (fun k => P (i + k * L)))).sum := by
  induction n with
  | zero =>
      simp [sum_map_zero]
  | succ n ih =>
      rw [Nat.succ_mul, List.range_add, List.countP_append, List.countP_map, ih]
      have hnew :
          (List.range L).countP (P ∘ fun x => n * L + x) =
            ((List.range L).map (fun i => if P (i + n * L) then 1 else 0)).sum := by
        rw [countP_eq_sum_map]
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        simp only [Function.comp_apply]
        simp [Nat.add_comm]
      rw [hnew]
      have hsum :
          ((List.range L).map
              (fun i => (List.range (n + 1)).countP (fun k => P (i + k * L)))).sum =
            ((List.range L).map (fun i => (List.range n).countP (fun k => P (i + k * L)))).sum +
              ((List.range L).map (fun i => if P (i + n * L) then 1 else 0)).sum := by
        rw [← sum_map_add]
        apply congrArg List.sum
        apply List.map_congr_left
        intro i hi
        rw [countP_range_succ]
      rw [hsum]

private theorem count_steps (n L i m : Nat) (hL : 0 < L) (hi : i < L) (hm : 0 < m) :
    (List.range n).countP (fun k => decide (i + k * L + m ≤ n * L)) =
      if i + m ≤ n * L then (n * L - i - m) / L + 1 else 0 := by
  by_cases hle : i + m ≤ n * L
  · simp [hle]
    let T := n * L - i - m
    have hTlt : T < n * L := by
      dsimp [T]
      omega
    have hTdiv : T / L < n := (Nat.div_lt_iff_lt_mul hL).mpr hTlt
    have hcount := countP_le_range (T / L) n hTdiv
    rw [← hcount]
    apply List.countP_congr
    intro k hk
    have hklt : k < n := by simpa [List.mem_range] using hk
    have hiff : (i + k * L + m ≤ n * L) ↔ k ≤ T / L := by
      constructor
      · intro h
        apply (Nat.le_div_iff_mul_le hL).mpr
        dsimp [T]
        omega
      · intro h
        have hmul : k * L ≤ T := (Nat.le_div_iff_mul_le hL).mp h
        dsimp [T] at hmul
        omega
    by_cases hcond : i + k * L + m ≤ n * L
    · have hkle : k ≤ T / L := hiff.mp hcond
      simp [hcond, hkle]
    · have hkle : ¬ k ≤ T / L := by
        intro hk'
        exact hcond (hiff.mpr hk')
      simp [hcond, hkle]
  · simp [hle]
    intro a ha
    have halt : a < n := by simpa using ha
    simp
    omega

theorem sound_ShortPower : Grammar.ShortPower.Goal := by
  intro s p n hs hp
  dsimp only
  let L := s.length
  let m := p.length
  let q := m / L + 2
  let probe := expand (.repeat q (.literal s))
  let repN := expand (.repeat n (.literal s))
  have hLpos : 0 < L := by
    dsimp [L]
    cases s with
    | nil => simp at hs
    | cons x xs => simp
  have hmpos : 0 < m := by
    dsimp [m]
    cases p with
    | nil => simp at hp
    | cons x xs => simp
  have htotal : (positions repN p).length = (List.range (n * L)).countP (hit repN p) := by
    rw [positions_length_count]
    have hrepLen : repN.length = n * L := by
      dsimp [repN]
      exact rep_length s n
    rw [hrepLen]
    rw [List.range_succ, List.countP_append]
    have hlast : hit repN p (n * L) = false := by
      rw [← hrepLen]
      unfold hit
      cases p with
      | nil => simp at hp
      | cons x xs => simp
    simp [hlast]
  have hphases :
      (List.range (n * L)).countP (hit repN p) =
        ((List.range L).map (fun i =>
          (List.range n).countP (fun k => decide (i + k * L + m ≤ n * L) && hit probe p i))).sum := by
    rw [countP_range_mul]
    apply congrArg List.sum
    apply List.map_congr_left
    intro i hi
    have hiL : i < L := by simpa [List.mem_range] using hi
    apply List.countP_congr
    intro k hk
    have hklt : k < n := by simpa [List.mem_range] using hk
    have hkn : k + 1 ≤ n := Nat.succ_le_of_lt hklt
    have hmul : (k + 1) * L ≤ n * L := Nat.mul_le_mul_right L hkn
    have hjlt : i + k * L < n * L := by
      rw [Nat.succ_mul] at hmul
      omega
    have hmod : (i + k * L) % L = i := by
      rw [Nat.add_mul_mod_self_right]
      exact Nat.mod_eq_of_lt hiL
    have hh := rep_hit_iff s p hs hp n (i + k * L)
    constructor
    · intro h
      have hb := hh.mp h
      have hb2 : hit probe p i = true := by simpa [probe, q, L, hmod] using hb.2
      have hb1 : i + k * L + m ≤ n * L := by simpa [L, m] using hb.1
      simp [hb1, hb2]
    · intro h
      have hb1 : i + k * L + m ≤ n * L := by
        have hcond := (and_true h).1
        exact decide_eq_true_eq.mp hcond
      have hb2 : hit probe p i = true := by
        exact (and_true h).2
      exact hh.mpr ⟨hb1, by simpa [probe, q, L, hmod] using hb2⟩
  have hphase2 :
      ((List.range L).map (fun i =>
          (List.range n).countP (fun k => decide (i + k * L + m ≤ n * L) && hit probe p i))).sum =
        ((List.range L).map (fun i =>
          if hit probe p i && i + m ≤ n * L then (n * L - i - m) / L + 1 else 0)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro i hi
    have hiL : i < L := by simpa [List.mem_range] using hi
    by_cases hprobe : hit probe p i = true
    · have hc := count_steps n L i m hLpos hiL hmpos
      have hc' : (List.range n).countP
          (fun k => decide (i + k * L + m ≤ n * L) && hit probe p i) =
          (List.range n).countP (fun k => decide (i + k * L + m ≤ n * L)) := by
        apply List.countP_congr
        intro k hk
        simp [hprobe]
      rw [hc', hc]
      by_cases hle : i + m ≤ n * L <;> simp [hprobe, hle]
    · have hz : (List.range n).countP
          (fun k => decide (i + k * L + m ≤ n * L) && hit probe p i) = 0 := by
        apply List.countP_eq_zero.mpr
        intro k hk
        simp [hprobe]
      rw [hz]
      simp [hprobe]
  rw [htotal, hphases, hphase2]
  simp [L, m, q, probe, hit, List.isPrefixOf_iff_prefix]

end TinylambOpt.ResearchProofs.Atlas.Goal_ShortPower
