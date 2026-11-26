import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.Compression.Wire
import Std

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans.Common

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## chain / power / applyT -/

theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain, List.length_map]

theorem applyT_range (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (List.range t.length) q = q := by
  show (List.range t.length)[q]?.getD q = q
  rw [List.getElem?_range (by simpa using hq)]
  rfl

theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  show (a.map (applyT b))[q]?.getD q = applyT b (a[q]?.getD q)
  rw [List.getElem?_map, List.getElem?_eq_getElem hq]
  rfl

theorem power_zero (t : Table) : power 0 t = List.range t.length := by
  rw [power, dite_eq_left rfl]

theorem power_even (n : Nat) (t : Table) (hn : n ≠ 0) (hmod : n % 2 = 0) :
    power n t = chain (power (n / 2) t) (power (n / 2) t) := by
  rw [power, dite_eq_right hn]
  rw [show (n % 2 == 0) = true from by rw [beq_iff_eq]; exact hmod]
  rfl

theorem power_odd (n : Nat) (t : Table) (hn : n ≠ 0) (hmod : n % 2 = 1) :
    power n t = chain (chain (power (n / 2) t) (power (n / 2) t)) t := by
  rw [power, dite_eq_right hn]
  rw [show (n % 2 == 0) = false from by
    have h : n % 2 = 1 := hmod
    rw [h]
    decide]
  rfl

theorem power_length (t : Table) : ∀ n, (power n t).length = t.length := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
    rcases Nat.eq_zero_or_pos n with rfl | hn
    · rw [power_zero, List.length_range]
    · have hlt : n / 2 < n := Nat.div_lt_self hn (by decide)
      rw [power, dite_eq_right (by omega : ¬n = 0)]
      rcases Nat.mod_two_eq_zero_or_one n with h | h
      · rw [show (n % 2 == 0) = true from by rw [beq_iff_eq]; exact h]
        show List.length (List.map (applyT (power (n / 2) t)) (power (n / 2) t)) = List.length t
        rw [List.length_map]
        exact ih (n / 2) hlt
      · rw [show (n % 2 == 0) = false from by
          have h2 : n % 2 = 1 := h
          rw [h2]
          decide]
        show List.length (List.map (applyT t)
            (List.map (applyT (power (n / 2) t)) (power (n / 2) t))) = List.length t
        rw [List.length_map, List.length_map]
        exact ih (n / 2) hlt

theorem compile_length {r : Reader} : ∀ x : Rope, (compile r x).length = r.states := by
  intro x
  induction x with
  | lit s => simp [compile, List.length_map, List.length_range]
  | cat a b ih1 _ => simp [compile, chain_length, ih1]
  | rep k a ih => rw [compile, power_length, ih]

theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  rw [power_zero, applyT_range _ _ hq]

/-! ## replicate / flatten / foldl helpers -/

theorem replicate_add' (a b : Nat) (x : Bytes) :
    List.replicate (a + b) x = List.replicate a x ++ List.replicate b x := by
  induction a generalizing b with
  | zero => rw [Nat.zero_add, List.replicate_zero, List.nil_append]
  | succ a ih =>
    rw [Nat.succ_add, List.replicate_succ, List.replicate_succ, ih, List.cons_append]

theorem flatten_replicate_length (y : Bytes) :
    ∀ n, ((List.replicate n y).flatten).length = n * y.length := by
  intro n
  induction n with
  | zero => rw [List.replicate_zero, List.flatten_nil, List.length_nil, Nat.zero_mul]
  | succ n ih =>
    rw [List.replicate_succ, List.flatten_cons, List.length_append, ih, Nat.succ_mul]
    omega

theorem replicate_succ_pred (m : Nat) (hm : 0 < m) (y : Bytes) :
    List.replicate m y = y :: List.replicate (m - 1) y := by
  obtain ⟨p, rfl⟩ : ∃ p, m = p + 1 := Nat.exists_eq_succ_of_ne_zero (by omega)
  rw [Nat.add_sub_cancel, List.replicate_succ]

theorem flatten_replicate_succ (y : Bytes) (n : Nat) :
    (List.replicate (n + 1) y).flatten = y ++ (List.replicate n y).flatten := by
  rw [List.replicate_succ, List.flatten_cons]

theorem flatten_replicate_rotate (y : Bytes) :
    ∀ m, y ++ (List.replicate m y).flatten = ((List.replicate m y).flatten) ++ y := by
  intro m
  induction m with
  | zero => rw [List.replicate_zero, List.flatten_nil, List.append_nil, List.nil_append]
  | succ m ih =>
      calc y ++ (List.replicate (m + 1) y).flatten
          = y ++ (y ++ (List.replicate m y).flatten) := by
              rw [List.replicate_succ, List.flatten_cons]
        _ = y ++ ((List.replicate m y).flatten ++ y) := by rw [ih]
        _ = (y ++ (List.replicate m y).flatten) ++ y := (List.append_assoc _ _ _).symm
        _ = (List.replicate (m + 1) y).flatten ++ y := by
              rw [List.replicate_succ, List.flatten_cons]

theorem take_drop_split (xs : Bytes) (a b : Nat) :
    xs.take (a + b) = xs.take a ++ (xs.drop a).take b := by
  induction xs generalizing a b with
  | nil => simp
  | cons x rest ih =>
    cases a with
    | zero => rw [Nat.zero_add, List.drop_zero, List.take_zero, List.nil_append]
    | succ a' =>
        have h1 : List.take (a'.succ + b) (x :: rest) = x :: List.take (a' + b) rest := by
          rw [Nat.succ_add, List.take_succ_cons]
        have h2 : List.take a'.succ (x :: rest) = x :: List.take a' rest := by
          rw [List.take_succ_cons]
        have h3 : List.drop a'.succ (x :: rest) = List.drop a' rest := by
          rw [List.drop_succ_cons]
        rw [h1, h2, h3, ih, ← List.cons_append]

theorem foldl_flatten_replicate_add (r : Reader) (y : Bytes) (a b : Nat) (q : Nat) :
    ((List.replicate (a + b) y).flatten).foldl r.step q =
      ((List.replicate b y).flatten).foldl r.step
        (((List.replicate a y).flatten).foldl r.step q) := by
  rw [replicate_add', List.flatten_append, List.foldl_append]

theorem foldl_replicate_fixed {r : Reader} (y : Bytes) (p : Nat) (q : Nat)
    (h : ((List.replicate p y).flatten).foldl r.step q = q) :
    ∀ m, ((List.replicate (p * m) y).flatten).foldl r.step q = q := by
  intro m
  induction m with
  | zero => rw [Nat.mul_zero, List.replicate_zero, List.flatten_nil, List.foldl_nil]
  | succ m ih =>
    rw [Nat.mul_succ, foldl_flatten_replicate_add, ih, h]

/-! ## StepBounded and compile correctness -/

def StepBounded (r : Reader) : Prop := ∀ q b, q < r.states → r.step q b < r.states

theorem foldl_step_bounded {r : Reader} (hb : StepBounded r) (bs : Bytes) (q : Nat)
    (hq : q < r.states) : bs.foldl r.step q < r.states := by
  induction bs generalizing q with
  | nil => exact hq
  | cons b rest ih =>
    rw [List.foldl_cons]
    exact ih _ (hb _ _ hq)

theorem foldl_flatten_replicate_bounded {r : Reader} (hb : StepBounded r) (n : Nat) (xs : Bytes)
    {q : Nat} (hq : q < r.states) :
    ((List.replicate n xs).flatten).foldl r.step q < r.states :=
  foldl_step_bounded hb _ _ hq

theorem applyT_compile_eq_foldl {r : Reader} (hb : StepBounded r) :
    ∀ (x : Rope) (q : Nat), q < r.states → applyT (compile r x) q = (expand x).foldl r.step q := by
  intro x
  induction x with
  | lit s =>
      intro q hq
      show ((List.range r.states).map (fun p => s.foldl r.step p))[q]?.getD q = s.foldl r.step q
      rw [List.getElem?_map, List.getElem?_range (by simpa using hq)]
      rfl
  | cat a b iha ihb =>
      intro q hq
      have hq' : applyT (compile r a) q = (expand a).foldl r.step q := iha q hq
      have hbnd : applyT (compile r a) q < r.states := by
        rw [hq']; exact foldl_step_bounded hb _ _ hq
      calc applyT (compile r (.cat a b)) q
          = applyT (compile r b) (applyT (compile r a) q) := by
              rw [show compile r (.cat a b) = chain (compile r a) (compile r b) from rfl]
              exact applyT_chain (compile r a) (compile r b) q
                (by rw [compile_length]; exact hq)
          _ = (expand b).foldl r.step (applyT (compile r a) q) := ihb _ hbnd
          _ = (expand b).foldl r.step ((expand a).foldl r.step q) := by rw [hq']
          _ = (expand a ++ expand b).foldl r.step q := by rw [List.foldl_append]
  | rep k a iha =>
      intro q hq
      have key : ∀ (n : Nat) (q : Nat), q < r.states → applyT (power n (compile r a)) q =
          ((List.replicate n (expand a)).flatten).foldl r.step q := by
        intro n
        induction n using Nat.strongRecOn with
        | ind n ihn =>
            intro q hq
            rcases Nat.eq_zero_or_pos n with rfl | hn0
            · rw [power_zero,
                applyT_range _ _ (by rw [compile_length]; exact hq),
                List.replicate_zero, List.flatten_nil, List.foldl_nil]
            · have hlt : n / 2 < n := Nat.div_lt_self hn0 (by decide)
              rcases Nat.mod_two_eq_zero_or_one n with hmod | hmod
              · -- even
                rw [power_even n (compile r a) (Nat.ne_of_gt hn0) hmod,
                  applyT_chain (power (n / 2) (compile r a)) (power (n / 2) (compile r a)) q
                    (by rw [power_length, compile_length]; exact hq)]
                have h1 : applyT (power (n / 2) (compile r a)) q =
                    ((List.replicate (n / 2) (expand a)).flatten).foldl r.step q :=
                  ihn (n / 2) hlt q hq
                rw [h1, ihn (n / 2) hlt _ (foldl_flatten_replicate_bounded hb (n / 2)
                    (expand a) hq)]
                rw [← foldl_flatten_replicate_add]
                rw [show n / 2 + n / 2 = n from by have := Nat.div_add_mod n 2; omega]
              · -- odd
                rw [power_odd n (compile r a) (Nat.ne_of_gt hn0) hmod,
                  applyT_chain (chain (power (n / 2) (compile r a)) (power (n / 2) (compile r a)))
                    (compile r a) q (by rw [chain_length, power_length, compile_length]; exact hq)]
                have hsq : applyT
                    (chain (power (n / 2) (compile r a)) (power (n / 2) (compile r a))) q =
                    ((List.replicate (n / 2 + n / 2) (expand a)).flatten).foldl r.step q := by
                  rw [applyT_chain (power (n / 2) (compile r a)) (power (n / 2) (compile r a)) q
                      (by rw [power_length, compile_length]; exact hq)]
                  rw [ihn (n / 2) hlt q hq,
                    ihn (n / 2) hlt _ (foldl_flatten_replicate_bounded hb (n / 2)
                      (expand a) hq),
                    ← foldl_flatten_replicate_add]
                rw [hsq]
                have hbnd2 :
                    ((List.replicate (n / 2 + n / 2) (expand a)).flatten).foldl r.step q < r.states :=
                  foldl_step_bounded hb _ _ hq
                rw [iha _ hbnd2]
                have hR : ∀ m, m + 1 = n → (List.replicate n (expand a)).flatten =
                    (expand a) ++ (List.replicate m (expand a)).flatten := by
                  intro m hm
                  subst hm
                  rw [flatten_replicate_succ]
                rw [hR (n / 2 + n / 2) (by have := Nat.div_add_mod n 2; omega),
                  flatten_replicate_rotate, List.foldl_append]
      rw [compile, expand]
      exact key k q hq


/-! ## Generic property proofs -/

theorem applyT_power_eq_foldl_replicate {r : Reader} (hb : StepBounded r) (x : Rope) (n q : Nat)
    (hq : q < r.states) :
    applyT (power n (compile r x)) q = ((List.replicate n (expand x)).flatten).foldl r.step q := by
  have h := applyT_compile_eq_foldl hb (.rep n x) q hq
  rw [compile, expand] at h
  exact h

theorem repeatedState_of_bounded {r : Reader} (hb : StepBounded r) : repeatedState r := by
  intro x n q hq
  exact applyT_power_eq_foldl_replicate hb x n q hq

theorem dictionaryMask_of_bounded {r : Reader} (hb : StepBounded r) : dictionaryMask r := by
  intro d ids q hq
  show ids.map (fun i =>
      ((d.map (fun x => r.accept (applyT (compile r x) q)))[i]?.getD false)) = _
  congr 1; funext i
  rw [List.getElem?_map]
  cases h : d[i]? with
  | none => simp
  | some x => simp [h, applyT_compile_eq_foldl hb x q hq]

theorem contextReuse_of_bounded {r : Reader} (hb : StepBounded r) : contextReuse r := by
  intro a b x y q hq hxy
  have h1 : (expand a).foldl r.step q < r.states := foldl_step_bounded hb _ _ hq
  have hx : (expand x).foldl r.step ((expand a).foldl r.step q) =
      applyT (compile r x) ((expand a).foldl r.step q) :=
    (applyT_compile_eq_foldl hb x _ h1).symm
  have hy : (expand y).foldl r.step ((expand a).foldl r.step q) =
      applyT (compile r y) ((expand a).foldl r.step q) :=
    (applyT_compile_eq_foldl hb y _ h1).symm
  show ((expand a ++ (expand x ++ expand b))).foldl r.step q =
      ((expand a ++ (expand y ++ expand b))).foldl r.step q
  rw [show (expand a ++ (expand x ++ expand b)).foldl r.step q =
        (expand b).foldl r.step ((expand x).foldl r.step ((expand a).foldl r.step q))
      from by rw [List.foldl_append, List.foldl_append],
    show (expand a ++ (expand y ++ expand b)).foldl r.step q =
        (expand b).foldl r.step ((expand y).foldl r.step ((expand a).foldl r.step q))
      from by rw [List.foldl_append, List.foldl_append],
    hx, hy, hxy]

theorem flatten_reverse_replicate (y : Bytes) :
    ∀ n, (List.replicate n (y.reverse)).flatten = ((List.replicate n y).flatten).reverse := by
  intro n
  induction n with
  | zero => simp only [List.replicate_zero, List.flatten_nil, List.reverse_nil]
  | succ n ih =>
      calc (List.replicate (n + 1) (y.reverse)).flatten
          = List.reverse y ++ (List.replicate n (y.reverse)).flatten := by
              rw [List.replicate_succ, List.flatten_cons]
        _ = List.reverse y ++ ((List.replicate n y).flatten).reverse := by rw [ih]
        _ = ((List.replicate n y).flatten ++ y).reverse :=
            List.reverse_append.symm
        _ = (List.replicate (n + 1) y).flatten.reverse := by
              rw [flatten_replicate_succ, ← flatten_replicate_rotate]

theorem expand_reverse : ∀ x : Rope, expand (reverse x) = (expand x).reverse := by
  intro x
  induction x with
  | lit s => rfl
  | cat a b ih1 ih2 => simp [reverse, expand, List.reverse_append, ih1, ih2]
  | rep k a ih =>
    show ((List.replicate k (expand (reverse a))).flatten) =
        ((List.replicate k (expand a)).flatten).reverse
    rw [ih, flatten_reverse_replicate]

theorem reverseScan_of_bounded {r : Reader} (hb : StepBounded r) : reverseScan r := by
  intro x q hq
  show r.accept (applyT (compile r (reverse x)) q) = _
  rw [applyT_compile_eq_foldl hb (reverse x) q hq, expand_reverse]

theorem foldl_replicate_periodic {r : Reader} (y : Bytes) (p : Nat) (q : Nat)
    (h : ((List.replicate p y).flatten).foldl r.step q = q) :
    ∀ k, ((List.replicate (k * p) y).flatten).foldl r.step q = q := by
  intro k
  induction k with
  | zero => rw [Nat.zero_mul, List.replicate_zero, List.flatten_nil, List.foldl_nil]
  | succ k ih =>
    rw [Nat.succ_mul, foldl_flatten_replicate_add, ih, h]

theorem periodJump_of_bounded {r : Reader} (hb : StepBounded r) : periodJump r := by
  intro x q period n hq hperiod hfix
  show r.accept (applyT (power n (compile r x)) q) =
    r.accept (applyT (power (n % period) (compile r x)) q)
  have hn : applyT (power n (compile r x)) q =
      ((List.replicate n (expand x)).flatten).foldl r.step q :=
    applyT_power_eq_foldl_replicate hb x n q hq
  have hpn : applyT (power (n % period) (compile r x)) q =
      ((List.replicate (n % period) (expand x)).flatten).foldl r.step q :=
    applyT_power_eq_foldl_replicate hb x (n % period) q hq
  have h1 : ((List.replicate period (expand x)).flatten).foldl r.step q = q := by
    rw [← applyT_power_eq_foldl_replicate hb x period q hq]
    exact hfix
  have hmul : ((List.replicate (period * (n / period)) (expand x)).flatten).foldl r.step q = q := by
    rw [Nat.mul_comm]
    exact foldl_replicate_periodic (expand x) period q h1 (n / period)
  have key : ((List.replicate n (expand x)).flatten).foldl r.step q =
      ((List.replicate (n % period) (expand x)).flatten).foldl r.step q := by
    have hsum : period * (n / period) + n % period = n := Nat.div_add_mod n period
    calc ((List.replicate n (expand x)).flatten).foldl r.step q
        = ((List.replicate (period * (n / period) + n % period) (expand x)).flatten).foldl
            r.step q := by rw [hsum]
      _ = ((List.replicate (n % period) (expand x)).flatten).foldl r.step
            (((List.replicate (period * (n / period)) (expand x)).flatten).foldl r.step q) :=
            (foldl_flatten_replicate_add r (expand x) (period * (n / period)) (n % period) q)
      _ = ((List.replicate (n % period) (expand x)).flatten).foldl r.step q := by rw [hmul]
  rw [hn, hpn, key]

theorem two_pow_width : ∀ n : Nat, n ≤ 2 ^ width (n - 1) := by
  intro n
  cases n with
  | zero => show (0 : Nat) ≤ 2 ^ width 0; exact Nat.zero_le _
  | succ n =>
    show n + 1 ≤ 2 ^ width n
    rcases Nat.eq_zero_or_pos n with rfl | hn
    · show (1 : Nat) ≤ 2 ^ width 0
      rw [show width 0 = 0 from by show (if (0 : Nat) == 0 then 0 else _) = _; rw [if_pos (by decide)]]
      decide
    · rw [show width n = n.log2 + 1 from by
        show (if n == 0 then 0 else n.log2 + 1) = _
        rw [if_neg (by rw [beq_iff_eq]; exact Nat.ne_of_gt hn : ¬(n == 0))]]
      exact Nat.succ_le_of_lt Nat.lt_log2_self

theorem tableBytes_of_bounded {r : Reader} (hb : StepBounded r) : tableBytes r := by
  intro x
  show ((List.range r.states).map (get (pack (width (r.states - 1)) (compile r x))
      (width (r.states - 1)))) = compile r x
  have hmapE : compile r x = (List.range r.states).map (fun q => (expand x).foldl r.step q) := by
    refine List.ext_getElem (by rw [compile_length, List.length_map, List.length_range])
      (fun i hi => ?_)
    intro h₂
    have hst : i < r.states := by rw [← compile_length x]; exact hi
    have key := applyT_compile_eq_foldl hb x i hst
    unfold applyT at key
    have ho : (compile r x)[i]? = some (compile r x)[i] := by
      rw [List.getElem?_eq_getElem]
    rw [ho] at key
    rw [List.getElem_map, List.getElem_range]
    exact key
  have hpi : ∀ y ∈ compile r x, y < 2 ^ width (r.states - 1) := by
    intro e he
    rw [hmapE] at he
    obtain ⟨i, hi, hie⟩ := List.mem_map.mp he
    subst hie
    have hbnd : (fun q => (expand x).foldl r.step q) i < r.states := by
      refine foldl_step_bounded hb _ _ ?_
      simpa using hi
    calc (fun q => (expand x).foldl r.step q) i < r.states := hbnd
      _ ≤ 2 ^ width (r.states - 1) := two_pow_width _
  have hkey := TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse
    (width (r.states - 1)) (compile r x) hpi
  rw [compile_length x] at hkey
  exact hkey

theorem expand_extent : ∀ x : Rope, (expand x).length = extent x := by
  intro x
  induction x with
  | lit s => rfl
  | cat a b ih1 ih2 => simp [expand, extent, List.length_append, ih1, ih2]
  | rep k a ih =>
    show ((List.replicate k (expand a)).flatten).length = k * extent a
    rw [flatten_replicate_length, ih]

theorem matched_eq_foldl {r : Reader} (hb : StepBounded r) (x : Rope) {q : Nat}
    (hq : q < r.states) : matched r x q = r.accept ((expand x).foldl r.step q) := by
  show r.accept (applyT (compile r x) q) = _
  rw [applyT_compile_eq_foldl hb x q hq]

theorem flatten_replicate_nil (k : Nat) : (List.replicate k ([] : Bytes)).flatten = [] := by
  induction k with
  | zero => rw [List.replicate_zero, List.flatten_nil]
  | succ k ih => rw [List.replicate_succ, List.flatten_cons, ih, List.nil_append]

theorem take_append_of_le (xs ys : Bytes) (n : Nat) (h : n ≤ xs.length) :
    (xs ++ ys).take n = xs.take n := by
  rw [List.take_append, Nat.sub_eq_zero_of_le h, List.take_zero, List.append_nil]

theorem take_flatten_replicate_lt (a : Rope) (k n : Nat)
    (hltk : n / extent a < k) (hpos : 0 < extent a) :
    ((List.replicate k (expand a)).flatten).take n =
      (List.replicate (min k (n / extent a)) (expand a)).flatten ++
        ((expand a).take (n % extent a)) := by
  have hfull : min k (n / extent a) = n / extent a :=
    Nat.min_eq_right (Nat.le_of_lt hltk)
  have hdpos : 0 < k - n / extent a := by
    have := Nat.le_of_lt hltk
    omega
  have hk : k = n / extent a + (k - n / extent a) := by
    have := Nat.le_of_lt hltk
    omega
  have hlen : ((List.replicate (n / extent a) (expand a)).flatten).length =
      (n / extent a) * extent a := by
    rw [flatten_replicate_length, ← expand_extent]
  have hle : (n / extent a) * extent a ≤ n := Nat.div_mul_le_self n (extent a)
  have hmod : n - (n / extent a) * extent a = n % extent a := by
    rw [Nat.mod_eq_sub_mul_div, Nat.mul_comm]
  have hsz : (expand a).length = extent a := expand_extent a
  rw [hfull]
  rw [hk, replicate_add', List.flatten_append, List.take_append]
  rw [List.take_of_length_le (show ((List.replicate (n / extent a) (expand a)).flatten).length ≤ n
      from by rw [hlen]; exact hle)]
  rw [hlen, hmod]
  rw [replicate_succ_pred (k - n / extent a) hdpos (expand a), List.flatten_cons,
      List.take_append,
      Nat.sub_eq_zero_of_le (show n % extent a ≤ (expand a).length from by
        rw [hsz]
        exact Nat.le_of_lt (Nat.mod_lt n hpos)),
      List.take_zero, List.append_nil]

theorem prefixSeek_of_bounded {r : Reader} (hb : StepBounded r) : prefixSeek r := by
  intro x
  induction x with
  | lit s => intro q n _; rfl
  | cat a b iha ihb =>
      intro q n hq
      show partialScan r (a.cat b) n q = List.foldl r.step q ((expand a ++ expand b).take n)
      rcases Nat.lt_or_ge (extent a) n with hlt | hge
      · -- else branch: extent a < n
        have h1 : applyT (compile r a) q = (expand a).foldl r.step q :=
          applyT_compile_eq_foldl hb a q hq
        have hbnd : applyT (compile r a) q < r.states := by
          rw [h1]; exact foldl_step_bounded hb _ _ hq
        rw [partialScan, if_neg (Nat.not_le.mpr hlt)]
        rw [ihb _ (n - extent a) hbnd]
        rw [List.take_append,
            List.take_of_length_le (show (expand a).length ≤ n from by
              rw [expand_extent]; exact Nat.le_of_lt hlt)]
        rw [List.foldl_append, expand_extent, ← h1]
      · -- then branch: n ≤ extent a
        rw [partialScan, if_pos hge]
        rw [iha q n hq]
        rw [List.take_append,
            Nat.sub_eq_zero_of_le (show n ≤ (expand a).length from by
              rw [expand_extent]; exact hge),
            List.take_zero, List.append_nil]
  | rep k a iha =>
      intro q n hq
      show partialScan r (Rope.rep k a) n q =
        List.foldl r.step q (((List.replicate k (expand a)).flatten).take n)
      rcases Nat.eq_zero_or_pos (extent a) with hzero | hpos
      · rw [partialScan, if_pos (by rw [beq_iff_eq]; exact hzero)]
        have hexp : expand a = [] :=
          List.eq_nil_of_length_eq_zero (by rw [expand_extent, hzero])
        rw [hexp, flatten_replicate_nil, List.take_nil, List.foldl_nil]
      · have hsz : ¬(extent a == 0) := by
          rw [beq_iff_eq]; exact Nat.ne_of_gt hpos
        have hmain : applyT (power (min k (n / extent a)) (compile r a)) q =
            ((List.replicate (min k (n / extent a)) (expand a)).flatten).foldl r.step q :=
          applyT_power_eq_foldl_replicate hb a (min k (n / extent a)) q hq
        have hnext :
            ((List.replicate (min k (n / extent a)) (expand a)).flatten).foldl r.step q < r.states :=
          foldl_step_bounded hb _ _ hq
        rcases Nat.lt_or_ge (n / extent a) k with hltk | hlek
        · -- full < k
          rw [partialScan, if_neg hsz]
          dsimp only
          rw [show (min k (n / extent a) == k) = false from by
            rw [Nat.min_eq_right (Nat.le_of_lt hltk)]
            exact beq_eq_false_iff_ne.mpr (Nat.ne_of_lt hltk)]
          rw [if_neg (by decide : ¬((false : Bool) = true)), hmain]
          rw [iha _ (n % extent a) hnext,
              take_flatten_replicate_lt a k n hltk hpos,
              List.foldl_append]
        · -- full == k
          have hfull : min k (n / extent a) = k := by omega
          rw [partialScan, if_neg hsz]
          dsimp only
          rw [show (min k (n / extent a) == k) = true from by rw [hfull]; simp]
          rw [if_pos rfl, hmain, hfull]
          rw [List.take_of_length_le (show ((List.replicate k (expand a)).flatten).length ≤ n from by
            rw [flatten_replicate_length, expand_extent]
            have hleq : k ≤ n / extent a := hlek
            calc k * extent a ≤ (n / extent a) * extent a := Nat.mul_le_mul_right _ hleq
              _ ≤ n := Nat.div_mul_le_self n (extent a))]


theorem checkpoint_of_bounded {r : Reader} (hb : StepBounded r) : checkpoint r := by
  intro x q a b hq
  rw [prefixSeek_of_bounded hb x q (a + b) hq, prefixSeek_of_bounded hb x q a hq]
  rw [take_drop_split (expand x) a b, List.foldl_append]

/-! ## StepBounded instances -/

theorem bounded_ite {c : Prop} [Decidable c] {x y n : Nat} (hx : x < n) (hy : y < n) :
    (if c then x else y) < n := by
  split <;> assumption

theorem ContainsSlash_stepBounded : StepBounded ContainsSlash.reader := by
  intro q b hq
  rw [show ContainsSlash.reader.states = 2 from rfl] at hq ⊢
  simp only [ContainsSlash.reader]
  simp
  repeat (first | refine bounded_ite ?_ ?_ | omega)

theorem EndsInDigit_stepBounded : StepBounded EndsInDigit.reader := by
  intro q b hq
  rw [show EndsInDigit.reader.states = 2 from rfl]
  simp only [EndsInDigit.reader]
  cases (decide (48 ≤ b ∧ b ≤ 57)) <;> decide

theorem QuotedField_stepBounded : StepBounded QuotedField.reader := by
  intro q b hq
  rw [show QuotedField.reader.states = 2 from rfl] at hq ⊢
  simp only [QuotedField.reader]
  rcases (by omega : q = 0 ∨ q = 1) with rfl | rfl
  all_goals (simp; repeat (first | refine bounded_ite ?_ ?_ | omega))

theorem PercentEscape_stepBounded : StepBounded PercentEscape.reader := by
  intro q b hq
  rw [show PercentEscape.reader.states = 4 from rfl] at hq ⊢
  simp only [PercentEscape.reader]
  rcases (by omega : q = 0 ∨ q = 1 ∨ q = 2 ∨ q = 3) with rfl | rfl | rfl | rfl
  all_goals (simp; repeat (first | refine bounded_ite ?_ ?_ | omega))

theorem LineBoundary_stepBounded : StepBounded LineBoundary.reader := by
  intro q b hq
  rw [show LineBoundary.reader.states = 3 from rfl] at hq ⊢
  simp only [LineBoundary.reader]
  rcases (by omega : q = 0 ∨ q = 1 ∨ q = 2) with rfl | rfl | rfl
  all_goals (simp; repeat (first | refine bounded_ite ?_ ?_ | omega))

theorem AsciiOnly_stepBounded : StepBounded AsciiOnly.reader := by
  intro q b hq
  rw [show AsciiOnly.reader.states = 2 from rfl] at hq ⊢
  simp only [AsciiOnly.reader]
  repeat (first | refine bounded_ite ?_ ?_ | omega)

theorem SlashDepth_stepBounded : StepBounded SlashDepth.reader := by
  intro q b hq
  rw [show SlashDepth.reader.states = 3 from rfl] at hq ⊢
  simp only [SlashDepth.reader]
  rcases (by omega : q = 0 ∨ q = 1 ∨ q = 2) with rfl | rfl | rfl
  all_goals (simp; repeat (first | refine bounded_ite ?_ ?_ | omega))

theorem DecimalMod_stepBounded : StepBounded DecimalMod.reader := by
  intro q b hq
  rw [show DecimalMod.reader.states = 8 from rfl] at hq ⊢
  simp only [DecimalMod.reader]
  simp
  repeat (first | refine bounded_ite ?_ ?_ | omega)

theorem EscapedTail_stepBounded : StepBounded EscapedTail.reader := by
  intro q b hq
  rw [show EscapedTail.reader.states = 2 from rfl] at hq ⊢
  simp only [EscapedTail.reader]
  rcases (by omega : q = 0 ∨ q = 1) with rfl | rfl
  all_goals (simp; repeat (first | refine bounded_ite ?_ ?_ | omega))

theorem CStringEnd_stepBounded : StepBounded CStringEnd.reader := by
  intro q b hq
  rw [show CStringEnd.reader.states = 3 from rfl] at hq ⊢
  simp only [CStringEnd.reader]
  rcases (by omega : q = 0 ∨ q = 1 ∨ q = 2) with rfl | rfl | rfl
  all_goals (simp; repeat (first | refine bounded_ite ?_ ?_ | omega))

/-! ## ContainsSlash specifics -/


/-! ## Reader equation lemmas -/

theorem qf_step_eq (q b : Nat) : QuotedField.reader.step q b = if (b == (34 : Nat)) then 1 - q else q := rfl
theorem cs_step_eq (q b : Nat) : ContainsSlash.reader.step q b = if (b == (47 : Nat)) then 1 else q := rfl
theorem eb_step_eq (q b : Nat) : EndsInDigit.reader.step q b = (decide (48 ≤ b ∧ b ≤ 57)).toNat := rfl
theorem lb_step_eq (q b : Nat) : LineBoundary.reader.step q b =
    if (b == (13 : Nat)) then 1 else if (q == (1 : Nat)) && (b == (10 : Nat)) then 2 else 0 := rfl
theorem ao_step_eq (q b : Nat) : AsciiOnly.reader.step q b = if (b < 128) then q else 1 := rfl
theorem sd_step_eq (q b : Nat) : SlashDepth.reader.step q b = if (b == (47 : Nat)) then min 2 (q + 1) else q := rfl
theorem dm_step_eq (q b : Nat) : DecimalMod.reader.step q b =
    if (q == (7 : Nat)) || (b < 48) || (57 < b) then 7 else (10 * q + b - 48) % 7 := rfl
theorem et_step_eq (q b : Nat) : EscapedTail.reader.step q b = if (b == (92 : Nat)) then 1 - q else 0 := rfl
theorem ce_step_eq (q b : Nat) : CStringEnd.reader.step q b =
    if (q == (0 : Nat)) then (if (b == (0 : Nat)) then 1 else 0) else 2 := rfl
theorem pe_step_eq (q b : Nat) : PercentEscape.reader.step q b =
    if (b == (37 : Nat)) then 1
    else if (48 ≤ b && b ≤ 57) || (65 ≤ b && b ≤ 70) || (97 ≤ b && b ≤ 102) then
      if (q == (1 : Nat)) || (q == (2 : Nat)) then q + 1 else 0
    else 0 := rfl

theorem cs_accept (v : Nat) : ContainsSlash.reader.accept v = (v == (1 : Nat)) := rfl
theorem eb_accept (v : Nat) : EndsInDigit.reader.accept v = (v == (1 : Nat)) := rfl
theorem qf_accept (v : Nat) : QuotedField.reader.accept v = (v == (1 : Nat)) := rfl
theorem pe_accept (v : Nat) : PercentEscape.reader.accept v = (v == (3 : Nat)) := rfl
theorem lb_accept (v : Nat) : LineBoundary.reader.accept v = (v == (2 : Nat)) := rfl
theorem ao_accept (v : Nat) : AsciiOnly.reader.accept v = (v == (0 : Nat)) := rfl
theorem sd_accept (v : Nat) : SlashDepth.reader.accept v = (v == (2 : Nat)) := rfl
theorem dm_accept (v : Nat) : DecimalMod.reader.accept v = (v == (0 : Nat)) := rfl
theorem et_accept (v : Nat) : EscapedTail.reader.accept v = (v == (1 : Nat)) := rfl
theorem ce_accept (v : Nat) : CStringEnd.reader.accept v = (v == (1 : Nat)) := rfl

theorem mod2_two (m : Nat) : (m + 2) % 2 = m % 2 := by omega

/-! ## beq helpers -/

theorem beq_flip_true {a b : Nat} (h : (a == b) = true) : ((b : Nat) == a) = true := by
  have hab : a = b := Nat.eq_of_beq_eq_true h
  subst hab
  simp

theorem beq_flip_false {a b : Nat} (h : (a == b) = false) : ((b : Nat) == a) = false := by
  rw [beq_eq_false_iff_ne] at h ⊢
  exact fun he => h he.symm

theorem beq_ne_false {a b : Nat} (hab : ¬a = b) : (a == b) = false := by
  rw [beq_eq_false_iff_ne]
  exact hab

theorem beq_comm_nat (a b : Nat) : (a == b) = (b == a) := by
  cases h : (a == b) with
  | true => exact (beq_flip_true h).symm
  | false =>
    exact (beq_eq_false_iff_ne.mpr (fun he => beq_eq_false_iff_ne.mp h he.symm)).symm

/-! ## ContainsSlash specifics -/

theorem ContainsSlash_foldl (xs : Bytes) (q : Nat) :
    xs.foldl ContainsSlash.reader.step q = if xs.contains (47 : Nat) then 1 else q := by
  induction xs generalizing q with
  | nil => simp
  | cons b bs ih =>
      rw [List.foldl_cons]
      by_cases hb : b = 47
      · subst hb
        have hstep : ContainsSlash.reader.step q 47 = 1 := by
          rw [cs_step_eq]
          rw [show ((47 : Nat) == (47 : Nat)) = true from rfl]
          rfl
        rw [hstep, ih, List.contains_cons]
        rw [show ((47 : Nat) == (47 : Nat)) = true from rfl]
        simp
      · have hstep : ContainsSlash.reader.step q b = q := by
          rw [cs_step_eq]
          rw [show (b == (47 : Nat)) = false from by
            rw [beq_eq_false_iff_ne]; exact hb]
          simp
        rw [hstep, ih, List.contains_cons]
        rw [show ((47 : Nat) == b) = false from by
          rw [beq_eq_false_iff_ne]; exact fun he => hb he.symm]
        simp

theorem sound_ContainsSlash_SlashWitness : ContainsSlash.SlashWitness.Goal := by
  intro x
  rw [matched_eq_foldl ContainsSlash_stepBounded x (by decide : 0 < ContainsSlash.reader.states)]
  rw [ContainsSlash_foldl]
  rw [cs_accept]
  cases h : (expand x).contains (47 : Nat) <;> simp [h]

theorem sound_ContainsSlash_SearchOnce : ContainsSlash.SearchOnce.Goal := by
  intro a k hk
  rw [sound_ContainsSlash_SlashWitness, sound_ContainsSlash_SlashWitness]
  rw [expand]
  by_cases hm : (47 : Nat) ∈ expand a
  · have hc : ((expand a).contains (47 : Nat)) = true := List.contains_iff_mem.mpr hm
    cases k with
    | zero => omega
    | succ m =>
        rw [List.replicate_succ, List.flatten_cons, List.contains_append, hc]
        simp
  · have hc : ((expand a).contains (47 : Nat)) = false := by
      rw [Bool.eq_false_iff]
      intro he
      exact hm (List.contains_iff_mem.mp he)
    have hc2 : (((List.replicate k (expand a)).flatten).contains (47 : Nat)) = false := by
      rw [Bool.eq_false_iff]
      intro he
      obtain ⟨l, hl, h47⟩ := List.mem_flatten.mp (List.contains_iff_mem.mp he)
      rw [List.mem_replicate] at hl
      exact hm (hl.2.symm ▸ h47)
    rw [hc2, hc]
/-! ## EndsInDigit specifics -/

theorem EndsInDigit_foldl (xs : Bytes) (q : Nat) :
    xs.foldl EndsInDigit.reader.step q =
      match xs.getLast? with
      | none => q
      | some b => (decide (48 ≤ b ∧ b ≤ 57)).toNat := by
  induction xs generalizing q with
  | nil => simp
  | cons b bs ih =>
      cases bs with
      | nil =>
          rw [List.foldl_cons, List.foldl_nil, eb_step_eq]
          simp
      | cons c cs =>
          rw [List.foldl_cons, ih]
          cases h : ((c :: cs : Bytes).getLast?) with
          | none =>
              rw [List.getLast?_eq_none_iff] at h
              cases h
          | some v =>
              have key : ((b :: c :: cs : Bytes).getLast?) = some (v : Nat) := by
                rw [List.getLast?_cons, h]
                rfl
              rw [key]

theorem sound_EndsInDigit_LastByte : EndsInDigit.LastByte.Goal := by
  intro x
  rw [matched_eq_foldl EndsInDigit_stepBounded x (by decide : 0 < EndsInDigit.reader.states)]
  rw [EndsInDigit_foldl]
  rw [eb_accept]
  cases h : (expand x).getLast? with
  | none => simp [h]
  | some b =>
      cases hbt : decide (48 ≤ b) <;> cases hbu : decide (b ≤ 57) <;>
        simp only [decide_eq_true_eq, decide_eq_false_iff_not] at hbt hbu <;>
        simp [h, hbt, hbu]

theorem sound_EndsInDigit_NonemptyTail : EndsInDigit.NonemptyTail.Goal := by
  intro a b hne
  rw [sound_EndsInDigit_LastByte, sound_EndsInDigit_LastByte]
  have hbne : (expand b) ≠ [] := by
    intro he
    have h₀ : (expand b).length = extent b := expand_extent b
    rw [he] at h₀
    simp at h₀
    omega
  cases hb : (expand b).getLast? with
  | none =>
      rw [List.getLast?_eq_none_iff] at hb
      exact absurd hb hbne
  | some v =>
      rw [expand, List.getLast?_append]
      simp [hb]
/-! ## QuotedField specifics -/

theorem QuotedField_step_invol (q b : Nat) (hq : q < 2) :
    QuotedField.reader.step (QuotedField.reader.step q b) b = q := by
  rcases (by omega : q = 0 ∨ q = 1) with rfl | rfl
  · rw [qf_step_eq, qf_step_eq]
    cases h : (b == (34 : Nat)) <;> simp [h]
  · rw [qf_step_eq, qf_step_eq]
    cases h : (b == (34 : Nat)) <;> simp [h]

theorem QuotedField_step_pres (q b : Nat) (hq : q < 2) :
    QuotedField.reader.step q b < 2 := by
  rw [qf_step_eq]
  cases h : (b == (34 : Nat)) <;> simp [h]
  <;> rcases (by omega : q = 0 ∨ q = 1) with rfl | rfl <;> omega

theorem QuotedField_foldl_parity (xs : Bytes) (q : Nat) (hq : q < 2) :
    xs.foldl QuotedField.reader.step q = if (xs.count (34 : Nat) + q) % 2 = 1 then 1 else 0 := by
  induction xs generalizing q with
  | nil =>
      rcases (by omega : q = 0 ∨ q = 1) with rfl | rfl <;> simp
  | cons b bs ih =>
      rw [List.foldl_cons, ih _ (QuotedField_step_pres q b hq), List.count_cons, qf_step_eq]
      rcases (by omega : q = 0 ∨ q = 1) with rfl | rfl
      · cases h : (b == (34 : Nat))
        · simp
        · simp
      · cases h : (b == (34 : Nat))
        · simp
        · simp
          rw [Nat.add_assoc, show (1 : Nat) + 1 = 2 from rfl,
            mod2_two (bs.count (34 : Nat))]

theorem QuotedField_foldl_invol (xs : Bytes) (q : Nat) (hq : q < 2) :
    xs.foldl QuotedField.reader.step (xs.foldl QuotedField.reader.step q) = q := by
  rw [QuotedField_foldl_parity xs (xs.foldl QuotedField.reader.step q)
        (foldl_step_bounded QuotedField_stepBounded xs q hq),
      QuotedField_foldl_parity xs q hq]
  rcases (by omega : q = 0 ∨ q = 1) with rfl | rfl
  · by_cases h : (xs.count (34 : Nat) + 0) % 2 = 1
    · rw [if_pos h, if_neg (by omega : ¬ (xs.count (34 : Nat) + 1) % 2 = 1)]
    · rw [if_neg h, if_neg (by omega : ¬ (xs.count (34 : Nat) + 0) % 2 = 1)]
  · by_cases h : (xs.count (34 : Nat) + 1) % 2 = 1
    · rw [if_pos h, if_pos h]
    · rw [if_neg h, if_pos (by omega : (xs.count (34 : Nat) + 0) % 2 = 1)]

theorem sound_QuotedField_QuoteParity : QuotedField.QuoteParity.Goal := by
  intro x
  rw [matched_eq_foldl QuotedField_stepBounded x (by decide : 0 < QuotedField.reader.states)]
  rw [QuotedField_foldl_parity (expand x) 0 (by decide)]
  rw [qf_accept, Nat.add_zero]
  by_cases h : (expand x).count (34 : Nat) % 2 = 1
  · rw [if_pos h]
    rw [show (decide ((expand x).count (34 : Nat) % 2 = 1) = true) from by
      rw [decide_eq_true_eq]; exact h]
    rfl
  · rw [if_neg h]
    rw [show (decide ((expand x).count (34 : Nat) % 2 = 1) = false) from by
      rw [decide_eq_false_iff_not]; exact h]
    rfl

theorem sound_QuotedField_DoublePhrase : QuotedField.DoublePhrase.Goal := by
  intro x
  apply List.ext_getElem
  · simp [compile_length, QuotedField.reader]
  · intro i hi1 hi2
    have hi : i < 2 := by
      simpa [compile_length, QuotedField.reader] using hi1
    have hcomp := applyT_compile_eq_foldl QuotedField_stepBounded (.rep 2 x) i (by
      simpa [QuotedField.reader] using hi)
    have happ : applyT (compile QuotedField.reader (.rep 2 x)) i = i := by
      rw [hcomp]
      simp only [expand, List.replicate_succ, List.replicate_zero, List.flatten_cons,
        List.flatten_nil, List.append_nil, List.foldl_append]
      exact QuotedField_foldl_invol (expand x) i hi
    have hget : (compile QuotedField.reader (.rep 2 x))[i] =
        applyT (compile QuotedField.reader (.rep 2 x)) i := by
      have hlen : List.length (compile QuotedField.reader (.rep 2 x)) = 2 := by
        simp [compile_length, QuotedField.reader]
      rw [applyT, List.getElem?_eq_getElem (hlen.symm ▸ hi)]
      rfl
    rw [hget, happ]
    show i = (List.range (2 : Nat))[i]
    rcases (by omega : i = 0 ∨ i = 1) with rfl | rfl <;> rfl
/-! ## PercentEscape specifics -/

theorem PercentEscape_foldl_pct_pct (q : Nat) :
    [37, 37].foldl PercentEscape.reader.step q = 1 := by
  rw [List.foldl_cons, List.foldl_cons, List.foldl_nil]
  simp [pe_step_eq]

theorem PercentEscape_foldl_escape (q : Nat) :
    [37, 52, 70].foldl PercentEscape.reader.step q = 3 := by
  rw [List.foldl_cons, List.foldl_cons, List.foldl_cons, List.foldl_nil]
  simp [pe_step_eq]

theorem sound_PercentEscape_LoneIntroducer : PercentEscape.LoneIntroducer.Goal := by
  intro x k
  have hq : k % 4 < PercentEscape.reader.states := by
    rw [show PercentEscape.reader.states = 4 from rfl]
    omega
  have hps := prefixSeek_of_bounded PercentEscape_stepBounded (.cat x (.lit [37, 37])) (k % 4)
    (extent x + 2) hq
  rw [hps]
  have htake : (expand (.cat x (.lit [37, 37]))).take (extent x + 2) =
      expand x ++ ([37, 37] : List Nat) := by
    rw [expand]
    rw [show (expand (Rope.lit [37, 37])) = ([37, 37] : List Nat) from rfl]
    exact List.take_of_length_le (by simp [expand_extent])
  rw [htake, List.foldl_append, PercentEscape_foldl_pct_pct]

theorem sound_PercentEscape_EscapeAcrossBoundary : PercentEscape.EscapeAcrossBoundary.Goal := by
  intro x
  have hq : (0 : Nat) < PercentEscape.reader.states := by
    rw [show PercentEscape.reader.states = 4 from rfl]
    omega
  rw [matched_eq_foldl PercentEscape_stepBounded (.cat x (.lit [37, 52, 70])) hq]
  rw [pe_accept]
  rw [expand, List.foldl_append]
  rw [show (expand (Rope.lit [37, 52, 70])) = ([37, 52, 70] : List Nat) from rfl]
  rw [PercentEscape_foldl_escape]
  rfl
section LineBoundarySec
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

theorem LineBoundary_flatten_rep_single (b : Nat) (n : Nat) :
    List.flatten (List.replicate (n + 1) [b]) = b :: List.replicate n b := by
  induction n with
  | zero =>
    rw [List.replicate_zero,
      show List.flatten (List.replicate (0 + 1) [b]) = ([b] : List Nat) from rfl]
  | succ n ih => rw [List.replicate_succ, List.flatten_cons, ih, List.replicate_succ]; rfl

theorem LineBoundary_foldl_run (b : Nat) :
    ∀ (n q : Nat), q < 3 → List.foldl LineBoundary.reader.step q (b :: List.replicate n b) =
      if b == 13 then 1 else if q == 1 && b == 10 && n == 0 then 2 else 0 := by
  intro n
  induction n with
  | zero =>
    intro q hq
    rw [List.replicate_zero, List.foldl_cons, List.foldl_nil, lb_step_eq]
    cases h13 : (b == 13) <;> cases h10 : (b == 10) <;> cases hq1 : (q == 1) <;>
      simp [h13, h10, hq1]
  | succ m ih =>
    intro q hq
    have hbnd : LineBoundary.reader.step q b < 3 := LineBoundary_stepBounded q b hq
    rw [show (b :: List.replicate (m + 1) b) = b :: b :: List.replicate m b from
      by rw [List.replicate_succ]]
    rw [List.foldl_cons, ih (LineBoundary.reader.step q b) hbnd]
    cases h13 : (b == 13) <;> cases h10 : (b == 10) <;> cases hq1 : (q == 1) <;>
      simp [lb_step_eq, h13, h10, hq1, Nat.succ_ne_zero, beq_comm_nat]

theorem sound_LineBoundary_RunBoundaryCRLF : LineBoundary.RunBoundaryCRLF.Goal := by
  intro x k hk hx
  rw [matched_eq_foldl LineBoundary_stepBounded (Rope.rep k x)
    (by decide : 0 < LineBoundary.reader.states)]
  rw [expand]
  have hlen : (expand x).length = 1 := by rw [expand_extent, hx]
  cases hbs : expand x with
  | nil => simp [hbs] at hlen
  | cons b bs =>
    cases bs with
    | cons c cs => simp [hbs] at hlen
    | nil =>
      obtain ⟨m, rfl⟩ : ∃ m, k = m + 2 := ⟨k - 2, by omega⟩
      rw [show List.flatten (List.replicate (m + 2) [b]) = b :: List.replicate (m + 1) b from
        LineBoundary_flatten_rep_single b (m + 1)]
      rw [LineBoundary_foldl_run b (m + 1) 0 (by decide)]
      rw [lb_accept]
      cases h : (b == 13) <;> simp [h]

theorem LineBoundary_trich (rl : List Nat) : ∀ q, q < 3 →
    ((rl.reverse.foldl LineBoundary.reader.step q == (2 : Nat)) =
        List.isPrefixOf [10, 13] (rl ++ if q == 1 then ([13] : List Nat)
          else if q == 2 then ([10, 13] : List Nat) else [])) ∧
    ((rl.reverse.foldl LineBoundary.reader.step q == (1 : Nat)) =
        List.isPrefixOf [13] (rl ++ if q == 1 then ([13] : List Nat)
          else if q == 2 then ([10, 13] : List Nat) else [])) := by
  induction rl with
  | nil =>
    intro q hq
    rcases (by omega : q = 0 ∨ q = 1 ∨ q = 2) with rfl | rfl | rfl <;>
      simp [List.isPrefixOf_cons_cons, List.isPrefixOf_cons_nil, List.isPrefixOf_nil_left]
  | cons y rest ih =>
    intro q hq
    obtain ⟨ta, tb⟩ := ih q hq
    refine ⟨?_, ?_⟩
    · rw [List.reverse_cons, List.foldl_append, List.foldl_cons, List.foldl_nil,
        List.cons_append, List.isPrefixOf_cons_cons, ← tb, lb_step_eq]
      cases hy13 : (y == 13) with
      | true =>
        obtain rfl : y = 13 := Nat.eq_of_beq_eq_true hy13
        simp [lb_step_eq]
      | false =>
        cases hy10 : (y == 10) with
        | true =>
          obtain rfl : y = 10 := Nat.eq_of_beq_eq_true hy10
          cases hf : (rest.reverse.foldl LineBoundary.reader.step q == 1) <;>
            simp [lb_step_eq, hf]
        | false => simp [hy13, hy10, beq_comm_nat]
    · rw [List.reverse_cons, List.foldl_append, List.foldl_cons, List.foldl_nil,
        List.cons_append, List.isPrefixOf_cons_cons, List.isPrefixOf_nil_left, lb_step_eq]
      cases hy13 : (y == 13) with
      | true =>
        obtain rfl : y = 13 := Nat.eq_of_beq_eq_true hy13
        simp [lb_step_eq]
      | false =>
        cases hf : (rest.reverse.foldl LineBoundary.reader.step q == 1) <;>
          cases hy10 : (y == 10) <;> simp [hf, hy10, hy13, beq_comm_nat]

theorem sound_LineBoundary_FinalPair : LineBoundary.FinalPair.Goal := by
  intro x
  rw [matched_eq_foldl LineBoundary_stepBounded x (by decide : 0 < LineBoundary.reader.states)]
  rw [lb_accept, List.isSuffixOf]
  obtain ⟨ta, _⟩ := LineBoundary_trich (expand x).reverse 0 (by decide)
  rw [show (if (0 : Nat) == 1 then ([13] : List Nat) else if (0 : Nat) == 2 then ([10, 13] : List Nat) else [])
      = ([] : List Nat) from rfl] at ta
  rw [List.reverse_reverse, List.append_nil] at ta
  exact ta

end LineBoundarySec
section AsciiOnlySec
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

theorem AsciiOnly_step1 (b : Nat) : AsciiOnly.reader.step 1 b = 1 := by
  rw [ao_step_eq]
  split <;> rfl

theorem AsciiOnly_foldl1 (xs : Bytes) : xs.foldl AsciiOnly.reader.step 1 = 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      rw [List.foldl_cons, AsciiOnly_step1]
      exact ih

theorem AsciiOnly_foldl0 (xs : Bytes) :
    xs.foldl AsciiOnly.reader.step 0 = if xs.all (fun b => b < 128) then 0 else 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      rw [List.foldl_cons, List.all_cons, ao_step_eq]
      by_cases hb : b < 128
      · have hbv : decide (b < 128) = true := by
          rw [decide_eq_true_eq]; exact hb
        rw [if_pos hb, ih, hbv, Bool.true_and]
      · have hbv : decide (b < 128) = false := by
          rw [decide_eq_false_iff_not]; exact hb
        rw [if_neg hb, AsciiOnly_foldl1, hbv, Bool.false_and]
        simp

theorem sound_AsciiOnly_SevenBitFastPath : AsciiOnly.SevenBitFastPath.Goal := by
  intro x
  rw [matched_eq_foldl AsciiOnly_stepBounded x (by decide : 0 < AsciiOnly.reader.states)]
  rw [AsciiOnly_foldl0, ao_accept]
  cases h : (expand x).all (fun b => b < 128) <;> simp [h]

theorem sound_AsciiOnly_RejectEarly : AsciiOnly.RejectEarly.Goal := by
  intro a b hma
  rw [matched_eq_foldl AsciiOnly_stepBounded (.cat a b)
    (by decide : 0 < AsciiOnly.reader.states)]
  rw [matched_eq_foldl AsciiOnly_stepBounded a (by decide : 0 < AsciiOnly.reader.states)] at hma
  rw [ao_accept] at hma ⊢
  rw [expand]
  rw [AsciiOnly_foldl0] at hma ⊢
  rw [List.all_append]
  cases ha : (expand a).all (fun x => x < 128) <;> simp [ha] at hma ⊢

end AsciiOnlySec
section SlashDepthSec
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

theorem SlashDepth_step2 (b : Nat) : SlashDepth.reader.step 2 b = 2 := by
  rw [sd_step_eq]
  split <;> rfl

theorem SlashDepth_foldl2 (xs : Bytes) : xs.foldl SlashDepth.reader.step 2 = 2 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      rw [List.foldl_cons, SlashDepth_step2]
      exact ih

theorem SlashDepth_foldl_ge2 (xs : Bytes) (q : Nat) (hq : q ≤ 2) (hc : 2 ≤ q + xs.count (47 : Nat)) :
    xs.foldl SlashDepth.reader.step q = 2 := by
  induction xs generalizing q with
  | nil =>
      rw [List.count_nil] at hc
      rw [List.foldl_nil]
      omega
  | cons b bs ih =>
      by_cases hb : b = 47
      · subst hb
        rw [List.count_cons] at hc
        rw [List.foldl_cons]
        have hstep : SlashDepth.reader.step q 47 = min 2 (q + 1) := by
          rw [sd_step_eq]
          simp
        rw [hstep]
        rw [show (if (47 : Nat) == (47 : Nat) then (1 : Nat) else 0) = (1 : Nat) from rfl] at hc
        by_cases hq2 : q ≥ 2
        · have hmin : min 2 (q + 1) = 2 := by omega
          rw [hmin, SlashDepth_foldl2]
        · have hmin : min 2 (q + 1) = q + 1 := by omega
          rw [hmin]
          refine ih (q + 1) (by omega) (by omega)
      · have hstep : SlashDepth.reader.step q b = q := by
          rw [sd_step_eq]
          rw [beq_eq_false_iff_ne.mpr hb]
          simp
        rw [List.foldl_cons, hstep]
        rw [List.count_cons] at hc
        rw [show (if (b : Nat) == 47 then (1 : Nat) else 0) = 0 from
          by rw [beq_eq_false_iff_ne.mpr hb]; simp] at hc
        refine ih q hq (by omega)

theorem SlashDepth_foldl_lt2 (xs : Bytes) (q : Nat) (hq : q ≤ 2) (hc : q + xs.count (47 : Nat) < 2) :
    xs.foldl SlashDepth.reader.step q = q + xs.count (47 : Nat) := by
  induction xs generalizing q with
  | nil =>
      rw [List.count_nil] at hc ⊢
      rw [List.foldl_nil]
      omega
  | cons b bs ih =>
      by_cases hb : b = 47
      · subst hb
        rw [List.count_cons] at hc
        rw [List.foldl_cons, List.count_cons]
        rw [show (if (47 : Nat) == (47 : Nat) then (1 : Nat) else 0) = (1 : Nat) from rfl] at hc ⊢
        have hq0 : q = 0 := by omega
        subst hq0
        have hc' : bs.count (47 : Nat) = 0 := by omega
        have hstep : SlashDepth.reader.step 0 47 = 1 := by
          rw [sd_step_eq]
          simp
        rw [hstep]
        have := ih 1 (by omega) (by omega)
        rw [hc'] at this
        rw [this, hc']
      · have hstep : SlashDepth.reader.step q b = q := by
          rw [sd_step_eq]
          rw [beq_eq_false_iff_ne.mpr hb]
          simp
        rw [List.foldl_cons, hstep]
        rw [List.count_cons] at hc ⊢
        rw [show (if (b : Nat) == 47 then (1 : Nat) else 0) = 0 from
          by rw [beq_eq_false_iff_ne.mpr hb]; simp] at hc ⊢
        refine ih q hq (by omega)

theorem sound_SlashDepth_TwoDelimiters : SlashDepth.TwoDelimiters.Goal := by
  intro x
  rw [matched_eq_foldl SlashDepth_stepBounded x (by decide : 0 < SlashDepth.reader.states)]
  rw [sd_accept]
  by_cases hc : 2 ≤ (expand x).count (47 : Nat)
  · rw [SlashDepth_foldl_ge2 (expand x) 0 (by omega)
      (show 2 ≤ 0 + (expand x).count (47 : Nat) from by rw [Nat.zero_add]; exact hc)]
    rw [show ((2 : Nat) == 2) = true from rfl, eq_comm, decide_eq_true_eq]
    exact hc
  · rw [SlashDepth_foldl_lt2 (expand x) 0 (by omega)
      (show 0 + (expand x).count (47 : Nat) < 2 from by rw [Nat.zero_add]; omega),
      Nat.zero_add]
    rcases (by omega : (expand x).count (47 : Nat) = 0 ∨ (expand x).count (47 : Nat) = 1)
        with hc0 | hc1
    · rw [hc0]
      rfl
    · rw [hc1]
      rfl

theorem count_flatten_replicate (xs : List Nat) (c n : Nat) :
    ((List.replicate n xs).flatten).count c = n * xs.count c := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons, List.count_append, ih]
      rw [Nat.succ_mul]
      omega

theorem sound_SlashDepth_RepeatedDelimiter : SlashDepth.RepeatedDelimiter.Goal := by
  intro x n hn
  rw [sound_SlashDepth_TwoDelimiters]
  rw [expand, count_flatten_replicate]
  by_cases hc : (expand x).count (47 : Nat) = 0
  · have hnone : ¬ (47 : Nat) ∈ expand x := by
      intro hm
      have hpos : 0 < (expand x).count (47 : Nat) := List.count_pos_iff.mpr hm
      omega
    have hc2 : (expand x).contains (47 : Nat) = false := by
      rw [Bool.eq_false_iff]
      exact fun hc' => hnone (List.contains_iff_mem.mp hc')
    have hmul : n * (expand x).count (47 : Nat) = 0 := by
      rw [hc]; omega
    rw [hc2, hmul]
    rw [decide_eq_false_iff_not]
    omega
  · have hcpos : 0 < (expand x).count (47 : Nat) := Nat.pos_of_ne_zero hc
    have hmem : (47 : Nat) ∈ expand x := List.count_pos_iff.mp hcpos
    have hc2 : (expand x).contains (47 : Nat) = true := List.contains_iff_mem.mpr hmem
    have hmul : 2 ≤ n * (expand x).count (47 : Nat) := by
      have h1 : 2 = 2 * 1 := rfl
      rw [h1]
      exact Nat.mul_le_mul hn (by omega)
    rw [hc2]
    rw [decide_eq_true_eq]
    exact hmul

end SlashDepthSec
section DecimalModSec
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

theorem DecimalMod_step_digit (q x : Nat) (hq : q < 7) (hx : x < 10) :
    DecimalMod.reader.step q (48 + x) = (10 * q + x) % 7 := by
  have hcond : ¬ ((q == (7 : Nat)) || decide (48 + x < 48) || decide (57 < 48 + x)) = true := by
    rw [show ((q : Nat) == 7) = false from beq_eq_false_iff_ne.mpr (by omega),
      show decide (48 + x < 48) = false from by rw [decide_eq_false_iff_not]; omega,
      show decide (57 < 48 + x) = false from by rw [decide_eq_false_iff_not]; omega]
    simp
  rw [dm_step_eq, if_neg hcond]
  omega

theorem sound_DecimalMod_DecimalSuffix : DecimalMod.DecimalSuffix.Goal := by
  intro a x y hx hy q hq7
  have hq0 : (0 : Nat) < DecimalMod.reader.states := by
    rw [show DecimalMod.reader.states = 8 from rfl]
    omega
  have hqeq : q = (expand a).foldl DecimalMod.reader.step 0 := by
    show applyT (compile DecimalMod.reader a) 0 = _
    rw [applyT_compile_eq_foldl DecimalMod_stepBounded a 0 hq0]
  have hq7' := hq7
  rw [hqeq] at hq7'
  rw [applyT_compile_eq_foldl DecimalMod_stepBounded (.cat a (.lit [48 + x, 48 + y])) 0 hq0]
  rw [expand, List.foldl_append]
  rw [show (expand (Rope.lit [48 + x, 48 + y])) = ([48 + x, 48 + y] : List Nat) from rfl]
  rw [List.foldl_cons, List.foldl_cons, List.foldl_nil]
  rw [← hqeq]
  have hq7'' : q < 7 := hq7
  rw [DecimalMod_step_digit _ x hq7'' hx]
  rw [DecimalMod_step_digit _ y (Nat.mod_lt _ (by decide)) hy]
  have h1 : (10 * ((10 * q + x) % 7) + y) % 7 = (100 * q + 10 * x + y) % 7 := by
    omega
  exact h1

theorem sound_DecimalMod_LeadingZero : DecimalMod.LeadingZero.Goal := by
  intro x
  have hq0 : (0 : Nat) < DecimalMod.reader.states := by
    rw [show DecimalMod.reader.states = 8 from rfl]
    omega
  rw [matched_eq_foldl DecimalMod_stepBounded (.cat (.lit [48]) x) hq0]
  rw [matched_eq_foldl DecimalMod_stepBounded x hq0]
  rw [expand, List.foldl_append]
  rw [show (expand (Rope.lit ([48] : List Nat))) = ([48] : List Nat) from rfl]
  rw [List.foldl_cons, List.foldl_nil]
  have hstep : DecimalMod.reader.step 0 48 = 0 := by
    rw [dm_step_eq]
    rw [show ((0 : Nat) == 7) = false from rfl,
      show decide (48 < 48) = false from rfl, show decide (57 < 48) = false from rfl]
    simp
  rw [hstep]

end DecimalModSec
section EscapedTailSec
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

theorem sound_EscapedTail_ResetAtDelimiter : EscapedTail.ResetAtDelimiter.Goal := by
  intro a
  rw [matched_eq_foldl EscapedTail_stepBounded (.cat a (.lit [47, 92]))
    (by decide : 0 < EscapedTail.reader.states)]
  rw [et_accept]
  rw [expand, List.foldl_append]
  rw [show (expand (Rope.lit ([47, 92] : List Nat))) = ([47, 92] : List Nat) from rfl]
  rw [List.foldl_cons, List.foldl_cons, List.foldl_nil]
  rw [et_step_eq]
  rw [et_step_eq]
  rw [show ((47 : Nat) == 92) = false from rfl]
  rw [show ((92 : Nat) == 92) = true from rfl]
  rfl

theorem escapedTail_helper (l : Bytes) :
    (l.reverse).foldl EscapedTail.reader.step 0 =
      ((l.takeWhile (· == (92 : Nat))).length % 2) := by
  induction l with
  | nil => simp
  | cons c cs ih =>
      rw [List.reverse_cons, List.foldl_append, List.foldl_cons, List.foldl_nil, ih]
      have hb : EscapedTail.reader.step ((cs.takeWhile (· == (92 : Nat))).length % 2) c =
          if (c == (92 : Nat)) then 1 - (cs.takeWhile (· == (92 : Nat))).length % 2 else 0 := by
        rw [et_step_eq]
      rw [hb]
      cases h : (c == (92 : Nat)) with
      | false =>
          rw [List.takeWhile_cons_of_neg (by simpa using h)]
          simp
      | true =>
          rw [List.takeWhile_cons_of_pos (by simpa using h), List.length_cons]
          have key : ∀ n : Nat, 1 - n % 2 = (n + 1) % 2 := by
            intro n
            rcases (Nat.mod_two_eq_zero_or_one n) with h0 | h1
            · rw [h0, show (n + 1) % 2 = 1 from by omega]
            · rw [h1, show (n + 1) % 2 = 0 from by omega]
          rw [key]
          simp

theorem EscapedTail_foldl (xs : Bytes) :
    xs.foldl EscapedTail.reader.step 0 = ((xs.reverse.takeWhile (· == (92 : Nat))).length % 2) := by
  have h := escapedTail_helper (xs.reverse)
  rw [List.reverse_reverse] at h
  exact h

theorem sound_EscapedTail_TrailingRun : EscapedTail.TrailingRun.Goal := by
  intro x
  rw [matched_eq_foldl EscapedTail_stepBounded x (by decide : 0 < EscapedTail.reader.states)]
  rw [EscapedTail_foldl, et_accept]
  have h : ((expand x).reverse.takeWhile (fun b => b == (92 : Nat))).length % 2 = 0 ∨
      ((expand x).reverse.takeWhile (fun b => b == (92 : Nat))).length % 2 = 1 :=
    Nat.mod_two_eq_zero_or_one _
  rcases h with h | h <;> rw [h] <;> rfl

end EscapedTailSec
section CStringEndSec
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

theorem CStringEnd_foldl_no0 (xs : Bytes) (h : xs.contains (0 : Nat) = false) :
    xs.foldl CStringEnd.reader.step 0 = 0 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      rw [List.contains_cons] at h
      cases h' : (b == (0 : Nat)) with
      | false =>
          rw [beq_flip_false h'] at h
          simp only [Bool.false_or] at h
          rw [List.foldl_cons]
          have hstep : CStringEnd.reader.step 0 b = 0 := by
            rw [ce_step_eq]
            rw [show ((0 : Nat) == 0) = true from rfl, h']
            rfl
          rw [hstep]
          exact ih h
      | true =>
          rw [beq_flip_true h'] at h
          simp at h

theorem CStringEnd_foldl_from2 (xs : Bytes) : xs.foldl CStringEnd.reader.step 2 = 2 := by
  induction xs with
  | nil => rfl
  | cons b bs ih =>
      rw [List.foldl_cons]
      have hstep : CStringEnd.reader.step 2 b = 2 := by
        rw [ce_step_eq]
        rw [show ((2 : Nat) == 0) = false from rfl]
        rfl
      rw [hstep]
      exact ih

theorem CStringEnd_foldl_has0 (xs : Bytes) (h : xs.contains (0 : Nat) = true) :
    xs.foldl CStringEnd.reader.step 0 = 1 ∨ xs.foldl CStringEnd.reader.step 0 = 2 := by
  induction xs with
  | nil => simp at h
  | cons b bs ih =>
      rw [List.contains_cons, Bool.or_eq_true] at h
      by_cases hb : b = 0
      · subst hb
        rw [List.foldl_cons]
        have hstep : CStringEnd.reader.step 0 0 = 1 := by
          rw [ce_step_eq]
          simp
        rw [hstep]
        cases bs with
        | nil => left; simp
        | cons c cs =>
            right
            rw [List.foldl_cons]
            have hstep1 : CStringEnd.reader.step 1 c = 2 := by
              rw [ce_step_eq]
              rw [show ((1 : Nat) == 0) = false from rfl]
              rfl
            rw [hstep1, CStringEnd_foldl_from2 cs]
      · have hb' : (b == (0 : Nat)) = false := beq_eq_false_iff_ne.mpr hb
        have hbs : bs.contains (0 : Nat) = true := by
          cases h with
          | inl h1 =>
              rw [beq_flip_false hb'] at h1
              exact absurd h1 (by simp)
          | inr h2 => exact h2
        rw [List.foldl_cons]
        have hstep : CStringEnd.reader.step 0 b = 0 := by
          rw [ce_step_eq]
          rw [show ((0 : Nat) == 0) = true from rfl, hb']
          rfl
        rw [hstep]
        exact ih hbs

theorem CStringEnd_foldl_from1 (xs : Bytes) :
    xs.foldl CStringEnd.reader.step 1 = if xs.isEmpty then 1 else 2 := by
  cases xs with
  | nil => simp
  | cons b bs =>
      rw [List.foldl_cons]
      have hstep : CStringEnd.reader.step 1 b = 2 := by
        rw [ce_step_eq]
        rw [show ((1 : Nat) == 0) = false from rfl]
        rfl
      rw [hstep, CStringEnd_foldl_from2 bs]
      simp [List.isEmpty]

theorem sound_CStringEnd_OneTerminator : CStringEnd.OneTerminator.Goal := by
  intro x
  rw [matched_eq_foldl CStringEnd_stepBounded (.cat x (.lit [0]))
    (by decide : 0 < CStringEnd.reader.states)]
  rw [ce_accept]
  rw [expand, List.foldl_append]
  rw [show (expand (Rope.lit ([0] : List Nat))) = ([0] : List Nat) from rfl]
  rw [List.foldl_cons, List.foldl_nil]
  cases h : (expand x).contains (0 : Nat) with
  | false =>
      rw [CStringEnd_foldl_no0 (expand x) h]
      rw [ce_step_eq]
      rw [show ((0 : Nat) == 0) = true from rfl]
      rfl
  | true =>
      have hres := CStringEnd_foldl_has0 (expand x) h
      rcases hres with h1 | h2
      · rw [h1, ce_step_eq]
        rw [show ((1 : Nat) == 0) = false from rfl]
        rfl
      · rw [h2, ce_step_eq]
        rw [show ((2 : Nat) == 0) = false from rfl]
        rfl

theorem sound_CStringEnd_TruncateAtTerminator : CStringEnd.TruncateAtTerminator.Goal := by
  intro pre tail hpre
  have hpre' : (expand pre).contains (0 : Nat) = false := by simpa using hpre
  rw [matched_eq_foldl CStringEnd_stepBounded (.cat (.cat pre (.lit [0])) tail)
    (by decide : 0 < CStringEnd.reader.states)]
  rw [ce_accept]
  rw [expand, List.foldl_append]
  rw [show (expand (Rope.cat pre (Rope.lit ([0] : List Nat)))) = (expand pre ++ ([0] : List Nat)) from rfl]
  rw [List.foldl_append, List.foldl_cons, List.foldl_nil]
  rw [CStringEnd_foldl_no0 (expand pre) hpre']
  rw [ce_step_eq]
  rw [show ((0 : Nat) == 0) = true from rfl]
  simp
  by_cases ht : extent tail = 0
  · have hempty : expand tail = ([] : List Nat) := by
      have h₀ : (expand tail).length = extent tail := expand_extent tail
      rw [ht] at h₀
      exact List.eq_nil_of_length_eq_zero h₀
    rw [hempty, List.foldl_nil, ht]
    rfl
  · have hne : (expand tail) ≠ [] := by
      intro he
      have h₀ : (expand tail).length = extent tail := expand_extent tail
      rw [he, List.length_nil] at h₀
      omega
    have h1 : (expand tail).foldl CStringEnd.reader.step 1 = 2 := by
      cases hlt : expand tail with
      | nil =>
          have h₀ := expand_extent tail
          rw [hlt, List.length_nil] at h₀
          exact absurd h₀.symm ht
      | cons b bs =>
          rw [List.foldl_cons]
          have hstep : CStringEnd.reader.step 1 b = 2 := by
            rw [ce_step_eq]
            rw [show ((1 : Nat) == 0) = false from rfl]
            rfl
          rw [hstep, CStringEnd_foldl_from2 bs]
    rw [h1]
    rw [show (decide (extent tail = 0) = false) from by
      rw [decide_eq_false_iff_not]; exact ht]
    rfl

end CStringEndSec




-- ContainsSlash
theorem sound_ContainsSlash_RepeatedState : ContainsSlash.RepeatedState.Goal := repeatedState_of_bounded ContainsSlash_stepBounded
theorem sound_ContainsSlash_PrefixSeek : ContainsSlash.PrefixSeek.Goal := prefixSeek_of_bounded ContainsSlash_stepBounded
theorem sound_ContainsSlash_Checkpoint : ContainsSlash.Checkpoint.Goal := checkpoint_of_bounded ContainsSlash_stepBounded
theorem sound_ContainsSlash_ReverseScan : ContainsSlash.ReverseScan.Goal := reverseScan_of_bounded ContainsSlash_stepBounded
theorem sound_ContainsSlash_DictionaryMask : ContainsSlash.DictionaryMask.Goal := dictionaryMask_of_bounded ContainsSlash_stepBounded
theorem sound_ContainsSlash_ContextReuse : ContainsSlash.ContextReuse.Goal := contextReuse_of_bounded ContainsSlash_stepBounded
theorem sound_ContainsSlash_PeriodJump : ContainsSlash.PeriodJump.Goal := periodJump_of_bounded ContainsSlash_stepBounded
theorem sound_ContainsSlash_TableBytes : ContainsSlash.TableBytes.Goal := tableBytes_of_bounded ContainsSlash_stepBounded

-- EndsInDigit
theorem sound_EndsInDigit_RepeatedState : EndsInDigit.RepeatedState.Goal := repeatedState_of_bounded EndsInDigit_stepBounded
theorem sound_EndsInDigit_PrefixSeek : EndsInDigit.PrefixSeek.Goal := prefixSeek_of_bounded EndsInDigit_stepBounded
theorem sound_EndsInDigit_Checkpoint : EndsInDigit.Checkpoint.Goal := checkpoint_of_bounded EndsInDigit_stepBounded
theorem sound_EndsInDigit_ReverseScan : EndsInDigit.ReverseScan.Goal := reverseScan_of_bounded EndsInDigit_stepBounded
theorem sound_EndsInDigit_DictionaryMask : EndsInDigit.DictionaryMask.Goal := dictionaryMask_of_bounded EndsInDigit_stepBounded
theorem sound_EndsInDigit_ContextReuse : EndsInDigit.ContextReuse.Goal := contextReuse_of_bounded EndsInDigit_stepBounded
theorem sound_EndsInDigit_PeriodJump : EndsInDigit.PeriodJump.Goal := periodJump_of_bounded EndsInDigit_stepBounded
theorem sound_EndsInDigit_TableBytes : EndsInDigit.TableBytes.Goal := tableBytes_of_bounded EndsInDigit_stepBounded

-- QuotedField
theorem sound_QuotedField_RepeatedState : QuotedField.RepeatedState.Goal := repeatedState_of_bounded QuotedField_stepBounded
theorem sound_QuotedField_PrefixSeek : QuotedField.PrefixSeek.Goal := prefixSeek_of_bounded QuotedField_stepBounded
theorem sound_QuotedField_Checkpoint : QuotedField.Checkpoint.Goal := checkpoint_of_bounded QuotedField_stepBounded
theorem sound_QuotedField_ReverseScan : QuotedField.ReverseScan.Goal := reverseScan_of_bounded QuotedField_stepBounded
theorem sound_QuotedField_DictionaryMask : QuotedField.DictionaryMask.Goal := dictionaryMask_of_bounded QuotedField_stepBounded
theorem sound_QuotedField_ContextReuse : QuotedField.ContextReuse.Goal := contextReuse_of_bounded QuotedField_stepBounded
theorem sound_QuotedField_PeriodJump : QuotedField.PeriodJump.Goal := periodJump_of_bounded QuotedField_stepBounded
theorem sound_QuotedField_TableBytes : QuotedField.TableBytes.Goal := tableBytes_of_bounded QuotedField_stepBounded

-- PercentEscape
theorem sound_PercentEscape_RepeatedState : PercentEscape.RepeatedState.Goal := repeatedState_of_bounded PercentEscape_stepBounded
theorem sound_PercentEscape_PrefixSeek : PercentEscape.PrefixSeek.Goal := prefixSeek_of_bounded PercentEscape_stepBounded
theorem sound_PercentEscape_Checkpoint : PercentEscape.Checkpoint.Goal := checkpoint_of_bounded PercentEscape_stepBounded
theorem sound_PercentEscape_ReverseScan : PercentEscape.ReverseScan.Goal := reverseScan_of_bounded PercentEscape_stepBounded
theorem sound_PercentEscape_DictionaryMask : PercentEscape.DictionaryMask.Goal := dictionaryMask_of_bounded PercentEscape_stepBounded
theorem sound_PercentEscape_ContextReuse : PercentEscape.ContextReuse.Goal := contextReuse_of_bounded PercentEscape_stepBounded
theorem sound_PercentEscape_PeriodJump : PercentEscape.PeriodJump.Goal := periodJump_of_bounded PercentEscape_stepBounded
theorem sound_PercentEscape_TableBytes : PercentEscape.TableBytes.Goal := tableBytes_of_bounded PercentEscape_stepBounded

-- LineBoundary
theorem sound_LineBoundary_RepeatedState : LineBoundary.RepeatedState.Goal := repeatedState_of_bounded LineBoundary_stepBounded
theorem sound_LineBoundary_PrefixSeek : LineBoundary.PrefixSeek.Goal := prefixSeek_of_bounded LineBoundary_stepBounded
theorem sound_LineBoundary_Checkpoint : LineBoundary.Checkpoint.Goal := checkpoint_of_bounded LineBoundary_stepBounded
theorem sound_LineBoundary_ReverseScan : LineBoundary.ReverseScan.Goal := reverseScan_of_bounded LineBoundary_stepBounded
theorem sound_LineBoundary_DictionaryMask : LineBoundary.DictionaryMask.Goal := dictionaryMask_of_bounded LineBoundary_stepBounded
theorem sound_LineBoundary_ContextReuse : LineBoundary.ContextReuse.Goal := contextReuse_of_bounded LineBoundary_stepBounded
theorem sound_LineBoundary_PeriodJump : LineBoundary.PeriodJump.Goal := periodJump_of_bounded LineBoundary_stepBounded
theorem sound_LineBoundary_TableBytes : LineBoundary.TableBytes.Goal := tableBytes_of_bounded LineBoundary_stepBounded

-- AsciiOnly
theorem sound_AsciiOnly_RepeatedState : AsciiOnly.RepeatedState.Goal := repeatedState_of_bounded AsciiOnly_stepBounded
theorem sound_AsciiOnly_PrefixSeek : AsciiOnly.PrefixSeek.Goal := prefixSeek_of_bounded AsciiOnly_stepBounded
theorem sound_AsciiOnly_Checkpoint : AsciiOnly.Checkpoint.Goal := checkpoint_of_bounded AsciiOnly_stepBounded
theorem sound_AsciiOnly_ReverseScan : AsciiOnly.ReverseScan.Goal := reverseScan_of_bounded AsciiOnly_stepBounded
theorem sound_AsciiOnly_DictionaryMask : AsciiOnly.DictionaryMask.Goal := dictionaryMask_of_bounded AsciiOnly_stepBounded
theorem sound_AsciiOnly_ContextReuse : AsciiOnly.ContextReuse.Goal := contextReuse_of_bounded AsciiOnly_stepBounded
theorem sound_AsciiOnly_PeriodJump : AsciiOnly.PeriodJump.Goal := periodJump_of_bounded AsciiOnly_stepBounded
theorem sound_AsciiOnly_TableBytes : AsciiOnly.TableBytes.Goal := tableBytes_of_bounded AsciiOnly_stepBounded

-- SlashDepth
theorem sound_SlashDepth_RepeatedState : SlashDepth.RepeatedState.Goal := repeatedState_of_bounded SlashDepth_stepBounded
theorem sound_SlashDepth_PrefixSeek : SlashDepth.PrefixSeek.Goal := prefixSeek_of_bounded SlashDepth_stepBounded
theorem sound_SlashDepth_Checkpoint : SlashDepth.Checkpoint.Goal := checkpoint_of_bounded SlashDepth_stepBounded
theorem sound_SlashDepth_ReverseScan : SlashDepth.ReverseScan.Goal := reverseScan_of_bounded SlashDepth_stepBounded
theorem sound_SlashDepth_DictionaryMask : SlashDepth.DictionaryMask.Goal := dictionaryMask_of_bounded SlashDepth_stepBounded
theorem sound_SlashDepth_ContextReuse : SlashDepth.ContextReuse.Goal := contextReuse_of_bounded SlashDepth_stepBounded
theorem sound_SlashDepth_PeriodJump : SlashDepth.PeriodJump.Goal := periodJump_of_bounded SlashDepth_stepBounded
theorem sound_SlashDepth_TableBytes : SlashDepth.TableBytes.Goal := tableBytes_of_bounded SlashDepth_stepBounded

-- DecimalMod
theorem sound_DecimalMod_RepeatedState : DecimalMod.RepeatedState.Goal := repeatedState_of_bounded DecimalMod_stepBounded
theorem sound_DecimalMod_PrefixSeek : DecimalMod.PrefixSeek.Goal := prefixSeek_of_bounded DecimalMod_stepBounded
theorem sound_DecimalMod_Checkpoint : DecimalMod.Checkpoint.Goal := checkpoint_of_bounded DecimalMod_stepBounded
theorem sound_DecimalMod_ReverseScan : DecimalMod.ReverseScan.Goal := reverseScan_of_bounded DecimalMod_stepBounded
theorem sound_DecimalMod_DictionaryMask : DecimalMod.DictionaryMask.Goal := dictionaryMask_of_bounded DecimalMod_stepBounded
theorem sound_DecimalMod_ContextReuse : DecimalMod.ContextReuse.Goal := contextReuse_of_bounded DecimalMod_stepBounded
theorem sound_DecimalMod_PeriodJump : DecimalMod.PeriodJump.Goal := periodJump_of_bounded DecimalMod_stepBounded
theorem sound_DecimalMod_TableBytes : DecimalMod.TableBytes.Goal := tableBytes_of_bounded DecimalMod_stepBounded

-- EscapedTail
theorem sound_EscapedTail_RepeatedState : EscapedTail.RepeatedState.Goal := repeatedState_of_bounded EscapedTail_stepBounded
theorem sound_EscapedTail_PrefixSeek : EscapedTail.PrefixSeek.Goal := prefixSeek_of_bounded EscapedTail_stepBounded
theorem sound_EscapedTail_Checkpoint : EscapedTail.Checkpoint.Goal := checkpoint_of_bounded EscapedTail_stepBounded
theorem sound_EscapedTail_ReverseScan : EscapedTail.ReverseScan.Goal := reverseScan_of_bounded EscapedTail_stepBounded
theorem sound_EscapedTail_DictionaryMask : EscapedTail.DictionaryMask.Goal := dictionaryMask_of_bounded EscapedTail_stepBounded
theorem sound_EscapedTail_ContextReuse : EscapedTail.ContextReuse.Goal := contextReuse_of_bounded EscapedTail_stepBounded
theorem sound_EscapedTail_PeriodJump : EscapedTail.PeriodJump.Goal := periodJump_of_bounded EscapedTail_stepBounded
theorem sound_EscapedTail_TableBytes : EscapedTail.TableBytes.Goal := tableBytes_of_bounded EscapedTail_stepBounded

-- CStringEnd
theorem sound_CStringEnd_RepeatedState : CStringEnd.RepeatedState.Goal := repeatedState_of_bounded CStringEnd_stepBounded
theorem sound_CStringEnd_PrefixSeek : CStringEnd.PrefixSeek.Goal := prefixSeek_of_bounded CStringEnd_stepBounded
theorem sound_CStringEnd_Checkpoint : CStringEnd.Checkpoint.Goal := checkpoint_of_bounded CStringEnd_stepBounded
theorem sound_CStringEnd_ReverseScan : CStringEnd.ReverseScan.Goal := reverseScan_of_bounded CStringEnd_stepBounded
theorem sound_CStringEnd_DictionaryMask : CStringEnd.DictionaryMask.Goal := dictionaryMask_of_bounded CStringEnd_stepBounded
theorem sound_CStringEnd_ContextReuse : CStringEnd.ContextReuse.Goal := contextReuse_of_bounded CStringEnd_stepBounded
theorem sound_CStringEnd_PeriodJump : CStringEnd.PeriodJump.Goal := periodJump_of_bounded CStringEnd_stepBounded
theorem sound_CStringEnd_TableBytes : CStringEnd.TableBytes.Goal := tableBytes_of_bounded CStringEnd_stepBounded
end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans.Common
