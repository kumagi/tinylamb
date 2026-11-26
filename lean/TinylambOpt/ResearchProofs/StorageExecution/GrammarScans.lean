import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.Compression.Wire

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ## Generic helpers -/

@[simp] theorem applyT_getD (t : Table) (q : Nat) : applyT t q = t[q]?.getD q := rfl

@[simp] theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

theorem chain_getElem?_eq (a b : Table) (i : Nat) (hi : i < a.length) :
    (chain a b)[i]? = some (applyT b (a[i]?.getD i)) := by
  simp [chain, hi]

theorem chain_getElem?_none (a b : Table) (i : Nat) (hi : a.length ≤ i) :
    (chain a b)[i]? = none := by
  simp [chain, List.getElem?_eq_none_iff.mpr hi]

theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length]
  simp [applyT, List.getElem?_eq_getElem hq', List.getElem?_eq_getElem hq, chain]

theorem applyT_chain_of_length_le (a b : Table) (q : Nat) (hq : a.length ≤ q) :
    applyT (chain a b) q = applyT b q := by
  have hq' : (chain a b).length ≤ q := by simpa [chain_length]
  simp [applyT, List.getElem?_eq_none hq', List.getElem?_eq_none hq]

@[simp] theorem compile_length (r : Reader) (x : Rope) : (compile r x).length = r.states := by
  induction x generalizing r with
  | lit s => simp [compile]
  | cat a b ihA ihB => simp [compile, chain_length, ihA, ihB]
  | rep n a ihA =>
      simp only [compile]
      induction n using Nat.strongRecOn with
      | _ n ihN =>
          unfold power
          split
          · simp
          · simp only
            split <;> simp [chain_length, ihA, ihN]

theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      unfold power
      split
      · simp
      · split <;> simp [chain_length, ih]

@[simp] theorem power_zero (t : Table) : power 0 t = List.range t.length := by
  unfold power
  simp

theorem power_one (t : Table) : power 1 t = t := by
  have h1 : ¬ (1 : Nat) = 0 := by decide
  unfold power
  simp [h1, chain]
  apply List.ext_getElem
  · simp [power_length]
  · intro i hi1 hi2
    have hi : i < t.length := by simpa [power_length] using hi1
    simp [applyT, List.getElem?_eq_getElem hi]

theorem getD_range (i n : Nat) (hi : i < n) : (List.range n)[i]?.getD i = i := by
  simp [List.getElem?_eq_getElem hi]

theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  simp [applyT, getD_range q t.length hq]

/-! ## expand / reverse -/

@[simp] theorem expand_reverse (x : Rope) : expand (reverse x) = (expand x).reverse := by
  induction x with
  | lit s => simp [reverse, expand]
  | cat a b ihA ihB => simp [reverse, expand, ihA, ihB]
  | rep k a ihA =>
      simp [reverse, expand, ihA]
      induction k with
      | zero => simp
      | succ k ihK =>
          simp [List.replicate_succ, List.flatten_cons, ihK]

/-! ## foldl composition -/

theorem foldl_append_step (r : Reader) (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl r.step q = ys.foldl r.step (xs.foldl r.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

theorem foldl_flatten_replicate_succ (r : Reader) (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl r.step q =
      xs.foldl r.step (((List.replicate n xs).flatten).foldl r.step q) := by
  rw [show List.replicate (n + 1) xs = List.replicate n xs ++ [xs] by
    simp [List.replicate_succ, List.replicate_append, List.replicate_one]]
  simp [List.flatten_append, foldl_append_step]

theorem foldl_flatten_replicate_add (r : Reader) (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl r.step q =
      ((List.replicate n xs).flatten).foldl r.step (((List.replicate m xs).flatten).foldl r.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      rw [show m + (n + 1) = (m + n) + 1 by omega]
      rw [foldl_flatten_replicate_succ, ih]

/-! ## Step boundedness and compile correctness -/

def StepBounded (r : Reader) : Prop := ∀ q b, q < r.states → r.step q b < r.states

theorem foldl_step_bounded {r : Reader} (hb : StepBounded r) (xs : Bytes) {q : Nat} (hq : q < r.states) :
    xs.foldl r.step q < r.states := by
  induction xs generalizing q with
  | nil => simpa
  | cons x xs ih => exact ih (hb q x hq)

theorem foldl_flatten_replicate_bounded {r : Reader} (hb : StepBounded r) (n : Nat) (xs : Bytes) {q : Nat}
    (hq : q < r.states) : ((List.replicate n xs).flatten).foldl r.step q < r.states :=
  foldl_step_bounded hb _ hq

theorem applyT_compile_eq_foldl {r : Reader} (hb : StepBounded r) (x : Rope) {q : Nat} (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      simp [compile, applyT, List.getElem?_eq_getElem hq]
  | cat a b ihA ihB =>
      have hqA : applyT (compile r a) q < r.states := by
        rw [ihA hq]; exact foldl_step_bounded hb _ hq
      simp [compile, applyT_chain (compile r a) (compile r b) q (by simp [compile_length, hq]),
        ihA hq, ihB hqA, expand, foldl_append_step]
  | rep n a ihA =>
      have hpow : ∀ m q, q < r.states →
          applyT (power m (compile r a)) q = ((List.replicate m (expand a)).flatten).foldl r.step q := by
        intro m
        induction m using Nat.strongRecOn with
        | _ m ihM =>
            intro q hq
            by_cases hm : m = 0
            · subst hm
              simp [applyT_power_zero, compile_length, hq]
            · have hpos : 0 < m := Nat.pos_of_ne_zero hm
              have hdiv : m / 2 < m := Nat.div_lt_self hpos (by decide)
              have ihH := ihM (m / 2) hdiv
              unfold power
              simp only [hm, dite_false]
              set h := power (m / 2) (compile r a)
              set sq := chain h h
              have hHq : applyT h q = ((List.replicate (m / 2) (expand a)).flatten).foldl r.step q := ihH q hq
              have hHqBound : ((List.replicate (m / 2) (expand a)).flatten).foldl r.step q < r.states :=
                foldl_step_bounded hb _ hq
              have hsq : applyT sq q =
                  ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl r.step q := by
                rw [show sq = chain h h from rfl,
                    applyT_chain h h q (by rw [power_length, compile_length]; exact hq)]
                rw [hHq, ihH _ hHqBound, foldl_flatten_replicate_add]
              have hsqBound : ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl r.step q < r.states :=
                foldl_step_bounded hb _ hq
              cases hmod : m % 2 with
              | zero =>
                  have hm2 : m / 2 + m / 2 = m := by omega
                  simp only [hmod, Nat.zero_eq, ↓reduceIte]
                  simpa [hm2] using hsq
              | succ k =>
                  have hk : k = 0 := by omega; subst hk
                  simp only [hmod]
                  have hm2 : m / 2 + m / 2 + 1 = m := by omega
                  rw [applyT_chain sq (compile r a) q (by rw [chain_length, power_length, compile_length]; exact hq)]
                  rw [hsq, ihA hsqBound]
                  have h1 : ((List.replicate (m / 2 + m / 2 + 1) (expand a)).flatten).foldl r.step q =
                      (expand a).foldl r.step (((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl r.step q) := by
                    rw [foldl_flatten_replicate_succ]
                  simpa [hm2] using h1.symm
      rw [hpow n q hq]
      simp [compile, expand]

theorem applyT_power_eq_foldl_replicate {r : Reader} (hb : StepBounded r) (x : Rope) (n q : Nat) (hq : q < r.states) :
    applyT (power n (compile r x)) q = ((List.replicate n (expand x)).flatten).foldl r.step q := by
  have h := applyT_compile_eq_foldl hb (.rep n x) hq
  simpa [compile, expand] using h

/-! ## Generic common goals -/

theorem repeatedState_of_bounded {r : Reader} (hb : StepBounded r) : repeatedState r := by
  intro x n q hq
  exact applyT_power_eq_foldl_replicate hb x n q hq

theorem dictionaryMask_of_bounded {r : Reader} (hb : StepBounded r) : dictionaryMask r := by
  intro d ids q hq
  simp [rows, matched]
  congr 1
  ext1 i
  cases hdi : d[i]? with
  | none => simp [hdi]
  | some x =>
      simp [hdi, applyT_compile_eq_foldl hb x hq]

theorem contextReuse_of_bounded {r : Reader} (hb : StepBounded r) : contextReuse r := by
  intro a b x y q hq hxy
  have hca : (expand a).foldl r.step q < r.states := foldl_step_bounded hb _ hq
  have hx1 := applyT_compile_eq_foldl hb x hca
  have hy1 := applyT_compile_eq_foldl hb y hca
  have hx' : (expand x).foldl r.step ((expand a).foldl r.step q) =
      (expand y).foldl r.step ((expand a).foldl r.step q) := by
    rw [← hx1, ← hy1, hxy]
  simp [expand, foldl_append_step, hx']

theorem reverseScan_of_bounded {r : Reader} (hb : StepBounded r) : reverseScan r := by
  intro x q hq
  simp [matched]
  rw [applyT_compile_eq_foldl hb (reverse x) hq, expand_reverse]

theorem periodJump_of_bounded {r : Reader} (hb : StepBounded r) : periodJump r := by
  intro x q period n hq hper hfix
  have hpow : ∀ k, applyT (power (k * period) (compile r x)) q = q := by
    intro k
    induction k with
    | zero => simp [applyT_power_zero, compile_length, hq]
    | succ k ih =>
        have hsum : (k + 1) * period = k * period + period := by ring
        rw [hsum, applyT_power_eq_foldl_replicate hb x (k * period + period) q hq, foldl_flatten_replicate_add]
        rw [← applyT_power_eq_foldl_replicate hb x period _ (by
          rw [← applyT_power_eq_foldl_replicate hb x (k * period) q hq, ih]; exact hq)]
        rw [hfix, ← applyT_power_eq_foldl_replicate hb x (k * period) q hq, ih]
  have hdecomp : n / period * period + n % period = n := Nat.div_add_mod n period
  simp [matched]
  conv_lhs => rw [← hdecomp]
  rw [applyT_power_eq_foldl_replicate hb x (n / period * period + n % period) q hq, foldl_flatten_replicate_add]
  have hleft : ((List.replicate (n / period * period) (expand x)).flatten).foldl r.step q = q := by
    rw [← applyT_power_eq_foldl_replicate hb x (n / period * period) q hq]
    exact hpow (n / period)
  rw [hleft, ← applyT_power_eq_foldl_replicate hb x (n % period) q hq]

/-! ## Width / TableBytes helpers -/

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst hn; simp [width]
  · rw [show width n = n.log2 + 1 by unfold width; simp [hn]]
    have hlog : n.log2 < n.log2 + 1 := Nat.lt_succ_self _
    exact (Nat.log2_lt (n := n) (b := n.log2 + 1) (by omega)).mp hlog

private theorem states_le_pow_width_sub (r : Reader) : r.states ≤ 2 ^ width (r.states - 1) := by
  have h := lt_two_pow_width (r.states - 1)
  omega

theorem tableBytes_of_bounded {r : Reader} (hb : StepBounded r) : tableBytes r := by
  intro x
  simp only [tableBytes]
  set t := compile r x
  have htlen : t.length = r.states := compile_length r x
  have hbound : ∀ v ∈ t, v < 2 ^ width (r.states - 1) := by
    intro v hv
    rw [List.mem_iff_getElem] at hv
    rcases hv with ⟨i, hi, rfl⟩
    have hi' : i < r.states := by simpa [htlen] using hi
    have hval : t[i] = applyT t i := by simp [applyT, List.getElem?_eq_getElem hi]
    rw [hval]
    rw [applyT_compile_eq_foldl hb x hi']
    have hbnd := foldl_step_bounded hb (expand x) hi'
    have hle := states_le_pow_width_sub r
    omega
  have hpi := TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse (width (r.states - 1)) t hbound
  simpa [htlen] using hpi

/-! ## extent / expand length -/

@[simp] theorem expand_length (x : Rope) : (expand x).length = extent x := by
  induction x with
  | lit s => simp [expand, extent]
  | cat a b ihA ihB => simp [expand, extent, ihA, ihB]
  | rep k a ihA =>
      simp [expand, extent, ihA]
      induction k with
      | zero => simp
      | succ k ihK => simp [List.replicate_succ, List.flatten_cons, ihK]; omega

/-! ## PrefixSeek / Checkpoint -/

theorem prefixSeek_of_bounded {r : Reader} (hb : StepBounded r) : prefixSeek r := by
  intro x q n hq
  induction x generalizing q n with
  | lit s =>
      simp [partialScan, expand]
  | cat a b ihA ihB =>
      simp only [partialScan]
      by_cases hn : n ≤ extent a
      · rw [if_pos hn]
        have hlenA : (expand a).length = extent a := expand_length a
        have ht : ((expand a) ++ (expand b)).take n = (expand a).take n := by
          apply List.take_append_of_le_length
          omega
        simp [expand, ht, ihA q n hq]
      · rw [if_neg hn]
        have hgt : extent a < n := Nat.lt_of_not_ge hn
        have hlenA : (expand a).length = extent a := expand_length a
        have hqa : applyT (compile r a) q < r.states := by
          rw [applyT_compile_eq_foldl hb a hq]
          exact foldl_step_bounded hb _ hq
        have ht : ((expand a) ++ (expand b)).take n = (expand a) ++ (expand b).take (n - (expand a).length) := by
          rw [List.take_append]
          have h1 : n - (expand a).length = n - extent a := by rw [hlenA]
          simp [hlenA, Nat.not_le.mpr hgt, h1]
        rw [ihB _ _ hqa]
        simp [expand, ht, foldl_append_step, applyT_compile_eq_foldl hb a hq, hlenA]
  | rep k a ihA =>
      simp only [partialScan]
      by_cases hsize : extent a = 0
      · simp [hsize]
        have hexp : expand a = [] := by
          have : (expand a).length = 0 := by simpa [expand_length] using hsize
          exact List.eq_nil_of_length_eq_zero this
        simp [expand, hexp]
        have : (List.replicate k ([] : Bytes)).flatten = [] := by
          induction k <;> simp [List.replicate_succ, List.flatten_cons, *]
        simp [this]
      · simp [hsize]
        set size := extent a
        set full := min k (n / size)
        have hsizePos : 0 < size := Nat.pos_of_ne_zero hsize
        have hfull_le_n : full * size ≤ n := by
          have h1 : full ≤ n / size := Nat.min_le_right _ _
          have h2 : full * size ≤ (n / size) * size := Nat.mul_le_mul_right _ h1
          have h3 : (n / size) * size ≤ n := Nat.div_mul_le_self n size
          omega
        have hnext : applyT (power full (compile r a)) q =
            ((List.replicate full (expand a)).flatten).foldl r.step q :=
          applyT_power_eq_foldl_replicate hb a full q hq
        have hnextBound : ((List.replicate full (expand a)).flatten).foldl r.step q < r.states :=
          foldl_step_bounded hb _ hq
        by_cases hfk : full = k
        · simp [hfk, hnext]
          have hkn : k * size ≤ n := by simpa [hfk] using hfull_le_n
          have hlen : ((List.replicate k (expand a)).flatten).length = k * size := by
            have := expand_length (.rep k a)
            simpa [expand, extent] using this
          rw [List.take_of_length_le (by omega)]
        · simp [hfk, hnext]
          have htake : ((List.replicate k (expand a)).flatten).take n =
              (List.replicate full (expand a)).flatten ++ ((expand a).take (n % size)) := by
            have hk : k = full + (k - full) := by omega
            conv_lhs => rw [hk]
            rw [List.replicate_add, List.flatten_append]
            have hlenFull : ((List.replicate full (expand a)).flatten).length = full * size := by
              have := expand_length (.rep full a)
              simpa [expand, extent, size] using this
            rw [List.take_append]
            have hn' : n - ((List.replicate full (expand a)).flatten).length = n % size := by
              rw [hlenFull]; omega
            have hleN : ((List.replicate full (expand a)).flatten).length ≤ n := by rw [hlenFull]; exact hfull_le_n
            simp [hleN, hn']
            have hkfull : 0 < k - full := by omega
            match hm : k - full with
            | 0 => omega
            | m + 1 =>
                simp [List.replicate_succ, List.flatten_cons, List.take_append_of_le_length (by
                  rw [expand_length]; exact Nat.le_of_lt (Nat.mod_lt n (Nat.pos_of_ne_zero hsize)))]
          rw [htake, foldl_append_step]
          rw [ihA _ (n % size) hnextBound]

private theorem take_add_eq_append (xs : List Nat) (a b : Nat) :
    xs.take (a + b) = xs.take a ++ (xs.drop a).take b := by
  induction xs generalizing a b with
  | nil => simp
  | cons x xs ih =>
      cases a with
      | zero => simp
      | succ a => simp [ih a b, List.take_succ_cons, List.drop_succ_cons]

theorem checkpoint_of_bounded {r : Reader} (hb : StepBounded r) : checkpoint r := by
  intro x q a b hq
  rw [prefixSeek_of_bounded hb x q (a + b) hq, prefixSeek_of_bounded hb x q a hq]
  rw [take_add_eq_append (expand x) a b, foldl_append_step]

/-! ## StepBounded for each reader -/

theorem ContainsSlash_stepBounded : StepBounded ContainsSlash.reader := by
  intro q b hq
  simp only [ContainsSlash.reader]
  interval_cases q <;> split <;> omega

theorem EndsInDigit_stepBounded : StepBounded EndsInDigit.reader := by
  intro q b hq
  simp only [EndsInDigit.reader]
  interval_cases q <;> simp <;> omega

theorem QuotedField_stepBounded : StepBounded QuotedField.reader := by
  intro q b hq
  simp only [QuotedField.reader]
  interval_cases q <;> split <;> omega

theorem PercentEscape_stepBounded : StepBounded PercentEscape.reader := by
  intro q b hq
  simp only [PercentEscape.reader]
  interval_cases q <;> simp <;> split <;> split <;> omega

theorem LineBoundary_stepBounded : StepBounded LineBoundary.reader := by
  intro q b hq
  simp only [LineBoundary.reader]
  interval_cases q <;> split <;> try split <;> omega

theorem AsciiOnly_stepBounded : StepBounded AsciiOnly.reader := by
  intro q b hq
  simp only [AsciiOnly.reader]
  interval_cases q <;> split <;> omega

theorem SlashDepth_stepBounded : StepBounded SlashDepth.reader := by
  intro q b hq
  simp only [SlashDepth.reader]
  interval_cases q <;> split <;> omega

theorem DecimalMod_stepBounded : StepBounded DecimalMod.reader := by
  intro q b hq
  simp only [DecimalMod.reader]
  interval_cases q <;> simp <;> omega

theorem EscapedTail_stepBounded : StepBounded EscapedTail.reader := by
  intro q b hq
  simp only [EscapedTail.reader]
  interval_cases q <;> split <;> omega

theorem CStringEnd_stepBounded : StepBounded CStringEnd.reader := by
  intro q b hq
  simp only [CStringEnd.reader]
  interval_cases q <;> split <;> split <;> omega

/-! ## Common goals per reader -/

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

/-! ## Matched helper -/

theorem matched_eq_foldl {r : Reader} (hb : StepBounded r) (x : Rope) {q : Nat} (hq : q < r.states) :
    matched r x q = r.accept ((expand x).foldl r.step q) := by
  simp [matched, applyT_compile_eq_foldl hb x hq]

/-! ## ContainsSlash specifics -/

private theorem ContainsSlash_foldl (xs : Bytes) (q : Nat) :
    xs.foldl ContainsSlash.reader.step q = if xs.contains 47 then 1 else q := by
  induction xs generalizing q with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.contains_cons]
      by_cases hb : b == 47
      · simp [ContainsSlash.reader, hb]
        have h1 : bs.foldl ContainsSlash.reader.step 1 = 1 := by
          clear ih
          induction bs with
          | nil => simp
          | cons c cs ih2 =>
              simp only [List.foldl_cons]
              have : ContainsSlash.reader.step 1 c = 1 := by simp [ContainsSlash.reader]
              rw [this, ih2]
        rw [h1]; simp
      · simp [ContainsSlash.reader, hb, ih]

theorem sound_ContainsSlash_SlashWitness : ContainsSlash.SlashWitness.Goal := by
  intro x
  rw [matched_eq_foldl ContainsSlash_stepBounded x (by decide : 0 < ContainsSlash.reader.states)]
  simp only [ContainsSlash.reader, ContainsSlash_foldl]
  cases h : (expand x).contains 47 <;> simp [h]

theorem sound_ContainsSlash_SearchOnce : ContainsSlash.SearchOnce.Goal := by
  intro a k hk
  rw [sound_ContainsSlash_SlashWitness, sound_ContainsSlash_SlashWitness]
  simp [expand]
  cases ha : (expand a).contains 47 with
  | false =>
      simp [ha]
      have hnone : 47 ∉ (expand a) := by simpa [List.contains_iff_mem] using ha
      have : ((List.replicate k (expand a)).flatten).contains 47 = false := by
        rw [List.contains_eq_false_iff_not_mem]
        intro hm
        rw [List.mem_flatten] at hm
        rcases hm with ⟨l, hl, h47⟩
        rw [List.mem_replicate] at hl
        exact hnone (hl.2 ▸ h47)
      simp [this]
  | true =>
      simp [ha]
      have hmem : 47 ∈ (expand a) := by simpa [List.contains_iff_mem] using ha
      cases k with
      | zero => omega
      | succ k =>
          rw [List.replicate_succ, List.flatten_cons]
          simp [List.contains_append, List.contains_iff_mem, hmem]

/-! ## EndsInDigit specifics -/

private theorem EndsInDigit_step_const (q b : Nat) :
    EndsInDigit.reader.step q b = (decide (48 ≤ b ∧ b ≤ 57)).toNat := by
  simp [EndsInDigit.reader]

private theorem EndsInDigit_foldl (xs : Bytes) (q : Nat) :
    xs.foldl EndsInDigit.reader.step q =
      match xs.getLast? with
      | none => q
      | some b => (decide (48 ≤ b ∧ b ≤ 57)).toNat := by
  induction xs generalizing q using List.reverseRecOn with
  | nil => simp
  | append_singleton xs x ih =>
      rw [List.foldl_append, List.foldl_cons, List.foldl_nil, ih]
      cases h : xs.getLast? with
      | none =>
          simp at h; subst h; simp [EndsInDigit_step_const]
      | some b =>
          simp [EndsInDigit_step_const, List.getLast?_append]

theorem sound_EndsInDigit_LastByte : EndsInDigit.LastByte.Goal := by
  intro x
  rw [matched_eq_foldl EndsInDigit_stepBounded x (by decide : 0 < EndsInDigit.reader.states)]
  simp only [EndsInDigit.reader, EndsInDigit_foldl]
  cases h : (expand x).getLast? with
  | none => simp [h]
  | some b => simp [h]

theorem sound_EndsInDigit_NonemptyTail : EndsInDigit.NonemptyTail.Goal := by
  intro a b hne
  rw [sound_EndsInDigit_LastByte, sound_EndsInDigit_LastByte]
  have hbne : (expand b) ≠ [] := by
    intro he
    have : extent b = 0 := by simpa [expand_length] using congrArg List.length he
    omega
  have hbne' : (expand b).getLast? ≠ none := by simpa [List.getLast?_eq_none_iff] using hbne
  cases hb : (expand b).getLast? with
  | none => exact absurd hb hbne'
  | some v => simp [expand, List.getLast?_append, hb]

/-! ## QuotedField specifics -/

private theorem QuotedField_step_invol (q b : Nat) (hq : q < 2) :
    QuotedField.reader.step (QuotedField.reader.step q b) b = q := by
  simp only [QuotedField.reader]; interval_cases q <;> split <;> simp <;> omega

private theorem QuotedField_step_pres (q b : Nat) (hq : q < 2) :
    QuotedField.reader.step q b < 2 := by
  simp only [QuotedField.reader]; interval_cases q <;> split <;> omega

private theorem QuotedField_foldl_invol (xs : Bytes) (q : Nat) (hq : q < 2) :
    xs.foldl QuotedField.reader.step (xs.foldl QuotedField.reader.step q) = q := by
  induction xs generalizing q with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons]
      have hq1 : QuotedField.reader.step q b < 2 := QuotedField_step_pres q b hq
      rw [ih _ hq1]; exact QuotedField_step_invol q b hq

private theorem QuotedField_foldl_parity (xs : Bytes) (q : Nat) (hq : q < 2) :
    xs.foldl QuotedField.reader.step q = if (xs.count 34 + q) % 2 = 1 then 1 else 0 := by
  induction xs generalizing q with
  | nil => interval_cases q <;> simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.count_cons]
      have hq1 : QuotedField.reader.step q b < 2 := QuotedField_step_pres q b hq
      rw [ih _ hq1]
      simp only [QuotedField.reader]
      by_cases hb : b = 34
      · subst hb; interval_cases q <;> simp [Nat.add_mod, Nat.mul_mod] <;> omega
      · have hb' : (b == 34) = false := by simpa using hb
        simp [hb']; interval_cases q <;> simp [Nat.add_mod] <;> omega

theorem sound_QuotedField_QuoteParity : QuotedField.QuoteParity.Goal := by
  intro x
  rw [matched_eq_foldl QuotedField_stepBounded x (by decide : 0 < QuotedField.reader.states)]
  simp only [QuotedField.reader, QuotedField_foldl_parity (expand x) 0 (by decide)]
  simp
  cases h : (expand x).count 34 % 2 <;> simp <;> omega

theorem sound_QuotedField_DoublePhrase : QuotedField.DoublePhrase.Goal := by
  intro x
  apply List.ext_getElem
  · simp [compile_length, QuotedField.reader]
  · intro i hi1 hi2
    have hi : i < 2 := by simpa [compile_length, QuotedField.reader] using hi1
    have hi' : i < QuotedField.reader.states := by simpa [QuotedField.reader]
    simp only [List.getElem_range]
    have hcomp := applyT_compile_eq_foldl QuotedField_stepBounded (.rep 2 x) hi'
    have happ : applyT (compile QuotedField.reader (.rep 2 x)) i = i := by
      rw [hcomp]
      simp [expand, List.replicate_succ, List.replicate_zero, List.flatten_cons, List.flatten_nil,
        List.append_nil, foldl_append_step]
      exact QuotedField_foldl_invol (expand x) i hi
    have hget : (compile QuotedField.reader (.rep 2 x))[i]'hi1 = applyT (compile QuotedField.reader (.rep 2 x)) i := by
      simp [applyT, List.getElem?_eq_getElem hi1]
    rw [hget, happ]

/-! ## PercentEscape specifics -/

private theorem PercentEscape_foldl_pct_pct (q : Nat) :
    [37, 37].foldl PercentEscape.reader.step q = 1 := by
  simp [PercentEscape.reader]

private theorem PercentEscape_foldl_escape (q : Nat) :
    [37, 52, 70].foldl PercentEscape.reader.step q = 3 := by
  simp [PercentEscape.reader]

theorem sound_PercentEscape_LoneIntroducer : PercentEscape.LoneIntroducer.Goal := by
  intro x k
  have hq : k % 4 < PercentEscape.reader.states := by simp [PercentEscape.reader]; omega
  have hps := prefixSeek_of_bounded PercentEscape_stepBounded (.cat x (.lit [37, 37])) (k % 4) (extent x + 2) hq
  rw [hps]
  have hlen : ((expand x) ++ [37, 37]).length = extent x + 2 := by simp [expand_length]
  rw [show ((expand (.cat x (.lit [37, 37]))).take (extent x + 2)) = (expand x) ++ [37, 37] by
    simp [expand, hlen]]
  rw [foldl_append_step, PercentEscape_foldl_pct_pct]

theorem sound_PercentEscape_EscapeAcrossBoundary : PercentEscape.EscapeAcrossBoundary.Goal := by
  intro x
  have hq : (0 : Nat) < PercentEscape.reader.states := by decide
  rw [matched_eq_foldl PercentEscape_stepBounded (.cat x (.lit [37, 52, 70])) hq]
  simp [expand, foldl_append_step, PercentEscape_foldl_escape, PercentEscape.reader]

/-! ## LineBoundary specifics -/

private theorem LineBoundary_foldl_rep_one (b : Nat) (k : Nat) (hk : 1 < k) (q : Nat) (hq : q < 3) :
    ((List.replicate k [b]).flatten).foldl LineBoundary.reader.step q ≠ 2 := by
  have hstep : ∀ q : Nat, q < 3 → LineBoundary.reader.step (LineBoundary.reader.step q b) b ≠ 2 := by
    intro q hq
    simp only [LineBoundary.reader]
    interval_cases q <;> by_cases hb13 : b = 13 <;> by_cases hb10 : b = 10 <;> simp [hb13, hb10]
  have hstep1 : ∀ q : Nat, q < 3 → LineBoundary.reader.step q b < 3 := by
    intro q hq
    simp only [LineBoundary.reader]; interval_cases q <;> split <;> split <;> omega
  induction k generalizing q with
  | zero => omega
  | succ k ih =>
      cases k with
      | zero => omega
      | succ k =>
          cases k with
          | zero =>
              simp [List.replicate_succ, List.flatten_cons, List.foldl_append, List.foldl_cons]
              exact hstep q hq
          | succ k =>
              have ih' := ih (by omega) (LineBoundary.reader.step q b) (hstep1 q hq)
              simp only [List.replicate_succ, List.flatten_cons, List.foldl_append, List.foldl_cons, List.foldl_nil]
              exact ih'

theorem sound_LineBoundary_RunBoundaryCRLF : LineBoundary.RunBoundaryCRLF.Goal := by
  intro x k hk hlen
  have hq : (0 : Nat) < LineBoundary.reader.states := by decide
  rw [matched_eq_foldl LineBoundary_stepBounded (.rep k x) hq]
  have hexpLen : (expand x).length = 1 := by simpa [expand_length] using hlen
  obtain ⟨b, hb⟩ : ∃ b, expand x = [b] := by
    match h : expand x with
    | [] => simp [h] at hexpLen
    | [b] => exact ⟨b, rfl⟩
    | b1 :: b2 :: rest => simp [h] at hexpLen
  simp only [expand, hb]
  have hne : List.foldl LineBoundary.reader.step 0 (List.replicate k [b]).flatten ≠ 2 :=
    LineBoundary_foldl_rep_one b k hk 0 (by decide)
  simp [LineBoundary.reader, beq_eq_false_iff_ne, hne]

private theorem isSuffixOf_13_10_append (xs : Bytes) (b : Nat) :
    [13, 10].isSuffixOf (xs ++ [b]) = (xs.getLast? == some 13 && b == 10) := by
  cases xs with
  | nil => simp [List.isSuffixOf]
  | cons a as =>
      simp only [List.cons_append, List.getLast?_cons_cons]
      rw [show [13, 10].isSuffixOf (a :: (as ++ [b])) = ((a :: (as ++ [b])).takeLast 2 == [13, 10]) by
        simp [List.isSuffixOf]]
      have h1 : (a :: (as ++ [b])).takeLast 2 = [(a :: as).getLast (by simp), b] := by
        have hlen : (a :: (as ++ [b])).length = as.length + 2 := by simp
        simp [List.takeLast, hlen]
        have : (as.length + 2 - 2) = as.length := by omega
        rw [this]
        simp [List.drop_append, List.drop_length]
        have hdrop : (a :: as).drop as.length = [(a :: as).getLast (by simp)] := by
          rw [show as.length = (a :: as).length - 1 by simp]
          rw [List.drop_length_sub_one (by simp)]
          simp [List.getLast_eq_getLast?_get, List.getLast?_eq_getLast (by simp)]
        rw [hdrop]; simp
      rw [h1]
      simp [List.getLast?_eq_getLast (by simp : (a :: as) ≠ []), beq_iff_eq]
      constructor <;> intro h <;> exact ⟨h.1, h.2⟩

private theorem LineBoundary_foldl_state (xs : Bytes) :
    xs.foldl LineBoundary.reader.step 0 =
      if [13, 10].isSuffixOf xs then 2
      else if xs.getLast? == some 13 then 1
      else 0 := by
  induction xs using List.reverseRecOn with
  | nil => simp [LineBoundary.reader]
  | append_singleton xs b ih =>
      rw [List.foldl_append, List.foldl_cons, List.foldl_nil, ih]
      rw [isSuffixOf_13_10_append]
      simp only [Bool.and_eq_true, beq_iff_eq]
      by_cases hb13 : b = 13
      · subst hb13
        simp [LineBoundary.reader]
        have hnsuf : ¬ (xs.getLast? == some 13 && (13 : Nat) == 10) := by simp
        simp [hnsuf]
        split <;> split <;> simp [LineBoundary.reader]
      · by_cases hb10 : b = 10
        · subst hb10
          by_cases hxs13 : xs.getLast? = some 13
          · simp [hxs13, LineBoundary.reader]
            have hnsuf_prev : [13, 10].isSuffixOf xs = false := by
              cases xs with
              | nil => simp [List.isSuffixOf]
              | cons a as =>
                  rw [show [13, 10].isSuffixOf (a :: as) = ((a :: as).takeLast 2 == [13, 10]) by simp [List.isSuffixOf]]
                  by_contra hc
                  simp at hc
                  have hlast10 : (a :: as).getLast (by simp) = 10 := by
                    have hlen : (a :: as).length ≥ 2 := by
                      have : ([13,10]:Bytes).length ≤ (a :: as).length := by
                        rw [← List.isSuffixOf_iff, show List.isSuffixOf [13,10] (a::as) = ((a::as).takeLast 2 == [13,10]) by simp [List.isSuffixOf]]
                        simp [hc]
                      simp at this; omega
                    have : ((a :: as).takeLast 2).getLast? = some 10 := by rw [hc]; simp
                    have hgl : (a :: as).getLast (by simp) = ((a :: as).takeLast 2).getLast (by
                      intro h; have := congrArg List.length h; simp at this; omega) := by
                      simp [List.getLast_takeLast]
                    rw [hgl]
                    simp [List.getLast?_eq_getLast] at this
                    exact this
                  have hxs13' : (a :: as).getLast (by simp) = 13 := by
                    have := hxs13; simp [List.getLast?_eq_getLast] at this; exact this
                  omega
            simp [hnsuf_prev, hxs13, LineBoundary.reader]
          · have hcond : ¬ (xs.getLast? == some 13 && (10 : Nat) == 10) := by simpa using hxs13
            simp [hcond, LineBoundary.reader]
            split <;> split <;> simp [LineBoundary.reader, hxs13]
        · have hcond : ¬ (xs.getLast? == some 13 && b == 10) := by simp [hb10]
          simp [hcond, LineBoundary.reader]
          split <;> split <;> simp [LineBoundary.reader, hb13, hb10]

theorem sound_LineBoundary_FinalPair : LineBoundary.FinalPair.Goal := by
  intro x
  rw [matched_eq_foldl LineBoundary_stepBounded x (by decide : 0 < LineBoundary.reader.states)]
  simp only [LineBoundary.reader, LineBoundary_foldl_state]
  cases h : [13, 10].isSuffixOf (expand x) <;> simp [h]

/-! ## AsciiOnly specifics -/

private theorem AsciiOnly_step1 (b : Nat) : AsciiOnly.reader.step 1 b = 1 := by
  simp [AsciiOnly.reader]

private theorem AsciiOnly_foldl1 (xs : Bytes) : xs.foldl AsciiOnly.reader.step 1 = 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih => simp [AsciiOnly_step1, ih]

private theorem AsciiOnly_foldl0 (xs : Bytes) :
    xs.foldl AsciiOnly.reader.step 0 = if xs.all (· < 128) then 0 else 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.all_cons]
      by_cases hb : b < 128
      · simp [AsciiOnly.reader, hb, ih]
        cases h : bs.all (· < 128) <;> simp [h, AsciiOnly_foldl1]
      · simp [AsciiOnly.reader, hb, AsciiOnly_foldl1]

theorem sound_AsciiOnly_SevenBitFastPath : AsciiOnly.SevenBitFastPath.Goal := by
  intro x
  rw [matched_eq_foldl AsciiOnly_stepBounded x (by decide : 0 < AsciiOnly.reader.states)]
  simp only [AsciiOnly.reader, AsciiOnly_foldl0]
  cases h : (expand x).all (· < 128) <;> simp [h]

theorem sound_AsciiOnly_RejectEarly : AsciiOnly.RejectEarly.Goal := by
  intro a b hma
  rw [matched_eq_foldl AsciiOnly_stepBounded (.cat a b) (by decide : 0 < AsciiOnly.reader.states)]
  rw [matched_eq_foldl AsciiOnly_stepBounded a (by decide : 0 < AsciiOnly.reader.states)] at hma
  simp only [AsciiOnly.reader] at hma ⊢
  rw [AsciiOnly_foldl0] at hma
  cases ha : (expand a).all (· < 128) with
  | true => simp [ha] at hma
  | false => simp [expand, foldl_append_step, AsciiOnly_foldl0, ha, AsciiOnly_foldl1]

/-! ## SlashDepth specifics -/

private theorem SlashDepth_step2 (b : Nat) : SlashDepth.reader.step 2 b = 2 := by
  simp [SlashDepth.reader]

private theorem SlashDepth_foldl2 (xs : Bytes) : xs.foldl SlashDepth.reader.step 2 = 2 := by
  induction xs with
  | nil => simp
  | cons b bs ih => simp [SlashDepth_step2, ih]

private theorem SlashDepth_foldl_ge2 (xs : Bytes) (q : Nat) (hq : q ≤ 2) (hc : 2 ≤ q + xs.count 47) :
    xs.foldl SlashDepth.reader.step q = 2 := by
  induction xs generalizing q with
  | nil => simp at hc; omega
  | cons b bs ih =>
      simp only [List.foldl_cons, List.count_cons]
      by_cases hb : b = 47
      · subst hb
        have hstep : SlashDepth.reader.step q 47 = min 2 (q + 1) := by simp [SlashDepth.reader]
        rw [hstep]
        by_cases hq2 : q ≥ 2
        · have : min 2 (q + 1) = 2 := by omega
          simp [this, SlashDepth_foldl2]
        · have : min 2 (q + 1) = q + 1 := by omega
          rw [this]
          apply ih (q + 1) (by omega)
          simp at hc ⊢; omega
      · have hstep : SlashDepth.reader.step q b = q := by simp [SlashDepth.reader, hb]
        rw [hstep]
        apply ih q hq
        simp at hc ⊢; omega

private theorem SlashDepth_foldl_lt2 (xs : Bytes) (q : Nat) (hq : q ≤ 2) (hc : q + xs.count 47 < 2) :
    xs.foldl SlashDepth.reader.step q = q + xs.count 47 := by
  induction xs generalizing q with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.count_cons]
      by_cases hb : b = 47
      · subst hb
        have hq0 : q = 0 := by simp at hc; omega
        subst hq0
        simp [SlashDepth.reader] at *
        have hc' : 1 + bs.count 47 < 2 := by omega
        have : bs.count 47 = 0 := by omega
        simp [this]
      · have hstep : SlashDepth.reader.step q b = q := by simp [SlashDepth.reader, hb]
        rw [hstep]
        have hc' : q + bs.count 47 < 2 := by simp at hc; omega
        have := ih q hq hc'
        simp [this]; omega

theorem sound_SlashDepth_TwoDelimiters : SlashDepth.TwoDelimiters.Goal := by
  intro x
  rw [matched_eq_foldl SlashDepth_stepBounded x (by decide : 0 < SlashDepth.reader.states)]
  simp only [SlashDepth.reader]
  by_cases hc : 2 ≤ (expand x).count 47
  · have h := SlashDepth_foldl_ge2 (expand x) 0 (by omega) (by simpa)
    simp [h, hc]
  · push_neg at hc
    have h := SlashDepth_foldl_lt2 (expand x) 0 (by omega) (by simpa)
    simp [h, show ¬ 2 ≤ (expand x).count 47 by omega]
    interval_cases h2 : (expand x).count 47 <;> simp

private theorem count_flatten_replicate (xs : List Nat) (c n : Nat) :
    ((List.replicate n xs).flatten).count c = n * xs.count c := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.replicate_succ, List.flatten_cons, List.count_append, ih]; ring

theorem sound_SlashDepth_RepeatedDelimiter : SlashDepth.RepeatedDelimiter.Goal := by
  intro x n hn
  rw [sound_SlashDepth_TwoDelimiters]
  simp [expand, count_flatten_replicate]
  by_cases hc : (expand x).count 47 = 0
  · have hnone : 47 ∉ expand x := by rwa [List.count_eq_zero] at hc
    simp [hc, List.contains_iff_mem, hnone]
  · have hcpos : 0 < (expand x).count 47 := Nat.pos_of_ne_zero hc
    have hmem : 47 ∈ expand x := List.count_pos_iff.mp hcpos
    simp [List.contains_iff_mem, hmem]
    have : 2 ≤ n * (expand x).count 47 := by nlinarith
    simp [decide_eq_true_eq.mpr this]

/-! ## DecimalMod specifics -/

private theorem DecimalMod_step_digit (q x : Nat) (hq : q < 7) (hx : x < 10) :
    DecimalMod.reader.step q (48 + x) = (10 * q + x) % 7 := by
  have hb1 : ¬ (48 + x < 48) := by omega
  have hb2 : ¬ (57 < 48 + x) := by omega
  have hq7 : (q == 7) = false := by interval_cases q <;> decide
  simp [DecimalMod.reader, hq7, hb1, hb2]
  congr 1; omega

theorem sound_DecimalMod_DecimalSuffix : DecimalMod.DecimalSuffix.Goal := by
  intro a x y hx hy hq7
  have hq0 : (0 : Nat) < DecimalMod.reader.states := by decide
  rw [applyT_compile_eq_foldl DecimalMod_stepBounded (.cat a (.lit [48 + x, 48 + y])) hq0]
  simp [expand, foldl_append_step, List.foldl_cons, List.foldl_nil]
  rw [← applyT_compile_eq_foldl DecimalMod_stepBounded a hq0]
  set q := applyT (compile DecimalMod.reader a) 0
  have hq7' : q < 7 := hq7
  rw [DecimalMod_step_digit q x hq7' hx]
  have hq1 : (10 * q + x) % 7 < 7 := Nat.mod_lt _ (by decide)
  rw [DecimalMod_step_digit ((10 * q + x) % 7) y hq1 hy]
  have hmod : (10 * ((10 * q + x) % 7) + y) % 7 = (100 * q + 10 * x + y) % 7 := by
    have h1 : (10 * ((10 * q + x) % 7) + y) % 7 = (10 * (10 * q + x) + y) % 7 := by
      conv_lhs => rw [Nat.add_mod, Nat.mul_mod, Nat.mod_mod]
      rw [← Nat.mul_mod, ← Nat.add_mod]
    rw [h1]; congr 1; ring
  exact hmod

theorem sound_DecimalMod_LeadingZero : DecimalMod.LeadingZero.Goal := by
  intro x
  have hq0 : (0 : Nat) < DecimalMod.reader.states := by decide
  simp only [matched]
  rw [applyT_compile_eq_foldl DecimalMod_stepBounded (.cat (.lit [48]) x) hq0]
  rw [applyT_compile_eq_foldl DecimalMod_stepBounded x hq0]
  simp [expand, foldl_append_step, List.foldl_cons, List.foldl_nil]
  have hstep : DecimalMod.reader.step 0 48 = 0 := by simp [DecimalMod.reader]
  rw [hstep]

/-! ## EscapedTail specifics -/

theorem sound_EscapedTail_ResetAtDelimiter : EscapedTail.ResetAtDelimiter.Goal := by
  intro a
  have hq0 : (0 : Nat) < EscapedTail.reader.states := by decide
  rw [matched_eq_foldl EscapedTail_stepBounded (.cat a (.lit [47, 92])) hq0]
  simp [expand, foldl_append_step, List.foldl_cons, List.foldl_nil, EscapedTail.reader]

private theorem EscapedTail_foldl (xs : Bytes) :
    xs.foldl EscapedTail.reader.step 0 = ((xs.reverse.takeWhile (· == 92)).length % 2) := by
  induction xs using List.reverseRecOn with
  | nil => simp
  | append_singleton xs b ih =>
      rw [List.foldl_append, List.foldl_cons, List.foldl_nil]
      by_cases hb : b = 92
      · subst hb
        have hstep : EscapedTail.reader.step (xs.foldl EscapedTail.reader.step 0) 92 =
            1 - xs.foldl EscapedTail.reader.step 0 := by
          have hbound : xs.foldl EscapedTail.reader.step 0 < 2 :=
            foldl_step_bounded EscapedTail_stepBounded xs (by decide)
          simp only [EscapedTail.reader]
          interval_cases h : xs.foldl EscapedTail.reader.step 0 <;> simp
        rw [hstep, ih]
        simp [List.reverse_append, List.takeWhile_cons]
      · have hstep : EscapedTail.reader.step (xs.foldl EscapedTail.reader.step 0) b = 0 := by
          simp [EscapedTail.reader, hb]
        rw [hstep]
        have hb' : (b == 92) = false := by simpa using hb
        simp [List.reverse_append, List.takeWhile_cons, hb']

theorem sound_EscapedTail_TrailingRun : EscapedTail.TrailingRun.Goal := by
  intro x
  rw [matched_eq_foldl EscapedTail_stepBounded x (by decide : 0 < EscapedTail.reader.states)]
  simp only [EscapedTail.reader, EscapedTail_foldl]
  cases h : ((expand x).reverse.takeWhile (· == 92)).length % 2 <;> simp <;> omega

/-! ## CStringEnd specifics -/

private theorem CStringEnd_foldl_no0 (xs : Bytes) (h : xs.contains 0 = false) :
    xs.foldl CStringEnd.reader.step 0 = 0 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.contains_cons, Bool.or_eq_false_iff, beq_eq_false_iff_ne] at h ⊢
      have hb : b ≠ 0 := h.1
      have hbs2 : bs.contains 0 = false := by simpa [List.contains_eq_false_iff_not_mem] using h.2
      have hstep : CStringEnd.reader.step 0 b = 0 := by simp [CStringEnd.reader, hb]
      rw [hstep]; exact ih hbs2

private theorem CStringEnd_foldl_has0 (xs : Bytes) (h : xs.contains 0 = true) :
    xs.foldl CStringEnd.reader.step 0 = 1 ∨ xs.foldl CStringEnd.reader.step 0 = 2 := by
  induction xs with
  | nil => simp at h
  | cons b bs ih =>
      simp only [List.foldl_cons, List.contains_cons, Bool.or_eq_true] at h ⊢
      by_cases hb : b = 0
      · subst hb
        simp [CStringEnd.reader]
        have h1 : ∀ ys, ys.foldl CStringEnd.reader.step 1 = 1 ∨ ys.foldl CStringEnd.reader.step 1 = 2 := by
          intro ys
          cases ys with
          | nil => left; rfl
          | cons c cs =>
              right
              simp [CStringEnd.reader]
              have h2 : ∀ zs, zs.foldl CStringEnd.reader.step 2 = 2 := by
                intro zs; induction zs with
                | nil => rfl
                | cons d ds ih2 => simp [CStringEnd.reader, ih2]
              exact h2 cs
        exact h1 bs
      · have hb' : (b == 0) = false := by simpa using hb
        have hbs : bs.contains 0 = true := by cases h with | inl h => simp [hb'] at h | inr h => exact h
        have hstep : CStringEnd.reader.step 0 b = 0 := by simp [CStringEnd.reader, hb]
        rw [hstep]; exact ih hbs

private theorem CStringEnd_foldl_from1 (xs : Bytes) :
    xs.foldl CStringEnd.reader.step 1 = if xs.isEmpty then 1 else 2 := by
  cases xs with
  | nil => simp
  | cons b bs =>
      simp [CStringEnd.reader]
      have h2 : ∀ ys, ys.foldl CStringEnd.reader.step 2 = 2 := by
        intro ys; induction ys with
        | nil => rfl
        | cons c cs ih => simp [CStringEnd.reader, ih]
      exact h2 bs

private theorem CStringEnd_foldl_from2 (xs : Bytes) : xs.foldl CStringEnd.reader.step 2 = 2 := by
  induction xs with
  | nil => rfl
  | cons b bs ih => simp [CStringEnd.reader, ih]

theorem sound_CStringEnd_OneTerminator : CStringEnd.OneTerminator.Goal := by
  intro x
  have hq0 : (0 : Nat) < CStringEnd.reader.states := by decide
  rw [matched_eq_foldl CStringEnd_stepBounded (.cat x (.lit [0])) hq0]
  simp only [expand, foldl_append_step, List.foldl_cons, List.foldl_nil, CStringEnd.reader]
  cases h : (expand x).contains 0 with
  | false =>
      rw [CStringEnd_foldl_no0 (expand x) h]
      simp [CStringEnd.reader]
  | true =>
      have hres := CStringEnd_foldl_has0 (expand x) h
      rcases hres with h1 | h2
      · rw [h1]; simp [CStringEnd.reader]
      · rw [h2]; simp [CStringEnd.reader, CStringEnd_foldl_from2]

theorem sound_CStringEnd_TruncateAtTerminator : CStringEnd.TruncateAtTerminator.Goal := by
  intro pre tail hpre
  have hq0 : (0 : Nat) < CStringEnd.reader.states := by decide
  rw [matched_eq_foldl CStringEnd_stepBounded (.cat (.cat pre (.lit [0])) tail) hq0]
  simp only [expand, foldl_append_step, List.foldl_cons, List.foldl_nil]
  rw [CStringEnd_foldl_no0 (expand pre) hpre]
  simp [CStringEnd.reader]
  by_cases ht : extent tail = 0
  · have hempty : expand tail = [] := by
      have : (expand tail).length = 0 := by simpa [expand_length] using ht
      exact List.eq_nil_of_length_eq_zero this
    simp [hempty, ht, CStringEnd.reader]
  · have hne : (expand tail) ≠ [] := by
      intro he; have := congrArg List.length he; simp [expand_length] at this; omega
    have hne' : (expand tail).isEmpty = false := by
      cases h : (expand tail).isEmpty with
      | false => rfl
      | true => simp [List.isEmpty_eq_true] at h; exact absurd h hne
    rw [CStringEnd_foldl_from1]
    simp [hne', ht, CStringEnd.reader]

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScans
