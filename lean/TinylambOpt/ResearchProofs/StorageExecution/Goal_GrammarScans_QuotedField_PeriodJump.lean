import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_QuotedField_PeriodJump

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField

/-! Truth check on small inputs.  Only triples satisfying the hypothesis
`q < reader.states`, `0 < period` and `applyT (power period (compile reader x)) q = q`
are required to satisfy the conclusion; the remaining triples are vacuous.
The sample phrases mix quote and non-quote bytes, empty phrases,
concatenations and repetitions. -/

private def samples : List Rope :=
  [ .lit [], .lit [34], .lit [65], .lit [34, 34], .lit [65, 34, 66],
    .cat (.lit [34]) (.lit [65]), .cat (.lit [65, 34]) (.lit [34]),
    .rep 0 (.lit [34]), .rep 2 (.lit [65]), .rep 3 (.lit [34, 34]),
    .rep 2 (.lit [34, 65]), .cat (.rep 2 (.lit [65])) (.lit [34]) ]

private def checkOne (x : Rope) (q period n : Nat) : Bool :=
  if _hq : q < reader.states then
    if _hper : 0 < period then
      if applyT (power period (compile reader x)) q = q then
        decide (matched reader (.rep n x) q = matched reader (.rep (n % period) x) q)
      else true
    else true
  else true

private def check : Bool :=
  samples.all fun x =>
    (List.range reader.states).all fun q =>
      [1, 2, 3, 4].all fun period =>
        (List.range 8).all fun n => checkOne x q period n

#eval check

example : check := by native_decide

/-! Generic table/expansion lemmas (the reader's step stays inside its state
space, so compiled tables describe byte-wise folding). -/

private theorem gs_power_length (n : Nat) (t : Table) :
    (power n t).length = t.length := by
  induction n using Nat.strongRecOn generalizing t with
  | ind n ih =>
      unfold power
      split
      · simp [List.length_range]
      · rename_i hn
        split
        · simp only [chain, List.length_map]
          exact ih (n / 2) (Nat.div_lt_self (by omega) (by decide)) t
        · simp only [chain, List.length_map]
          exact ih (n / 2) (Nat.div_lt_self (by omega) (by decide)) t

private theorem gs_compile_length (r : Reader) (x : Rope) :
    (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha ihb => simp [compile, chain, iha]
  | rep k a ih => simp [compile, gs_power_length, ih]

private theorem gs_applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, chain, List.getElem?_map, List.getElem?_eq_getElem hq]

private def GsClosed (r : Reader) : Prop :=
  ∀ q b, q < r.states → r.step q b < r.states

private theorem gs_foldl_closed (r : Reader) (h : GsClosed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem gs_flatten_replicate_add (a b : Nat) (s : Bytes) :
    (List.replicate (a + b) s).flatten =
      (List.replicate a s).flatten ++ (List.replicate b s).flatten := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.flatten_cons, ih,
        List.replicate_succ, List.flatten_cons]
      simp [List.append_assoc]

private theorem gs_power_apply_of (r : Reader) (h : GsClosed r) (x : Rope)
    (ha : ∀ q, q < r.states →
      applyT (compile r x) q = (expand x).foldl r.step q) (n : Nat)
    (q : Nat) (hq : q < r.states) :
    applyT (power n (compile r x)) q =
      ((List.replicate n (expand x)).flatten).foldl r.step q := by
  induction n using Nat.strongRecOn generalizing q with
  | ind n ih =>
      rw [power]
      split
      · rename_i hn
        subst n
        rw [gs_compile_length]
        change (List.range r.states)[q]?.getD q = q
        simp [List.getElem?_range hq]
      · rename_i hn
        let m := n / 2
        let ht := power m (compile r x)
        have hm : m < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih m hm
        have hlen : ht.length = r.states := by
          simp [ht, gs_power_length, gs_compile_length]
        split
        · rename_i heven
          change applyT (chain ht ht) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod0 : n % 2 = 0 := by simpa using heven
          have heq : n = m + m := by
            change n = n / 2 + n / 2
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, gs_flatten_replicate_add, List.foldl_append]
          rw [gs_applyT_chain ht ht q (by simpa [hlen] using hq), ih' q hq]
          exact ih' _ (gs_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)
        · rename_i hodd
          change applyT (chain (chain ht ht) (compile r x)) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod1 : n % 2 = 1 := by
            have hne : n % 2 ≠ 0 := by
              intro hz
              apply hodd
              simp [hz]
            have hlt := Nat.mod_lt n (by decide : 0 < 2)
            omega
          have heq : n = m + m + 1 := by
            change n = n / 2 + n / 2 + 1
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, List.replicate_succ', List.flatten_append]
          simp only [List.flatten_cons, List.flatten_nil]
          rw [List.foldl_append, List.foldl_append]
          rw [gs_applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; simpa [hlen] using hq)]
          rw [gs_applyT_chain ht ht q (by simpa [hlen] using hq), ih' q hq]
          rw [ih' _ (gs_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)]
          simp only [List.foldl_nil]
          rw [gs_flatten_replicate_add, List.foldl_append]
          have hq1 := gs_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq
          have hq2 := gs_foldl_closed r h
            ((List.replicate m (expand x)).flatten) _ hq1
          rw [ha _ hq2]

private theorem gs_compile_apply (r : Reader) (h : GsClosed r) (x : Rope) (q : Nat)
    (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      simp only [compile, applyT, List.getElem?_map,
        List.getElem?_range (by simpa using hq), Option.map_some, Option.getD_some]
      rfl
  | cat a b iha ihb =>
      rw [compile, gs_applyT_chain _ _ q (by simpa [gs_compile_length] using hq),
        iha q hq]
      rw [ihb _ (gs_foldl_closed r h (expand a) q hq)]
      simp [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      exact gs_power_apply_of r h a ih k q hq

private theorem gs_periodJump_of_closed (r : Reader) (h : GsClosed r) : periodJump r := by
  intro x q period n hq hper hfix
  have hpow : ∀ (m s : Nat), s < r.states →
      applyT (power m (compile r x)) s =
        ((List.replicate m (expand x)).flatten).foldl r.step s :=
    gs_power_apply_of r h x (gs_compile_apply r h x)
  have hiter : ∀ k, applyT (power (k * period) (compile r x)) q = q := by
    intro k
    induction k with
    | zero =>
        simp only [Nat.zero_mul]
        rw [hpow 0 q hq]
        simp
    | succ k ih =>
        have hsum : (k + 1) * period = k * period + period := by
          simp [Nat.add_mul]
        rw [hsum, hpow (k * period + period) q hq, gs_flatten_replicate_add,
          List.foldl_append]
        rw [← hpow (k * period) q hq, ih]
        rw [← hpow period q hq]
        exact hfix
  unfold matched
  rw [gs_compile_apply r h (.rep n x) q hq,
    gs_compile_apply r h (.rep (n % period) x) q hq]
  simp only [expand]
  let k := n / period
  let rem := n % period
  have hn : k * period + rem = n := by
    dsimp [k, rem]
    rw [Nat.mul_comm (n / period) period]
    exact Nat.div_add_mod n period
  have hleft : ((List.replicate (k * period) (expand x)).flatten).foldl r.step q = q := by
    rw [← hpow (k * period) q hq]
    exact hiter k
  have hstate :
      ((List.replicate n (expand x)).flatten).foldl r.step q =
        ((List.replicate rem (expand x)).flatten).foldl r.step q := by
    calc
      ((List.replicate n (expand x)).flatten).foldl r.step q
          = ((List.replicate (k * period + rem) (expand x)).flatten).foldl r.step q := by
              rw [hn]
      _ = (((List.replicate (k * period) (expand x)).flatten ++
              (List.replicate rem (expand x)).flatten)).foldl r.step q := by
              rw [gs_flatten_replicate_add]
      _ = ((List.replicate rem (expand x)).flatten).foldl r.step
            (((List.replicate (k * period) (expand x)).flatten).foldl r.step q) := by
              rw [List.foldl_append]
      _ = ((List.replicate rem (expand x)).flatten).foldl r.step q := by
              rw [hleft]
  rw [hstate]

/-! QuotedField's step function flips a bit on a quote byte and otherwise
preserves the incoming state; both successors stay below `reader.states`. -/

private theorem quote_closed : GsClosed reader := by
  intro q b hq
  dsimp [reader] at hq ⊢
  by_cases hb : b == 34
  · simp [hb]
    omega
  · simpa [hb] using hq

theorem sound_PeriodJump : QuotedField.PeriodJump.Goal :=
  gs_periodJump_of_closed reader quote_closed

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_QuotedField_PeriodJump
