import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_DecimalMod_PeriodJump

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.DecimalMod

/-! Small-input truth check.  Inputs that do not satisfy the cycle hypothesis
    are ignored by the `||` guard. -/

private def samples : List Rope :=
  [ .lit [], .lit [48], .lit [49], .lit [57], .lit [65], .lit [48, 48], .lit [49, 50, 51],
    .lit [48, 65, 57], .cat (.lit [49]) (.lit [50]), .cat (.lit [49, 65]) (.lit [50, 51]),
    .rep 0 (.lit [57]), .rep 2 (.lit [48]), .rep 3 (.lit [49, 50]), .rep 2 (.lit [49, 65]),
    .cat (.rep 2 (.lit [49])) (.lit [50]) ]

private def check : Bool :=
  samples.all fun x => (List.range reader.states).all fun q =>
    (List.range 5).all fun p0 =>
      let period := p0 + 1
      (List.range 8).all fun n =>
        applyT (power period (compile reader x)) q != q ||
          matched reader (.rep n x) q == matched reader (.rep (n % period) x) q

#eval check

example : check := by native_decide

/-! Generic table/compile facts (the decimal-mod reader keeps every successor
    state inside the eight-state set). -/

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
  | cat a b iha ihb => simp [compile, chain, iha, ihb]
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
        simp [applyT, List.getElem?_range hq]
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
          rw [gs_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          exact ih' _ (gs_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)
        · rename_i hodd
          change applyT (chain (chain ht ht) (compile r x)) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have heq : n = m + m + 1 := by
            change n = n / 2 + n / 2 + 1
            have hd := Nat.div_add_mod n 2
            have hmod0 : n % 2 ≠ 0 := by
              intro hz
              apply hodd
              simp [hz]
            have hmod1 : n % 2 = 1 := by
              have hlt := Nat.mod_lt n (by decide : 0 < 2)
              omega
            omega
          rw [heq, List.replicate_succ', List.flatten_append]
          simp only [List.flatten_cons, List.flatten_nil, List.nil_append]
          rw [List.foldl_append, List.foldl_append]
          rw [gs_applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; rw [hlen]; exact hq)]
          rw [gs_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
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

private theorem gs_decimal_closed : GsClosed reader := by
  intro q b hq
  dsimp [reader] at hq ⊢
  split
  · omega
  · have hmod : (10 * q + b - 48) % 7 < 7 := Nat.mod_lt _ (by decide)
    omega

private theorem matched_fold (x : Rope) (q : Nat) (hq : q < reader.states) :
    matched reader x q = reader.accept ((expand x).foldl reader.step q) := by
  unfold matched
  rw [gs_compile_apply reader gs_decimal_closed x q hq]

/-! A cycle back to the starting state makes whole periods invisible to the
    fold; only the final partial period can affect acceptance. -/

theorem sound_PeriodJump : DecimalMod.PeriodJump.Goal := by
  intro x q period n hq hperiod hcycle
  have ha : ∀ s, s < reader.states →
      applyT (compile reader x) s = (expand x).foldl reader.step s :=
    gs_compile_apply reader gs_decimal_closed x
  have hperiod_fold : ((List.replicate period (expand x)).flatten).foldl reader.step q = q := by
    rw [← gs_power_apply_of reader gs_decimal_closed x ha period q hq]
    exact hcycle
  have hwhole : ∀ k : Nat,
      ((List.replicate (k * period) (expand x)).flatten).foldl reader.step q = q := by
    intro k
    induction k with
    | zero => simp
    | succ k ih =>
        have hmul : (k + 1) * period = k * period + period := by
          exact Nat.succ_mul k period
        rw [hmul, gs_flatten_replicate_add, List.foldl_append, ih, hperiod_fold]
  let k := n / period
  let r := n % period
  have hn_decomp : n = k * period + r := by
    dsimp [k, r]
    rw [Nat.mul_comm (n / period) period]
    exact (Nat.div_add_mod n period).symm
  have hfold : ((List.replicate n (expand x)).flatten).foldl reader.step q =
      ((List.replicate r (expand x)).flatten).foldl reader.step q := by
    rw [hn_decomp, gs_flatten_replicate_add, List.foldl_append, hwhole k]
  change matched reader (.rep n x) q = matched reader (.rep r x) q
  rw [matched_fold (.rep n x) q hq, matched_fold (.rep r x) q hq]
  dsimp [expand]
  rw [hfold]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_DecimalMod_PeriodJump
