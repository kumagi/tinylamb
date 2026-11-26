import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `SlashDepth.DictionaryMask` — Cache acceptance once per phrase; a missing local
  dictionary code is rejected.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_SlashDepth_DictionaryMask

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.SlashDepth

/-! Small-input truth check.  Every scanned state satisfies `q < reader.states`
(three saturating delimiter counts 0,1,2).  `sampleIds` also contains an
out-of-range code to exercise the `getD false` fallback. -/

private def samples : List Rope :=
  [ .lit [], .lit [47], .lit [65], .lit [47, 47], .lit [47, 65, 47],
    .lit [65, 66, 67], .cat (.lit [65]) (.lit [47]), .cat (.lit [47, 65]) (.lit [66, 47]),
    .rep 0 (.lit [47]), .rep 2 (.lit [47]), .rep 3 (.lit [65]),
    .rep 2 (.lit [47, 65, 47]), .cat (.rep 2 (.lit [65])) (.lit [47]) ]

private def sampleIds : List Nat := [0, 1, 2, 9]

private def check : Bool :=
  (List.range reader.states).all fun q =>
    rows reader samples sampleIds q =
      sampleIds.map (fun i =>
        (samples[i]?.map (fun x => reader.accept ((expand x).foldl reader.step q))).getD false)

#eval check

example : check := by native_decide

/-! Generic transition-table facts, specialized to readers whose step function
stays inside the packed state set.  The SlashDepth reader is closed in this
sense, so compiled tables agree with direct folding for every rope. -/

private def SdClosed (r : Reader) : Prop :=
  ∀ q b, q < r.states → r.step q b < r.states

private theorem sd_foldl_closed (r : Reader) (h : SdClosed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem sd_power_length (n : Nat) (t : Table) :
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

private theorem sd_compile_length (r : Reader) (x : Rope) :
    (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha ihb => simp [compile, chain, iha, ihb]
  | rep k a ih => simp [compile, sd_power_length, ih]

private theorem sd_applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, chain, List.getElem?_map, List.getElem?_eq_getElem hq]

private theorem sd_flatten_replicate_add (a b : Nat) (s : Bytes) :
    (List.replicate (a + b) s).flatten =
      (List.replicate a s).flatten ++ (List.replicate b s).flatten := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.flatten_cons, ih,
        List.replicate_succ, List.flatten_cons]
      simp [List.append_assoc]

private theorem sd_power_apply_of (r : Reader) (h : SdClosed r) (x : Rope)
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
        rw [sd_compile_length]
        change (List.range r.states)[q]?.getD q = q
        simp [applyT, List.getElem?_range hq]
      · rename_i hn
        let m := n / 2
        let ht := power m (compile r x)
        have hm : m < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih m hm
        have hlen : ht.length = r.states := by
          simp [ht, sd_power_length, sd_compile_length]
        split
        · rename_i heven
          change applyT (chain ht ht) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod0 : n % 2 = 0 := by simpa using heven
          have heq : n = m + m := by
            change n = n / 2 + n / 2
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, sd_flatten_replicate_add, List.foldl_append]
          rw [sd_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          exact ih' _ (sd_foldl_closed r h
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
          simp only [List.flatten_cons, List.flatten_nil, List.nil_append]
          rw [List.foldl_append, List.foldl_append]
          rw [sd_applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; rw [hlen]; exact hq)]
          rw [sd_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          rw [ih' _ (sd_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)]
          simp only [List.foldl_nil]
          rw [sd_flatten_replicate_add, List.foldl_append]
          have hq1 := sd_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq
          have hq2 := sd_foldl_closed r h
            ((List.replicate m (expand x)).flatten) _ hq1
          rw [ha _ hq2]

private theorem sd_compile_apply (r : Reader) (h : SdClosed r) (x : Rope) (q : Nat)
    (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      simp only [compile, applyT, List.getElem?_map,
        List.getElem?_range (by simpa using hq), Option.map_some, Option.getD_some]
      rfl
  | cat a b iha ihb =>
      rw [compile, sd_applyT_chain _ _ q (by simpa [sd_compile_length] using hq),
        iha q hq]
      rw [ihb _ (sd_foldl_closed r h (expand a) q hq)]
      simp [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      exact sd_power_apply_of r h a ih k q hq

private theorem sd_dictionaryMask_of_closed (r : Reader) (h : SdClosed r) :
    dictionaryMask r := by
  intro d ids q hq
  unfold rows matched
  apply List.map_congr_left
  intro i hi
  simp only [List.getElem?_map]
  cases hd : d[i]? with
  | none => rfl
  | some x =>
      simpa [hd, matched] using congrArg r.accept (sd_compile_apply r h x q hq)

/-! SlashDepth saturates at 2: a slash sends `q` to `min 2 (q+1) ≤ 2 < 3`,
and non-slash bytes leave `q` unchanged. -/

private theorem sd_closed : SdClosed reader := by
  intro q b hq
  dsimp [reader] at hq ⊢
  split
  · exact Nat.lt_of_le_of_lt (Nat.min_le_left 2 (q + 1)) (by decide)
  · exact hq

theorem sound_DictionaryMask : SlashDepth.DictionaryMask.Goal := by
  exact sd_dictionaryMask_of_closed reader sd_closed

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_SlashDepth_DictionaryMask
