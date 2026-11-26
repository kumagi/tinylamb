import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_Checkpoint

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit

/-! Truth check on small inputs (offsets and lengths also cover phrase
boundaries, empty phrases and repetitions). -/

private def samples : List Rope :=
  [ .lit [], .lit [48], .lit [57], .lit [65], .lit [48, 57], .lit [57, 65, 48],
    .cat (.lit [48]) (.lit [65]), .cat (.lit [65, 49]) (.lit [57]),
    .rep 0 (.lit [48]), .rep 2 (.lit [57]), .rep 3 (.lit []),
    .rep 2 (.lit [48, 65, 57]), .cat (.rep 2 (.lit [65])) (.lit [50]) ]

private def check : Bool :=
  samples.all fun x => (List.range 7).all fun a =>
    (List.range 7).all fun b =>
      (List.range reader.states).all fun q =>
        partialScan reader x (a + b) q ==
          (((expand x).drop a).take b).foldl reader.step (partialScan reader x a q)

#eval check

example : check := by native_decide

/-! The EndsInDigit step ignores the current state: it only inspects the last
byte.  Hence a scan is fully determined by the last byte of the prefix. -/

private abbrev digit (b : Nat) : Nat := (decide (48 ≤ b ∧ b ≤ 57)).toNat

private abbrev scan (xs : Bytes) (q : Nat) : Nat := xs.foldl reader.step q

private theorem step_eq_digit (q b : Nat) : reader.step q b = digit b := rfl

private theorem digit_lt_two (b : Nat) : digit b < 2 := by
  unfold digit
  by_cases h : 48 ≤ b ∧ b ≤ 57 <;> simp [h]

private theorem getLast?_eq_none_iff (xs : Bytes) : xs.getLast? = none ↔ xs = [] := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      cases xs with
      | nil => simp
      | cons y ys => simp [ih]

private theorem scan_getLast (xs : Bytes) (q : Nat) :
    scan xs q = match xs.getLast? with | none => q | some b => digit b := by
  induction xs generalizing q with
  | nil => simp [scan]
  | cons b bs ih =>
      simp only [scan, List.foldl_cons]
      rw [step_eq_digit]
      change scan bs (digit b) = match (b :: bs).getLast? with
        | none => q | some c => digit c
      rw [ih (digit b)]
      cases bs with
      | nil => simp
      | cons c cs =>
          cases hs : (c :: cs).getLast? with
          | none =>
              have hnil : c :: cs = [] := (getLast?_eq_none_iff (c :: cs)).mp hs
              simp at hnil
          | some d => simp [hs]

private theorem scan_bounded (xs : Bytes) (q : Nat) (hq : q < 2) : scan xs q < 2 := by
  rw [scan_getLast]
  cases h : xs.getLast? with
  | none => simpa using hq
  | some b => exact digit_lt_two b

private theorem scan_idem (xs : Bytes) (q : Nat) : scan xs (scan xs q) = scan xs q := by
  cases h : xs.getLast? <;> simp [scan_getLast, h]

private theorem scan_append (xs ys : Bytes) (q : Nat) :
    scan (xs ++ ys) q = scan ys (scan xs q) := by
  induction xs generalizing q with
  | nil => simp [scan]
  | cons x xs ih => simp [scan, ih]

private theorem replicate_add (m n : Nat) (xs : Bytes) :
    List.replicate (m + n) xs = List.replicate m xs ++ List.replicate n xs := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [Nat.succ_add, List.replicate_succ, ih]
      simp [List.replicate_succ]

private theorem scan_replicate (xs : Bytes) (n : Nat) (q : Nat) :
    scan ((List.replicate n xs).flatten) q = if n = 0 then q else scan xs q := by
  cases n with
  | zero => simp [scan]
  | succ m =>
      induction m generalizing q with
      | zero => simp [scan]
      | succ m ih =>
          have hrep : List.replicate (Nat.succ (Nat.succ m)) xs =
              xs :: List.replicate (Nat.succ m) xs := by
            rw [show Nat.succ (Nat.succ m) = Nat.succ m + 1 by omega, List.replicate_succ]
          rw [hrep, List.flatten_cons, scan_append]
          have ih' : scan ((List.replicate (Nat.succ m) xs).flatten) (scan xs q) =
              scan xs (scan xs q) := by
            simpa using ih (scan xs q)
          rw [ih']
          have hpos : ¬ (m + 1 + 1 = 0) := by omega
          rw [ite_eq_right hpos]
          exact scan_idem xs q

private theorem take_add_eq_append (xs : List Nat) (a b : Nat) :
    xs.take (a + b) = xs.take a ++ (xs.drop a).take b := by
  induction xs generalizing a b with
  | nil => simp
  | cons x xs ih =>
      cases a with
      | zero => simp
      | succ a => simp [Nat.succ_add, ih]

private theorem expand_length (x : Rope) : (expand x).length = extent x := by
  induction x with
  | lit s => simp [expand, extent]
  | cat a b ihA ihB => simp [expand, extent, ihA, ihB]
  | rep k a ihA => simp [expand, extent, ihA]

/-! Transition tables produced by this reader have two entries, all of them in
the state space. -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · simp [power, hn]
      · have hhalf : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
        unfold power
        simp only [hn, dite_false]
        by_cases he : n % 2 == 0
        · simp [he, chain_length, ih (n / 2) hhalf]
        · simp [he, chain_length, ih (n / 2) hhalf]

private theorem compile_length (r : Reader) (x : Rope) : (compile r x).length = r.states := by
  induction x generalizing r with
  | lit s => simp [compile]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep k a ihA =>
      rw [show compile r (Rope.rep k a) = power k (compile r a) by rfl]
      rw [power_length]
      exact ihA r

private theorem applyT_chain_bdd (a b : Table) (ha : a.length = 2) (hb : b.length = 2)
    (hab : ∀ s, s < 2 → applyT a s < 2) (q : Nat) (hq : q < 2) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hc : (chain a b).length = 2 := by simpa [chain_length] using ha
  have hq1 : q < (chain a b).length := by omega
  have hq2 : q < a.length := by omega
  have hq3 : applyT a q < b.length := by
    have := hab q hq
    omega
  simp [applyT, chain, List.getElem?_eq_getElem hq2]

/-- Squaring an idempotent two-state transition function does not change it. -/
private theorem power_applyT_idem (t : Table) (g : Nat → Nat)
    (htlen : t.length = 2)
    (hg_bdd : ∀ s, s < 2 → g s < 2)
    (hg_idem : ∀ s, s < 2 → g (g s) = g s)
    (ht : ∀ s, s < 2 → applyT t s = g s) :
    ∀ n q, q < 2 → applyT (power n t) q = if n = 0 then q else g q := by
  intro n
  induction n using Nat.strongRecOn with
  | _ n ih =>
      intro q hq
      by_cases hn : n = 0
      · subst n
        have hq' : q < t.length := by omega
        have hq'' : q < (List.range t.length).length := by simpa using hq'
        simp [power, applyT, List.getElem?_eq_getElem hq'']
      · rw [ite_eq_right hn]
        have hhalf : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
        have ihH := ih (n / 2) hhalf
        have hlenp : (power (n / 2) t).length = 2 := by rw [power_length]; exact htlen
        have hbddp : ∀ s, s < 2 → applyT (power (n / 2) t) s < 2 := by
          intro s hs
          rw [ihH s hs]
          by_cases h0 : n / 2 = 0
          · simpa [h0] using hs
          · simpa [h0] using hg_bdd s hs
        have hsq : ∀ s, s < 2 → applyT (chain (power (n / 2) t) (power (n / 2) t)) s =
            if n / 2 = 0 then s else g s := by
          intro s hs
          rw [applyT_chain_bdd (power (n / 2) t) (power (n / 2) t) hlenp hlenp hbddp s hs]
          by_cases h0 : n / 2 = 0
          · have hid : ∀ w, w < 2 → applyT (power 0 t) w = w := by
              intro w hw
              simpa [h0] using ihH w hw
            rw [h0]
            rw [hid s hs, hid s hs]
            simp
          · rw [ite_eq_right h0]
            rw [ihH s hs]
            simp only [h0, ↓reduceIte]
            rw [ihH (g s) (hg_bdd s hs)]
            simp [h0, hg_idem s hs]
        have hsqbdd : ∀ s, s < 2 →
            applyT (chain (power (n / 2) t) (power (n / 2) t)) s < 2 := by
          intro s hs
          rw [hsq s hs]
          by_cases h0 : n / 2 = 0
          · simpa [h0] using hs
          · simpa [h0] using hg_bdd s hs
        unfold power
        simp only [hn, dite_false]
        by_cases he : n % 2 == 0
        · simp [he]
          rw [hsq q hq]
          have hhalf_ne : n / 2 ≠ 0 := by
            intro hz
            have hn1 : n = 1 := by omega
            subst n
            simp at he
          simp [hhalf_ne]
        · simp [he]
          have hsqlen : (chain (power (n / 2) t) (power (n / 2) t)).length = 2 := by
            simpa [chain_length] using hlenp
          rw [applyT_chain_bdd (chain (power (n / 2) t) (power (n / 2) t)) t
            hsqlen htlen hsqbdd q hq]
          rw [hsq q hq]
          by_cases h0 : n / 2 = 0
          · simp [h0, ht q hq]
          · simp [h0, ht (g q) (hg_bdd q hq), hg_idem q hq]

/-- A rope compiles to a table whose entry at `q` is the scan of its expansion;
moreover its powers agree with scanning repeated expansions. -/
private theorem compile_applyT_pair (x : Rope) :
    (∀ q, q < 2 → applyT (compile reader x) q = scan (expand x) q) ∧
    (∀ n q, q < 2 → applyT (power n (compile reader x)) q =
      if n = 0 then q else scan (expand x) q) := by
  induction x with
  | lit s =>
      have hfirst : ∀ q, q < 2 → applyT (compile reader (.lit s)) q = scan s q := by
        intro q hq
        have hq' : q < (List.range reader.states).length := by simp [reader]; omega
        simp [compile, applyT, scan, List.getElem?_eq_getElem hq']
      exact ⟨hfirst, power_applyT_idem (compile reader (.lit s)) (scan s)
        (by simpa [reader] using compile_length reader (.lit s))
        (fun w hw => scan_bounded s w hw)
        (fun w hw => scan_idem s w)
        hfirst⟩
  | cat a b ihA ihB =>
      have hlenA : (compile reader a).length = 2 := by simpa [reader] using compile_length reader a
      have hlenB : (compile reader b).length = 2 := by simpa [reader] using compile_length reader b
      have hbddA : ∀ w, w < 2 → applyT (compile reader a) w < 2 := by
        intro w hw
        rw [ihA.1 w hw]
        exact scan_bounded (expand a) w hw
      have hfirst : ∀ q, q < 2 →
          applyT (compile reader (.cat a b)) q = scan (expand (.cat a b)) q := by
        intro q hq
        have hstate : applyT (compile reader a) q < 2 := by
          rw [ihA.1 q hq]
          exact scan_bounded (expand a) q hq
        rw [show compile reader (Rope.cat a b) = chain (compile reader a) (compile reader b) by rfl]
        rw [applyT_chain_bdd (compile reader a) (compile reader b) hlenA hlenB hbddA q hq]
        rw [ihB.1 _ hstate, ihA.1 q hq]
        simp [expand]
      exact ⟨hfirst, power_applyT_idem (compile reader (.cat a b)) (scan (expand (.cat a b)))
        (by simpa [reader] using compile_length reader (.cat a b))
        (fun w hw => scan_bounded (expand (.cat a b)) w hw)
        (fun w hw => scan_idem (expand (.cat a b)) w)
        hfirst⟩
  | rep k a ihA =>
      have hfirst : ∀ q, q < 2 →
          applyT (compile reader (.rep k a)) q = scan (expand (.rep k a)) q := by
        intro q hq
        rw [show compile reader (Rope.rep k a) = power k (compile reader a) by rfl]
        rw [ihA.2 k q hq]
        simpa [expand] using (scan_replicate (expand a) k q).symm
      exact ⟨hfirst, power_applyT_idem (compile reader (.rep k a)) (scan (expand (.rep k a)))
        (by simpa [reader] using compile_length reader (.rep k a))
        (fun w hw => scan_bounded (expand (.rep k a)) w hw)
        (fun w hw => scan_idem (expand (.rep k a)) w)
        hfirst⟩

private theorem compile_applyT (x : Rope) (q : Nat) (hq : q < 2) :
    applyT (compile reader x) q = scan (expand x) q :=
  (compile_applyT_pair x).1 q hq

private theorem power_compile_applyT (a : Rope) (n q : Nat) (hq : q < 2) :
    applyT (power n (compile reader a)) q = if n = 0 then q else scan (expand a) q :=
  (compile_applyT_pair a).2 n q hq

/-! Prefix scanning agrees with folding over the taken prefix. -/

private theorem prefixSeek_ed : ∀ x q n, q < 2 →
    partialScan reader x n q = scan ((expand x).take n) q := by
  intro x
  induction x with
  | lit s =>
      intro q n hq
      rfl
  | cat a b ihA ihB =>
      intro q n hq
      simp only [partialScan]
      by_cases hn : n ≤ extent a
      · rw [ite_eq_left hn]
        have hlen : (expand a).length = extent a := expand_length a
        have ht : ((expand a) ++ (expand b)).take n = (expand a).take n := by
          apply List.take_append_of_le_length
          omega
        rw [ihA q n hq]
        simp [expand, ht]
      · rw [ite_eq_right hn]
        have hgt : extent a < n := Nat.lt_of_not_ge hn
        have hlen : (expand a).length = extent a := expand_length a
        have hqa : applyT (compile reader a) q < 2 := by
          rw [compile_applyT a q hq]
          exact scan_bounded (expand a) q hq
        have ht_len : (expand a).length ≤ n := by omega
        have ht : ((expand a) ++ (expand b)).take n =
            (expand a) ++ (expand b).take (n - (expand a).length) := by
          rw [List.take_append, List.take_of_length_le ht_len]
        rw [ihB _ _ hqa]
        rw [compile_applyT a q hq]
        simp only [expand]
        rw [ht, scan_append]
        simp [hlen]
  | rep k a ihA =>
      intro q n hq
      by_cases hsize : extent a = 0
      · have hexp : expand a = [] := by
          apply List.eq_nil_of_length_eq_zero
          simpa [expand_length] using hsize
        have hexpRep : expand (.rep k a) = [] := by simp [expand, hexp]
        simp [partialScan, hsize, hexpRep, scan]
      · have hsizePos : 0 < extent a := Nat.pos_of_ne_zero hsize
        let full := min k (n / extent a)
        have hfull_le_n : full * extent a ≤ n := by
          have h1 : full ≤ n / extent a := Nat.min_le_right _ _
          have h2 : full * extent a ≤ (n / extent a) * extent a := Nat.mul_le_mul_right _ h1
          have h3 : (n / extent a) * extent a ≤ n := Nat.div_mul_le_self n (extent a)
          omega
        have hnext : applyT (power full (compile reader a)) q =
            scan ((List.replicate full (expand a)).flatten) q := by
          rw [power_compile_applyT a full q hq, scan_replicate (expand a) full q]
        have hnextBound : scan ((List.replicate full (expand a)).flatten) q < 2 :=
          scan_bounded _ q hq
        have hbeq0 : ¬ ((extent a == 0) = true) := by simp [hsize]
        simp only [partialScan]
        rw [ite_eq_right hbeq0]
        change (if full == k then applyT (power full (compile reader a)) q
          else partialScan reader a (n % extent a) (applyT (power full (compile reader a)) q)) =
            scan ((expand (.rep k a)).take n) q
        by_cases hfk : full = k
        · have hbeqk : (full == k) = true := by simp [hfk]
          rw [ite_eq_left hbeqk]
          have hkn : k * extent a ≤ n := by simpa [hfk] using hfull_le_n
          have hlen : ((List.replicate k (expand a)).flatten).length = k * extent a := by
            have := expand_length (.rep k a)
            simpa [expand, extent] using this
          rw [expand, List.take_of_length_le (by omega)]
          simpa [expand, hfk] using hnext
        · have hbeqk : ¬ ((full == k) = true) := by simp [hfk]
          rw [ite_eq_right hbeqk]
          have htake : ((List.replicate k (expand a)).flatten).take n =
              (List.replicate full (expand a)).flatten ++ ((expand a).take (n % extent a)) := by
            have hk : k = full + (k - full) := by omega
            have hfull_eq : full = n / extent a := by
              change min k (n / extent a) = n / extent a
              by_cases hle : n / extent a ≤ k
              · rw [Nat.min_comm]
                exact Nat.min_eq_left hle
              · have hklt : k < n / extent a := Nat.lt_of_not_ge hle
                exfalso
                apply hfk
                change min k (n / extent a) = k
                rw [Nat.min_comm]
                exact Nat.min_eq_right (Nat.le_of_lt hklt)
            calc
              ((List.replicate k (expand a)).flatten).take n =
                  ((List.replicate (full + (k - full)) (expand a)).flatten).take n := by rw [← hk]
              _ = ((List.replicate full (expand a) ++
                    List.replicate (k - full) (expand a)).flatten).take n := by rw [replicate_add]
              _ = ((List.replicate full (expand a)).flatten ++
                    (List.replicate (k - full) (expand a)).flatten).take n := by rw [List.flatten_append]
              _ = (List.replicate full (expand a)).flatten ++
                    ((List.replicate (k - full) (expand a)).flatten).take (n -
                      (List.replicate full (expand a)).flatten.length) := by
                    rw [List.take_append]
                    have hlenFull0 : ((List.replicate full (expand a)).flatten).length = full * extent a := by
                      have := expand_length (.rep full a)
                      simpa [expand, extent] using this
                    have hleN : ((List.replicate full (expand a)).flatten).length ≤ n := by
                      rw [hlenFull0]
                      exact hfull_le_n
                    rw [List.take_of_length_le hleN]
              _ = (List.replicate full (expand a)).flatten ++ ((expand a).take (n % extent a)) := by
                    have hlenFull : ((List.replicate full (expand a)).flatten).length = full * extent a := by
                      have := expand_length (.rep full a)
                      simpa [expand, extent] using this
                    rw [hlenFull]
                    have hmodadd := Nat.mod_add_div n (extent a)
                    have hn' : n - full * extent a = n % extent a := by
                      rw [hfull_eq, Nat.mul_comm (n / extent a) (extent a)]
                      omega
                    rw [hn']
                    have hkfull : 0 < k - full := by omega
                    cases hm : k - full with
                    | zero => omega
                    | succ m =>
                        simp [List.replicate_succ, List.flatten_cons,
                          List.take_append_of_le_length (by
                            rw [expand_length]
                            exact Nat.le_of_lt (Nat.mod_lt n (Nat.pos_of_ne_zero hsize)))]
          rw [hnext]
          rw [ihA _ (n % extent a) hnextBound]
          rw [expand, htake, scan_append]

/-! The checkpoint theorem itself. -/

theorem sound_Checkpoint : EndsInDigit.Checkpoint.Goal := by
  intro x q a b hq
  rw [prefixSeek_ed x q (a + b) hq, prefixSeek_ed x q a hq]
  rw [take_add_eq_append (expand x) a b, scan_append]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_Checkpoint
