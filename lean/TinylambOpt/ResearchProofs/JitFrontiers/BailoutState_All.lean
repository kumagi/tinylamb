import TinylambOpt.ResearchGoals.JitFrontiers.BailoutState

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.BailoutStateAll

open TinylambOpt.ResearchGoals.JitFrontiers.BailoutState

theorem drive_prefix (p : Policy) :
    ∀ (cap i : Nat) (s : State) (rs : List Row),
      ∃ n, n ≤ rs.length ∧ drive p cap i s rs = ⟨i + n, refRun p s (rs.take n)⟩ := by
  intro cap i s rs
  induction rs generalizing i s with
  | nil => exact ⟨0, Nat.zero_le _, by simp [drive, refRun]⟩
  | cons r rs ih =>
      by_cases hc : canCommit p cap s r = true
      · obtain ⟨n, hn, he⟩ := ih (i+1) (step p s r)
        refine ⟨n+1, Nat.succ_le_succ hn, ?_⟩
        simp [drive, hc, he, refRun, List.take_succ_cons, List.foldl_cons]
        omega
      · refine ⟨0, Nat.zero_le _, ?_⟩
        simp [drive, hc, refRun]

theorem committedPrefix_all (p : Policy) : committedPrefix p := by
  intro cap s rs
  obtain ⟨n, hn, he⟩ := drive_prefix p cap 0 s rs
  unfold run
  rw [he]
  constructor
  · simpa using hn
  · simp [refRun]

theorem refRun_append (p : Policy) (s : State) (a b : List Row) :
    refRun p (refRun p s a) b = refRun p s (a ++ b) := by
  simp [refRun, List.foldl_append]

theorem restartRow_all (p : Policy) : restartRow p := by
  intro cap s rs
  obtain ⟨n, hn, he⟩ := drive_prefix p cap 0 s rs
  unfold resume run
  rw [he]
  simp only
  rw [refRun_append]
  simpa [Nat.zero_add] using congrArg (refRun p s) (List.take_append_drop n rs)

theorem drive_succ (p : Policy) (cap : Nat) :
    ∀ (i : Nat) (s : State) (rs : List Row),
      drive p cap (i+1) s rs =
        (match drive p cap i s rs with | e => ⟨e.cursor+1, e.state⟩) := by
  intro i s rs
  induction rs generalizing i s with
  | nil => simp [drive]
  | cons r rs ih =>
      unfold drive
      by_cases hc : canCommit p cap s r = true
      · simp only [hc, ite_true]
        rw [ih (i+1), ih i]
      · simp [hc]

theorem drive_offset (p : Policy) (cap : Nat) :
    ∀ (i : Nat) (s : State) (rs : List Row),
      drive p cap i s rs =
        (match drive p cap 0 s rs with | e => ⟨i+e.cursor, e.state⟩) := by
  intro i
  induction i with
  | zero => intro s rs; simp
  | succ i ih =>
      intro s rs
      rw [drive_succ p cap i s rs, ih s rs]
      simp only [Exit.mk.injEq]
      constructor
      · omega
      · trivial

theorem run_bail' (p : Policy) :
    ∀ (cap : Nat) (s : State) (rs : List Row) (r : Row),
      (run p cap s rs).cursor < rs.length →
      rs[(run p cap s rs).cursor]? = some r →
      canCommit p cap (run p cap s rs).state r = false := by
  intro cap s rs
  induction rs generalizing s with
  | nil => intro r h; simp [run, drive] at h
  | cons r0 rs ih =>
      intro r hlt hidx
      by_cases hc : canCommit p cap s r0 = true
      · simp [run, drive, hc] at hlt hidx ⊢
        rw [drive_succ] at hlt hidx ⊢
        dsimp at hlt hidx ⊢
        have hlt' : (drive p cap 0 (step p s r0) rs).cursor < rs.length := by omega
        have hidx' : rs[(drive p cap 0 (step p s r0) rs).cursor]? = some r := by simpa using hidx
        have hf := ih (step p s r0) r (by simpa [run] using hlt') (by simpa [run] using hidx')
        simpa [run] using hf
      · simp [run, drive, hc] at hlt hidx ⊢
        subst r
        cases hb : canCommit p cap s r0 <;> simp [hb] at hc ⊢

theorem bailReason_all (p : Policy) : bailReason p := by
  intro cap s rs i r
  dsimp only
  intro he hi
  subst he
  have hlt : (run p cap s rs).cursor < rs.length := by
    rcases List.getElem?_eq_some_iff.mp hi with ⟨h, _⟩
    exact h
  exact run_bail' p cap s rs r hlt hi

theorem canCommit_mono (p : Policy) {a b : Nat} (s : State) (r : Row) (hab : a ≤ b) :
    canCommit p a s r = true → canCommit p b s r = true := by
  intro h
  unfold canCommit at h ⊢
  rw [Bool.and_eq_true] at h ⊢
  obtain ⟨hh, hk⟩ := h
  constructor
  · exact hh
  · rw [Option.all_eq_true] at hk ⊢
    intro k hk'
    have h1 := hk k hk'
    rw [Bool.or_eq_true] at h1 ⊢
    rcases h1 with h1 | h1
    · exact Or.inl h1
    · have h1' : s.length < a := by simpa using h1
      exact Or.inr (by simpa using Nat.lt_of_lt_of_le h1' hab)

theorem increaseCapacity_all (p : Policy) : increaseCapacity p := by
  intro a b s rs hab
  induction rs generalizing s with
  | nil => simp [run, drive]
  | cons r rs ih =>
      by_cases hc : canCommit p a s r = true
      · have hcb := canCommit_mono p s r hab hc
        have ha : (run p a s (r :: rs)).cursor = (run p a (step p s r) rs).cursor + 1 := by
          simp [run, drive, hc, drive_succ]
        have hb : (run p b s (r :: rs)).cursor = (run p b (step p s r) rs).cursor + 1 := by
          simp [run, drive, hcb, drive_succ]
        rw [ha, hb]
        exact Nat.succ_le_succ (ih (step p s r))
      · rw [run, drive, if_neg hc]
        simp only
        omega

theorem fullTableHit_all (p : Policy) : fullTableHit p := by
  intro cap s r k hhot hkey hlook
  apply Eq.trans ?_ (show (⟨0 + 1, step p s r⟩ : Exit) = ⟨1, step p s r⟩ by simp)
  unfold run drive canCommit
  rw [if_pos]
  · rfl
  · rw [Bool.and_eq_true]
    constructor
    · exact hhot
    · rw [Option.all_eq_true]
      intro k' hk'
      rw [hkey] at hk'
      simp at hk'
      subst hk'
      rw [Bool.or_eq_true]
      exact Or.inl (by simpa using hlook)


theorem drive_append_full (p : Policy) (cap : Nat) :
    ∀ (s : State) (a b : List Row),
      (run p cap s a).cursor = a.length →
      run p cap s (a++b) = drive p cap a.length (run p cap s a).state b := by
  intro s a
  induction a generalizing s with
  | nil =>
      intro b h
      simp [run, drive]
  | cons r a ih =>
      intro b he
      have hc : canCommit p cap s r = true := by
        cases hb : canCommit p cap s r
        · simp [run, drive, hb] at he
        · simp [hb]
      simp [run, drive, hc] at he ⊢
      rw [drive_offset p cap 1 (step p s r) a] at he
      simp only at he
      have he_tail0 : (drive p cap 0 (step p s r) a).cursor = a.length := by omega
      have he_tail : (run p cap (step p s r) a).cursor = a.length := by simpa [run] using he_tail0
      have ht := ih (step p s r) b he_tail
      rw [drive_offset p cap 1 (step p s r) (a++b)]
      rw [drive_offset p cap 1 (step p s r) a]
      change (match run p cap (step p s r) (a++b) with | e => ⟨1+e.cursor, e.state⟩) =
        drive p cap (a.length+1) (run p cap (step p s r) a).state b
      rw [ht]
      rw [drive_succ p cap a.length (run p cap (step p s r) a).state b]
      simp only [Exit.mk.injEq]
      constructor
      · omega
      · trivial

theorem chunkHandoff_all (p : Policy) : chunkHandoff p := by
  intro s a b cap
  dsimp only
  intro he
  exact drive_append_full p cap s a b he

theorem eraseDups_nodup : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      simp only [List.nodup_cons]
      constructor
      · intro h
        rw [List.mem_eraseDups, List.mem_filter] at h
        simpa using h.2
      · exact eraseDups_nodup (as.filter fun b => ! b == a)
termination_by l => l.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le _ as)

theorem lookup_isSome_of_mem_map_fst :
    ∀ (s : State) (k : Nat), (s.map Prod.fst).Nodup → k ∈ s.map Prod.fst → (s.lookup k).isSome
  | [], k, hnod, hk => by simp at hk
  | e :: es, k, hnod, hk => by
      simp only [List.map_cons, List.nodup_cons] at hnod
      simp only [List.map_cons, List.mem_cons] at hk
      rw [List.lookup_cons]
      split
      · simp
      · rename_i hne
        apply lookup_isSome_of_mem_map_fst es k hnod.2
        rcases hk with hke | hk
        · exfalso
          have hbeq : (k == e.1) = true := beq_iff_eq.mpr hke
          rw [hbeq] at hne
          contradiction
        · exact hk

theorem length_lt_of_subset_not_mem {l K : List Nat}
    (hl : l.Nodup) (hK : K.Nodup) (hsub : l ⊆ K) (hkK : k ∈ K) (hknot : k ∉ l) :
    l.length < K.length := by
  have hconsN : (k :: l).Nodup := by simpa [List.nodup_cons] using And.intro hknot hl
  have hconsSub : k :: l ⊆ K := by
    intro x hx
    simp only [List.mem_cons] at hx
    rcases hx with rfl | hx
    · exact hkK
    · exact hsub hx
  have hle := hconsN.length_le_of_subset hconsSub
  simp only [List.length_cons] at hle
  omega

theorem step_preserves (p : Policy) {K : List Nat} {s : State} {r : Row} {k : Nat}
    (hsN : (s.map Prod.fst).Nodup) (hsSub : s.map Prod.fst ⊆ K)
    (hkey : p.key r = some k) (hkK : k ∈ K) :
    ((step p s r).map Prod.fst).Nodup ∧ (step p s r).map Prod.fst ⊆ K := by
  unfold step
  rw [hkey]
  simp only [List.map_cons]
  constructor
  · simp only [List.nodup_cons]
    constructor
    · intro hmem
      rw [List.mem_map] at hmem
      rcases hmem with ⟨e, he, heq⟩
      rw [List.mem_filter] at he
      simp [heq] at he
    · have hsl : (List.filter (fun e => (e : Nat × Int).1 != k) s).Sublist s := List.filter_sublist
      exact (hsl.map Prod.fst).nodup hsN
  · intro x hx
    simp only [List.mem_cons] at hx
    rcases hx with rfl | hx
    · exact hkK
    · rw [List.mem_map] at hx
      rcases hx with ⟨e, he, rfl⟩
      rw [List.mem_filter] at he
      exact hsSub (by rw [List.mem_map]; exact ⟨e, he.1, rfl⟩)

theorem canCommit_of_reserved (p : Policy) {K : List Nat} {cap : Nat} {s : State} {r : Row}
    (hcap : cap = K.length) (hK : K.Nodup)
    (hsN : (s.map Prod.fst).Nodup) (hsSub : s.map Prod.fst ⊆ K)
    (hhot : p.hot r = true) (hkeyK : (p.key r).all (fun k => k ∈ K) = true) :
    canCommit p cap s r = true := by
  unfold canCommit
  simp only [hhot, Bool.true_and]
  cases hkr : p.key r with
  | none => simp
  | some k =>
      simp only [hkr, Option.all_some]
      by_cases hkmem : k ∈ s.map Prod.fst
      · rw [Bool.or_eq_true]
        exact Or.inl (by simpa using lookup_isSome_of_mem_map_fst s k hsN hkmem)
      · rw [Bool.or_eq_true]
        right
        rw [hcap]
        simpa using length_lt_of_subset_not_mem hsN hK hsSub (by simpa [hkr] using hkeyK) hkmem

theorem run_all_fixed (p : Policy) (K : List Nat) (cap : Nat)
    (hcap : cap = K.length) (hK : K.Nodup) :
    ∀ (s : State) (rs : List Row),
      (s.map Prod.fst).Nodup → s.map Prod.fst ⊆ K →
      (∀ r ∈ rs, p.hot r = true) →
      (∀ r ∈ rs, (p.key r).all (fun k => k ∈ K) = true) →
      (run p cap s rs).cursor = rs.length := by
  intro s rs
  induction rs generalizing s with
  | nil => intro; simp [run, drive]
  | cons r rs ih =>
      intro hsN hsSub hhot hkeyK
      have hhr := hhot r (by simp)
      have hkr := hkeyK r (by simp)
      have hcan := canCommit_of_reserved p hcap hK hsN hsSub hhr hkr
      simp [run, drive, hcan]
      rw [drive_succ p cap 0 (step p s r) rs]
      simp only
      have htailhot : ∀ x ∈ rs, p.hot x = true := by
        intro x hx; exact hhot x (by simp [hx])
      have htailkey : ∀ x ∈ rs, (p.key x).all (fun k => k ∈ K) = true := by
        intro x hx; exact hkeyK x (by simp [hx])
      cases hkeyr : p.key r with
      | none =>
          simp [step, hkeyr]
          have ht := ih s hsN hsSub htailhot htailkey
          have ht' : (drive p cap 0 s rs).cursor = rs.length := by simpa [run] using ht
          omega
      | some k =>
          have hkK : k ∈ K := by simpa [hkeyr] using hkr
          have hp := step_preserves p hsN hsSub hkeyr hkK
          have ht := ih (step p s r) hp.1 hp.2 htailhot htailkey
          have ht' : (drive p cap 0 (step p s r) rs).cursor = rs.length := by simpa [run] using ht
          omega

theorem reserveExactly_all (p : Policy) : reserveExactly p := by
  intro rs s hsNodup hhot
  let K := keys p s rs
  have hK : K.Nodup := by
    dsimp [K, keys]
    exact eraseDups_nodup _
  have hsSub : s.map Prod.fst ⊆ K := by
    intro k hk
    change k ∈ keys p s rs
    simp [keys, hk]
  have hkeys : ∀ r ∈ rs, (p.key r).all (fun k => k ∈ K) = true := by
    intro r hr
    change Option.all (fun k => decide (k ∈ keys p s rs)) (p.key r) = true
    cases hkr : p.key r with
    | none => simp
    | some k =>
        simp only [hkr, Option.all_some]
        have hmemK : k ∈ keys p s rs := by
          rw [keys, List.mem_eraseDups, List.mem_append]
          right
          rw [List.mem_filterMap]
          exact ⟨r, hr, hkr⟩
        simpa using hmemK
  have hhot' : ∀ r ∈ rs, p.hot r = true := by
    intro r hr
    exact List.all_eq_true.mp hhot r hr
  have hrun := run_all_fixed p K K.length rfl hK s rs hsNodup hsSub hhot' hkeys
  have hcp : committedPrefix p := committedPrefix_all p
  have hcp' := hcp K.length s rs
  dsimp only at hcp'
  rw [hrun] at hcp'
  cases e : run p (keys p s rs).length s rs with
  | mk c st =>
      simp only [Exit.mk.injEq]
      constructor
      · simpa [e, K] using hrun
      · simpa [e, K] using hcp'.2

theorem lookup_filter_ne (s : State) (k rk : Nat) (hrk : rk ≠ k) :
    (s.filter (fun e => e.1 != rk)).lookup k = s.lookup k := by
  induction s with
  | nil => rfl
  | cons e es ih =>
      by_cases h : (e.1 == rk) = true
      · have hf : ¬ (fun x : Nat × Int => x.1 != rk) e = true := by simp [bne, h]
        rw [List.filter_cons_of_neg (p := fun x : Nat × Int => x.1 != rk) hf]
        rw [List.lookup_cons]
        have hkfalse : (k == e.1) = false := by
          cases hk : (k == e.1)
          · rfl
          · have hke : k = e.1 := beq_iff_eq.mp hk
            have her : e.1 = rk := beq_iff_eq.mp h
            exact (hrk (by omega)).elim
        rw [hkfalse]
        exact ih
      · have hf : (fun x : Nat × Int => x.1 != rk) e = true := by simp [bne, h]
        rw [List.filter_cons_of_pos (p := fun x : Nat × Int => x.1 != rk) hf]
        rw [List.lookup_cons, List.lookup_cons]
        by_cases hk : (k == e.1) = true
        · rw [hk]
        · have hkfalse : (k == e.1) = false := by
            cases h' : (k == e.1)
            · rfl
            · contradiction
          rw [hkfalse]
          exact ih

theorem countAux : ∀ (s : State) (rs : List Row) (k : Nat),
    ((refRun CountRows.policy s rs).lookup k).getD 0 =
      ((s.lookup k).getD 0 : Int) + (rs.countP (fun r => r.1 == k) : Int) := by
  intro s rs
  induction rs generalizing s with
  | nil => intro k; simp [refRun]
  | cons r rs ih =>
      intro k
      rw [refRun, List.foldl_cons]
      change ((refRun CountRows.policy (step CountRows.policy s r) rs).lookup k).getD 0 = _
      rw [ih (step CountRows.policy s r) k]
      by_cases h : r.1 == k
      · have heq : r.1 = k := beq_iff_eq.mp h
        subst heq
        simp [step, CountRows.policy, bne]
        omega
      · have hne : r.1 ≠ k := by intro heq; exact h (by subst heq; simp)
        have hstep : (step CountRows.policy s r).lookup k = s.lookup k := by
          unfold step
          simp only [CountRows.policy]
          rw [List.lookup_cons]
          have hkfalse : (k == r.1) = false := by
            cases hk : (k == r.1)
            · rfl
            · have hke : k = r.1 := beq_iff_eq.mp hk
              exact (hne hke.symm).elim
          rw [hkfalse]
          exact lookup_filter_ne s k r.1 hne
        rw [hstep]
        simp [h]

theorem firstWinsAux (s : State) (k : Nat) (x : Int) (hs : s.lookup k = some x) :
    ∀ rs, (refRun FirstValue.policy s rs).lookup k = some x := by
  intro rs
  induction rs generalizing s with
  | nil => simpa [refRun] using hs
  | cons r rs ih =>
      rw [refRun, List.foldl_cons]
      change (refRun FirstValue.policy (step FirstValue.policy s r) rs).lookup k = some x
      apply ih
      unfold step
      simp only [FirstValue.policy]
      by_cases h : (r.1 == k) = true
      · have heq : r.1 = k := beq_iff_eq.mp h
        subst heq
        simp [bne, h, hs]
      · have hne : r.1 ≠ k := by intro heq; exact h (by subst heq; simp)
        rw [List.lookup_cons]
        have hkfalse : (k == r.1) = false := by
          cases hk : (k == r.1)
          · rfl
          · have hke : k = r.1 := beq_iff_eq.mp hk
            exact (hne hke.symm).elim
        rw [hkfalse]
        rw [lookup_filter_ne s k r.1 hne]
        exact hs

theorem changedKeeps (s : State) (k : Nat) (hs : s.lookup k = some 1) :
    ∀ rs, (refRun ChangedOnly.policy s rs).lookup k = some 1 := by
  intro rs
  induction rs generalizing s with
  | nil => simpa [refRun] using hs
  | cons r rs ih =>
      rw [refRun, List.foldl_cons]
      apply ih
      by_cases hz : (r.2 == some 0) = true
      · have hzero : r.2 = some 0 := beq_iff_eq.mp hz
        simp [step, ChangedOnly.policy, hzero]
        exact hs
      · have hzn : r.2 ≠ some 0 := by
          intro heq
          exact hz (beq_iff_eq.mpr heq)
        simp [step, ChangedOnly.policy, hzn]
        by_cases hk : (r.1 == k) = true
        · have heq : r.1 = k := beq_iff_eq.mp hk
          subst k
          simp [bne]
        · have hrk : r.1 ≠ k := by intro heq; exact hk (by subst heq; simp)
          rw [List.lookup_cons]
          have hkfalse : (k == r.1) = false := by
            cases h' : (k == r.1)
            · rfl
            · have hke : k = r.1 := beq_iff_eq.mp h'
              exact (hrk hke.symm).elim
          rw [hkfalse]
          rw [lookup_filter_ne s k r.1 hrk]
          exact hs

theorem admissionAux : ∀ (s : State) (rs : List Row) (k : Nat),
    (refRun ChangedOnly.policy s rs).lookup k = some 1 ↔
      s.lookup k = some 1 ∨ ∃ r ∈ rs, r.1 = k ∧ r.2 ≠ some 0 := by
  intro s rs
  induction rs generalizing s with
  | nil => intro k; simp [refRun]
  | cons r rs ih =>
      intro k
      rw [refRun, List.foldl_cons]
      change (refRun ChangedOnly.policy (step ChangedOnly.policy s r) rs).lookup k = some 1 ↔ _
      by_cases hz : (r.2 == some 0) = true
      · have hzero : r.2 = some 0 := beq_iff_eq.mp hz
        have hskip : step ChangedOnly.policy s r = s := by
          simp [step, ChangedOnly.policy, hzero]
        rw [hskip]
        simpa [hzero] using ih s k
      · have hzn : r.2 ≠ some 0 := by
          intro heq
          exact hz (beq_iff_eq.mpr heq)
        by_cases hk : (r.1 == k) = true
        · have heq : r.1 = k := beq_iff_eq.mp hk
          subst k
          have hhead : (step ChangedOnly.policy s (r.1,r.2)).lookup r.1 = some 1 := by
            simp [step, ChangedOnly.policy, hzn]
          have hkeep := changedKeeps (step ChangedOnly.policy s (r.1,r.2)) r.1 hhead rs
          simp only [hkeep, true_iff]
          exact Or.inr ⟨(r.1,r.2), by simp, rfl, hzn⟩
        · have hrk : r.1 ≠ k := by intro heq; exact hk (by subst heq; simp)
          have hstep : (step ChangedOnly.policy s r).lookup k = s.lookup k := by
            simp [step, ChangedOnly.policy, hzn]
            rw [List.lookup_cons]
            have hkfalse : (k == r.1) = false := by
              cases h' : (k == r.1)
              · rfl
              · have hke : k = r.1 := beq_iff_eq.mp h'
                exact (hrk hke.symm).elim
            rw [hkfalse]
            exact lookup_filter_ne s k r.1 hrk
          rw [ih (step ChangedOnly.policy s r) k, hstep]
          simp [hzn, hrk, hk]

theorem parityAux (s : State) (k p : Nat)
    (hp : (s.lookup k).getD 0 = ((p % 2 : Nat) : Int)) :
    ∀ rs, ((refRun ParityCounter.policy s rs).lookup k).getD 0 =
      ((((p + rs.countP (fun r => r.1 == k)) % 2 : Nat) : Int)) := by
  intro rs
  induction rs generalizing s p with
  | nil => simpa [refRun] using hp
  | cons r rs ih =>
      rw [refRun, List.foldl_cons]
      change ((refRun ParityCounter.policy (step ParityCounter.policy s r) rs).lookup k).getD 0 = _
      by_cases hk : (r.1 == k) = true
      · have heq : r.1 = k := beq_iff_eq.mp hk
        subst k
        have hnew : ((step ParityCounter.policy s r).lookup r.1).getD 0 =
            1 - (s.lookup r.1).getD 0 := by
          unfold step
          simp [ParityCounter.policy, bne]
        have hp' : ((step ParityCounter.policy s r).lookup r.1).getD 0 =
            ((((p + 1) % 2 : Nat) : Int)) := by
          rw [hnew, hp]
          omega
        simpa [hk, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (step ParityCounter.policy s r) (p+1) hp'
      · have hrk : r.1 ≠ k := by intro heq; exact hk (by subst heq; simp)
        have hstep : (step ParityCounter.policy s r).lookup k = s.lookup k := by
          unfold step
          simp only [ParityCounter.policy]
          rw [List.lookup_cons]
          have hkfalse : (k == r.1) = false := by
            cases h' : (k == r.1)
            · rfl
            · have hke : k = r.1 := beq_iff_eq.mp h'
              exact (hrk hke.symm).elim
          rw [hkfalse]
          exact lookup_filter_ne s k r.1 hrk
        have hp' : ((step ParityCounter.policy s r).lookup k).getD 0 = ((p % 2 : Nat) : Int) := by
          rw [hstep, hp]
        simpa [hk, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (step ParityCounter.policy s r) p hp'


theorem CountRows_CommittedPrefix : CountRows.CommittedPrefix.Goal := committedPrefix_all CountRows.policy
theorem CountRows_RestartRow : CountRows.RestartRow.Goal := restartRow_all CountRows.policy
theorem CountRows_BailReason : CountRows.BailReason.Goal := bailReason_all CountRows.policy
theorem CountRows_FullTableHit : CountRows.FullTableHit.Goal := fullTableHit_all CountRows.policy
theorem CountRows_IncreaseCapacity : CountRows.IncreaseCapacity.Goal := increaseCapacity_all CountRows.policy
theorem CountRows_ReserveExactly : CountRows.ReserveExactly.Goal := reserveExactly_all CountRows.policy
theorem CountRows_ChunkHandoff : CountRows.ChunkHandoff.Goal := chunkHandoff_all CountRows.policy
theorem CountRows_earlyAdvance : ¬ CountRows.EarlyAdvance.Goal := by
  intro h
  have hh := h 1 [(99,0)] [(0,none)]
  simp [run, drive, canCommit, refRun, step, CountRows.policy] at hh

theorem CountRows_ReplayDoubles : CountRows.ReplayDoubles.Goal := by intro k; simp [refRun, step, CountRows.policy]
theorem CountRows_CountByKey : CountRows.CountByKey.Goal := by intro rs k; simpa using countAux [] rs k

theorem SumValid_CommittedPrefix : SumValid.CommittedPrefix.Goal := committedPrefix_all SumValid.policy
theorem SumValid_RestartRow : SumValid.RestartRow.Goal := restartRow_all SumValid.policy
theorem SumValid_BailReason : SumValid.BailReason.Goal := bailReason_all SumValid.policy
theorem SumValid_FullTableHit : SumValid.FullTableHit.Goal := fullTableHit_all SumValid.policy
theorem SumValid_IncreaseCapacity : SumValid.IncreaseCapacity.Goal := increaseCapacity_all SumValid.policy
theorem SumValid_ReserveExactly : SumValid.ReserveExactly.Goal := reserveExactly_all SumValid.policy
theorem SumValid_ChunkHandoff : SumValid.ChunkHandoff.Goal := chunkHandoff_all SumValid.policy
theorem SumValid_earlyAdvance : ¬ SumValid.EarlyAdvance.Goal := by
  intro h
  have hh := h 1 [(99,0)] [(0,some 1)]
  simp [run, drive, canCommit, refRun, step, SumValid.policy] at hh

theorem SumValid_NoNullAllocation : SumValid.NoNullAllocation.Goal := by intro s k; simp [run, drive, canCommit, step, SumValid.policy]
theorem SumValid_CancelInGroup : SumValid.CancelInGroup.Goal := by intro k x; simp [refRun, step, SumValid.policy]; omega

theorem MinValid_CommittedPrefix : MinValid.CommittedPrefix.Goal := committedPrefix_all MinValid.policy
theorem MinValid_RestartRow : MinValid.RestartRow.Goal := restartRow_all MinValid.policy
theorem MinValid_BailReason : MinValid.BailReason.Goal := bailReason_all MinValid.policy
theorem MinValid_FullTableHit : MinValid.FullTableHit.Goal := fullTableHit_all MinValid.policy
theorem MinValid_IncreaseCapacity : MinValid.IncreaseCapacity.Goal := increaseCapacity_all MinValid.policy
theorem MinValid_ReserveExactly : MinValid.ReserveExactly.Goal := reserveExactly_all MinValid.policy
theorem MinValid_ChunkHandoff : MinValid.ChunkHandoff.Goal := chunkHandoff_all MinValid.policy
theorem MinValid_earlyAdvance : ¬ MinValid.EarlyAdvance.Goal := by
  intro h
  have hh := h 1 [(99,0)] [(0,some 1)]
  simp [run, drive, canCommit, refRun, step, MinValid.policy] at hh

theorem MinValid_RepeatMin : MinValid.RepeatMin.Goal := by
  intro s r
  cases r with
  | mk k v =>
      cases v <;> simp [step, MinValid.policy]
theorem MinValid_DominatedPayload : MinValid.DominatedPayload.Goal := by intro s k x y hxy; simp [refRun, step, MinValid.policy]; omega

theorem LastValue_CommittedPrefix : LastValue.CommittedPrefix.Goal := committedPrefix_all LastValue.policy
theorem LastValue_RestartRow : LastValue.RestartRow.Goal := restartRow_all LastValue.policy
theorem LastValue_BailReason : LastValue.BailReason.Goal := bailReason_all LastValue.policy
theorem LastValue_FullTableHit : LastValue.FullTableHit.Goal := fullTableHit_all LastValue.policy
theorem LastValue_IncreaseCapacity : LastValue.IncreaseCapacity.Goal := increaseCapacity_all LastValue.policy
theorem LastValue_ReserveExactly : LastValue.ReserveExactly.Goal := reserveExactly_all LastValue.policy
theorem LastValue_ChunkHandoff : LastValue.ChunkHandoff.Goal := chunkHandoff_all LastValue.policy
theorem LastValue_earlyAdvance : ¬ LastValue.EarlyAdvance.Goal := by
  intro h
  have hh := h 1 [(99,0)] [(0,some 1)]
  simp [run, drive, canCommit, refRun, step, LastValue.policy] at hh

theorem LastValue_LastWins : LastValue.LastWins.Goal := by intro s k a b; simp [refRun, step, LastValue.policy]
theorem LastValue_DistinctOrder : LastValue.DistinctOrder.Goal := by intro k a b hab; simp [refRun, step, LastValue.policy]; exact hab.symm

theorem FirstValue_CommittedPrefix : FirstValue.CommittedPrefix.Goal := committedPrefix_all FirstValue.policy
theorem FirstValue_RestartRow : FirstValue.RestartRow.Goal := restartRow_all FirstValue.policy
theorem FirstValue_BailReason : FirstValue.BailReason.Goal := bailReason_all FirstValue.policy
theorem FirstValue_FullTableHit : FirstValue.FullTableHit.Goal := fullTableHit_all FirstValue.policy
theorem FirstValue_IncreaseCapacity : FirstValue.IncreaseCapacity.Goal := increaseCapacity_all FirstValue.policy
theorem FirstValue_ReserveExactly : FirstValue.ReserveExactly.Goal := reserveExactly_all FirstValue.policy
theorem FirstValue_ChunkHandoff : FirstValue.ChunkHandoff.Goal := chunkHandoff_all FirstValue.policy
theorem FirstValue_earlyAdvance : ¬ FirstValue.EarlyAdvance.Goal := by
  intro h
  have hh := h 1 [(99,0)] [(0,some 1)]
  simp [run, drive, canCommit, refRun, step, FirstValue.policy] at hh

theorem FirstValue_FirstWins : FirstValue.FirstWins.Goal := by
  intro k x rs
  rw [refRun, List.foldl_cons]
  exact firstWinsAux (step FirstValue.policy [] (k,x)) k (x.getD 0) (by simp [step, FirstValue.policy]) rs
theorem FirstValue_InitialHit : FirstValue.InitialHit.Goal := by intro k x y; simp [step, FirstValue.policy]

theorem Packed32_CommittedPrefix : Packed32.CommittedPrefix.Goal := committedPrefix_all Packed32.policy
theorem Packed32_RestartRow : Packed32.RestartRow.Goal := restartRow_all Packed32.policy
theorem Packed32_BailReason : Packed32.BailReason.Goal := bailReason_all Packed32.policy
theorem Packed32_FullTableHit : Packed32.FullTableHit.Goal := fullTableHit_all Packed32.policy
theorem Packed32_IncreaseCapacity : Packed32.IncreaseCapacity.Goal := increaseCapacity_all Packed32.policy
theorem Packed32_ReserveExactly : Packed32.ReserveExactly.Goal := reserveExactly_all Packed32.policy
theorem Packed32_ChunkHandoff : Packed32.ChunkHandoff.Goal := chunkHandoff_all Packed32.policy
theorem Packed32_earlyAdvance : ¬ Packed32.EarlyAdvance.Goal := by
  intro h
  have hh := h 1 [(99,0)] [(0,some 1)]
  simp [run, drive, canCommit, refRun, step, Packed32.policy] at hh

theorem Packed32_FirstWide : Packed32.FirstWide.Goal := by intro s v cap; simp [run, drive, canCommit, Packed32.policy]
theorem Packed32_LastNarrow : Packed32.LastNarrow.Goal := by
  intro v
  have hlt : 2^32 - 1 < 2^32 := by omega
  simp [run, drive, canCommit, Packed32.policy, hlt]

theorem BucketCount_CommittedPrefix : BucketCount.CommittedPrefix.Goal := committedPrefix_all BucketCount.policy
theorem BucketCount_RestartRow : BucketCount.RestartRow.Goal := restartRow_all BucketCount.policy
theorem BucketCount_BailReason : BucketCount.BailReason.Goal := bailReason_all BucketCount.policy
theorem BucketCount_FullTableHit : BucketCount.FullTableHit.Goal := fullTableHit_all BucketCount.policy
theorem BucketCount_IncreaseCapacity : BucketCount.IncreaseCapacity.Goal := increaseCapacity_all BucketCount.policy
theorem BucketCount_ReserveExactly : BucketCount.ReserveExactly.Goal := reserveExactly_all BucketCount.policy
theorem BucketCount_ChunkHandoff : BucketCount.ChunkHandoff.Goal := chunkHandoff_all BucketCount.policy
theorem BucketCount_earlyAdvance : ¬ BucketCount.EarlyAdvance.Goal := by
  intro h
  have hh := h 1 [(99,0)] [(0,none)]
  simp [run, drive, canCommit, refRun, step, BucketCount.policy] at hh

theorem BucketCount_SingleBucket : BucketCount.SingleBucket.Goal := by
  intro k
  have h0 : (64*k)/64 = k := Nat.mul_div_right k (by decide : 0 < 64)
  have h1 : (64*k+63)/64 = k := by
    exact Nat.div_eq_of_lt_le (k:=k) (n:=64) (m:=64*k+63) (by omega) (by omega)
  simp [run, drive, canCommit, step, BucketCount.policy, h0, h1]
theorem BucketCount_BoundaryGrow : BucketCount.BoundaryGrow.Goal := by
  intro k
  have h1 : (64*k+63)/64 = k := by
    exact Nat.div_eq_of_lt_le (k:=k) (n:=64) (m:=64*k+63) (by omega) (by omega)
  have h2 : (64*(k+1))/64 = k+1 := Nat.mul_div_right (k+1) (by decide : 0 < 64)
  simp [run, drive, canCommit, step, BucketCount.policy, h1, h2]

theorem SignedDigit_CommittedPrefix : SignedDigit.CommittedPrefix.Goal := committedPrefix_all SignedDigit.policy
theorem SignedDigit_RestartRow : SignedDigit.RestartRow.Goal := restartRow_all SignedDigit.policy
theorem SignedDigit_BailReason : SignedDigit.BailReason.Goal := bailReason_all SignedDigit.policy
theorem SignedDigit_FullTableHit : SignedDigit.FullTableHit.Goal := fullTableHit_all SignedDigit.policy
theorem SignedDigit_IncreaseCapacity : SignedDigit.IncreaseCapacity.Goal := increaseCapacity_all SignedDigit.policy
theorem SignedDigit_ReserveExactly : SignedDigit.ReserveExactly.Goal := reserveExactly_all SignedDigit.policy
theorem SignedDigit_ChunkHandoff : SignedDigit.ChunkHandoff.Goal := chunkHandoff_all SignedDigit.policy
theorem SignedDigit_earlyAdvance : ¬ SignedDigit.EarlyAdvance.Goal := by
  intro h
  have hh := h 1 [(99,0)] [(0,some 1)]
  simp [run, drive, canCommit, refRun, step, SignedDigit.policy] at hh

theorem SignedDigit_TwoDigits : SignedDigit.TwoDigits.Goal := by intro k a b; simp [refRun, step, SignedDigit.policy]
theorem SignedDigit_EscapeDigit : SignedDigit.EscapeDigit.Goal := by intro k; simp [run, drive, canCommit, SignedDigit.policy]

theorem ChangedOnly_CommittedPrefix : ChangedOnly.CommittedPrefix.Goal := committedPrefix_all ChangedOnly.policy
theorem ChangedOnly_RestartRow : ChangedOnly.RestartRow.Goal := restartRow_all ChangedOnly.policy
theorem ChangedOnly_BailReason : ChangedOnly.BailReason.Goal := bailReason_all ChangedOnly.policy
theorem ChangedOnly_FullTableHit : ChangedOnly.FullTableHit.Goal := fullTableHit_all ChangedOnly.policy
theorem ChangedOnly_IncreaseCapacity : ChangedOnly.IncreaseCapacity.Goal := increaseCapacity_all ChangedOnly.policy
theorem ChangedOnly_ReserveExactly : ChangedOnly.ReserveExactly.Goal := reserveExactly_all ChangedOnly.policy
theorem ChangedOnly_ChunkHandoff : ChangedOnly.ChunkHandoff.Goal := chunkHandoff_all ChangedOnly.policy
theorem ChangedOnly_earlyAdvance : ¬ ChangedOnly.EarlyAdvance.Goal := by
  intro h
  have hh := h 1 [(99,0)] [(0,some 1)]
  simp [run, drive, canCommit, refRun, step, ChangedOnly.policy] at hh

theorem ChangedOnly_ZeroSkip : ChangedOnly.ZeroSkip.Goal := by intro k s; simp [step, ChangedOnly.policy]
theorem ChangedOnly_Admission : ChangedOnly.Admission.Goal := by intro k rs; simpa using admissionAux [] rs k

theorem ParityCounter_CommittedPrefix : ParityCounter.CommittedPrefix.Goal := committedPrefix_all ParityCounter.policy
theorem ParityCounter_RestartRow : ParityCounter.RestartRow.Goal := restartRow_all ParityCounter.policy
theorem ParityCounter_BailReason : ParityCounter.BailReason.Goal := bailReason_all ParityCounter.policy
theorem ParityCounter_FullTableHit : ParityCounter.FullTableHit.Goal := fullTableHit_all ParityCounter.policy
theorem ParityCounter_IncreaseCapacity : ParityCounter.IncreaseCapacity.Goal := increaseCapacity_all ParityCounter.policy
theorem ParityCounter_ReserveExactly : ParityCounter.ReserveExactly.Goal := reserveExactly_all ParityCounter.policy
theorem ParityCounter_ChunkHandoff : ParityCounter.ChunkHandoff.Goal := chunkHandoff_all ParityCounter.policy
theorem ParityCounter_earlyAdvance : ¬ ParityCounter.EarlyAdvance.Goal := by
  intro h
  have hh := h 1 [(99,0)] [(0,some 1)]
  simp [run, drive, canCommit, refRun, step, ParityCounter.policy] at hh

theorem ParityCounter_EvenPairs : ParityCounter.EvenPairs.Goal := by intro k v; simp [refRun, step, ParityCounter.policy]
theorem ParityCounter_ClosedCount : ParityCounter.ClosedCount.Goal := by intro rs k; simpa using parityAux [] k 0 (by simp) rs

end TinylambOpt.ResearchProofs.JitFrontiers.BailoutStateAll
