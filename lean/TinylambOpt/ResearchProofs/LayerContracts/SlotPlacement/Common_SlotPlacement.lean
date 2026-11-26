import TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Named results include `probes_sorted`, `mem_probes`, `find`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

/-! Reusable list/allocator facts shared by the ten page-slot models. -/

theorem probes_sorted (m : Model) (r : Req) (u : List Nat) :
    (probes m r u).Pairwise (fun x y : Nat => x ≤ y) := by
  unfold probes
  let l := (List.map (m.snap r) (0 :: u.map (fun x => x + 1))).eraseDups
  let le : Nat → Nat → Bool := fun a b => decide (a ≤ b)
  change (l.mergeSort le).Pairwise (fun x y => x ≤ y)
  have hs := List.pairwise_mergeSort (le := le) (by
      intro a b c hab hbc
      simp [le] at hab hbc ⊢
      omega) (by
      intro a b
      simp [le]
      omega) l
  simpa [le] using hs

theorem mem_probes (m : Model) (r : Req) (u : List Nat) (i : Nat) :
    i ∈ probes m r u ↔ i = m.snap r 0 ∨ ∃ x ∈ u, m.snap r (x+1) = i := by
  simp [probes, eq_comm]

theorem find?_eq_some_of_sorted {l : List Nat} {p : Nat → Bool} {a : Nat}
    (hs : l.Pairwise (fun x y : Nat => x ≤ y)) (ha : a ∈ l)
    (hpa : p a = true) (hmin : ∀ b ∈ l, b < a → p b = false) :
    l.find? p = some a := by
  induction l with
  | nil => simp at ha
  | cons hd tl ih =>
      simp only [List.pairwise_cons] at hs
      by_cases hphd : p hd = true
      · by_cases heq : a = hd
        · subst a
          simp [hphd]
        · have hatl : a ∈ tl := by
            simp only [List.mem_cons] at ha
            rcases ha with h | h
            · exact (heq h).elim
            · exact h
          have hda : hd ≤ a := hs.1 a hatl
          have hlt : hd < a := by omega
          have hh := hmin hd (by simp) hlt
          simp [hphd] at hh
      · simp [hphd]
        have hatl' : a ∈ tl := by
          simp only [List.mem_cons] at ha
          rcases ha with h | h
          · subst a
            exact (hphd hpa).elim
          · exact h
        exact ih hs.2 hatl' (by
          intro b hb hblt
          exact hmin b (by simp [hb]) hblt)

theorem range_pairwise_le (n : Nat) : (List.range n).Pairwise (fun x y : Nat => x ≤ y) :=
  List.Pairwise.imp (fun h => Nat.le_of_lt h) List.pairwise_lt_range

theorem align_ge (i base step : Nat) (hs : 0 < step) : i ≤ align i base step := by
  unfold align
  by_cases hle : base ≤ i
  · have hle_div : i - base + step - 1 ≤ (i - base + step - 1) / step * step + step - 1 :=
      (Nat.div_le_iff_le_mul hs).1 (Nat.le_refl _)
    omega
  · omega

theorem align_mod (i base step : Nat) (hs : 0 < step) (hb : base < step) :
    align i base step % step = base := by
  unfold align
  rw [Nat.add_mul_mod_self_right]
  exact Nat.mod_eq_of_lt hb

theorem align_eq_of_mod (i base step : Nat) (hs : 0 < step) (hb : base < step)
    (hm : i % step = base) : align i base step = i := by
  have hdecomp : step * (i / step) + i % step = i := Nat.div_add_mod i step
  rw [Nat.mul_comm step (i / step)] at hdecomp
  have hi_eq : i = base + (i / step) * step := by omega
  have hsub : i - base = (i / step) * step := by omega
  have hdiv : (i - base + step - 1) / step = i / step := by
    apply Nat.div_eq_of_lt_le
    · rw [hsub]
      omega
    · rw [hsub, Nat.succ_mul]
      omega
  unfold align
  rw [hdiv]
  omega

theorem align_eq_of_between (i base step k y : Nat) (hs : 0 < step) (hb : base < step)
    (hi : i = base + k * step) (hk : 0 < k) (hylo : i - step < y) (hyhi : y ≤ i) :
    align y base step = i := by
  have hkstep : step ≤ k * step := by
    simpa using Nat.mul_le_mul_right step hk
  have hbasey : base < y := by omega
  have hlow : k * step ≤ y - base + step - 1 := by omega
  have hsucc : (k + 1) * step = k * step + step := by rw [Nat.succ_mul]
  have hhigh : y - base + step - 1 < (k + 1) * step := by
    rw [hsucc]
    omega
  have hdiv : (y - base + step - 1) / step = k := Nat.div_eq_of_lt_le hlow hhigh
  unfold align
  rw [hdiv, hi]

theorem align_mono (i j base step : Nat) (hij : i ≤ j) (hs : 0 < step) :
    align i base step ≤ align j base step := by
  unfold align
  by_cases hi : base ≤ i
  · have hsub : i - base ≤ j - base := Nat.sub_le_sub_right hij base
    have hdiv : (i - base + step - 1) / step ≤ (j - base + step - 1) / step :=
      Nat.div_le_div_right (by omega)
    have hmul : (i - base + step - 1) / step * step ≤
        (j - base + step - 1) / step * step := Nat.mul_le_mul_right step hdiv
    omega
  · have hi0 : i - base = 0 := Nat.sub_eq_zero_of_le (Nat.le_of_lt (Nat.lt_of_not_ge hi))
    have hdivi : (i - base + step - 1) / step = 0 := by
      rw [hi0]
      exact Nat.div_eq_of_lt (by omega)
    rw [hdivi]
    omega

theorem band_subset {i n m : Nat} (hnm : n ≤ m) :
    ∀ x ∈ band i n, x ∈ band i m := by
  simp [band]
  intro j hj
  omega

theorem fits_mono_used (m : Model) (r : Req) (cap : Nat) (u v : List Nat) (i : Nat)
    (hsub : ∀ x, x ∈ u → x ∈ v) (hfit : fits m r cap v i = true) :
    fits m r cap u i = true := by
  have h := hfit
  simp [fits, List.all_eq_true] at h
  simp [fits, List.all_eq_true]
  refine ⟨⟨h.1.1, h.1.2⟩, ?_⟩
  intro x hx
  have hh := h.2 x hx
  exact ⟨hh.1, fun hxu => hh.2 (hsub x hxu)⟩

theorem bool_false_of_not_true {b : Bool} (h : ¬ b = true) : b = false := by
  cases b <;> simp_all

theorem exhaustive_some_of_fits (m : Model) (r : Req) (cap : Nat) (u : List Nat) (i : Nat)
    (hfit : fits m r cap u i = true) :
    ∃ j, exhaustive m r cap u = some j ∧ j ≤ i := by
  have hicap : i < cap := by
    have h := hfit
    simp [fits] at h
    omega
  have hmem : i ∈ List.range cap := by simp [hicap]
  by_cases hnone : (List.range cap).find? (fits m r cap u) = none
  · have hall := (List.find?_eq_none).1 hnone
    have hh := hall i hmem
    simp [hfit] at hh
  · obtain ⟨j, hj⟩ := (Option.ne_none_iff_exists).1 hnone
    have hfind : (List.range cap).find? (fits m r cap u) = some j := hj.symm
    have hres := (List.find?_range_eq_some).1 hfind
    have hjle : j ≤ i := by
      by_cases hji : j ≤ i
      · exact hji
      · have hij : i < j := by omega
        have hn := hres.2.2 i hij
        simp [hfit] at hn
    exact ⟨j, by simpa [exhaustive] using hfind, hjle⟩

theorem growInPlace_of_firstFit (m : Model) (hff : firstFit m) : growInPlace m := by
  intro r cap u hfit
  obtain ⟨i, hei, hle⟩ := exhaustive_some_of_fits m r cap u r.old hfit
  refine ⟨i, ?_, hle⟩
  rw [hff r cap u]
  exact hei

theorem afterFree_of_firstFit (m : Model) (hff : firstFit m) : afterFree m := by
  intro r cap u x i hqi
  have hfiti : fits m r cap u i = true := List.find?_some (by simpa [quick] using hqi)
  let u' := u.filter (fun y => y != x)
  have hfiti' : fits m r cap u' i = true := by
    apply fits_mono_used m r cap u' u i
    · intro y hy
      simp [u'] at hy
      exact hy.1
    · exact hfiti
  obtain ⟨j, hej, hle⟩ := exhaustive_some_of_fits m r cap u' i hfiti'
  refine ⟨j, ?_, hle⟩
  rw [hff r cap u']
  exact hej

theorem exactFailure_of_firstFit (m : Model) (hff : firstFit m) : exactFailure m := by
  intro r cap u
  constructor
  · intro hq i
    by_cases hfi : fits m r cap u i = true
    · obtain ⟨j, hej, _⟩ := exhaustive_some_of_fits m r cap u i hfi
      have hqj : quick m r cap u = some j := by
        rw [hff r cap u]
        exact hej
      rw [hq] at hqj
      contradiction
    · exact bool_false_of_not_true hfi
  · intro hall
    rw [hff r cap u]
    apply (List.find?_eq_none).2
    intro j hj
    have hjcap : j < cap := by simpa using hj
    cases hfj : fits m r cap u j with
    | false => simp
    | true =>
        have hh := hall j
        simp [hfj] at hh

theorem reservation_of (m : Model) : reservation m := by
  intro a b cap u i j hqa hqb
  have hfitb : fits m b cap (m.footprint a i ++ u) j = true :=
    List.find?_some (by simpa [quick] using hqb)
  have hall : ∀ x ∈ m.footprint b j, x < cap ∧ x ∉ m.footprint a i ++ u := by
    have h := hfitb
    simp [fits, List.all_eq_true] at h
    intro x hx
    have hh := h.2 x hx
    simpa using hh
  intro x hxfp hxfpb
  have hh := hall x hxfpb
  exact hh.2 (by simp [hxfp])

/-! Hypotheses for the generic first-fit and boundary arguments. -/

def PrevSpec (m : Model) (prev : Req → Nat → Nat) : Prop :=
  ∀ r cap u i, 0 < i → fits m r cap u i = true → m.snap r 0 ≠ i →
    let j := prev r i
    j < i ∧ m.legal r j = true ∧
    (∀ x ∈ m.footprint r j, j ≤ x) ∧
    (∀ x ∈ m.footprint r j, x < i ∨ x ∈ m.footprint r i) ∧
    (∀ x, j ≤ x → x < i → m.snap r (x+1) = i)

theorem firstFit_of (m : Model) (prev : Req → Nat → Nat)
    (hfix : ∀ r i, m.legal r i = true → m.snap r i = i)
    (hprev : PrevSpec m prev) : firstFit m := by
  intro r cap u
  cases he : exhaustive m r cap u with
  | none =>
      rw [quick]
      apply (List.find?_eq_none).2
      intro p hp
      by_cases hpf : fits m r cap u p = true
      · have hpcap : p < cap := by
          have h := hpf
          simp [fits] at h
          omega
        have hnonef : (List.range cap).find? (fits m r cap u) = none := by
          simpa [exhaustive] using he
        have hall := (List.find?_eq_none).1 hnonef
        have hf := hall p (by simpa using hpcap)
        exact (hf hpf).elim
      · exact hpf
  | some i =>
      have hfind : (List.range cap).find? (fits m r cap u) = some i := by
        simpa [exhaustive] using he
      have hfi : fits m r cap u i = true := List.find?_some hfind
      have hmini : ∀ j, j < i → fits m r cap u j = false := by
        intro j hj
        have hres := (List.find?_range_eq_some).1 hfind
        have hh := hres.2.2 j hj
        exact bool_false_of_not_true (by simpa using hh)
      have hicap : i < cap := by
        have h := hfi
        simp [fits] at h
        omega
      have hlegali : m.legal r i = true := by
        have h := hfi
        simp [fits] at h
        exact h.1.2
      have hfree : ∀ x ∈ m.footprint r i, x ∉ u := by
        have h := hfi
        simp [fits, List.all_eq_true] at h
        intro x hx
        have hh := h.2 x hx
        simpa using hh.2
      have hiprobes : i ∈ probes m r u := by
        rw [mem_probes]
        by_cases hs0 : m.snap r 0 = i
        · exact Or.inl hs0.symm
        · right
          by_cases hi0 : i = 0
          · subst i
            have hsnapi : m.snap r 0 = 0 := hfix r 0 (by
              have h := hfi
              simp [fits] at h
              exact h.1.2)
            exact (hs0 hsnapi).elim
          · have hipos : 0 < i := by omega
            by_cases huprev : u.contains (i-1) = true
            · refine ⟨i-1, ?_, ?_⟩
              · simpa using huprev
              · have hs : i - 1 + 1 = i := by omega
                rw [hs]
                exact hfix r i hlegali
            · have hp := hprev r cap u i hipos hfi hs0
              rcases hp with ⟨hjlt, hjlegal, hjlo, hjhi, hjsnap⟩
              let j := prev r i
              have hcapj : j < cap := by omega
              have hex : ∃ x ∈ m.footprint r j, x ∈ u := by
                by_cases hex0 : ∃ x ∈ m.footprint r j, x ∈ u
                · exact hex0
                · have hnone : ∀ x ∈ m.footprint r j, x ∉ u := by
                    intro x hx hxu
                    exact hex0 ⟨x, hx, hxu⟩
                  have hfj : fits m r cap u j = true := by
                    simp [fits, List.all_eq_true]
                    refine ⟨⟨hcapj, hjlegal⟩, ?_⟩
                    intro x hx
                    have hxlt_or := hjhi x hx
                    have hxcap : x < cap := by
                      cases hxlt_or with
                      | inl hlt => omega
                      | inr hxfp =>
                          have h := hfi
                          simp [fits, List.all_eq_true] at h
                          have hh := h.2 x hxfp
                          simpa using hh.1
                    have hxnotu := hnone x hx
                    have hxcontain : u.contains x = false := by simpa using hxnotu
                    exact ⟨hxcap, by simpa using hxcontain⟩
                  have hnot := hmini j hjlt
                  cases hfjv : fits m r cap u j <;> simp_all
              obtain ⟨x, hxfp, hxu⟩ := hex
              refine ⟨x, hxu, ?_⟩
              have hjx : j ≤ x := hjlo x hxfp
              have hxlt : x < i := by
                have hxlt_or := hjhi x hxfp
                cases hxlt_or with
                | inl hlt => exact hlt
                | inr hxfp_i =>
                    have hfreex := hfree x hxfp_i
                    exact (hfreex hxu).elim
              exact hjsnap x hjx hxlt
      rw [quick]
      exact find?_eq_some_of_sorted (probes_sorted m r u) hiprobes hfi (by
        intro b hb hblt
        exact hmini b hblt)

/-! Boundary, payload and occupancy monotonicity. -/

def UpSpec (m : Model) : Prop := ∀ r i, i ≤ m.snap r i

def BeforeLegalSpec (m : Model) : Prop :=
  ∀ r i j, i ≤ j → m.legal r j = true → m.snap r i ≤ j

def SnapLegalSpec (m : Model) : Prop :=
  ∀ r i j, i ≤ j → m.legal r j = true → m.legal r (m.snap r i) = true

theorem mem_drop_range {i cap b : Nat} (h : b ∈ (List.range cap).drop i) : i ≤ b ∧ b < cap := by
  have hbcap : b < cap := by
    have h' : b ∈ List.range cap := List.mem_of_mem_drop h
    simpa using h'
  rw [List.mem_drop_iff_getElem] at h
  obtain ⟨k, hk, heq⟩ := h
  simp at hk heq
  omega

theorem nextBoundary_of (m : Model) (hup : UpSpec m)
    (hbefore : BeforeLegalSpec m) (hfix : ∀ r i, m.legal r i = true → m.snap r i = i)
    (hlegal : SnapLegalSpec m) : nextBoundary m := by
  intro r cap used i j hij hj
  have hjcap : j < cap := by
    have h := hj
    simp [fits] at h
    omega
  have hlegj : m.legal r j = true := by
    have h := hj
    simp [fits] at h
    exact h.1.2
  dsimp only
  refine ⟨hup r i, hbefore r i j hij hlegj, hlegal r i j hij hlegj, ?_⟩
  intro hfitfirst
  have hfirstcap : m.snap r i < cap := by
    have := hbefore r i j hij hlegj
    omega
  have hfirstmem : m.snap r i ∈ (List.range cap).drop i := by
    have hi_le : i ≤ m.snap r i := hup r i
    rw [List.mem_drop_iff_getElem]
    refine ⟨m.snap r i - i, ?_, ?_⟩
    · simp
      omega
    · simp
      omega
  apply find?_eq_some_of_sorted (List.Pairwise.imp (fun h => Nat.le_of_lt h)
    (List.Pairwise.drop (List.pairwise_lt_range :
      (List.range cap).Pairwise (fun x y : Nat => x < y)))) hfirstmem hfitfirst
  intro b hb hblt
  have hbge : i ≤ b := (mem_drop_range hb).1
  by_cases hlegb : m.legal r b = true
  · have hfirstleb : m.snap r i ≤ b := hbefore r i b hbge hlegb
    omega
  · simp [fits, hlegb]

def FpSubSpec (m : Model) : Prop :=
  ∀ r n i, n ≤ r.bytes → ∀ x ∈ m.footprint { r with bytes := n } i, x ∈ m.footprint r i

def LegalMonoSpec (m : Model) : Prop :=
  ∀ r n i, n ≤ r.bytes → m.legal r i = true → m.legal { r with bytes := n } i = true

theorem payloadMonotone_of (m : Model) (hfp : FpSubSpec m) (hlegal : LegalMonoSpec m) :
    payloadMonotone m := by
  intro r n cap u i hn hfit
  have h := hfit
  simp [fits, List.all_eq_true] at h
  simp [fits, List.all_eq_true]
  refine ⟨⟨h.1.1, hlegal r n i hn h.1.2⟩, ?_⟩
  intro x hx
  have hh := h.2 x (hfp r n i hn x hx)
  simpa using hh

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
