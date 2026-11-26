import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

/-!
SQL operators may batch or defer work under the modeled memory and resource ownership
conditions.

Proved

- `Exchanges.ReceiverCoalesce` — `∀ cap xs (route : Nat → Nat), 0 < cap → traffic cap
  (xs.map (fun (d,w) => (route d,w))) ≤ traffic cap xs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_ReceiverCoalesce

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges

/-! 物理宛先を一つの関数で統合しても、各宛先のパケット数（ceil）の和は増えない。
    ceil の劣加法性と、元の宛先ごとの volume への再グループ化で示す。 -/

#eval let xs : List Bucket := [(0, 1), (1, 1), (2, 5)]
      [traffic 4 (xs.map (fun p => (id p.1, p.2))),
       traffic 4 (xs.map (fun p => ((fun d => d / 2) p.1, p.2))),
       traffic 4 (xs.map (fun p => ((fun _ => 0) p.1, p.2))),
       traffic 4 xs]

private theorem nodupEraseDups [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro h
        have hf : a ∈ List.filter (fun b => ! b == a) as := (List.mem_eraseDups.mp h)
        have hp := (List.mem_filter.mp hf).2
        simp at hp
      · exact nodupEraseDups (as.filter (fun b => ! b == a))
termination_by l => l.length
decreasing_by
  change (List.filter (fun b => ! b == a) as).length < as.length + 1
  exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) (Nat.lt_succ_self _)

private theorem perm_sum_eq {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons x p ih => simpa using congrArg (fun n => x + n) ih
  | swap x y l => simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
  | trans p1 p2 ih1 ih2 => exact ih1.trans ih2

private theorem eraseDups_perm_of_perm [BEq α] [LawfulBEq α] {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    l₁.eraseDups.Perm l₂.eraseDups := by
  apply (List.perm_ext_iff_of_nodup (nodupEraseDups l₁) (nodupEraseDups l₂)).mpr
  intro x
  simpa [List.mem_eraseDups] using p.mem_iff (a := x)

/-! l の eraseDups に対する和を、r を含むかどうかで二つに分ける。 -/
private theorem eraseDups_sum_erase (l : List Nat) (r : Nat) (h : Nat → Nat) :
    (l.eraseDups.map h).sum =
      (if r ∈ l then h r else 0) + (((l.filter (fun b => ! b == r)).eraseDups.map h).sum) := by
  cases l with
  | nil => simp
  | cons a as =>
      rw [List.eraseDups_cons]
      simp only [List.map_cons, List.sum_cons]
      by_cases har : a == r
      · have haeq : a = r := beq_iff_eq.mp har
        subst a
        simp
      · have hane : a ≠ r := by intro h'; exact har (beq_iff_eq.mpr h')
        have hane' : r ≠ a := fun h' => hane h'.symm
        have hfilt : List.filter (fun b => ! b == r) (a :: as) = a :: List.filter (fun b => ! b == r) as := by simp [har]
        rw [hfilt, List.eraseDups_cons]
        simp only [List.map_cons, List.sum_cons]
        simp [hane']
        have ih := eraseDups_sum_erase (List.filter (fun b => ! b == a) as) r h
        rw [ih]
        simp [hane']
        have hpred : (fun x : Nat => ! x == r && ! x == a) = (fun x => ! x == a && ! x == r) := by
          funext x
          cases h1 : x == r <;> cases h2 : x == a <;> simp [h1, h2]
        rw [hpred]
        omega
termination_by l.length
decreasing_by
  have hle := List.length_filter_le (fun b => ! b == a) as
  simp only [List.length_cons]
  omega

private theorem sum_filter_eq_sum_ite (l : List Nat) (P : Nat → Bool) (h : Nat → Nat) :
    ((l.filter P).map h).sum = (l.map (fun x => if P x then h x else 0)).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases hp : P x = true <;> simp [hp, ih]

private theorem vol_zero_of_not_mem : ∀ (xs : List Bucket) (d : Nat),
    d ∉ (xs.map Prod.fst).eraseDups → volume xs d = 0
  | [], d, hd => by simp [volume]
  | p :: rest, d, hd => by
      have hd' : d ∉ (rest.map Prod.fst).eraseDups := by
        intro hmem
        apply hd
        have hdrest : d ∈ rest.map Prod.fst := List.mem_eraseDups.mp hmem
        exact List.mem_eraseDups.mpr (by simp [hdrest])
      have hpne : p.1 ≠ d := by
        intro h
        apply hd
        exact List.mem_eraseDups.mpr (by simp [h])
      have hrest := vol_zero_of_not_mem rest d hd'
      simpa [volume, hpne] using hrest

/-! 宛先 d ごとの volume で重み付けた和を、述語 P を満たすバッチの重みの和に戻す。 -/
private theorem sum_group_of_buckets (xs : List Bucket) (P : Nat → Bool) :
    (((xs.map Prod.fst).eraseDups.filter P).map (fun d => volume xs d)).sum =
      ((xs.filter (fun p => P p.1)).map Prod.snd).sum := by
  cases xs with
  | nil => simp
  | cons p rest =>
      rcases p with ⟨d, w⟩
      let E := (List.filter (fun e => ! e == d) (rest.map Prod.fst)).eraseDups
      have hEne : ∀ e ∈ E, e ≠ d := by
        intro e he
        have hm : e ∈ List.filter (fun b => ! b == d) (rest.map Prod.fst) := by
          simpa [E] using (List.mem_eraseDups.mp he)
        exact by simpa using (List.mem_filter.mp hm).2
      have hcong : ((E.filter P).map (fun e => volume ((d, w) :: rest) e)).sum =
                   ((E.filter P).map (fun e => volume rest e)).sum := by
        have hm := List.map_congr_left (l := E.filter P) (f := fun e => volume ((d, w) :: rest) e)
          (g := fun e => volume rest e) (by
            intro e he
            have hed : e ≠ d := hEne e (List.mem_filter.mp he).1
            simp [volume, hed.symm])
        exact congrArg List.sum hm
      have hfilterE := sum_filter_eq_sum_ite E P (fun e => volume rest e)
      have hfilterds := sum_filter_eq_sum_ite (rest.map Prod.fst).eraseDups P (fun e => volume rest e)
      have herase := eraseDups_sum_erase (rest.map Prod.fst) d (fun e => if P e then volume rest e else 0)
      rw [← hfilterE] at herase
      rw [← hfilterds] at herase
      have hdmem : (if d ∈ rest.map Prod.fst then (if P d then volume rest d else 0) else 0) =
          (if P d then volume rest d else 0) := by
        by_cases hd : d ∈ rest.map Prod.fst
        · simp [hd]
        · have hz : volume rest d = 0 := vol_zero_of_not_mem rest d (by simpa using hd)
          simp [hd, hz]
      rw [hdmem] at herase
      have hih := sum_group_of_buckets rest P
      rw [hih] at herase
      rw [List.map_cons, List.eraseDups_cons]
      by_cases hPd : P d
      · simp [hPd] at herase
        simp [hPd]
        rw [hcong, herase]
        simp [volume]
        omega
      · simp [hPd]
        rw [hcong]
        have hzpd : (if P d then volume rest d else 0) = 0 := by simp [hPd]
        rw [hzpd] at herase
        omega
termination_by xs.length
decreasing_by
  simp only [List.length_cons]
  omega

private theorem packets_zero (cap : Nat) (hc : 0 < cap) : packets cap 0 = 0 := by
  unfold packets
  have hlt : 0 + cap - 1 < cap := by omega
  exact Nat.div_eq_of_lt hlt

/-! ceil の劣加法性。 -/
private theorem packets_add_le (cap a b : Nat) (hc : 0 < cap) :
    packets cap (a + b) ≤ packets cap a + packets cap b := by
  unfold packets
  have ha : a ≤ cap * ((a + cap - 1) / cap) := by
    have h := (Nat.div_le_iff_le_mul_add_pred hc).mp (Nat.le_refl ((a + cap - 1) / cap))
    omega
  have hb : b ≤ cap * ((b + cap - 1) / cap) := by
    have h := (Nat.div_le_iff_le_mul_add_pred hc).mp (Nat.le_refl ((b + cap - 1) / cap))
    omega
  apply (Nat.div_le_iff_le_mul_add_pred hc).mpr
  rw [Nat.mul_add]
  omega

private theorem packets_sum_le (cap : Nat) (hc : 0 < cap) : ∀ l : List Nat,
    packets cap l.sum ≤ (l.map (fun n => packets cap n)).sum
  | [] => by simp [packets_zero cap hc]
  | x :: xs => by
      simp only [List.sum_cons, List.map_cons]
      exact Nat.le_trans (packets_add_le cap x xs.sum hc)
        (Nat.add_le_add_left (packets_sum_le cap hc xs) _)

private theorem sum_le_sum_of_forall {l : List Nat} {f g : Nat → Nat}
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.add_le_add (h x (by simp)) (ih (fun y hy => h y (by simp [hy])))

private theorem group_zero_of_not_mem : ∀ (rest : List Nat) (route : Nat → Nat) (f : Nat → Nat) (d' : Nat),
    d' ∉ (rest.map route).eraseDups →
      ((rest.filter (fun x => route x == d')).map f).sum = 0
  | [], route, f, d', hd => by simp
  | x :: xs, route, f, d', hd => by
      have hx : route x ≠ d' := by
        intro h
        apply hd
        exact List.mem_eraseDups.mpr (by simp [h])
      have hxs : d' ∉ (xs.map route).eraseDups := by
        intro hm
        apply hd
        have hm0 : d' ∈ xs.map route := List.mem_eraseDups.mp hm
        exact List.mem_eraseDups.mpr (by simp [hm0])
      have ih := group_zero_of_not_mem xs route f d' hxs
      simp [hx, ih]

/-! route による分類は、元の nodup な宛先列の分割になる。 -/
private theorem group_sum_eq (ds : List Nat) (route : Nat → Nat) (f : Nat → Nat)
    (hnd : ds.Nodup) :
    (((ds.map route).eraseDups.map (fun d' =>
      ((ds.filter (fun d => route d == d')).map f).sum)).sum) = (ds.map f).sum := by
  cases ds with
  | nil => simp
  | cons d rest =>
      have hr : rest.Nodup := (List.nodup_cons.mp hnd).2
      let E := (List.filter (fun e => ! e == route d) (rest.map route)).eraseDups
      have hEne : ∀ e ∈ E, e ≠ route d := by
        intro e he
        have hm : e ∈ List.filter (fun b => ! b == route d) (rest.map route) := by
          simpa [E] using (List.mem_eraseDups.mp he)
        exact by simpa using (List.mem_filter.mp hm).2
      have hfilter_head : (((d :: rest).filter (fun x => route x == route d)).map f).sum =
          f d + ((rest.filter (fun x => route x == route d)).map f).sum := by
        simp
      have hcongE : ((E.map (fun d' => (((d :: rest).filter (fun x => route x == d')).map f).sum)).sum) =
          ((E.map (fun d' => ((rest.filter (fun x => route x == d')).map f).sum)).sum) := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro d' hd'
        have hed : d' ≠ route d := hEne d' hd'
        simp [hed.symm]
      have herase := eraseDups_sum_erase (rest.map route) (route d)
        (fun d' => ((rest.filter (fun x => route x == d')).map f).sum)
      have hcond : (if route d ∈ rest.map route then
          ((rest.filter (fun x => route x == route d)).map f).sum else 0) =
          ((rest.filter (fun x => route x == route d)).map f).sum := by
        by_cases hm : route d ∈ rest.map route
        · simp [hm]
        · have hz := group_zero_of_not_mem rest route f (route d) (by simpa using hm)
          simp [hm, hz]
      rw [hcond] at herase
      have hih := group_sum_eq rest route f hr
      rw [hih] at herase
      rw [List.map_cons, List.eraseDups_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [hfilter_head, hcongE, herase]
      dsimp [E]
      omega

private theorem filter_mapRoute : ∀ (rest : List Bucket) (route : Nat → Nat) (d' : Nat),
    (rest.map (fun p => (route p.1, p.2))).filter (fun q => q.1 == d') =
      (rest.filter (fun p => route p.1 == d')).map (fun p => (route p.1, p.2))
  | [], route, d' => by simp
  | p :: rest, route, d' => by
      by_cases h : route p.1 == d' <;> simp [h, filter_mapRoute rest route d']

private theorem volume_mapRoute : ∀ (xs : List Bucket) (route : Nat → Nat) (d' : Nat),
    volume (xs.map (fun p => (route p.1, p.2))) d' =
      ((xs.filter (fun p => route p.1 == d')).map Prod.snd).sum
  | [], route, d' => by simp [volume]
  | p :: rest, route, d' => by
      by_cases h : route p.1 == d' <;>
        simp [volume, filter_mapRoute, h, List.map_map, Function.comp_def,
          volume_mapRoute rest route d']

theorem sound_Exchanges_ReceiverCoalesce :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges.ReceiverCoalesce.Goal := by
  intro cap xs route hcap
  let ds := (xs.map Prod.fst).eraseDups
  let rxs := xs.map (fun p : Bucket => (route p.1, p.2))
  have hvol : ∀ d', volume rxs d' =
      ((ds.filter (fun d => route d == d')).map (fun d => volume xs d)).sum := by
    intro d'
    rw [show rxs = xs.map (fun p => (route p.1, p.2)) by rfl]
    rw [volume_mapRoute]
    exact (sum_group_of_buckets xs (fun d => route d == d')).symm
  have hrds_perm : (rxs.map Prod.fst).eraseDups.Perm (ds.map route).eraseDups := by
    apply (List.perm_ext_iff_of_nodup (nodupEraseDups _) (nodupEraseDups _)).mpr
    intro z
    simp [List.mem_eraseDups, List.mem_map, ds, rxs]
  dsimp [traffic]
  calc
    (((rxs.map Prod.fst).eraseDups.map (fun d => packets cap (volume rxs d))).sum)
        = ((ds.map route).eraseDups.map (fun d => packets cap (volume rxs d))).sum := by
            exact perm_sum_eq (hrds_perm.map (fun d => packets cap (volume rxs d)))
    _ = ((ds.map route).eraseDups.map (fun d' =>
          packets cap (((ds.filter (fun d => route d == d')).map (fun d => volume xs d)).sum))).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro d' hd'
            rw [hvol d']
    _ ≤ ((ds.map route).eraseDups.map (fun d' =>
          ((ds.filter (fun d => route d == d')).map (fun d => packets cap (volume xs d))).sum)).sum := by
            apply sum_le_sum_of_forall
            intro d' hd'
            simpa [List.map_map, Function.comp_def] using
              (packets_sum_le cap hcap ((ds.filter (fun d => route d == d')).map (fun d => volume xs d)))
    _ = (ds.map (fun d => packets cap (volume xs d))).sum := by
            exact group_sum_eq ds route (fun d => packets cap (volume xs d)) (nodupEraseDups _)
    _ = ((xs.map Prod.fst).eraseDups.map (fun d => packets cap (volume xs d))).sum := by rfl

end TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_ReceiverCoalesce
