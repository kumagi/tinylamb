import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-! These proposals narrow aggregate and join state before expanding retail fact rows. Monetary totals are exact or require an independent rounding guard; nullable inputs are modeled explicitly. -/

namespace TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- For exact amounts, comparing a customer against six fifths of the store average can reuse the peer sum and the number of valid peers. -/
def ReturnStoreThresholdFromPeerSums.Goal : Prop := ∀ (peers : List (Option Int)) (own : Int),
  let vs := peers.filterMap id
  (5*((vs.length:Int)+1)*own > 6*(own+vs.sum)) ↔ (5*(vs.length:Int)-1)*own > 6*vs.sum

/-- A nullable weekday sum can consume a run once with its length as weight, skipping zero-length runs. -/
def WeekdayPivotBySparseDateRuns.Goal : Prop := ∀ (runs : List ((Nat × Option Int) × Nat)) weekday,
  let full := runs.flatMap (fun (r,n) => List.replicate n r)
  nsum (forKey full weekday) = nsum ((runs.filter (fun r => r.1.1 == weekday && 0 < r.2)).map
    (fun ((_,v),n) => v.map (· * (n:Int))))

/-- Scaling the two competitors to a common positive denominator allows one dominance comparison against their maximum. -/
def DominantChannelRatioTournament.Goal : Prop := ∀ (cat store web dc ds dw : Int), 0 < dc → 0 < ds → 0 < dw →
  (cat*ds > store*dc ∧ cat*dw > web*dc) ↔ cat*ds*dw > max (store*dc*dw) (web*dc*ds)

/-- Four independent average states can share a denominator if the four validity masks agree on the selected rows. -/
def SharedValidityAverageDenominators.Goal : Prop := ∀ (rs : List (Fin 4 → Option Int)), (∀ r ∈ rs, ∀ j, (r j).isSome = (r 0).isSome) →
  ∀ j, ((rs.filterMap (· j)).sum,(rs.filterMap (· j)).length) =
    ((rs.filterMap (· j)).sum,(rs.filterMap (· 0)).length)

/-- Within a customer, distinct tags determine whether all four required partitions exist even when source rows repeat. -/
def CompleteYearPairsFromTagRuns.Goal : Prop := ∀ (tags : List (Fin 4)), tags.eraseDups.length = 4 ↔ ∀ k : Fin 4, k ∈ tags

/-- Translating an intersection of label classes back to physical item IDs preserves sales for all historical versions of each class. -/
def CrossChannelItemWitnessBeforeFactExpansion.Goal : Prop := ∀ (ids : List Nat) (label : Nat → Nat) (a b c : List Nat),
  let common := members (members a b) c
  let keep := ids.eraseDups.filter (fun id => common.contains (label id))
  ids.filter keep.contains = ids.filter (fun id => a.contains (label id) && b.contains (label id) && c.contains (label id))

/-- Exact partition by the price arm of a disjunction preserves the bag of selected postal-group inputs under nullable prices. -/
def PriceBandBypassesAddressPredicate.Goal : Prop := ∀ (rs : List (Bool × Option Int)),
  let high := fun (r : Bool × Option Int) => r.2.any (500 < ·)
  (rs.filter (fun r => r.1 || high r)).Perm ((rs.filter high) ++ rs.filter (fun r => !high r && r.1))

/-- Two distinct non-null warehouse witnesses suffice for every non-null probe, even if the probe itself is absent from the order. -/
def TwoDistinctWarehousesWitness.Goal : Prop := ∀ (warehouses : List (Option Nat)) k,
  let sample := (warehouses.filterMap id).eraseDups.take 2
  sample.any (· != k) = warehouses.any (fun x => x.any (· != k))

/-- Two nonempty sample blocks can merge their variance numerators with a between-block correction using exact arithmetic. -/
def ParallelMomentMergeWithCenterCorrection.Goal : Prop := ∀ (a b : List Int), a ≠ [] → b ≠ [] →
  let n : Int := a.length
  let m : Int := b.length
  let varNum := fun (xs : List Int) => (xs.length:Int)*total xs (fun x => x*x)-xs.sum^2
  n*m*varNum (a++b) = (n+m)*(m*varNum a+n*varNum b)+(m*a.sum-n*b.sum)^2

/-- Positive before totals and certified bounds on both sums can reject an infeasible two-thirds to three-halves ratio early. -/
def InventoryRatioFeasibilityEnvelope.Goal : Prop := ∀ (before after bLo bHi aLo aHi : Int), 0 < before →
  bLo ≤ before → before ≤ bHi → aLo ≤ after → after ≤ aHi →
  (3*aHi < 2*bLo ∨ 3*bHi < 2*aLo) → ¬ (2*before ≤ 3*after ∧ 2*after ≤ 3*before)

/-- For a fixed full join key, replicating each branch subtotal by the other two cardinalities yields all three sums. -/
def TicketJoinBlockCartesianMoments.Goal : Prop := ∀ (s r c : List Int),
  let bag := s.flatMap fun x => r.flatMap fun y => c.map fun z => (x,y,z)
  (total bag Prod.fst,total bag (fun p => p.2.1),total bag (fun p => p.2.2)) =
    (s.sum*(r.length:Int)*c.length,r.sum*(s.length:Int)*c.length,c.sum*(s.length:Int)*r.length)

/-- Empty catalog slices may be removed from the ticket-side stream before expanding the three quantity columns. -/
def QuantityJoinSparseBranchElision.Goal : Prop := ∀ (tickets : List (Nat × Int × Int)) (catalog : Nat → List Int),
  let expand := fun (t : Nat × Int × Int) => (catalog t.1).map (fun v => (t.2.1,t.2.2,v))
  (tickets.filter (fun t => !(catalog t.1).isEmpty)).flatMap expand = tickets.flatMap expand

/-- Counting repeated customer subtotals by value recovers the state's average state without counting null subtotals as customers with zero returns. -/
def NullableStateAverageFromCountedReturnBuckets.Goal : Prop := ∀ (xs : List (Option Int)),
  let hist := frequency (xs.filterMap id)
  (total hist (fun (v,n) => v*(n:Int)),(hist.map Prod.snd).sum) =
    ((xs.filterMap id).sum,(xs.filterMap id).length)

/-- A sample of distinct qualifying groups certifies a cutoff on the first hundred groups. Evaluating existence only below that cutoff preserves every winning key and its full count. -/
def WitnessedGroupBoundaryBeforeExistenceProbes.Goal : Prop := ∀ (customers : List (Nat × Nat)) (buys : Nat → Bool) (witnesses : List Nat) cut,
  let eligible := fun (rs : List (Nat × Nat)) => (rs.filter (fun r => buys r.2)).map Prod.fst
  (∀ k ∈ witnesses, k ∈ eligible customers) → 100 ≤ (witnesses.filter (· ≤ cut)).eraseDups.length →
  let topGroups := fun (ks : List Nat) => (best 100 ks.eraseDups).map (fun k => (k,ks.count k))
  topGroups (eligible (customers.filter (fun r => r.1 ≤ cut))) = topGroups (eligible customers)

/-- For bounded nonnegative samples, a range-width bound on the sample variance numerator can rule out high dispersion before computing squares. -/
def RangeWidthBoundOnVarianceNumerator.Goal : Prop := ∀ (xs : List Int) lo hi, 0 ≤ lo → (∀ v ∈ xs, lo ≤ v ∧ v ≤ hi) →
  let n : Int := xs.length
  n*total xs (fun v => v*v)-xs.sum^2 ≤ n*(n-1)*(hi-lo)^2/2

/-- Certified disjoint keys make every sale unmatched; its net price then equals its original price once. -/
def UnmatchedRefundPageFastPath.Goal : Prop := ∀ (sales refunds : List (Nat × Int)), (∀ s ∈ sales, ¬ s.1 ∈ refunds.map Prod.fst) →
  (sales.flatMap (fun (k,p) => let vs := forKey refunds k
                               if vs.isEmpty then [p] else vs.map (fun r => p-r))) = sales.map Prod.snd

/-- Projecting four unique quarter slots preserves the nullable average state without treating absent quarters as zero-valued quarters. -/
def QuarterlyOutliersFromPresentSlots.Goal : Prop := ∀ (quarters : List (Fin 4 × Option Int)), (quarters.map Prod.fst).Nodup →
  let slots := (List.finRange 4).filterMap (fun k => ((forKey quarters k).head?).join)
  (slots.sum,slots.length) = ((quarters.filterMap Prod.snd).sum,(quarters.filterMap Prod.snd).length)

/-- Four cumulative delay thresholds and the valid count recover exclusive histogram bins; null dates do not enter the last bin. -/
def CumulativeDelayCountersToExclusiveBins.Goal : Prop := ∀ (xs : List (Option Int)),
  let ds := xs.filterMap id
  let n := fun (t : Int) => countWhere ds (· ≤ t)
  [n 30,n 60-n 30,n 90-n 60,n 120-n 90,ds.length-n 120] =
    (List.range 5).map (fun i => countWhere ds (fun d => bucket [30,60,90,120] d == i))

/-- A monthly nullable conditional sum can append a single zero witness if any other month occurred. -/
def MonthCaseZeroAndNullState.Goal : Prop := ∀ (rs : List (Nat × Option Int)) month,
  nsum (rs.map (fun (m,v) => if m == month then v else some 0)) =
    nsum (forKey rs month ++ if rs.any (fun r => r.1 != month) then [some 0] else [])

/-- For a fixed output brand and minute, channel partial sums over qualifying fact times preserve a nullable union-all sum. -/
def TimeSliceRevenueRollupAcrossUnionAll.Goal : Prop := ∀ (channels : List (List (Nat × Option Int))) (qualifies : Nat → Bool),
  let project := fun (xs : List (Nat × Option Int)) => (xs.filter (fun r => qualifies r.1)).map Prod.snd
  nsum (channels.map (fun c => nsum (project c))) = nsum (project channels.flatten)

/-- Min/max stock bounds allow full, empty and mixed pages to contribute directly to the strict inequality join count. -/
def StockRangeFullAcceptanceCounts.Goal : Prop := ∀ (pages : List (List Int)) sold,
  let contribution := fun (p : List Int) => if p.max?.any (· < sold) then p.length
    else if p.min?.any (sold ≤ ·) then 0 else countWhere p (· < sold)
  (pages.map contribution).sum = countWhere pages.flatten (· < sold)

/-- Selection by a retained year distributes through union distinct without conflating payloads from other years. -/
def YearSelectionCommutesWithUnionDedup.Goal : Prop := ∀ (channels : List (List (Nat × Int × Int))),
  let keep := fun (p : Nat × Int × Int) => p.1 == 2001 || p.1 == 2002
  ((channels.flatMap (List.filter keep)).eraseDups) = channels.flatten.eraseDups.filter keep

/-- Within an equal visible sort-key prefix, counting ties by completed address-group profit preserves the sorted output bag. -/
def DistinctAddressMultiplicityAfterProjection.Goal : Prop := ∀ (profits : List Int),
  ((frequency profits).mergeSort (fun a b => decide (a.1 ≤ b.1))).flatMap (fun (v,n) => List.replicate n v) =
    profits.mergeSort

/-- The matching demographic class selects which price intervals to test. Overlapping endpoints still yield one Boolean decision per row. -/
def DemographicPriceIntervalDispatch.Goal : Prop := ∀ (rules : List (Nat × Int × Int)) k price,
  (forKey rules k).any (fun (l,h) => l ≤ price && price ≤ h) =
    rules.any (fun (c,l,h) => c == k && l ≤ price && price ≤ h)

/-- A positive relative-deviation threshold with a signed mean admits no negative denominator; the remaining exact comparison can use a product. -/
def SignedMeanDeviationGuard.Goal : Prop := ∀ (delta mean : Rat), mean ≠ 0 →
  (max delta (-delta) / mean > 1/10 ↔ 0 < mean ∧ 10*max delta (-delta) > mean)

/-- Within a constant sold-date run, shifting every bucket threshold allows classifying ship dates directly. -/
def ConstantSoldDateShiftedThresholds.Goal : Prop := ∀ (sold shipped : Int),
  bucket [30,60,90,120] (shipped-sold) = bucket ([30,60,90,120].map (sold+·)) shipped

end TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates
