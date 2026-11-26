# 最新 TLCS で負けたクエリと未証明の改善命題

2026年11月26日16時18分（日本時間）完了の `fresh-tlcs-20261126` を対象に、正解照合が通り tinylamb の中央値が少なくとも一方の相手より遅かった **89クエリ**に、演算と保証を表す名前の Lean 命題を1件ずつ割り当てた。対 DuckDB が86敗、対 ClickHouse が23敗で、20クエリは両方に負けた。前回の `after-ed-cb-20261126` と比べ、75件が継続、14件が今回新たに敗北、8件が敗北を脱した。[前回の対応表](november26-queries.md)

TPC-H SF10（22問）、TPC-DS SF10（99問）、ClickBench 全量（43問）、派生分析5種（26問）、クラウド・BI の tinylamb 計測あり23ケースを調べた。TPC-DS Q44 は結果照合不一致。Iceberg native は tinylamb の時間がなく、CostBench・PBench の比較可能なケースに敗北はなかった。主要3系列の ClickHouse の結果は164問すべて前回の保存記録と同一。通常の速度比較は8スレッド、ウォームアップ後3回の中央値を用い、僅差も含む。公式測定のサンプルを追加したり、統計的有意差を判定したりはしていない。[主要3系列の測定概要](../../../build/benchmarks/three-db-sf10-20261122/fresh-tlcs-20261126/summary.json)・[派生分析の測定](../../../build/benchmarks/three-db-sf10-20261122/fresh-tlcs-20261126/analytics/results.jsonl)・[クラウド・BI の測定](../../../build/benchmarks/three-db-sf10-20261122/fresh-tlcs-20261126/cloud_bi/measurements.json)

## 実データで確認した二つの候補

**ClickBench Q28** の tinylamb 中央値は前回の186.618 msから558.930 msになった。DuckDB は240.492 ms、ClickHouse は96.780 ms。URL 長の平均を求める際、同じ CounterID が続けば、累積終端の差がその区間の長さの総和になる。全量入力 TSV をファイル位置で12等分し、各開始点の完全な行から8,192行ずつ読むと、98,304行の中に CounterID の連続区間は17、空 URL は112あった。各区間の端点差と非空数は行別計算と一致した。この標本は入力順の調査で、TLCS の実ページや辞書の境界とは別。次回は端点を利用できたページの割合と実際の復号量を測る。[クエリ別の計測](../../../build/benchmarks/three-db-sf10-20261122/fresh-tlcs-20261126/clickbench/results.jsonl)

**TPC-DS Q35** は363.616 msで、DuckDB は53.782 ms。参照DBでは購買条件を満たす78,327行の住所・人口属性の物理キー組も78,327種類あり、このキーで事前集約しても行数は減らなかった。そこで、顧客キーが64の倍数の標本から条件を満たす異なる100グループを得て、最も後ろのソートキーを境界にした。元の SQL の NULL 順を含む6項目で数えると、住所・人口属性に結合した482,697行のうち、境界内は39,145行だった。命題は境界の同順位を含めて再走査したとき、上位100キーと件数が一致するかを問う。標本の照会費用を含む tinylamb の時間は次回測る。[クエリ別の計測](../../../build/benchmarks/three-db-sf10-20261122/fresh-tlcs-20261126/tpcds/results.jsonl)

以下の時間は ms。相手の列は負けた組だけを表示する。数値は表示だけ小数6桁に丸めた。命題は個々の候補演算についての仮説で、キーの一意性、辞書の照合順、NULL、有効ビット、順位の同点処理、正確な積和の前提は適用先でも確認する。FLOAT の再結合や整数幅の変更には、モデルの整数・有理数での同値性に加えて実機の丸めとオーバーフローの検証が必要になる。

## TPC-H

| クエリ | 前回比 | tinylamb | DuckDB | ClickHouse | 未証明命題 | 次回検証する改善 |
|---|---|---:|---:|---:|---|---|
| Q1 | 継続 | 125.156556 | 73.455461 | — | [ExceptionalShipDatesSubtractFromGroupState.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L8) | 締切後の少数行の数量・件数を全体の状態から引く。例外行率を測る。 |
| Q3 | 継続 | 88.461703 | 50.971412 | — | [SegmentOrderClosureBeforeLineProbe.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L15) | 顧客セグメントと注文日から先に注文キー集合を作り、出荷明細プローブ数を減らす。 |
| Q6 | 継続 | 65.217520 | 22.803149 | — | [DiscountBlockRevenueDotProduct.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L21) | 価格 FOR の基準額ぶんをまとめて掛け、範囲通過行だけで残差の積和を求める。 |
| Q7 | 継続 | 85.589214 | 53.636623 | — | [OppositeNationBitIntersection.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L25) | 仕入先と顧客の国を2枚のビットマスクにして反対向きの組を列挙する。 |
| Q8 | 継続 | 71.266028 | 51.314697 | — | [VolumeNeutralDimensionRejection.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L30) | 地域と品種に一致する明細でのみ PERU 判定を評価し、分子・分母を同時に更新する。 |
| Q13 | 新規 | 192.219362 | 155.571169 | — | [UnmatchedCustomersInZeroOrderBin.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L36) | 該当注文のない顧客を個別に外部結合せず、顧客総数と一致顧客数の差から0件の階級を作る。 |
| Q19 | 継続 | 73.306356 | 36.274760 | — | [QuantityBandCandidatePartMask.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L40) | 数量条件を通れるブランド枝のビットを明細側で作り、品目プローブの候補を限定する。 |
| Q20 | 継続 | 115.299201 | 45.899630 | — | [SupplyThresholdEarlyFailure.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L46) | 数量が非負なら途中の合計が在庫の2倍に達した組は棄却し、残りの数量を読まない。 |

## TPC-DS

| クエリ | 前回比 | tinylamb | DuckDB | ClickHouse | 未証明命題 | 次回検証する改善 |
|---|---|---:|---:|---:|---|---|
| Q1 | 新規 | 13.983308 | 13.111242 | — | [ReturnStoreThresholdFromPeerSums.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L8) | 顧客自身を含む店舗平均の条件を「本人の返品額と他顧客合計」の比較に変え、平均との差の評価をまとめる。 |
| Q2 | 継続 | 59.011673 | 35.603978 | 58.184059 | [WeekdayPivotBySparseDateRuns.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L13) | 同じ曜日を指す売上 run を1回の更新で曜日スロットに渡す。NULL 金額の週を保持する。 |
| Q4 | 継続 | 278.830209 | 244.577263 | — | [DominantChannelRatioTournament.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L19) | 正分母の候補だけで catalog の成長比が他2経路の最大値を超えるかを1比較にまとめる。 |
| Q7 | 新規 | 29.830327 | 28.254576 | — | [SharedValidityAverageDenominators.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L23) | 4列の有効ビットが一致するブロックだけ AVG 分母を共有する。共有できたブロック率を測る。 |
| Q11 | 継続 | 184.333765 | 145.692825 | — | [CompleteYearPairsFromTagRuns.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L28) | 顧客ごとに年とチャネルの既出タグを持ち、4スロットが揃った顧客だけ比率を評価する。 |
| Q14 | 継続 | 425.867483 | 226.052667 | — | [CrossChannelItemWitnessBeforeFactExpansion.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L31) | カテゴリ・クラス・ブランドが全経路に出るかを商品辞書で一度解決し、該当商品の事実だけを読む。 |
| Q15 | 継続 | 19.454081 | 14.049234 | — | [PriceBandBypassesAddressPredicate.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L37) | 売価が500超の行は住所述語を省き、それ以外だけ住所マスクを読む。NULL 売価も区別する。 |
| Q16 | 継続 | 14.148654 | 6.722004 | — | [TwoDistinctWarehousesWitness.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L42) | 非 NULL 倉庫を注文ごとに2種類まで記録し、別倉庫の EXISTS を定数時間で解く。 |
| Q17 | 継続 | 43.635003 | 41.951737 | — | [ParallelMomentMergeWithCenterCorrection.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L47) | STDDEV の部分状態を件数・和・分散分子でマージし、結合後の全数量の保持を省く。 |
| Q21 | 継続 | 21.725350 | 8.286353 | — | [InventoryRatioFeasibilityEnvelope.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L54) | 未処理在庫の上下界から before/after 比率の復帰が不可能なグループを棄却する。 |
| Q25 | 継続 | 40.168169 | 13.443937 | — | [TicketJoinBlockCartesianMoments.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L59) | 一致した販売・返品・catalog の各ブロックから3本の利益・損失を別々に復元し、結合行を作らない。 |
| Q29 | 継続 | 44.690570 | 42.425162 | — | [QuantityJoinSparseBranchElision.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L65) | 数量3列を読む前に customer・item ごとの catalog 側の空区画を判定する。 |
| Q30 | 継続 | 21.689993 | 16.156941 | — | [NullableStateAverageFromCountedReturnBuckets.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L70) | 同額の顧客返品合計を州内でまとめ、平均の分子・非 NULL 人数を頻度から得る。 |
| Q35 | 継続 | 363.615609 | 53.782081 | 106.609329 | [WitnessedGroupBoundaryBeforeExistenceProbes.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L76) | 標本で購入条件を満たした100種類の人口属性グループを見つけ、最大の順位キーを超える顧客への EXISTS 照会を省く。 |
| Q38 | 継続 | 198.295872 | 74.904209 | 182.945677 | [NameDayBitmapTriplePopulation.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L8) | 同姓同名を1キーに寄せ、その氏名が現れた日付ビットを3経路で AND して数える。 |
| Q39 | 継続 | 103.347456 | 57.264114 | — | [RangeWidthBoundOnVarianceNumerator.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L83) | 非負在庫の月別値域から変動係数がしきい値を超え得ない組を二乗和の計算前に落とす。 |
| Q40 | 継続 | 10.580096 | 5.415157 | — | [UnmatchedRefundPageFastPath.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L88) | 売上ページと返品キー集合が非交差なら、外部結合を行わず全件を返金ゼロの売上として渡す。 |
| Q41 | 継続 | 6.672750 | 5.947323 | — | [DemandedManufacturerWitnessCache.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L13) | 外側の製造元 ID 範囲に実在するメーカー名だけに、内側 item の存在ビットを作る。 |
| Q45 | 継続 | 176.471162 | 25.002241 | 33.336832 | [DisjointPostalAndItemFactSelections.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L19) | 郵便番号と商品履歴の2本の選択ベクトルを行位置で和集合し、両方に一致する売上を二重加算しない。 |
| Q49 | 継続 | 51.940652 | 38.878195 | — | [PeerRankBoundsFromPartitionExtrema.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L27) | 比率区画の min/max で先行する順位数をまとめ、境界区画だけ値を比較する。 |
| Q53 | 新規 | 23.328784 | 20.999364 | — | [QuarterlyOutliersFromPresentSlots.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L93) | 製造元ごとの四半期4スロットで、欠測期を平均分母に加えず偏差を計算する。 |
| Q57 | 継続 | 59.770343 | 46.830759 | — | [PeerRunNeighborJoinCardinality.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L33) | 同順位 run の件数から前後の rank が実在するかを判定し、飛び順位を誤って隣接扱いしない。 |
| Q58 | 継続 | 33.762429 | 24.484252 | — | [RevenueBandEarlyPairFilter.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L40) | store と catalog の売上が互いの許容域に入る商品だけが web 側を引く。web 集約の参照回数を測る。 |
| Q59 | 継続 | 127.054954 | 91.929279 | — | [ShiftedStoreWeekIndexWithBoundaryHalo.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L47) | 週で分割した年比較の各ワーカーには、担当週を52ずらした右側区間だけを渡す。 |
| Q62 | 継続 | 21.948046 | 11.962917 | — | [CumulativeDelayCountersToExclusiveBins.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L98) | 発送日数の累積件数の差で5区分を再構成し、境界日と NULL 日付を分ける。 |
| Q63 | 継続 | 24.026247 | 20.267095 | — | [ManagerMonthlyEqualTotalsFastReject.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L52) | manager 内の非 NULL 月計がすべて同額のとき、偏差計算とウィンドウの出力を省く。 |
| Q66 | 継続 | 46.707544 | 22.759096 | — | [MonthCaseZeroAndNullState.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L105) | 月スロットの未更新・全 NULL・別月による ELSE 0 を区別し、12本の CASE 更新を省く。 |
| Q71 | 新規 | 22.159590 | 22.145273 | — | [TimeSliceRevenueRollupAcrossUnionAll.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L110) | 朝食・夕食の時刻スロット単位で各チャネルを事前集約し、UNION ALL 後にブランドと時分を統合する。 |
| Q72 | 継続 | 124.680646 | 75.631529 | — | [StockRangeFullAcceptanceCounts.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L115) | 同一週・倉庫の在庫区画は最大数量が販売数量未満なら全件を採用し、境界区画だけ比較する。 |
| Q74 | 継続 | 191.527711 | 128.584215 | — | [ZeroPriorSalesGroupEarlyAbandon.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L57) | 前年の部分和に残りの上界を足しても正にならない場合、成長率比較の顧客状態を破棄する。 |
| Q75 | 継続 | 164.865447 | 106.591043 | — | [YearSelectionCommutesWithUnionDedup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L121) | 年が完全行に含まれるため、必要な2年に絞ってからチャネル間 DISTINCT を取る。 |
| Q79 | 継続 | 56.630744 | 43.768108 | — | [DistinctAddressMultiplicityAfterProjection.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L126) | 最終出力で住所を落とす際、同じ利益額に重なる住所グループを個数つきでソートに渡す。 |
| Q81 | 継続 | 43.080765 | 32.908078 | — | [ReturnStateThresholdSortedSuffix.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L61) | 州別の返品総額を順序づけ、平均の1.2倍を厳密に超える接尾区間だけで住所を引く。 |
| Q83 | 継続 | 23.541320 | 8.633786 | — | [CalendarBlockDateWitnessSemijoin.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L66) | 該当する週の日付キーを返品ページの局所辞書に写し、該当日がないページを全スキップする。 |
| Q85 | 継続 | 21.177654 | 19.218080 | — | [DemographicPriceIntervalDispatch.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L131) | 人口属性コードごとに該当する売価の閉区間だけを調べ、3枝の重複評価を省く。 |
| Q86 | 継続 | 11.165724 | 10.354825 | — | [RollupParentTotalsFromLeafPartitions.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L71) | カテゴリ・クラス別の nullable SUM からカテゴリ小計を作り、順位付け前の事実行の再走査を省く。 |
| Q87 | 継続 | 195.111979 | 84.345077 | — | [NegativeFilterShortcutForSetDifference.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L75) | catalog・web の除外側フィルタで確実に存在しないキーは即採用し、陽性だけを厳密照合する。 |
| Q89 | 継続 | 28.745044 | 23.306973 | — | [SignedMeanDeviationGuard.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L136) | 平均が負の区画はこの SQL の正の相対偏差を通らないため、除算とソート入力を省く。 |
| Q90 | 継続 | 9.689443 | 5.536148 | — | [SharedDimensionFilterBeforeTimeSplit.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SparseSetAndWindowEvaluation.lean#L81) | 世帯・ページで不適格な時刻キーを先に落とし、午前と午後の COUNT に同じ選択ベクトルを使う。 |
| Q99 | 継続 | 48.309214 | 14.890431 | — | [ConstantSoldDateShiftedThresholds.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/PartitionedAggregateStates.lean#L140) | 同じ販売日の区画は発送日列に境界を平行移動し、行ごとの日付差を作らない。 |

## ClickBench

| クエリ | 前回比 | tinylamb | DuckDB | ClickHouse | 未証明命題 | 次回検証する改善 |
|---|---|---:|---:|---:|---|---|
| Q3 | 新規 | 18.934448 | — | 18.460018 | [DefaultLaneSumWithPatches.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L8) | 広告 ID の既定値と例外差分から SUM を求める。例外密度と復号バイトを測る。 |
| Q4 | 新規 | 43.293410 | 27.688114 | 31.080542 | [FrameResidualOrderedAverageFold.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L12) | UserID の基準値と残差から直接 AVG に渡す。中間の展開列バイトを測る。 |
| Q9 | 新規 | 231.189022 | 194.900095 | — | [SegmentedDistinctAcrossRangeSeams.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L17) | 地域内で整列した UserID の隣接変化を数え、区画境界にまたがる重複を1回引く。 |
| Q11 | 新規 | 65.111639 | 40.375806 | 56.385273 | [ModelPostingUnionBeforeDistinctUsers.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L22) | 同じ機種名を指す辞書コードの RID リストを先に統合し、UserID の負荷を測る。 |
| Q12 | 継続 | 86.747217 | 46.325524 | 66.666470 | [DistinctUsersFromObservedPairOrdinals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L28) | 実在する端末・機種の組だけに密なグループ番号を割り当て、ユーザーとの直積表を縮める。 |
| Q13 | 新規 | 278.539424 | 181.686888 | 203.926219 | [GlobalPhraseFrequencyBoundFromOmittedPageMaxima.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L34) | 局所上位外の頻度上界を区画ごとに足し、検索語候補から漏れ得る最大件数を測る。 |
| Q14 | 継続 | 386.262690 | 376.271180 | 310.305558 | [PhraseUserRunTransitions.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L40) | 検索語ごとに整列したユーザーの run 先頭だけを COUNT DISTINCT に渡す。 |
| Q15 | 新規 | 323.287384 | 198.225338 | 237.778295 | [SparsePhraseEngineCounters.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L45) | 検索語を外側キーにし、実在する SearchEngineID だけを局所カウンタに持つ。 |
| Q16 | 継続 | 209.166964 | 177.948986 | 153.759833 | [DisjointOwnerTopFrequencyMerge.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L51) | UserID を所有区画に分けてから局所上位10を返す。最終マージ件数を測る。 |
| Q17 | 新規 | 664.774943 | 452.229001 | 507.452350 | [HierarchicalPairFrequencyScreen.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L57) | UserID と検索語の周辺頻度の小さい方で組の頻度を上から押さえ、2列キー表への挿入を減らす。 |
| Q18 | 継続 | 54.431101 | — | 8.659509 | [SeededGroupCompletionFromSuffix.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L62) | 最初の10組を発見した時点の件数を保持し、後続は既出組だけ数える。 |
| Q20 | 継続 | 17.231585 | 6.125264 | 8.318037 | [ResidualPointProbeBeforeUnpacking.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L66) | 対象 UserID を区画の基準値からの差に変換し、残差のまま等値検索する。 |
| Q22 | 継続 | 561.751025 | 343.057678 | — | [CollationOrderedUrlMinimumCode.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L69) | 照合順に並んだ URL 辞書では最小コードを1回復号して MIN(URL) を得る。 |
| Q23 | 継続 | 702.559957 | 337.164839 | 663.477129 | [LateDistinctOnWinningSearchPhrases.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L73) | 件数順位で残った検索語だけに DISTINCT UserID の状態を開く。同順位を含む候補数を測る。 |
| Q24 | 継続 | 65.747223 | 26.500685 | — | [OrderedRunQualifyingPrefixMerge.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L78) | 時刻順の各 run が返す最初の LIKE 一致10行だけをマージする。 |
| Q25 | 新規 | 19.949207 | 9.380503 | — | [SparseQualifyingTimestampSeek.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L82) | 時刻順の行で非空の検索語の位置を先に10件求めて文字列を読む。 |
| Q27 | 継続 | 21.258127 | 9.520712 | — | [LexicographicPrefixRunSelection.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L87) | 時刻同値の run ごとに残り枠ぶんだけ検索語を整列し、後続時刻のソートを減らす。 |
| Q28 | 継続 | 558.930074 | 240.491847 | 96.780356 | [CounterRunOffsetTelescoping.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L96) | 同じ CounterID の run で URL 本体の端点差から長さの総和を求め、空文字数だけ分母から除く。 |
| Q31 | 継続 | 311.910836 | 185.836948 | 142.070447 | [DeferredPayloadAcrossCountTies.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L103) | 件数で選んだ SearchEngineID・IP の組だけに SUM と AVG の入力列を復元する。 |
| Q32 | 継続 | 380.974576 | 225.865440 | 166.242505 | [SingletonCompletionAfterDuplicateDetection.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L107) | 重複する WatchID・IP の組だけを別表にし、単発組から上位枠を補う。 |
| Q36 | 継続 | 152.018331 | — | 128.114833 | [DeltaDomainIpHistogram.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L115) | 区画基準 IP からの差で度数表を持ち、IP の復元をグループごとに行う。 |
| Q37 | 継続 | 90.461724 | 10.834666 | 18.775190 | [SparseFlagExceptionsSubtractFromUrlCounts.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L119) | 条件を外すフラグが疎なら URL 別全件数から例外 RID を一度だけ引く。 |
| Q38 | 継続 | 12.847357 | 7.688486 | 10.228506 | [ConstantTitlePagePopulation.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L124) | Title が定数のページはビットの通過件数をそのまま加算する。Title の復号数を測る。 |
| Q39 | 継続 | 8.573010 | 8.165608 | — | [SkipStrictlyHigherRankPrefix.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L129) | 上位1000件を占めると確定した URL をソートから除き、残る OFFSET を補正する。 |
| Q40 | 継続 | 38.376956 | 24.609109 | — | [ConditionalReferrerKeyInterning.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L136) | CASE で空になる参照元を専用コードに寄せ、他の参照元だけ辞書化する。 |
| Q42 | 継続 | 1.911983 | 1.667265 | — | [ResolutionDomainProductOffsetBound.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L142) | 解像度2列の相異値数の積が OFFSET 以下なら、組の集約を省く。 |
| Q43 | 継続 | 2.722252 | 1.713693 | — | [MinuteOccupancySelectBeforeCounting.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/EncodedScanAggregates.lean#L146) | 占有する分のビットから1001番目の分を選び、返却範囲だけを数える。 |

## JOB

| クエリ | 前回比 | tinylamb | DuckDB | ClickHouse | 未証明命題 | 次回検証する改善 |
|---|---|---:|---:|---:|---|---|
| J1 | 継続 | 4.710490 | 3.345953 | — | [QualifiedTitleYearHistogram.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L50) | 条件を通った会社との結合件数を制作年ヒストグラムから COUNT と SUM に合成する。 |
| J3 | 継続 | 16.038444 | 6.300939 | — | [QualifiedYearCountsFromMoviePersonPostings.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L55) | 映画IDで配役をバケット化し、同じ映画の年を1回解決して人物の通過件数を加算する。 |

## STATS-CEB

| クエリ | 前回比 | tinylamb | DuckDB | ClickHouse | 未証明命題 | 次回検証する改善 |
|---|---|---:|---:|---:|---|---|
| C2 | 継続 | 19.034850 | 10.872805 | — | [OwnerElisionForEmptyCommenterIntersection.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L61) | 未読投稿のコメント数を上界にし、一致が0件と確定した owner をグループ表に作らない。 |
| C3 | 新規 | 11.125678 | 6.549384 | — | [TypedLinkEndpointRoleRouting.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L66) | リンクの両端で必要な列を分け、リンク先の種類を通った行だけ元投稿の score を引く。 |

## H2O

| クエリ | 前回比 | tinylamb | DuckDB | ClickHouse | 未証明命題 | 次回検証する改善 |
|---|---|---:|---:|---:|---|---|
| H2 | 継続 | 15.113086 | 10.545649 | — | [PairHistogramByRadixRuns.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L71) | 連結キーの上位桁で分割し、下位キーの run を数えてから全体の件数順に戻す。 |
| H4 | 継続 | 21.558171 | 8.194157 | — | [PredicateDictionarySelectionForAverage.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L76) | id3 辞書の通過コードを一度求め、AVG に渡す非 NULL 値の順序を維持する。 |
| H5 | 継続 | 13.201254 | 12.538538 | — | [SkewSplitWeightedJoinPartial.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L82) | 重い結合キーの右側だけを分割し、両側の SUM をタイルから復元する。最長タスクの時間を測る。 |

## LDBC SNB

| クエリ | 前回比 | tinylamb | DuckDB | ClickHouse | 未証明命題 | 次回検証する改善 |
|---|---|---:|---:|---:|---|---|
| L2 | 継続 | 212.863317 | 188.371911 | — | [MemberYearThresholdSuffixGather.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L88) | 加入年順のフォーラム会員リストで2010年未満を読み飛ばした後にだけ年齢を照合する。 |
| L4 | 継続 | 8.034762 | 3.248446 | — | [ScoreNullMaskIndependentReplyCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L94) | 返信件数と非 NULL score の分母を別に持ち、フォーラム別 AVG の NULL 行を復元せずに済ませる。 |

## BigBench

| クエリ | 前回比 | tinylamb | DuckDB | ClickHouse | 未証明命題 | 次回検証する改善 |
|---|---|---:|---:|---:|---|---|
| B3 | 継続 | 13.768908 | 4.190807 | — | [StarGroupSelectiveYearSlots.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L101) | 6年分のスロットに年を直接振り分け、region・category 内で年のハッシュを省く。 |
| B4 | 継続 | 5.240904 | 2.865795 | — | [TrigramNegativePageWithCountryExceptions.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L106) | 必須の3文字列がないページでは US 行だけで集計し、検索語の本文を読まない。 |
| B5 | 継続 | 5.449612 | 2.670202 | — | [PriceRangeBlockAcceptance.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L110) | 価格下限が100超の品目ページは全採用し、上限が100以下なら全棄却する。 |

## Public BI

| クエリ | 前回比 | tinylamb | DuckDB | ClickHouse | 未証明命題 | 次回検証する改善 |
|---|---|---:|---:|---:|---|---|
| MLB Q108 | 継続 | 3.396069 | 0.789138 | 2.199643 | [CachedExtremaAcrossCoveringMasks.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L117) | 4指標の MIN/MAX を、条件通過行を被覆する区画の極値から合成する。再走査行数を測る。 |

## Iceberg SQL

| クエリ | 前回比 | tinylamb | DuckDB | ClickHouse | 未証明命題 | 次回検証する改善 |
|---|---|---:|---:|---:|---|---|
| base | 継続 | 0.680380 | 0.520003 | — | [ReferentialManifestJoinElision.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/KeyRoutedJoinStates.lean#L124) | ファイルIDの一意性と全行からの参照を確認し、base スキャンの manifest 結合を外す。 |

## 再確認

リポジトリ直下で実行する。監査は2回分の生データから勝敗を計算し直し、各クエリの行・数値・宣言名・参照先を照合する。`--probe` を付けると、上記の入力標本と SF10 参照DBを使った実験も再現する（DuckDB 2.0.0 が必要）。[監査スクリプト](fixtures/audit_fresh_tlcs.py)

```sh
python3 lean/issues/research/fixtures/audit_fresh_tlcs.py
(cd lean && lake build && bash scripts/verify_lean_goals.sh)
```
