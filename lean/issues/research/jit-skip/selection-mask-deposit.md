# 圧縮後の選択マスクを PDEP で元のレーンへ戻す

**未証明。** 二段フィルタで、二段目は一段目を通った行だけを連番で見る。二段目の選択を元の行番号に戻す処理をビット deposit に変換する。

## 提出型

[`SelectionMaskDeposit.Goal`](../../../TinylambOpt/ResearchGoals/SelectionMaskDeposit.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.SelectionMaskDeposit.Goal` を提出する。

最下位セットビットを消費する PDEP モデルが、親の有効行を元順で列挙し子の順位で選んだ列を返すこと。親が疎でも行順が変わらない。

## 境界例と適用範囲

親のレーン0、3、63に対し子の順位0、2は元レーン0、63。空の親では子の余ったビットを無視する。ISA ごとの速度や命令選択はこの命題の対象外。
