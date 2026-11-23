# pin 中の版を保つ compaction で安全に列を遅延読取りする

## 証明済みの命題

可視性水位より古い版をまとめる compaction と slot 再利用のもとでも、登録済みの各 snapshot が読む値と未マッチ・削除の区別を保存する。

## モデルと適用条件

複数読者の pin/unpin、削除墓標、版参照の世代、失効時の再検証を明示する。

## 回帰例

古い読者がいる間に削除と同キー再挿入を行う。新しい物理スロットで旧 RID を再利用すると値が入れ替わる。

## 完了条件

任意の有効な操作列で eager と late gather を照合し、最後の pin 解放後に回収できる版の条件も示す。

実装との照合先は [table/full_scan_iterator.cpp](../../../table/full_scan_iterator.cpp)。
