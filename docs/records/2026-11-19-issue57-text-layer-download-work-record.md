# Issue #57 作業記録: text_layer ワークフローのダウンロード配置の修正

## 対象

- チケット: Issue #57「text_layer ワークフローのダウンロード配置がシンボリックリンクの rename で壊れる」
- 仕様: https://github.com/ksk-labs/edinet-pipeline/issues/15（工程2仕様 §6、P2-T7 の後続）
- きっかけ: text_layer.yml の初回 dispatch（run 44473568484）で、ダウンロードステップが success だったのに抽出ステップが `FileNotFoundError: 'work/text/filing.zip'` で失敗した。

## 原因

`.github/workflows/text_layer.yml` のダウンロードステップのインラインスクリプトが、`hf_hub_download` の戻りパスを `os.rename` で配置先へ移していた。`hf_hub_download` が返すのは HF キャッシュ内の相対シンボリックリンクであり、別ディレクトリへ rename で移すとリンク先が解決できなくなり、後段の open が FileNotFoundError になる。対象は2箇所（提出 zip と `master/edinet_codes.parquet`）。

### 再現（実施役が自分の手で確認）

公開データセット `Yoshi-Dai/financial-lakehouse-v2` の小さいファイル（`master/summary.json`、1,126バイト）を huggingface_hub 2.4.0（requirements.txt の固定版）で取得して確認した。

- 戻りパスはシンボリックリンクで、リンク先は相対パス `../../../blobs/df9b401d…`。
- rename 前は open でき、サイズ 1,126 バイトを読める。
- 別ディレクトリへ `os.rename` した後は `os.path.exists` が False になり、open が FileNotFoundError になる。

## 修正方式と根拠

配置の手順をモジュール側の既存の仕組みへ寄せた（Issue 本文の第一候補）。

- `pipeline/text_layer.py` に `download` サブコマンドと `run_download(repo, doc_id, submit_date, zip_out, master_dir)` を追加した。配置は既存の `_download_one` を通す。`_download_one` は `shutil.copyfile` でリンク先の実体をコピーするため、配置先はキャッシュの配置に依存しない通常ファイルになる。再試行（`_with_retry`）もモジュール側の既存の経路に乗る。
- `.github/workflows/text_layer.yml` のダウンロードステップは、インラインスクリプトをやめ、`python -m pipeline.text_layer download --doc-id … --submit-date … --zip-out work/text/filing.zip --master-dir work/text/master` の呼び出しだけにした。配置先のパスは従来どおりで、後段の extract／build ステップは無変更。

インラインスクリプトの copyfile 化（第二候補）でなくモジュールへ寄せた根拠は、ワークフローのインラインスクリプトはテストで直接縛れないのに対し、モジュール側の手順は単体テストで回帰を固定できるためである。再発防止の形はこのテストによる固定（層 (2)）である。

## 回帰テスト

`tests/test_text_layer.py` に `RunDownloadTest`（2件）を追加した。`hf_hub_download` を、実ファイルへの相対シンボリックリンクを返す偽物に差し替えて `run_download` と CLI（`main(["download", …])`）の両経路を通し、配置先がリンクでない実体ファイルで内容が一致することと、要求したファイル名が想定の2件であることを固定する。

突然変異による確認: `_download_one` の `shutil.copyfile` を `os.rename` へ戻した写しでは、追加した2件とも失敗した（`assertFalse(os.path.islink(dest))` が True is not false）。修正後の本体では合格する。

## 検証結果

- 全テスト: `python -m unittest discover -s tests` で 294件を実行し全件合格（追加前は 292件。text_layer 分は 27件→29件）。
- 実データの確認: 標本 S100ZI7X（提出日 2026-11-17）で `download` を実経路で実行し、提出 zip（913,935バイト、有効な zip でエントリ32件）と `edinet_codes.parquet`（178,609バイト、11,836行）がいずれもリンクでない実体ファイルとして配置されることを確認した。

## 変更範囲

- `pipeline/text_layer.py`: `run_download` と `download` サブコマンドの追加、モジュール docstring の使い方への1行追加。
- `.github/workflows/text_layer.yml`: ダウンロードステップをモジュール呼び出しへ置き換え、冒頭の手順一覧へ download の行を追加。
- `tests/test_text_layer.py`: `RunDownloadTest`（2件）の追加。
- 本作業記録の追加。

## 残件

Issue #57 の完了条件のうち「text_layer.yml を dispatch して success し、HF の text/ に成果物が置かれること」の確認は、この PR のマージ後の dispatch で行う（本作業の範囲は実装と PR 作成まで）。
