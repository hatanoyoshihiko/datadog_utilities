# Datadog モニターのエクスポート・インポートスクリプト

## 概要

Datadogのモニターをエクスポート（バックアップ）とインポート（リストア）を行うスクリプトです

## 実行条件

- Python 3.8 以上
- `datadog_api_client` ライブラリ（Datadog公式クライアント）が導入済みであること
  - 確認方法 `$ pip3 list | grep -i datadog-api-client`
  - インストール `$ pip3 install datadog-api-client`
- 実行環境から Datadog API エンドポイントへネットワーク到達できること　※HTTPSでインターネット接続が出来れば良い

## エクスポート手順

### エクスポート対象モニターCSVの作成

2行目以降にエクスポートしたいモニターIDを記載下さい  
IDはモニターURLの末尾数字です  

```csv
id
180972065
180972199
```

### エクスポートの実行

`$ python3 datadog_export_import_monitor.py export export_monitor.csv`

### APIキーの入力

Datadog API Key と Application Keyを入力します  

### エクスポート結果

スクリプトを実行したディレクトリに `id.json` 形式でJSONファイルが生成されます  

## インポート手順

### インポート対象モニターJSONの作成

モニターはJSON形式です  
`*.json` や `./monitors/??????.json` でjsonファイルのパスを指定して下さい  
JSONファイルは、モニターのエクスポートを実行したときのJSON形式でファイルを作成して下さい  

### インポートの実行

**JSONファイルのダブルクォーテーションを忘れないこと**  
`$ python3 datadog_export_import_monitor.py import "xxx.json"`  
`"*.json"`の指定も可能です

### APIキーの入力

Datadog API Key と Application Keyを入力します  

### インポート結果

Datadog Organizationにログインし、Monitorsから作成されたモニターを検索し、インポートされたことを確認します
JSONファイル内の `name:` がモニターのタイトルにあたるのでこれで検索します

## スクリプトの仕様

### 引数の違い

export : モニター設定をDatadog Organizationから取得し、JSONでエクスポートします  
import : モニター設定JSONを読み込み、モニターをDatadog Organizationに新規作成します

### 処理の流れ

1. 実行時に **Datadog API Key** と **Application Key** を対話入力します
2. `export`
   - 指定した CSV から Monitor ID を読み取る
   - `GET /api/v1/monitor/{id}` でモニター定義を取得する
   - `<id>.json` という名前で保存する
3. `import`
   - 指定パターンに一致する JSON ファイルを逐次読み取る
   - 各 JSON を `POST /api/v1/monitor` で新規モニターとして登録する
4. 成功／失敗は `datadog_monitor.log`（INFO レベル以上）に記録されます
