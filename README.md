# FGO-Drop-Analizer

FGO-Drop-Reporter のデータベースからデータを集め FGO アイテム効率劇場用のデータを作成する

## 必要環境

- Python が動作する環境

## インストール

### Pip を使用する場合

```
cd fgo-drop-analyzer
pip install -r requerements.txt
cd conf
copy config-dist.ini config.ini
```

管理者から APIKEY をもらい、config.ini の (INPUT API KEY) となっているところに記述する

### Poetry を使用する場合(熟練者向け)

1. 当 Project は Python3.10 で構築しているので Python3.10 を使用できるようにする
2. 下記を実行

```
poetry install --no-dev
cd conf
copy config-dist.ini config.ini
```

管理者から APIKEY をもらい、config.ini の (INPUT API KEY) となっているところに記述する

## 使用方法

### Pip でインストールした場合

```
python -m fgo_drop_analyzer 出力Excelファイル名
```

### Poetry でインストールした場合

```
poetry run python -m fgo_drop_analyzer 出力Excelファイル名
```

## 出力ファイル

出力される Excel ファイルは syutagcnt とほぼ互換性があります
