import csv
import re
import unicodedata
from pathlib import Path
from typing import Callable
from typing import Dict

import jaconv  # type: ignore
import pandas as pd

base_dir = Path(__file__).resolve().parents[1]
regex_patterns = [
    r"(剣|弓|槍|騎|術|殺|狂)(輝|魔)",
    r"(剣|弓|槍|騎|術|殺|狂)(灯火|大火|猛火|業火)",
    r"(剣|弓|槍|騎|術|殺|狂)(モ|ピ)",
]


def validate_drop_rates(reports_df: pd.DataFrame) -> pd.DataFrame:
    """報告データのドロップ率がおかしくないか検証する
       おかしい場合は、Errorカテゴリに分類しエラー情報を付与する

    Args:
        reports_df (pd.DataFrame): 入力データ

    Returns:
        pd.DataFrame: 検証したデータ
    """

    def read_item_csv(file_path):
        with open(file_path, mode="r", encoding="utf-8-sig") as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # ヘッダー行をスキップ
            item_dict = {}
            for row in reader:
                rarity, name, _, _ = row
                item_dict[name] = rarity
        return item_dict

    # CSVファイルからデータを読み込む
    rarity_dict = read_item_csv(base_dir / "data" / "item.csv")

    # 'object_name'が辞書のキーに一致する行に対して前処理を適用
    for idx, row in reports_df.iterrows():
        if any(re.match(pattern, row["object_name"]) for pattern in regex_patterns):
            continue

        if row["object_name"] in rarity_dict:
            rarity = rarity_dict[row["object_name"]]
            if (
                row["category"] not in ["修練場", "冠位戴冠戦", "その他クエスト"]
                and row["war_name"] != "オーディール・コール"
            ):
                if (
                    (
                        rarity == "金"
                        and (
                            row["num"] > row["runs"]
                            or (row["runs"] >= 100 and row["num"] > row["runs"] * 0.5)
                        )
                    )
                    or (
                        rarity == "銀"
                        and (
                            row["num"] > row["runs"] * 2
                            or (row["runs"] >= 100 and row["num"] > row["runs"] * 0.7)
                        )
                    )
                    or (
                        rarity == "銅"
                        and (
                            row["num"] > row["runs"] * 3
                            or (row["runs"] >= 100 and row["num"] > row["runs"] * 0.9)
                        )
                    )
                ):
                    reports_df.at[idx, "category"] = "Error"
                    reports_df.at[idx, "object_name"] = "[E: 泥率]" + row["object_name"]

    return reports_df


def check_nonexistent_items(
    reports_df: pd.DataFrame, freequest_df: pd.DataFrame
) -> pd.DataFrame:
    """通常フリクエで本来ドロップしないアイテムが報告されていないかチェックする
       そういうアイテムがあったら"Error"カテゴリに分類しエラー情報を付与する

    Args:
        reports_df (pd.DataFrame): 報告データ
        freequest_df (pd.DataFrame): フリクエ情報

    Returns:
        pd.DataFrame: チェックしたデータ
    """
    # 以下のカテゴリを対象とします
    target_categories = ["修練場", "フリクエ1部", "フリクエ1.5部", "フリクエ2部"]

    # 対象のcategoryの行のみを取り出します
    target_reports_df = reports_df[reports_df["category"].isin(target_categories)]

    # war_nameとquest_nameを結合してuniqueなquest identifierを作成
    freequest_df["unique_quest_name"] = (
        freequest_df["war_name"] + ":" + freequest_df["quest_name"]
    )

    # unique_quest_nameをインデックスとして設定し、アイテムに関するカラムのみを抽出して辞書に変換
    quest_items_dict = (
        freequest_df.set_index("unique_quest_name").filter(like="item").to_dict("index")
    )

    # 対象の行を順に処理します
    for idx, row in target_reports_df.iterrows():
        unique_quest_name = row["war_name"] + ":" + row["quest_name"]
        object_name = row["object_name"]

        # QPと星1-3種火は除外
        if object_name == "QP" or any(
            object_name.endswith(s) for s in ["大火", "灯火", "種火"]
        ):
            continue

        # quest_nameがfreequest_dfに存在し、かつ、object_nameがそのクエストのアイテムリストにない場合
        if (
            unique_quest_name in quest_items_dict
            and object_name not in quest_items_dict[unique_quest_name].values()
        ):
            # object_nameを "[E: 非存在]" + object_nameに変更します
            reports_df.loc[idx, "object_name"] = "[E: 非存在]" + object_name  # type: ignore

            # 同じ"id"カラムを持つすべての行のcategoryを"Error"に変更します
            reports_df.loc[reports_df["id"] == row["id"], "category"] = "Error"

    return reports_df


def remove_drop_up(df: pd.DataFrame) -> pd.DataFrame:
    """ドロップアップ礼装を積んでいるという報告のアイテムを集計から除外する

    Args:
        df (pd.DataFrame): 入力データ

    Returns:
        pd.DataFrame: 除外が完了したデータ
    """

    # row["note"]の全角文字を半角文字に変換
    # note列にパターンが含まれるかどうかをチェックする関数
    def check_pattern(row):
        note = jaconv.z2h(row["note"], kana=False, digit=True, ascii=True)
        object_name = re.escape(row["object_name"])
        pattern = re.compile(rf"{object_name}泥UP\s*([0-9]+)\s*%")
        match = pattern.search(note)
        if match:
            number = re.sub(r"\s", "", match.group(1))
            if number != "0" and number != "":
                return True
        return False

    # パターンに一致するかどうかの新しい列を作成
    df["remove"] = df.apply(check_pattern, axis=1)

    # 条件に一致する行を削除
    df = df[~df["remove"]].drop(columns=["remove"])
    return df


def make_sort_name(s: str) -> str:
    """アイテム名を正規化する
       クエスト情報を読みこむときと周回データを読み込むときに使用する

    Args:
        s (str): アイテム名

    Returns:
        str: 正規化されたアイテム名
    """
    replacements = {
        # スキル石
        r"(剣|弓|槍|騎|術|殺|狂)の(輝|魔|秘)石": r"\1\2",
        # 種火
        r"(剣|弓|槍|騎|術|殺|狂)の叡智の(灯火|大火|猛火|業火)": r"\1\2",
        # ピース
        r"セイバーピース": r"剣ピ",
        r"アーチャーピース": r"弓ピ",
        r"ランサーピース": r"槍ピ",
        r"ライダーピース": r"騎ピ",
        r"アサシンピース": r"殺ピ",
        r"バーサーカーピース": r"狂ピ",
        # モニュメント
        r"セイバーモニュメント": r"剣モ",
        r"アーチャーモニュメント": r"弓モ",
        r"ランサーモニュメント": r"槍モ",
        r"ライダーモニュメント": r"騎モ",
        r"アサシンモニュメント": r"殺モ",
        r"バーサーカーモニュメント": r"狂モ",
    }

    for pattern, replacement in replacements.items():
        s = re.sub(pattern, replacement, s)

    return s


def create_item_normalizer() -> Callable[[str], str]:
    """CSVファイルで指定されたアイテム名を正規化する

    Returns:
        Callable[[str], str]: 正規化関数
    """

    def read_item_csv(file_path: Path) -> Dict[str, str]:
        with open(file_path, mode="r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # ヘッダー行をスキップ
            item_dict = {}
            for row in reader:
                _, name, alias1, alias2 = row
                item_dict[alias1] = name
                if alias2:
                    item_dict[alias2] = name
        return item_dict

    # CSVファイルからデータを読み込む
    item_dict = read_item_csv(base_dir / "data" / "item.csv")

    def normalize_item_name(item_name: str) -> str:
        # item_name に含まれる半角カナを全角カナに変換
        item_name = unicodedata.normalize("NFKC", item_name)

        for key, value in item_dict.items():
            if re.fullmatch(key, item_name):  # 正規表現でマッチするか確認
                return value
        return item_name

    return normalize_item_name


# ノーマライザ関数を作成
normalize_item_name = create_item_normalizer()


def normalize_item_and_name(item_name: str) -> str:
    """_summary_

    Args:
        item_name (str): アイテム名

    Returns:
        str: 正規化されたアイテム名
    """
    return make_sort_name(normalize_item_name(item_name))


def normalize_item(df: pd.DataFrame, freequest_df: pd.DataFrame) -> pd.DataFrame:
    """一連の正規化を動かす

    Args:
        df (pd.DataFrame): 入力データ
        freequest_df (pd.DataFrame): フリクエデータ

    Returns:
        pd.DataFrame: 正規化されたデータ
    """
    df = remove_drop_up(df)
    df["object_name"] = df["object_name"].apply(normalize_item_and_name)

    # "num" 列が -1 の行を削除する
    df = df[df["num"] != -1]

    # "id" 列の値の先頭に URL を追加する
    df["url"] = "https://fgodrop.max747.org/reports/" + df["id"]

    # timestampを日付に変換
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

    # タイムスタンプで降順ソート
    df = df.sort_values(by="timestamp", ascending=False)

    return df


def normalize_quest(df: pd.DataFrame, freequest_df: pd.DataFrame) -> pd.DataFrame:
    """指定された条件に基づいてdfのquest_name、categoryを更新し、
    特定のパターンに一致するquest_nameに対してcategoryを'修練場'に設定し、
    最終的にcategoryが決まらない場合は'その他クエスト'と設定する。

    Args:
        df (pd.DataFrame): 入力データ
        freequest_df (pd.DataFrame): フリクエデータ

    Returns:
        pd.DataFrame: 更新されたデータフレーム
    """
    # 正規表現パターンの定義
    pattern = re.compile(r"(剣|弓|槍|騎|術|殺|狂)の修練場 (初|中|上|超|極)級")

    # dfをループして行ごとに処理
    for index, row in df.iterrows():
        # categoryが最終的に決まらなかった場合のデフォルト値を設定
        df.at[index, "category"] = "その他クエスト"

        # quest_nameが正規表現に一致する場合、categoryを'修練場'に設定し、ループから抜ける
        if pattern.match(row["quest_name"]):
            df.at[index, "category"] = "修練場"
            continue  # 一致したらループを抜ける

        # freequest_dfの対応する行を探す
        matching_rows = freequest_df[freequest_df["war_name"] == row["war_name"]]

        for _, freequest_row in matching_rows.iterrows():
            # quest_nameが一致する場合
            if row["quest_name"] == freequest_row["quest_name"]:
                df.at[index, "quest_name"] = freequest_row["counter_name"]
                df.at[index, "category"] = freequest_row["category"]
                break  # 一致したらループを抜ける

            # quest_nameが一致しないが、spotが一致する場合
            elif row["quest_name"] == freequest_row["spot"]:
                df.at[index, "quest_name"] = freequest_row["counter_name"]
                df.at[index, "category"] = freequest_row["category"]
                break  # 一致したらループを抜ける

    return df
