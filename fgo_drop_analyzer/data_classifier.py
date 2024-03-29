import pandas as pd


def classify_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """報告データを読み込んでカテゴリー分けする

    Args:
        df (pd.DataFrame): 報告データ

    Returns:
        pd.DataFrame: カテゴリーを付けたデータ
    """
    # 条件をリストや辞書に格納
    war_names = {
        "フリクエ1部": ["冬木", "オルレアン", "セプテム", "オケアノス", "ロンドン", "北米", "キャメロット", "バビロニア"],
        "フリクエ1.5部": ["新宿", "アガルタ", "下総国", "セイレム"],
        "フリクエ2部": [
            "アナスタシア",
            "ゲッテルデメルング",
            "シン",
            "ユガ・クシェートラ",
            "アトランティス",
            "オリュンポス",
            "平安京",
            "アヴァロン",
            "トラオム",
            "ナウイ・ミクトラン",
        ],
        "奏章": ["オーディール・コール", "ペーパームーン", "イド"],
    }

    training_quests = [
        "弓の修練場 極級",
        "弓の修練場 超級",
        "弓の修練場 上級",
        "弓の修練場 中級",
        "弓の修練場 初級",
        "槍の修練場 極級",
        "槍の修練場 超級",
        "槍の修練場 上級",
        "槍の修練場 中級",
        "槍の修練場 初級",
        "狂の修練場 極級",
        "狂の修練場 超級",
        "狂の修練場 上級",
        "狂の修練場 中級",
        "狂の修練場 初級",
        "騎の修練場 極級",
        "騎の修練場 超級",
        "騎の修練場 上級",
        "騎の修練場 中級",
        "騎の修練場 初級",
        "術の修練場 極級",
        "術の修練場 超級",
        "術の修練場 上級",
        "術の修練場 中級",
        "術の修練場 初級",
        "殺の修練場 極級",
        "殺の修練場 超級",
        "殺の修練場 上級",
        "殺の修練場 中級",
        "殺の修練場 初級",
        "剣の修練場 極級",
        "剣の修練場 超級",
        "剣の修練場 上級",
        "剣の修練場 中級",
        "剣の修練場 初級",
    ]

    def categorize(row):
        # カテゴリに基づいて各行を評価
        for category, names in war_names.items():
            if row["war_name"] in names:
                return category

        if row["quest_name"] in training_quests:
            return "修練場"

        return "その他クエスト"

    # 各行にカテゴリを適用
    df["category"] = df.apply(categorize, axis=1)

    return df


def modify_war_and_quest_columns(df: pd.DataFrame) -> pd.DataFrame:
    spot_list = [
        "ブラックヒルズ",
        "リバートン",
        "デンバー",
        "デミング",
        "ダラス",
        "アルカトラズ",
        "デモイン",
        "モントゴメリー",
        "ラボック",
        "アレクサンドリア",
        "カーニー",
        "シャーロット",
        "ワシントン",
        "シカゴ",
    ]

    # 'war_name' columnがspot_listに含まれている行を特定
    mask = df["war_name"].isin(spot_list)

    # 'quest_name' columnに元の'war_name'の値を代入
    df.loc[mask, "quest_name"] = df.loc[mask, "war_name"]

    # 'war_name' columnの値を'北米'に変更
    df.loc[mask, "war_name"] = "北米"

    return df
