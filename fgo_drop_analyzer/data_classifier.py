import pandas as pd


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
