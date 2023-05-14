import pandas as pd

from fgo_drop_analyzer.data_cleaning import check_nonexistent_items
from fgo_drop_analyzer.data_cleaning import create_item_normalizer
from fgo_drop_analyzer.data_cleaning import remove_drop_up
from fgo_drop_analyzer.data_cleaning import validate_drop_rates


def test_validate_drop_rates_gold_num():
    """金素材の周回数が100未満のときの
    数 > 周回数
    """
    reports_df = pd.DataFrame(
        {
            "category": ["フリクエ1部"] * 3,
            "object_name": ["心臓"] * 3,
            "num": [74, 75, 76],
            "runs": [75] * 3,
        }
    )

    # validate_drop_rates関数を呼び出し
    result = validate_drop_rates(reports_df)

    # 期待する出力データを作成
    expected_output = pd.DataFrame(
        {
            "category": ["フリクエ1部", "フリクエ1部", "Error"],
            "object_name": ["心臓", "心臓", "[E: 泥率]心臓"],
            "num": [74, 75, 76],
            "runs": [75] * 3,
        }
    )

    # 実際の出力と期待する出力が一致することを確認
    pd.testing.assert_frame_equal(result, expected_output)


def test_validate_drop_rates_gold_100run():
    """金素材の周回数100の境界"""
    # テスト用の入力データを作成
    reports_df = pd.DataFrame(
        {
            "category": ["フリクエ1部"] * 3,
            "object_name": ["心臓"] * 3,
            "num": [75] * 3,
            "runs": [99, 100, 101],
        }
    )

    # validate_drop_rates関数を呼び出し
    result = validate_drop_rates(reports_df)

    # 期待する出力データを作成
    expected_output = pd.DataFrame(
        {
            "category": ["フリクエ1部", "Error", "Error"],
            "object_name": ["心臓", "[E: 泥率]心臓", "[E: 泥率]心臓"],
            "num": [75] * 3,
            "runs": [99, 100, 101],
        }
    )

    # 実際の出力と期待する出力が一致することを確認
    pd.testing.assert_frame_equal(result, expected_output)


def test_validate_drop_rates_gold_drop():
    """金素材の周回数100以上のときのドロップ数の境界"""
    # テスト用の入力データを作成
    reports_df = pd.DataFrame(
        {
            "category": ["フリクエ1部"] * 3,
            "object_name": ["心臓"] * 3,
            "num": [499, 500, 501],
            "runs": [1000] * 3,
        }
    )

    # validate_drop_rates関数を呼び出し
    result = validate_drop_rates(reports_df)

    # 期待する出力データを作成
    expected_output = pd.DataFrame(
        {
            "category": ["フリクエ1部", "フリクエ1部", "Error"],
            "object_name": ["心臓", "心臓", "[E: 泥率]心臓"],
            "num": [499, 500, 501],
            "runs": [1000] * 3,
        }
    )

    # 実際の出力と期待する出力が一致することを確認
    pd.testing.assert_frame_equal(result, expected_output)


def test_validate_drop_rates_silver_num():
    """銀素材の周回数が100未満のときの
    数 > 周回数 * 2
    """
    reports_df = pd.DataFrame(
        {
            "category": ["フリクエ1部", "フリクエ1部", "フリクエ1部"],
            "object_name": ["種", "種", "種"],
            "num": [149, 150, 151],
            "runs": [75] * 3,
        }
    )

    # validate_drop_rates関数を呼び出し
    result = validate_drop_rates(reports_df)

    # 期待する出力データを作成
    expected_output = pd.DataFrame(
        {
            "category": ["フリクエ1部", "フリクエ1部", "Error"],
            "object_name": ["種", "種", "[E: 泥率]種"],
            "num": [149, 150, 151],
            "runs": [75] * 3,
        }
    )

    # 実際の出力と期待する出力が一致することを確認
    pd.testing.assert_frame_equal(result, expected_output)


def test_validate_drop_rates_silver_100run():
    """銀素材の周回数100の境界"""
    # テスト用の入力データを作成
    reports_df = pd.DataFrame(
        {
            "category": ["フリクエ1部"] * 3,
            "object_name": ["種"] * 3,
            "num": [75] * 3,
            "runs": [99, 100, 101],
        }
    )

    # validate_drop_rates関数を呼び出し
    result = validate_drop_rates(reports_df)

    # 期待する出力データを作成
    expected_output = pd.DataFrame(
        {
            "category": ["フリクエ1部", "Error", "Error"],
            "object_name": ["種", "[E: 泥率]種", "[E: 泥率]種"],
            "num": [75] * 3,
            "runs": [99, 100, 101],
        }
    )

    # 実際の出力と期待する出力が一致することを確認
    pd.testing.assert_frame_equal(result, expected_output)


def test_validate_drop_rates_silver_drop():
    """銀素材の周回数100以上のときのドロップ数の境界"""
    # テスト用の入力データを作成
    reports_df = pd.DataFrame(
        {
            "category": ["フリクエ1部"] * 3,
            "object_name": ["種"] * 3,
            "num": [699, 700, 701],
            "runs": [1000] * 3,
        }
    )

    # validate_drop_rates関数を呼び出し
    result = validate_drop_rates(reports_df)

    # 期待する出力データを作成
    expected_output = pd.DataFrame(
        {
            "category": ["フリクエ1部", "フリクエ1部", "Error"],
            "object_name": ["種", "種", "[E: 泥率]種"],
            "num": [699, 700, 701],
            "runs": [1000] * 3,
        }
    )

    # 実際の出力と期待する出力が一致することを確認
    pd.testing.assert_frame_equal(result, expected_output)


def test_validate_drop_rates_bronze_num():
    """銅素材の周回数が100未満のときの
    数 > 周回数 * 3
    """
    reports_df = pd.DataFrame(
        {
            "category": ["フリクエ1部"] * 3,
            "object_name": ["証"] * 3,
            "num": [29, 30, 31],
            "runs": [10] * 3,
        }
    )

    # validate_drop_rates関数を呼び出し
    result = validate_drop_rates(reports_df)

    # 期待する出力データを作成
    expected_output = pd.DataFrame(
        {
            "category": ["フリクエ1部", "フリクエ1部", "Error"],
            "object_name": ["証", "証", "[E: 泥率]証"],
            "num": [29, 30, 31],
            "runs": [10] * 3,
        }
    )

    # 実際の出力と期待する出力が一致することを確認
    pd.testing.assert_frame_equal(result, expected_output)


def test_validate_drop_rates_bronze_100run():
    """銅素材の周回数100の境界"""
    # テスト用の入力データを作成
    reports_df = pd.DataFrame(
        {
            "category": ["フリクエ1部"] * 3,
            "object_name": ["証"] * 3,
            "num": [95] * 3,
            "runs": [99, 100, 101],
        }
    )

    # validate_drop_rates関数を呼び出し
    result = validate_drop_rates(reports_df)

    # 期待する出力データを作成
    expected_output = pd.DataFrame(
        {
            "category": ["フリクエ1部", "Error", "Error"],
            "object_name": ["証", "[E: 泥率]証", "[E: 泥率]証"],
            "num": [95] * 3,
            "runs": [99, 100, 101],
        }
    )

    # 実際の出力と期待する出力が一致することを確認
    pd.testing.assert_frame_equal(result, expected_output)


def test_validate_drop_rates_bronze_drop():
    """銅素材の周回数100以上のときのドロップ数の境界"""
    # テスト用の入力データを作成
    reports_df = pd.DataFrame(
        {
            "category": ["フリクエ1部"] * 3,
            "object_name": ["証"] * 3,
            "num": [899, 900, 901],
            "runs": [1000] * 3,
        }
    )

    # validate_drop_rates関数を呼び出し
    result = validate_drop_rates(reports_df)

    # 期待する出力データを作成
    expected_output = pd.DataFrame(
        {
            "category": ["フリクエ1部", "フリクエ1部", "Error"],
            "object_name": ["証", "証", "[E: 泥率]証"],
            "num": [899, 900, 901],
            "runs": [1000] * 3,
        }
    )

    # 実際の出力と期待する出力が一致することを確認
    pd.testing.assert_frame_equal(result, expected_output)


def test_preprocess_category():
    """カテゴリーの違いによる処理のテスト"""
    reports_df = pd.DataFrame(
        {
            "category": ["その他イベント", "修練場", "フリクエ1部", "フリクエ1.5部", "フリクエ2部"],
            "object_name": ["心臓"] * 5,
            "num": [76] * 5,
            "runs": [150] * 5,
        }
    )

    # validate_drop_rates関数を呼び出し
    result = validate_drop_rates(reports_df)

    # 期待する出力データを作成
    expected_output = pd.DataFrame(
        {
            "category": ["その他イベント"] + ["Error"] * 4,
            "object_name": ["心臓"] + ["[E: 泥率]心臓"] * 4,
            "num": [76] * 5,
            "runs": [150] * 5,
        }
    )

    # 実際の出力と期待する出力が一致することを確認
    pd.testing.assert_frame_equal(result, expected_output)


def test_check_nonexistent_items():
    input_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 3, 4],
            "category": ["フリクエ1部", "フリクエ1部", "フリクエ1部", "フリクエ1部", "その他イベント"],
            "quest_name": ["クエスト1", "クエスト1", "クエスト2", "クエスト2", "クエスト3"],
            "object_name": ["証", "骨", "種", "存在しないアイテム", "存在しないアイテム"],
        }
    )

    freequest_df = pd.DataFrame(
        {
            "quest_name": ["クエスト1", "クエスト2"],
            "item1": ["証", "種"],
            "item2": ["骨", "ランタン"],
            "item3": ["爪", "心臓"],
        }
    )

    expected_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 3, 4],
            "category": ["フリクエ1部", "フリクエ1部", "Error", "Error", "その他イベント"],
            "quest_name": ["クエスト1", "クエスト1", "クエスト2", "クエスト2", "クエスト3"],
            "object_name": ["証", "骨", "種", "[E: 非存在]存在しないアイテム", "存在しないアイテム"],
        }
    )

    output_df = check_nonexistent_items(input_df, freequest_df)

    pd.testing.assert_frame_equal(output_df, expected_df)


def test_create_item_normalizer():
    # テスト対象の関数を呼び出す
    normalize_item_name = create_item_normalizer()

    # アイテム名が正しく正規化されることを確認
    assert normalize_item_name("英雄の証") == "証"
    assert normalize_item_name("針") == "毒針"
    assert normalize_item_name("宵哭の鉄杭") == "鉄杭"
    assert normalize_item_name("宵哭きの鉄杭") == "鉄杭"
    assert normalize_item_name("鳳凰の羽") == "羽根"
    assert normalize_item_name("鳳凰の羽根") == "羽根"
    assert normalize_item_name("奇奇神酒") == "神酒"
    assert normalize_item_name("奇々神酒") == "神酒"
    assert normalize_item_name("煌星のカケラ") == "カケラ"
    assert normalize_item_name("煌星の欠片") == "カケラ"
    assert normalize_item_name("煌星のかけら") == "カケラ"
    assert normalize_item_name("カケラ") == "カケラ"
    assert normalize_item_name("欠片") == "カケラ"
    assert normalize_item_name("かけら") == "カケラ"


def test_remove_drop_up():
    # テスト用のデータフレームを作成
    data = {
        "object_name": [
            "証",
            "骨",
            "牙",
            "塵",
            "鎖",
            "毒針",
            "髄液",
            "鉄杭",
            "火薬",
            "小鐘",
            "剣",
            "刃",
            "種",
            "ランタン",
            "八連",
            "蛇玉",
            "羽根",
            "歯車",
            "頁",
            "ホム",
            "蹄鉄",
            "勲章",
        ],
        "note": [
            "",
            "骨泥UP%",
            "牙泥UP %",  # 半角スペース
            "塵泥UP  %",  # 半角スペース*2
            "鎖泥UP　%",  # 全角スペース
            "毒針泥UP　　%",  # 全角スペース*2
            "髄液泥UP0%",
            "鉄杭泥UP 0%",
            "火薬泥UP0 %",
            "小鐘泥UP 0 %",
            "剣泥UP０%",  # 全角
            "刃泥UP ０%",  # 全角
            "種泥UP０ %",  # 全角
            "ランタン泥UP ０ %",  # 全角
            "八連泥UP5%",
            "蛇玉泥UP 5%",
            "羽根泥UP5 %",
            "歯車泥UP 5 %",
            "頁泥UP５%",  # 全角
            "ホム泥UP ５%",  # 全角
            "蹄鉄泥UP５ %",  # 全角
            "勲章泥UP ５ %",  # 全角
        ],
    }
    df = pd.DataFrame(data)

    # テスト対象の関数を呼び出す
    df = remove_drop_up(df)

    # 関数が期待どおりに行を削除していることを確認
    assert "証" in df["object_name"].values
    assert "骨" in df["object_name"].values
    assert "牙" in df["object_name"].values
    assert "塵" in df["object_name"].values
    assert "毒針" in df["object_name"].values
    assert "髄液" in df["object_name"].values
    assert "鉄杭" in df["object_name"].values
    assert "火薬" in df["object_name"].values
    assert "小鐘" in df["object_name"].values
    assert "刃" in df["object_name"].values
    assert "種" in df["object_name"].values
    assert "ランタン" in df["object_name"].values
    assert "八連" not in df["object_name"].values
    assert "蛇玉" not in df["object_name"].values
    assert "羽根" not in df["object_name"].values
    assert "歯車" not in df["object_name"].values
    assert "頁" not in df["object_name"].values
    assert "ホム" not in df["object_name"].values
    assert "蹄鉄" not in df["object_name"].values
    assert "勲章" not in df["object_name"].values
    assert df.shape[0] == 14
