import argparse
import configparser
import logging
import sys
from pathlib import Path
from typing import List
from typing import Tuple

import pandas as pd
from openpyxl import Workbook

from .create_report import append_rows_to_sheet
from .create_report import create_list
from .create_report import create_statics
from .data_classifier import classify_dataframe
from .data_classifier import modify_war_and_quest_columns
from .data_cleaning import check_nonexistent_items
from .data_cleaning import normalize_item
from .data_cleaning import validate_drop_rates
from .data_fetcher import fetch_reports

logger = logging.getLogger(__name__)

base_dir = Path(__file__).resolve().parents[1]
config = configparser.ConfigParser()
config_path = base_dir / "conf/config.ini"


def parse_arguments() -> argparse.Namespace:
    """オプションの設定

    Returns:
        argparse.Namespace: 設定されたオプション
    """
    parser = argparse.ArgumentParser(
        prog="fgo_drop_analyzer", description="FGO Drop Analyzer"
    )
    parser.add_argument("filename", help="出力Excelファイル名")
    parser.add_argument("-l", "--loglevel", choices=("debug", "info"), default="info")
    return parser.parse_args()


def setup_logging(args):
    """ロギングのセットアップ

    Args:
        args (_type_): オプション
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s <%(filename)s-L%(lineno)s> [%(levelname)s] %(message)s",
    )
    logger.setLevel(args.loglevel.upper())


def read_config() -> Tuple[int, List[str]]:
    """config.iniが存在する場合読み込む

    Returns:
        Tuple[int, List[str]]: unixtime と キャッシュされたidのリスト
    """
    if config_path.exists():
        config.read(config_path)
    return config.getint("DEFAULT", "last_unixtime", fallback=0), config.get(
        "DEFAULT", "last_ids", fallback=""
    ).split(",")


def prepare_workbook() -> Workbook:
    """Excelワークブックの初期設定

    Returns:
        Workbook: 初期化されたワークブック
    """
    wb = Workbook()
    del wb["Sheet"]
    return wb


def update_spot(row):
    if row["category"] in ["フリクエ1部", "フリクエ1.5部", "フリクエ2部", "カルデアゲート"]:
        return row["spot"]
    elif row["category"] == "奏章" and row["war_name"] in ["オーディール・コール", "ペーパームーン"]:
        return row["spot"]
    else:
        return row["quest_name"]


def prepare_dataframe() -> pd.DataFrame:
    """フリーククエストデータの準備

    Returns:
        pd.DataFrame: フリークエストデータ
    """
    freequest_df = pd.read_csv(
        base_dir / "data" / "freequest.csv", encoding="utf-8-sig"
    )
    freequest_df.loc[
        (freequest_df["war_name"] == "オケアノス")
        & (freequest_df["spot"] == "群島")
        & (freequest_df["quest_name"] == "隠された島"),
        "spot",
    ] = "隠された島"
    freequest_df.loc[
        (freequest_df["war_name"] == "下総国")
        & (freequest_df["spot"] == "裏山")
        & (freequest_df["quest_name"] == "戦戦恐恐"),
        "spot",
    ] = "戦戦恐恐"
    freequest_df.loc[
        (freequest_df["war_name"] == "オーディール・コール")
        & (freequest_df["spot"] == "北大西洋エリア")
        & (freequest_df["quest_name"] == "光糸導く迷宮"),
        "spot",
    ] = "光糸導く迷宮"
    freequest_df.loc[
        (freequest_df["war_name"] == "オーディール・コール")
        & (freequest_df["spot"] == "北大西洋エリア")
        & (freequest_df["quest_name"] == "久遠の微笑"),
        "spot",
    ] = "久遠の微笑"
    freequest_df.loc[
        (freequest_df["spot"] == "カルデアゲート"),
        "spot",
    ] = freequest_df["quest_name"]
    freequest_df["quest_name"] = freequest_df.apply(update_spot, axis=1)
    freequest_df = freequest_df.loc[:, ~freequest_df.columns.str.contains("^Unnamed")]
    return freequest_df


def main():
    args = parse_arguments()
    setup_logging(args)
    last_unixtime, last_ids = read_config()
    wb = prepare_workbook()
    freequest_df = prepare_dataframe()

    reports_df = fetch_reports(last_unixtime)
    # 最新の10件のレポートIDとマッチするものを除外
    if not reports_df.empty:
        reports_df = reports_df[~reports_df["id"].isin(last_ids)]
        if reports_df.empty:
            logger.info("新規データがありません")
            sys.exit()
        reports_df = modify_war_and_quest_columns(reports_df)
        reports_df = classify_dataframe(reports_df)
        # 処理する前に最新の unixtime を取得
        latest_unixtime = reports_df["timestamp"].max()

        reports_df = normalize_item(reports_df, freequest_df)

        ws = wb.create_sheet(title="全データ")
        append_rows_to_sheet(ws, reports_df)

        reports_df = validate_drop_rates(reports_df)
        reports_df = check_nonexistent_items(reports_df, freequest_df)
        create_list(wb, reports_df)
        create_statics(wb, reports_df, freequest_df)

        # Workbookを保存します
        if args.filename.endswith(".xlsx") is True:
            filename = args.filename
        else:
            filename = args.filename + ".xlsx"
        wb.save(filename)

        # 最新の取得ポイントと最新の10件のレポートIDを config.ini に保存
        config.set("DEFAULT", "last_unixtime", str(latest_unixtime))

        # 最新の10件のレポートIDを更新
        latest_ids = reports_df["id"].head(10).tolist()
        config.set("DEFAULT", "last_ids", ",".join(latest_ids))

        with config_path.open("w") as f:
            config.write(f)
    else:
        logger.info("新規データがありません")
