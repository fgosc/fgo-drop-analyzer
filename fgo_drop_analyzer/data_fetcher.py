import configparser
import json
from pathlib import Path

import pandas as pd
import requests

base_dir = Path(__file__).resolve().parents[1]
config_path = base_dir / "conf" / "config.ini"

config = configparser.ConfigParser()
config.read(config_path)

API_KEY = config.get("appsync", "api_key")
GRAPHQL_ENDPOINT = config.get("appsync", "graphql_endpoint")


def replace_quest_name(df):
    # 変換ルールを辞書として定義
    conversion_dict = {
        "繁華街": "かいもの帰り",
        "自宅": "ないしょの待ち合わせ",
        "学校": "しずかな放課後",
        "通学路": "いつもの通い道",
        "東京駅": "たびだちの駅",
        "西新宿": "であいの交差点",
        "お台場": "あいにくの空模様",
        "新宿御苑": "バードウォッチング"
    }

    # warNameが"イド"で、questNameが変換ルールに含まれる行のみを対象にする
    mask = (df['war_name'] == 'イド') & (df['quest_name'].isin(conversion_dict.keys()))
    df.loc[mask, 'quest_name'] = df.loc[mask, 'quest_name'].map(conversion_dict)
    return df


def fetch_reports(timestamp: int) -> pd.DataFrame:
    """GraphQLを使用してデータベースからデータを取得する

    Args:
        timestamp (int): この時刻(unixtime)より新しいデータを取得

    Raises:
        ValueError: データベースからのデータ取得失敗

    Returns:
        pd.DataFrame: 取得されたデータ
    """
    query = """
    query ListReportsSortedByTimestamp($type: String!, $nextToken: String, $timestamp: ModelIntKeyConditionInput) {
        listReportsSortedByTimestamp(type: $type, timestamp: $timestamp, nextToken: $nextToken) {
            items {
                id
                owner
                name
                twitterId
                twitterName
                twitterUsername
                type
                warName
                questType
                questName
                timestamp
                runs
                note
                dropObjects {
                    objectName
                    drops {
                        num
                        stack
                    }
                }
            }
            nextToken
        }
    }
    """
    headers = {"Content-Type": "application/json", "x-api-key": API_KEY}

    next_token = None
    reports = []

    while True:
        response = requests.post(
            GRAPHQL_ENDPOINT,
            json={
                "query": query,
                "variables": {
                    "type": "open",
                    "nextToken": next_token,
                    "timestamp": {"gt": timestamp},
                },
            },
            headers=headers,
        )

        if response.status_code != 200:
            raise ValueError(f"Failed to fetch data from AppSync: {response.text}")

        response_data = json.loads(response.text)
        report_items = response_data["data"]["listReportsSortedByTimestamp"]["items"]
        next_token = response_data["data"]["listReportsSortedByTimestamp"]["nextToken"]

        for item in report_items:
            for drop_obj in item["dropObjects"]:
                for drop in drop_obj["drops"]:
                    report = {
                        "id": item["id"],
                        "owner": item["owner"],
                        "name": item["name"],
                        "twitter_id": item["twitterId"],
                        "twitter_name": item["twitterName"],
                        "twitter_username": item["twitterUsername"],
                        "report_type": item["type"],
                        "war_name": item["warName"],
                        "quest_type": item["questType"],
                        "quest_name": item["questName"],
                        "timestamp": item["timestamp"],
                        "runs": item["runs"],
                        "note": item["note"],
                        "object_name": drop_obj["objectName"],
                        "num": drop["num"],
                        "stack": drop["stack"],
                    }
                    reports.append(report)

        if not next_token:
            break

    df_reports = pd.DataFrame(reports)
    df_reports = replace_quest_name(df_reports)

    return df_reports
