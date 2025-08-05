import os
import time
import re
from datetime import datetime, date
import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, render_template, jsonify, request, Response
import numpy as np

app = Flask(__name__)

# --- 定数定義 ---
PAST_RACE_DATA_CSV_PATH = os.path.join(os.path.dirname(__file__), 'data', '20250801_3Y.csv')
RACE_CARD_DIR = os.path.join(os.path.dirname(__file__), 'data', 'race_cards')
JRA_VENUES = ["札幌", "函館", "福島", "東京", "中山", "中京", "京都", "阪神", "小倉", "新潟"]

# --- グローバル変数 ---
df_past_races = pd.DataFrame()

# --- ヘルパー関数 ---
def zen_to_han(text):
    """全角数字を半角に変換する"""
    if isinstance(text, str):
        return text.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
    return text

def parse_scraped_distance_and_track_type(dist_str):
    """スクレイピングした距離文字列からトラック種別と距離を抽出する"""
    if pd.isna(dist_str) or not isinstance(dist_str, str): return None, None
    match = re.match(r'([芝ダ])', dist_str)
    track_type = match.group(1) if match else None
    dist_match = re.search(r'(\d+)m', dist_str)
    distance = int(dist_match.group(1)) if dist_match else None
    return track_type, distance


# --- データロード・整形関数 ---
def load_past_race_data():
    """過去のレース結果CSVを読み込み、DataFrameを整形"""
    global df_past_races
    try:
        df = pd.read_csv(PAST_RACE_DATA_CSV_PATH, encoding='utf-8')
        df.columns = df.columns.str.strip()

        if '馬名' not in df.columns:
            print("ERROR: CSVファイルに'馬名'カラムが見つかりません。")
            df_past_races = pd.DataFrame()
            return

        df['馬名'] = df['馬名'].astype(str).str.strip()
        cleaned_dates = df["日付"].str.strip().str.replace(r"\s*\.\s*", ".", regex=True)
        df["日付"] = pd.to_datetime(cleaned_dates, format="%Y.%m.%d", errors="coerce").dt.date
        
        required_cols = ['日付', '馬名', '着順', '場所', '距離', '馬場状態', '補正', '補9', 'クラス名']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"ERROR: 以下の必須カラムがCSVに見つかりません: {missing_cols}")
            df_past_races = pd.DataFrame()
            return
        
        # 過去データのクラス名を正規化
        df['着順_numeric'] = pd.to_numeric(df['着順'], errors='coerce')
        
        df_past_races = df.dropna(subset=['着順_numeric']).reset_index(drop=True)

    except FileNotFoundError:
        print(f"ERROR: 過去のレースデータCSVファイルが見つかりません: {PAST_RACE_DATA_CSV_PATH}")
    except Exception as e:
        print(f"ERROR: データのロードまたは整形中にエラー: {e}")
        import traceback
        traceback.print_exc()

with app.app_context():
    load_past_race_data()

# --- スクレイパー機能 ---
def races_scraper(year, month):
    schedule_url = f"https://sports.yahoo.co.jp/keiba/schedule/monthly?year={year}&month={month}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}
    race_days = []
    try:
        response = requests.get(schedule_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        date_cells = soup.find_all('td', class_='hr-tableSchedule__data hr-tableSchedule__data--date')
        for cell in date_cells:
            link = cell.find('a')
            if not link: continue
            date_text = zen_to_han(cell.contents[0].strip())
            day_match = re.search(r'(\d+)日', date_text)
            if not day_match: continue
            day = int(day_match.group(1))
            date_iso = f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}"
            raw_venue = link.text.strip()
            venue = next((v for v in JRA_VENUES if v in raw_venue), "不明")
            href = link.get('href', '')
            id_match = re.search(r'/(\d{8})$', href)
            if not id_match: continue
            base_id = id_match.group(1)
            race_days.append({"date": date_iso, "venue": venue, "base_id": base_id})
        if race_days:
            unique_race_days = [dict(t) for t in {tuple(d.items()) for d in race_days}]
            return sorted(unique_race_days, key=lambda x: x['date'])
        else:
            return []
    except requests.exceptions.RequestException as e:
        print(f"エラー: スケジュールページの取得に失敗しました: {e}")
        return []

def horses_scraper(base_race_id):
    all_horses_df = pd.DataFrame()
    for i in range(1, 13):
        full_race_id = f"{base_race_id}{str(i).zfill(2)}"
        race_url = f"https://sports.yahoo.co.jp/keiba/race/denma/{full_race_id}"
        try:
            time.sleep(1)
            response = requests.get(race_url)
            
            if response.status_code != 200: continue
            
            soup = BeautifulSoup(response.content, 'html.parser')
            info_elements = soup.find_all('div', class_='hr-predictRaceInfo__text')
            race_name_element = soup.find('h2', class_='hr-predictRaceInfo__title')
            info_elements2 = soup.find_all('span', class_='hr-predictRaceInfo__text')
            
            if not (info_elements and len(info_elements) >= 3 and race_name_element):
                continue
            
            class_name = "不明"
            race_name = race_name_element.get_text(strip=True)
            if len(info_elements2) >= 5:
                raw_class_info = info_elements2[4].get_text(strip=True)
                
                if 'オープン' in raw_class_info:
                    grade_match = re.search(r'(G[1-3]|G[I]{1,3}|L)', zen_to_han(race_name))
                    if grade_match:
                        grade_str = grade_match.group(1)
                        if grade_str == 'L':
                             class_name = 'OP(L)'
                        elif grade_str == 'GI':
                             class_name = 'Ｇ１'
                        elif grade_str == 'GII':
                             class_name = 'Ｇ２'
                        elif grade_str == 'GIII':
                             class_name = 'Ｇ３'
                        else:
                            class_name = grade_str
                    else:
                        class_name = "ｵｰﾌﾟﾝ"
                else:
                    class_match = re.match(r'(未勝利|新馬|[1-3]勝)', raw_class_info)
                    if class_match:
                        class_name = class_match.group(1)
                    else:
                        class_name = "不明"
            

            date_str, raw_venue, start_time_raw = [el.text.strip() for el in info_elements[:3]]
            venue = next((v for v in JRA_VENUES if v in raw_venue), "不明")
            start_time = start_time_raw.replace("発走", "")
            distance_str = info_elements2[0].text.strip()
            track_type, distance_num = parse_scraped_distance_and_track_type(distance_str)
            
            dfs = pd.read_html(str(soup))
            if not dfs: continue
            df = dfs[0]
            actual_columns = ['枠番', '馬番', '馬名性齢/毛色', '騎手名斤量', '調教師名(所属)', '父馬名母馬名(母父馬名)', '馬体重', '人気(オッズ)']
            if len(df.columns) != len(actual_columns): continue
            df.columns = actual_columns
            extract_horse_info = df['馬名性齢/毛色'].str.extract(r'(.+?)([牡牝]|せん)(\d+)/.*')
            df['馬名'] = extract_horse_info[0].str.strip()
            df['性別'] = extract_horse_info[1]
            df['年齢'] = pd.to_numeric(extract_horse_info[2], errors='coerce')
            extract_jockey_info = df['騎手名斤量'].str.extract(r'(.+?)(\d+\.\d)')
            df['騎手'] = extract_jockey_info[0]
            df['斤量'] = pd.to_numeric(extract_jockey_info[1], errors='coerce')
            df.rename(columns={'調教師名(所属)': '調教師'}, inplace=True)
            df = df.drop(columns=['馬名性齢/毛色', '騎手名斤量', '人気(オッズ)'])
            
            df['race_id'] = full_race_id; df['日付'] = date_str; df['場所'] = venue
            df['発走時刻'] = start_time; df['レース名'] = race_name
            df['レース番号'] = f"{i}R"
            df['距離'] = f"{track_type}{distance_num}" if track_type and distance_num else None
            df['クラス名'] = class_name
            
            all_horses_df = pd.concat([all_horses_df, df], ignore_index=True)
        except Exception as e:
            print(f"  エラー: {i}R の取得中に予期せぬエラーが発生しました: {e}")
    return all_horses_df

# === odds_scraper 関数 ===
def odds_scraper(full_race_id):
    odds_url = f"https://sports.yahoo.co.jp/keiba/race/odds/tfw/{full_race_id}"
    try:
        time.sleep(1)
        response = requests.get(odds_url)
        response.raise_for_status()
        dfs = pd.read_html(response.text, header=0)
        target_odds_df = pd.DataFrame()
        for i, df_candidate in enumerate(dfs):
            if '馬番' in df_candidate.columns and ('単勝' in df_candidate.columns or '複勝' in df_candidate.columns):
                target_odds_df = df_candidate.copy()
                break
        if target_odds_df.empty: return pd.DataFrame()
        
        if '単勝' in target_odds_df.columns:
            target_odds_df['単勝'] = target_odds_df['単勝'].replace('****', np.nan)
        
        target_odds_df['馬番'] = pd.to_numeric(target_odds_df['馬番'], errors='coerce')
        target_odds_df = target_odds_df.dropna(subset=['馬番']).reset_index(drop=True)
        
        if '単勝' in target_odds_df.columns:
            target_odds_df['オッズ'] = pd.to_numeric(target_odds_df['単勝'], errors='coerce')
            target_odds_df['人気'] = target_odds_df['オッズ'].rank(method='min', ascending=True).astype('Int64')
        else:
            target_odds_df['人気'] = None
            target_odds_df['オッズ'] = None
            
        result_df = target_odds_df[['馬番', '人気', 'オッズ']].copy()
        result_df.rename(columns={'馬番': '馬番_odds'}, inplace=True)
        return result_df
    except: return pd.DataFrame()


@app.route('/scraper')
def scraper_page():
    return render_template('scraper.html')

@app.route('/run-scraper', methods=['POST'])
def run_scraper():
    year, month, day = int(request.form.get('year')), int(request.form.get('month')), int(request.form.get('day'))
    def generate_logs():
        target_date_str = f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}"
        yield f"--- 処理開始: {target_date_str} ---\n"
        os.makedirs(RACE_CARD_DIR, exist_ok=True)
        race_days_list = races_scraper(year, month)
        if isinstance(race_days_list, str):
            yield race_days_list
            yield "--- 処理終了 ---\n"
            return
        target_venues_info = [d for d in race_days_list if d.get('date') == target_date_str]
        if not target_venues_info:
            yield f"エラー: {target_date_str} に開催されるレースが見つかりませんでした。\n"; yield "--- 終了 ---\n"; return
        for day_info in target_venues_info:
            venue_name, base_id = day_info['venue'], day_info['base_id']
            yield f"\n--- {venue_name}競馬場の処理を開始します ---\n"
            daily_horses_df = horses_scraper(base_id)
            if daily_horses_df is None or daily_horses_df.empty:
                yield f"{venue_name}競馬場の出馬表データ取得に失敗しました。\n"; continue
            
            for race_num_str in sorted(daily_horses_df['レース番号'].unique()):
                race_num_int = int(re.search(r'\d+', race_num_str).group())
                single_race_df = daily_horses_df[daily_horses_df['レース番号'] == race_num_str]
                filename = f"{target_date_str}_{venue_name}_{race_num_int}.csv"
                filepath = os.path.join(RACE_CARD_DIR, filename)
                columns_to_save = ['枠番', '馬番', '馬名', '性別', '年齢', '斤量', '騎手', '調教師', 'race_id', '日付', '場所', '発走時刻', 'レース名', 'レース番号','距離', 'クラス名']
                final_df = single_race_df[[col for col in columns_to_save if col in single_race_df.columns]]
                
                final_df.to_csv(filepath, index=False, encoding='utf-8-sig')
                yield f"=> {filepath} に保存しました。\n"
        yield "\n--- 全ての処理が完了しました ---"
    return Response(generate_logs(), mimetype='text/plain')

# --- メインアプリの機能 ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/races/<date_str>/<venue>')
def api_get_daily_races(date_str, venue):
    races = []
    for i in range(1, 13):
        filepath = os.path.join(RACE_CARD_DIR, f"{date_str}_{venue}_{i}.csv")
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath, encoding='utf-8', nrows=1)
                
                if not df.empty and 'レース名' in df.columns and '距離' in df.columns and 'クラス名' in df.columns:
                    race_data = {
                        'Ｒ': i,
                        'レース名': df['レース名'].iloc[0],
                        '距離': df['距離'].iloc[0],
                        'クラス名': df['クラス名'].iloc[0]
                    }

                    for key, value in race_data.items():
                        if pd.isna(value):
                            race_data[key] = None
                    
                    races.append(race_data)
            except Exception as e:
                print(f"Error reading {filepath}: {e}")
                continue 
    
    return jsonify(races)

@app.route('/api/race_card/<date_str>/<venue>/<int:race_num>')
def api_get_race_card(date_str, venue, race_num):
    filepath = os.path.join(RACE_CARD_DIR, f"{date_str}_{venue}_{race_num}.csv")
    try:
        df_race_card = pd.read_csv(filepath, encoding='utf-8')
        if 'race_id' in df_race_card.columns and not df_race_card.empty:
            full_race_id = df_race_card['race_id'].iloc[0]
            odds_df = odds_scraper(full_race_id)
            if not odds_df.empty:
                merge_df = pd.merge(df_race_card, odds_df[['馬番_odds', '人気', 'オッズ']], left_on='馬番', right_on='馬番_odds', how='left')
                merge_df.drop(columns=['馬番_odds'], inplace=True)
                
                merge_df = merge_df.replace({np.nan: None, pd.NA: None})
                
                return jsonify(merge_df.to_dict(orient='records'))
        
        df_race_card = df_race_card.replace({np.nan: None, pd.NA: None})
        return jsonify(df_race_card.to_dict(orient='records'))
    except FileNotFoundError: return jsonify([])
    except Exception as e:
        print(f"APIエラー: {e}"); return jsonify({"error": str(e)}), 500

@app.route('/api/benchmark_times/<venue>/<track_and_distance>/<race_class>')
def api_get_benchmark_times(venue, track_and_distance, race_class):
    print(venue)
    print(track_and_distance)
    print(race_class)
    global df_past_races
    if df_past_races.empty:
        return jsonify({"error": "過去データ未ロード"}), 500

    filtered_df = df_past_races[
        (df_past_races['場所'] == venue) &
        (df_past_races['距離'] == track_and_distance) &
        (df_past_races['クラス名'] == race_class)
    ].copy()
    print("フィルターしたデータベースは")
    print(filtered_df)
    good_performance_df = filtered_df[filtered_df['着順_numeric'] <= 3].copy()
    print("パフォーマンスは")
    print(good_performance_df)
    
    if not good_performance_df.empty:
        avg_corrected_time = good_performance_df['補正'].mean()
        avg_corrected9m_time = good_performance_df['補9'].mean()
        
        benchmark_data = {
            "avg_corrected_time": avg_corrected_time,
            "avg_corrected9m_time": avg_corrected9m_time
        }
        print("benchmark_dataが作られました")
        return jsonify(benchmark_data)
    else:
        print("benchmark_data失敗")
        return jsonify({})


@app.route('/api/horse_past_data/<horse_name>')
def api_get_horse_past_data(horse_name):
    global df_past_races
    horse_name = horse_name.strip()
    if df_past_races.empty: return jsonify({"error": "過去データ未ロード"}), 500
    horse_df_all = df_past_races[df_past_races['馬名'] == horse_name].copy()
    if horse_df_all.empty: return jsonify({"past_races": []})

    past_races_json = horse_df_all.copy()
    past_races_json['日付'] = past_races_json['日付'].apply(lambda x: x.isoformat() if pd.notna(x) else None)
    past_races_json = past_races_json.replace({pd.NA: None, np.nan: None})
    
    return jsonify({"past_races": past_races_json.to_dict(orient='records')})

if __name__ == '__main__':
    if not os.path.exists(RACE_CARD_DIR): os.makedirs(RACE_CARD_DIR)
    app.run(debug=True, use_reloader=False)