import os
import time
import re
from datetime import datetime, date
import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, render_template, jsonify, request, Response

app = Flask(__name__)

# --- 定数定義 ---
PAST_RACE_DATA_CSV_PATH = os.path.join(os.path.dirname(__file__), 'data', '20250801_3Y.csv')
RACE_CARD_DIR = os.path.join(os.path.dirname(__file__), 'data', 'race_cards')
JRA_VENUES = ["札幌", "函館", "福島", "東京", "中山", "中京", "京都", "阪神", "小倉"]

# --- グローバル変数 ---
df_past_races = pd.DataFrame()

# --- ヘルパー関数 ---
def zen_to_han(text):
    """全角数字を半角に変換する"""
    return text.translate(str.maketrans('０１２３４５６７８９', '0123456789'))

# --- データロード・整形関数 ---
def load_past_race_data():
    """過去のレース結果CSVを読み込み、DataFrameを整形"""
    global df_past_races
    try:
        df = pd.read_csv(PAST_RACE_DATA_CSV_PATH, encoding='utf-8')
        
        df.columns = df.columns.str.strip() 

        if '馬名' in df.columns:
            df['馬名'] = df['馬名'].astype(str).str.strip()
        else:
            print("ERROR: CSVファイルに'馬名'カラムが見つかりません。")
            df_past_races = pd.DataFrame() 
            return 

        df['日付'] = pd.to_datetime(df['日付'], errors='coerce') 
        df['日付'] = df['日付'].dt.date 

        def parse_distance_and_track_type(dist_str):
            if pd.isna(dist_str) or not isinstance(dist_str, str): return None, None
            dist_str_half = zen_to_han(str(dist_str).strip()) 
            match = re.match(r'([芝ダ])(\d+)', dist_str_half)
            if match:
                track = '芝' if match.group(1) == '芝' else 'ダート'
                distance = int(match.group(2))
                return track, distance
            return None, None
        
        df[['トラック種別', '距離']] = df['距離'].apply(lambda x: pd.Series(parse_distance_and_track_type(x)))
        
        required_cols = ['日付', '馬名', '着順', '場所', 'トラック種別', '距離', '馬場状態']
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"ERROR: 以下の必須カラムがCSVファイルに見つかりません: {missing_cols}")
            df_past_races = pd.DataFrame()
            return

        df_past_races = df.dropna(subset=required_cols).reset_index(drop=True)

    except FileNotFoundError:
        print(f"ERROR: 過去のレースデータCSVファイルが見つかりません: {PAST_RACE_DATA_CSV_PATH}")
    except Exception as e:
        print(f"ERROR: データのロードまたは整形中に予期せぬエラーが発生しました: {e}")
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
            if not (info_elements and len(info_elements) >= 3 and race_name_element): continue
            date_str, raw_venue, start_time_raw = [el.text.strip() for el in info_elements[:3]]
            venue = next((v for v in JRA_VENUES if v in raw_venue), "不明")
            start_time = start_time_raw.replace("発走", "")
            race_name = race_name_element.get_text(strip=True)
            
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
            
            df['race_id'] = full_race_id; df['日付'] = date_str; df['開催地'] = venue
            df['発走時刻'] = start_time; df['レース名'] = race_name
            df['レース番号'] = f"{i}R"
            
            all_horses_df = pd.concat([all_horses_df, df], ignore_index=True)
        except Exception as e:
            print(f"  エラー: {i}R の取得中に予期せぬエラーが発生しました: {e}")
    return all_horses_df

def odds_scraper(full_race_id):
    odds_url = f"https://sports.yahoo.co.jp/keiba/race/odds/tfw/{full_race_id}"
    try:
        time.sleep(1)
        response = requests.get(odds_url)
        response.raise_for_status()

        dfs = pd.read_html(response.text, header=0) 
        
        target_odds_df = pd.DataFrame() 
        
        for i, df_candidate in enumerate(dfs):
            if '馬番' in df_candidate.columns and \
               ('単勝' in df_candidate.columns or '複勝' in df_candidate.columns):
                target_odds_df = df_candidate.copy()
                break 

        if target_odds_df.empty:
            return pd.DataFrame()

        target_odds_df['馬番'] = pd.to_numeric(target_odds_df['馬番'], errors='coerce')
        target_odds_df = target_odds_df.dropna(subset=['馬番']).reset_index(drop=True)

        if '単勝' in target_odds_df.columns:
            target_odds_df['オッズ'] = pd.to_numeric(target_odds_df['単勝'], errors='coerce')
            target_odds_df['人気'] = target_odds_df['オッズ'].rank(method='min', ascending=True).astype(int)
        else:
            target_odds_df['人気'] = None
            target_odds_df['オッズ'] = None

        result_df = target_odds_df[['馬番', '人気', 'オッズ']].copy()
        result_df.rename(columns={'馬番': '馬番_odds'}, inplace=True)
        
        return result_df

    except requests.exceptions.RequestException:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

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
            yield f"エラー: {target_date_str} に開催されるレースが見つかりませんでした。\n"; yield "--- 処理終了 ---\n"; return
        
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
                columns_to_save = ['枠番', '馬番', '馬名', '性別', '年齢', '斤量', '騎手', '調教師', 'race_id', '日付', '開催地', '発走時刻', 'レース名', 'レース番号']
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
    """指定された日付と開催地の全レース一覧を返すAPI"""
    races = []
    for i in range(1, 13):
        filename = f"{date_str}_{venue}_{i}.csv"
        filepath = os.path.join(RACE_CARD_DIR, filename)
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath, encoding='utf-8', nrows=1)
                if not df.empty and 'レース名' in df.columns:
                    races.append({
                        'Ｒ': i, 
                        'レース名': df['レース名'].iloc[0]
                    })
            except Exception as e:
                print(f"Error reading {filepath}: {e}")
                continue
    return jsonify(races)

@app.route('/api/race_card/<date_str>/<venue>/<int:race_num>')
def api_get_race_card(date_str, venue, race_num):
    
    filename = f"{date_str}_{venue}_{race_num}.csv"
    filepath = os.path.join(RACE_CARD_DIR, filename)
    try:
        df_race_card = pd.read_csv(filepath, encoding='utf-8')

        if 'race_id' in df_race_card.columns and not df_race_card.empty:
            full_race_id = df_race_card['race_id'].iloc[0]
            
            odds_df = odds_scraper(full_race_id)

            if not odds_df.empty:
                merge_df = pd.merge(df_race_card, odds_df[['馬番_odds', '人気', 'オッズ']],
                                    left_on='馬番', right_on='馬番_odds', how='left')
                merge_df.drop(columns=['馬番_odds'], inplace=True)
                return jsonify(merge_df.to_dict(orient='records'))
            else:
                return jsonify(df_race_card.to_dict(orient='records'))
        else:
            return jsonify(df_race_card.to_dict(orient='records'))

    except FileNotFoundError:
        return jsonify([])
    except Exception as e:
        print(f"APIエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/odds/<full_race_id>')
def api_get_odds(full_race_id):
    """リアルタイムオッズを取得するAPI"""
    odds_df = odds_scraper(full_race_id)
    if odds_df.empty: return jsonify([])
    return jsonify(odds_df.to_dict(orient='records'))

@app.route('/api/horse_past_data/<horse_name>')
def api_get_horse_past_data(horse_name):
    """
    指定された馬の過去走データと、指定された条件での好走率を返すAPI
    クエリパラメータ:
        track_type (str): '芝' or 'ダート'
        distance_min (int): 距離の最小値
        distance_max (int): 距離の最大値
        venue (str): 競馬場名
        track_condition (str): 馬場状態 ('良', '稍重', '重', '不良')
    """
    global df_past_races 
    
    # 渡されたhorse_nameもトリムする
    horse_name = horse_name.strip()

    if df_past_races.empty:
        return jsonify({"error": "過去データがロードされていません。"}), 500

    horse_filtered_df_all_past_races = df_past_races[df_past_races['馬名'] == horse_name].copy()

    if horse_filtered_df_all_past_races.empty:
        return jsonify({"past_races": [], "good_performance_rates": {}})

    past_races_data_for_json = horse_filtered_df_all_past_races.copy()
    past_races_data_for_json['日付'] = past_races_data_for_json['日付'].apply(
        lambda x: x.isoformat() if isinstance(x, (date, datetime)) and pd.notna(x) else None
    )
    past_races_data_for_json = past_races_data_for_json.replace({pd.NA: None, float('nan'): None})

    horse_filtered_df_for_rates = horse_filtered_df_all_past_races.copy()

    # --- クエリパラメータによるフィルタリング ---
    
    track_type = request.args.get('track_type')
    if track_type and track_type in ['芝', 'ダート'] and 'トラック種別' in horse_filtered_df_for_rates.columns:
        horse_filtered_df_for_rates = horse_filtered_df_for_rates[
            (horse_filtered_df_for_rates['トラック種別'].astype(str) == track_type) &
            (horse_filtered_df_for_rates['トラック種別'].notna()) 
        ]

    distance_min = request.args.get('distance_min') 
    distance_max = request.args.get('distance_max') 
    
    if (distance_min or distance_max) and '距離' in horse_filtered_df_for_rates.columns:
        temp_df = horse_filtered_df_for_rates.dropna(subset=['距離']).copy()
        temp_df['距離_numeric'] = pd.to_numeric(temp_df['距離'], errors='coerce')
        temp_df = temp_df.dropna(subset=['距離_numeric']) 

        if distance_min:
            try:
                min_val = int(distance_min)
                temp_df = temp_df[temp_df['距離_numeric'] >= min_val]
            except ValueError:
                pass
        if distance_max:
            try:
                max_val = int(distance_max)
                temp_df = temp_df[temp_df['距離_numeric'] <= max_val]
            except ValueError:
                pass
        
        horse_filtered_df_for_rates = temp_df.drop(columns=['距離_numeric'])


    venue = request.args.get('venue')
    if venue and '場所' in horse_filtered_df_for_rates.columns:
        horse_filtered_df_for_rates = horse_filtered_df_for_rates[
            (horse_filtered_df_for_rates['場所'].astype(str) == venue) &
            (horse_filtered_df_for_rates['場所'].notna()) 
        ]

    track_condition = request.args.get('track_condition')
    if track_condition and '馬場状態' in horse_filtered_df_for_rates.columns:
        if track_condition == '良':
            horse_filtered_df_for_rates = horse_filtered_df_for_rates[
                horse_filtered_df_for_rates['馬場状態'].astype(str).str.contains('良', na=False)
            ]
        elif track_condition == '稍重':
            horse_filtered_df_for_rates = horse_filtered_df_for_rates[
                horse_filtered_df_for_rates['馬場状態'].astype(str).str.contains('稍', na=False)
            ]
        elif track_condition == '重':
            horse_filtered_df_for_rates = horse_filtered_df_for_rates[
                horse_filtered_df_for_rates['馬場状態'].astype(str).str.contains('重', na=False)
            ]
        elif track_condition == '不良':
            horse_filtered_df_for_rates = horse_filtered_df_for_rates[
                horse_filtered_df_for_rates['馬場状態'].astype(str).str.contains('不', na=False)
            ]


    # --- フィルタリングここまで ---

    if horse_filtered_df_for_rates.empty:
        return jsonify({"past_races": past_races_data_for_json.to_dict(orient='records'), "good_performance_rates": {}})

    good_performance_rates_raw = {
        "distance_track": {},
        "racecourse": {},
        "track_condition": {},
        "distance": {},
        "track_type": {}
    }

    for _, row in horse_filtered_df_for_rates.iterrows():
        着順 = row.get('着順')
        if pd.isna(着順):
            continue
        try:
            着順 = int(着順)
        except (ValueError, TypeError):
            continue

        distance_val = row.get('距離')
        track_type_val = row.get('トラック種別')
        venue_val = row.get('場所')
        track_condition_val = row.get('馬場状態')

        conditions_to_process = []
        if track_type_val is not None and pd.notna(track_type_val) and distance_val is not None and pd.notna(distance_val):
            try:
                conditions_to_process.append(("distance_track", f"{track_type_val}{int(distance_val)}m"))
            except ValueError:
                pass
        
        if venue_val is not None and pd.notna(venue_val):
            conditions_to_process.append(("racecourse", venue_val))
            
        if track_condition_val is not None and pd.notna(track_condition_val):
            if '稍重' in str(track_condition_val):
                conditions_to_process.append(("track_condition", '稍重'))
            elif '不' in str(track_condition_val) or '不良' in str(track_condition_val):
                conditions_to_process.append(("track_condition", '不良'))
            elif '重' in str(track_condition_val):
                conditions_to_process.append(("track_condition", '重'))
            elif '良' in str(track_condition_val):
                conditions_to_process.append(("track_condition", '良'))
            else:
                 conditions_to_process.append(("track_condition", track_condition_val))
            
        if distance_val is not None and pd.notna(distance_val):
            try:
                conditions_to_process.append(("distance", f"{int(distance_val)}m"))
            except ValueError:
                pass
        
        if track_type_val is not None and pd.notna(track_type_val):
            conditions_to_process.append(("track_type", track_type_val))

        for category, key in conditions_to_process:
            if key not in good_performance_rates_raw[category]:
                good_performance_rates_raw[category][key] = {"total": 0, "wins": 0, "top3s": 0}
            
            good_performance_rates_raw[category][key]["total"] += 1
            if 着順 == 1:
                good_performance_rates_raw[category][key]["wins"] += 1
            if 着順 <= 3:
                good_performance_rates_raw[category][key]["top3s"] += 1

    final_good_performance_rates = {}
    for category, items in good_performance_rates_raw.items():
        sorted_items_for_category = []
        for key, stats in items.items():
            total = stats["total"]
            wins = stats["wins"]
            top3s = stats["top3s"]

            win_rate = (wins / total * 100) if total > 0 else 0
            top3_rate = (top3s / total * 100) if total > 0 else 0

            sorted_items_for_category.append({
                "condition": key,
                "total_races": total,
                "wins": wins,
                "top3s": top3s,
                "win_rate": round(win_rate, 2),
                "top3_rate": round(top3_rate, 2)
            })
        
        final_good_performance_rates[category] = sorted(
            sorted_items_for_category,
            key=lambda x: x['total_races'],
            reverse=True
        )
    
    return jsonify({"past_races": past_races_data_for_json.to_dict(orient='records'), "good_performance_rates": final_good_performance_rates})

if __name__ == '__main__':
    if not os.path.exists(RACE_CARD_DIR):
        os.makedirs(RACE_CARD_DIR)
    app.run(debug=True, use_reloader=False)

