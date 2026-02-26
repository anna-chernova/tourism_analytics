import os
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import numpy as np
import psycopg2
import json

load_dotenv()

app = Flask(__name__)
CORS(app)

def convert_for_json(value):
    """Преобразование значений для json"""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (np.int64, np.int32, np.integer)):
        return int(value)
    if isinstance(value, (np.float64, np.float32, np.floating)):
        return float(value)
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    return value

def connection_db():
    """Подключение к базе данных"""
    
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    dbname = os.getenv('DBNAME')
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    sslmode = os.getenv('SSLMODE')
    
    required_vars = {
        'HOST': host,
        'PORT': port,
        'DBNAME': dbname,
        'USER': user,
        'PASSWORD': password
    }
    missing = [name for name, value in required_vars.items() if not value]
    
    if missing:
        raise Exception(f"Отсутствуют переменные окружения: {', '.join(missing)}")
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode if sslmode else 'disable',
        sslrootcert=os.path.expanduser('~/.postgresql/root.crt') if sslmode == 'verify-full' else None
    )
    
    engine = create_engine('postgresql+psycopg2://', creator=lambda: conn)
    return engine

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'name': 'tourism analytics',
        'status': 'running',
        'endpoints': [
            '/api/question/1', '/api/question/2', '/api/question/3',
            '/api/question/4', '/api/question/5', '/api/question/6',
            '/api/health', '/api/export'
        ]
    })

@app.route('/api/health', methods=['GET'])
def health():
    try:
        engine = connection_db()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/question/1', methods=['GET'])
def question_1():
    try:
        engine = connection_db()
        
        query = """
        SELECT SUM(visitors_cnt) as total_visitors
        FROM visits
        WHERE territory_name LIKE '%Нижний Новгород%'
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            row = result.fetchone()
            
        total = convert_for_json(row[0]) if row and row[0] else 0
        if total is None:
            total = 0
            
        return jsonify({
            'question': 'Сколько туристов посетило Нижний Новгород за весь диапазон дат?',
            'answer': {
                'total_visitors': total,
                'total_visitors_formatted': f"{total:,} человек"
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/question/2', methods=['GET'])
def question_2():
    try:
        start = request.args.get('start_date')
        end = request.args.get('end_date')
        
        engine = connection_db()
        
        sql = """
        SELECT 
            TO_CHAR(date_of_arrival, 'YYYY-MM') as month,
            SUM(visitors_cnt) as visitors,
            COUNT(*) as trips,
            SUM(spent) as spent
        FROM visits
        WHERE territory_name LIKE '%Нижний Новгород%'
        """
        
        params = {}
        if start and end:
            sql += " AND date_of_arrival BETWEEN :start AND :end"
            params['start'] = start
            params['end'] = end
            period = f"с {start} по {end}"
        else:
            period = "за весь период"
        
        sql += " GROUP BY month ORDER BY month"
        
        with engine.connect() as conn:
            res = conn.execute(text(sql), params)
            rows = res.fetchall()
        
        months = []
        for row in rows:
            spent = (convert_for_json(row[3]) or 0) * 1_000_000
            months.append({
                'month': row[0],
                'visitors': convert_for_json(row[1]) or 0,
                'trips': convert_for_json(row[2]) or 0,
                'spent': round(spent, 0)
            })
        
        total = None
        if start and end:
            sql_total = """
            SELECT 
                SUM(visitors_cnt) as visitors,
                SUM(spent) as spent
            FROM visits
            WHERE territory_name LIKE '%Нижний Новгород%'
                AND date_of_arrival BETWEEN :start AND :end
            """
            
            with engine.connect() as conn:
                res = conn.execute(text(sql_total), params)
                row = res.fetchone()
            
            spent = (convert_for_json(row[1]) or 0) * 1_000_000
            total = {
                'visitors': convert_for_json(row[0]) or 0,
                'spent': round(spent, 0)
            }
        
        return jsonify({
            'question': 'Сколько туристов посещало Нижний Новгород каждый месяц?',
            'period': period,
            'answer': {
                'months': months,
                'total': total,
                'count': len(months)
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/question/3', methods=['GET'])
def question_3():
    try:
        engine = connection_db()
        
        query = """
        SELECT 
            home_country,
            home_region,
            home_city,
            SUM(visitors_cnt) AS total_visitors,
            COUNT(*) AS trips_count,
            SUM(spent) AS total_spent
        FROM 
            visits
        WHERE 
            territory_name LIKE '%Нижний Новгород%'
            AND home_country != 'неизвестно'
        GROUP BY 
            home_country,
            home_region,
            home_city
        ORDER BY 
            total_visitors DESC
        """
        
        with engine.connect() as conn:
            res = conn.execute(text(query))
            rows = res.fetchall()
        
        total_query = """
        SELECT SUM(visitors_cnt) 
        FROM visits 
        WHERE territory_name LIKE '%Нижний Новгород%'
        """
        with engine.connect() as conn:
            total_res = conn.execute(text(total_query))
            total_all = convert_for_json(total_res.fetchone()[0]) or 1
        
        countries = {}
        regions = []
        
        for row in rows:
            country = convert_for_json(row[0])
            region = convert_for_json(row[1])
            city = convert_for_json(row[2])
            visitors = convert_for_json(row[3]) or 0
            trips = convert_for_json(row[4]) or 0
            spent = (convert_for_json(row[5]) or 0) * 1_000_000
            
            if country:
                countries[country] = countries.get(country, 0) + visitors
            
            percent = (visitors / total_all * 100) if total_all else 0
            
            regions.append({
                'country': country,
                'region': region,
                'city': city,
                'visitors': visitors,
                'trips': trips,
                'spent': round(spent, 2),
                'percent': round(percent, 2)
            })
        
        summary = "Территориальное распределение: "
        if regions:
            main = regions[0]
            summary += f"больше всего из {main['region']} ({main['percent']}%)"
        else:
            summary += "нет данных"
        
        return jsonify({
            'question': 'Как представлено территориальное распределение туристов?',
            'answer': {
                'total': len(regions),
                'countries': countries,
                'regions': regions,
                'summary': summary
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/question/4', methods=['GET'])
def question_4():
    try:
        engine = connection_db()
        
        age_q = """
        SELECT 
            age,
            SUM(visitors_cnt) AS visitors,
            COUNT(*) AS trips,
            SUM(spent) AS spent
        FROM 
            visits
        WHERE 
            territory_name LIKE '%Нижний Новгород%'
            AND age != 'неизвестно'
        GROUP BY 
            age
        ORDER BY 
            CASE 
                WHEN age LIKE 'до%' THEN 1
                WHEN age LIKE 'от 18%' THEN 2
                WHEN age LIKE 'от 25%' THEN 3
                WHEN age LIKE 'от 35%' THEN 4
                WHEN age LIKE 'от 45%' THEN 5
                WHEN age LIKE 'от 55%' THEN 6
                WHEN age LIKE 'старше%' THEN 7
                ELSE 8
            END
        """
        
        gender_q = """
        SELECT 
            gender,
            SUM(visitors_cnt) AS visitors,
            COUNT(*) AS trips,
            SUM(spent) AS spent
        FROM 
            visits
        WHERE 
            territory_name LIKE '%Нижний Новгород%'
            AND gender != 'неизвестно'
        GROUP BY 
            gender
        """
        
        with engine.connect() as conn:
            age_res = conn.execute(text(age_q))
            age_rows = age_res.fetchall()
            
            gender_res = conn.execute(text(gender_q))
            gender_rows = gender_res.fetchall()
        
        total = 0
        for row in age_rows:
            total += convert_for_json(row[1]) or 0
        
        ages = []
        for row in age_rows:
            visitors = convert_for_json(row[1]) or 0
            percent = (visitors / total * 100) if total > 0 else 0
            spent = (convert_for_json(row[3]) or 0) * 1_000_000
            
            ages.append({
                'group': convert_for_json(row[0]),
                'visitors': visitors,
                'trips': convert_for_json(row[2]) or 0,
                'spent': round(spent, 2),
                'percent': round(percent, 2)
            })
        
        genders = []
        for row in gender_rows:
            visitors = convert_for_json(row[1]) or 0
            spent = (convert_for_json(row[3]) or 0) * 1_000_000
            genders.append({
                'gender': convert_for_json(row[0]),
                'visitors': visitors,
                'trips': convert_for_json(row[2]) or 0,
                'spent': round(spent, 2)
            })
        
        summary = "Преобладают туристы "
        
        if ages:
            summary += f"{ages[0]['group']}"
        if genders:
            summary += f", пол: {genders[0]['gender']}"
        
        return jsonify({
            'question': 'Как представлено демографическое распределение туристов?',
            'answer': {
                'ages': ages,
                'genders': genders,
                'summary': summary
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/question/5', methods=['GET'])
def question_5():
    try:
        engine = connection_db()
        
        ai_q = """
        SELECT 
            age,
            income,
            COUNT(*) AS trips,
            SUM(visitors_cnt) AS visitors,
            SUM(spent) AS spent,
            AVG(days_cnt) AS days,
            AVG(spent / NULLIF(visitors_cnt, 0)) AS spent_person
        FROM 
            visits
        WHERE 
            territory_name LIKE '%Нижний Новгород%'
            AND age != 'неизвестно'
            AND income != 'неизвестно'
        GROUP BY 
            age, income
        ORDER BY 
            spent_person DESC NULLS LAST
        LIMIT 10
        """
        
        goal_q = """
        SELECT 
            goal,
            COUNT(*) AS trips,
            SUM(visitors_cnt) AS visitors,
            SUM(spent) AS spent,
            AVG(days_cnt) AS days,
            AVG(spent / NULLIF(visitors_cnt, 0)) AS spent_person
        FROM 
            visits
        WHERE 
            territory_name LIKE '%Нижний Новгород%'
            AND goal != 'неизвестно'
        GROUP BY 
            goal
        ORDER BY 
            spent_person DESC NULLS LAST
        """
        
        with engine.connect() as conn:
            ai_res = conn.execute(text(ai_q))
            ai_rows = ai_res.fetchall()
            
            goal_res = conn.execute(text(goal_q))
            goal_rows = goal_res.fetchall()
        
        ai_list = []
        for row in ai_rows:
            spent = (convert_for_json(row[4]) or 0) * 1_000_000
            spent_person = (convert_for_json(row[6]) or 0) * 1_000_000
            
            ai_list.append({
                'age': convert_for_json(row[0]),
                'income': convert_for_json(row[1]),
                'trips': convert_for_json(row[2]) or 0,
                'visitors': convert_for_json(row[3]) or 0,
                'spent_rub': round(spent, 0),
                'days': round(convert_for_json(row[5]) or 0, 1),
                'spent_person': round(spent_person, 0)
            })
        
        goals = []
        for row in goal_rows:
            spent = (convert_for_json(row[3]) or 0) * 1_000_000
            spent_person = (convert_for_json(row[5]) or 0) * 1_000_000
            
            goals.append({
                'goal': convert_for_json(row[0]),
                'trips': convert_for_json(row[1]) or 0,
                'visitors': convert_for_json(row[2]) or 0,
                'spent': round(spent, 0),
                'days': round(convert_for_json(row[4]) or 0, 1),
                'spent_person': round(spent_person, 0)
            })
        
        best = ai_list[0] if ai_list else None
        
        recs = []
        if best:
            recs.append(f"Ориентироваться на {best['age']} с доходом {best['income']}")
            recs.append(f"Тратят на человека: {best['spent_person']:,.0f} руб.")
            if goals:
                recs.append(f"Цель: {goals[0]['goal']}")
        
        return jsonify({
            'question': 'Под какую категорию туристов выгоднее всего планировать мероприятия?',
            'answer': {
                'age_income': ai_list,
                'goals': goals,
                'best': best,
                'recs': recs
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/question/6', methods=['GET'])
def question_6():
    try:
        engine = connection_db()
        
        avg_q = """
        SELECT 
            AVG(days_cnt) AS avg_days,
            AVG(visitors_cnt) AS avg_group,
            AVG(spent) AS avg_trip,
            AVG(spent / NULLIF(visitors_cnt, 0)) AS avg_person,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_cnt) AS med_days,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY spent / NULLIF(visitors_cnt, 0)) AS med_person
        FROM visits
        WHERE territory_name LIKE '%Нижний Новгород%'
        """
        
        mode_age = """
        SELECT age FROM visits 
        WHERE territory_name LIKE '%Нижний Новгород%' AND age != 'неизвестно'
        GROUP BY age ORDER BY COUNT(*) DESC LIMIT 1
        """
        
        mode_gender = """
        SELECT gender FROM visits 
        WHERE territory_name LIKE '%Нижний Новгород%' AND gender != 'неизвестно'
        GROUP BY gender ORDER BY COUNT(*) DESC LIMIT 1
        """
        
        mode_income = """
        SELECT income FROM visits 
        WHERE territory_name LIKE '%Нижний Новгород%' AND income != 'неизвестно'
        GROUP BY income ORDER BY COUNT(*) DESC LIMIT 1
        """
        
        mode_goal = """
        SELECT goal FROM visits 
        WHERE territory_name LIKE '%Нижний Новгород%' AND goal != 'неизвестно'
        GROUP BY goal ORDER BY COUNT(*) DESC LIMIT 1
        """
        
        mode_trip = """
        SELECT trip_type FROM visits 
        WHERE territory_name LIKE '%Нижний Новгород%' AND trip_type != 'неизвестно'
        GROUP BY trip_type ORDER BY COUNT(*) DESC LIMIT 1
        """
        
        mode_region = """
        SELECT home_region FROM visits 
        WHERE territory_name LIKE '%Нижний Новгород%' AND home_region != 'неизвестно'
        GROUP BY home_region ORDER BY COUNT(*) DESC LIMIT 1
        """
        
        mode_city = """
        SELECT home_city FROM visits 
        WHERE territory_name LIKE '%Нижний Новгород%' AND home_city != 'неизвестно'
        GROUP BY home_city ORDER BY COUNT(*) DESC LIMIT 1
        """
        
        with engine.connect() as conn:
            avg_res = conn.execute(text(avg_q))
            avg = avg_res.fetchone()
            
            age_res = conn.execute(text(mode_age))
            age = age_res.fetchone()
            
            gender_res = conn.execute(text(mode_gender))
            gender = gender_res.fetchone()
            
            income_res = conn.execute(text(mode_income))
            income = income_res.fetchone()
            
            goal_res = conn.execute(text(mode_goal))
            goal = goal_res.fetchone()
            
            trip_res = conn.execute(text(mode_trip))
            trip = trip_res.fetchone()
            
            region_res = conn.execute(text(mode_region))
            region = region_res.fetchone()
            
            city_res = conn.execute(text(mode_city))
            city = city_res.fetchone()
        
        def get(row, default=None):
            if row is not None and len(row) > 0 and row[0] is not None:
                return convert_for_json(row[0])
            return default
        
        age = get(age, 'не указан')
        gender = get(gender, 'человек')
        income = get(income, 'не указан')
        goal = get(goal, 'разные цели')
        trip_type = get(trip, 'разные')
        region = get(region, 'разных регионов')
        city = get(city, 'не указан')
        
        trip = (convert_for_json(avg[2]) or 0) * 1_000_000
        person = (convert_for_json(avg[3]) or 0) * 1_000_000
        med_person = (convert_for_json(avg[5]) or 0) * 1_000_000 if len(avg) > 5 else 0
        
        desc = f"Профиль туриста: {gender} в возрасте {age}, "
        desc += f"с доходом {income}. Приезжает из {region} "
        desc += f"(город {city}). Цель - {goal}, "
        desc += f"тип поездки - {trip_type}. Останавливается на {round(convert_for_json(avg[0]) or 0, 1)} дней, "
        desc += f"группа {round(convert_for_json(avg[1]) or 0, 1)} чел. Тратит за поездку {round(trip, 0):,.0f} руб "
        desc += f"({round(person, 0):,.0f} руб./чел)."
        
        return jsonify({
            'question': 'Как выглядит профиль среднестатистического туриста?',
            'answer': {
                'numbers': {
                    'days': round(convert_for_json(avg[0]) or 0, 1),
                    'group': round(convert_for_json(avg[1]) or 0, 1),
                    'trip': round(trip, 0),
                    'person': round(person, 0),
                    'med_days': convert_for_json(avg[4]) if len(avg) > 4 else None,
                    'med_person': round(med_person, 0) if len(avg) > 5 else None
                },
                'categories': {
                    'age': age,
                    'gender': gender,
                    'income': income,
                    'goal': goal,
                    'trip': trip_type,
                    'region': region,
                    'city': city
                },
                'text': desc
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export', methods=['POST'])
def export_data_simple():
    """Экспорт данных в JSON файл"""
    try:
        data = request.get_json()
        
        if not data or 'questions' not in data:
            question_ids = [1, 2, 3, 4, 5, 6]
        else:
            question_ids = data['questions']
        
        if not os.path.exists('exports'):
            os.makedirs('exports')
        
        saved_files = []
        
        with app.test_client() as client:
            for q_id in question_ids:
                response = client.get(f'/api/question/{q_id}')
                
                if response.status_code == 200:
                    filename = f'exports/q{q_id}.json'
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(response.get_json(), f, ensure_ascii=False, indent=2)
                    
                    saved_files.append(filename)
        
        return jsonify({
            'success': True,
            'files': saved_files,
            'folder': 'exports/'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("Запуск API:")
    print("   - http://localhost:5000/")
    print("   - http://localhost:5000/api/health")
    print("   - http://localhost:5000/api/question/1")
    print("   - http://localhost:5000/api/question/2")
    print("   - http://localhost:5000/api/question/2?start_date=2021-01-01&end_date=2021-05-01")
    print("   - http://localhost:5000/api/question/3")
    print("   - http://localhost:5000/api/question/4")
    print("   - http://localhost:5000/api/question/5")
    print("   - http://localhost:5000/api/question/6")
    app.run(debug=True, host='0.0.0.0', port=5000)