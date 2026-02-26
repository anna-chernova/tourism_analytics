# Аналитика туризма в Нижнем Новгороде

## Описание проекта
Исследование о состоянии туризма в городе для принятия управленческих решений.
В рамках проекта настроена автоматическая обработка данных от третьей стороны, разработан REST API для передачи данных в ERP систему и подготовлена аналитика по 6 ключевым вопросам для принятия управленческих решений.

## Данные

### Источник данных
Данные предоставляются третьей стороной в формате CSV выгрузок. Для разработки использована тестовая выгрузка, структура которой соответствует реальным данным.

### О наборе данных
Набор данных содержит информацию о туристических поездках в Нижний Новгород. Каждая запись включает:

- **TERRITORY_CODE / TERRITORY_NAME** - код и название территории
- **DATE_OF_ARRIVAL** - дата прибытия
- **TRIP_TYPE / VISIT_TYPE** - тип поездки и визита
- **HOME_COUNTRY / HOME_REGION / HOME_CITY** - место прибытия туриста
- **GOAL** - цель поездки
- **GENDER / AGE / INCOME** - демографические характеристики
- **DAYS_CNT** - продолжительность пребывания
- **VISITORS_CNT** - количество туристов в группе
- **SPENT** - сумма трат (в млн рублей)

## Цели проекта

1. **Автоматизация ETL**: настроить автоматическую обработку и загрузку данных в PostgreSQL на Yandex.Cloud
2. **REST API**: разработать интерфейс для передачи данных во внутреннюю ERP систему
3. **Аналитика**: подготовить ответы на 6 ключевых вопросов Главы города:
   - Общее количество туристов за весь период
   - Помесячная динамика посещаемости
   - Территориальное распределение (откуда приехали)
   - Демографическое распределение
   - Наиболее выгодные категории туристов
   - Профиль среднестатистического туриста


## Установка и запуск

### Предварительные требования
- Python 3+
- PostgreSQL (Yandex.Cloud Managed Service)
- Аккаунт в Yandex.Cloud (бесплатный пробный период)

### Пошаговая инструкция

1. **Клонируйте репозиторий:**
```bash
git clone https://github.com/anna-chernova/tourism_analytics.git
```
2. **Создайте виртуальное окружение:**
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows
```
3. **Установите зависимости:**
```bash
pip install -r requirements.txt
```

4. **Настройте базу данных в Yandex.Cloud:**
- Зарегистрируйтесь в [Yandex.Cloud](https://yandex.cloud/ru?utm_referrer=about%3Ablank)
- Создайте кластер Managed Service for PostgreSQL
- Создайте базу данных и пользователя
- Скачайте SSL сертификат в ~/.postgresql/root.crt

5. **Настройте переменные окружения:**
- Создайте файл .env в корне проекта:
```bash
HOST=rc1a-xxxxxxxxxxxxx.mdb.yandexcloud.net
PORT=6432
DBNAME=your_database_name
USER=your_username
PASSWORD=your_password
SSLMODE=verify-full
```
6. **Подготовьте данные:**
- Поместите тестовую выгрузку final.csv в папку data/

7. **Загрузите данные в БД:**
- Создайте таблицу visits в базе данных:
```bash
CREATE TABLE IF NOT EXISTS visits (
    id SERIAL PRIMARY KEY,
    territory_code VARCHAR(20),
    territory_name VARCHAR(100),
    date_of_arrival DATE NOT NULL,
    trip_type VARCHAR(40),
    visit_type VARCHAR(50),
    home_country VARCHAR(100),
    home_region VARCHAR(100),
    home_city VARCHAR(100),
    goal VARCHAR(100),
    gender VARCHAR(20),
    age VARCHAR(30),
    income VARCHAR(50),
    days_cnt INTEGER,
    visitors_cnt INTEGER,
    spent NUMERIC(10, 3)
);
```
- Запустите: 
```bash
python load_data.py
```
8. **Запустите API:**
```bash
python analytics.py
```
9. **Запустите Jupyter:**
```bash
jupyter notebook
```
10. **Откройте ноутбук:**
- `tourism.ipynb`

11. **Если нужно экспортировать ответы:**
```bash
curl -X POST http://localhost:5000/api/export -H "Content-Type: application/json" -d '{}'
```
