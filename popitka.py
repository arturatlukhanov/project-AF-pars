import requests
import os
import time
import pandas as pd
from datetime import datetime, timedelta
import json
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# Получаем текущую дату и время
current_datetime = datetime.now()
current_datetime = current_datetime - timedelta(days=1)
current_datetime_minus_1_day = (current_datetime).strftime('%Y-%m-%d')

current_datetime_m = current_datetime.replace(day=1).strftime('%Y-%m-%d')

# Чтение токенов из файла
with open("/AF/Oth/kereas/tokens.json", "r", encoding="utf-8") as f:
    json_data = json.load(f)

tokens_df = pd.DataFrame(list(json_data.items()), columns=['Token_name', 'Token_value'])

WA = tokens_df.loc[tokens_df['Token_name'] == 'Token_api_P', 'Token_value'].iloc[0]
IB = tokens_df.loc[tokens_df['Token_name'] == 'Token_api_IB', 'Token_value'].iloc[0]
DB = tokens_df.loc[tokens_df['Token_name'] == 'Token_api_HR', 'Token_value'].iloc[0]
AP = tokens_df.loc[tokens_df['Token_name'] == 'Token_api_AD', 'Token_value'].iloc[0]
MO = tokens_df.loc[tokens_df['Token_name'] == 'Token_api_WA', 'Token_value'].iloc[0]
DA = tokens_df.loc[tokens_df['Token_name'] == 'Token_api_DA', 'Token_value'].iloc[0]

tokens = [WA, IB, DB, AP, MO, DA]

# Чтение файла с Апп айди и ивент неймами
apps_df = pd.read_excel("/state_data/apps.xlsx", sheet_name='UA')
apps_df_r = pd.read_excel("/state_data/apps.xlsx", sheet_name='retarget')
# Универсальная функция для выгрузки данных
def download_app_data(app_id, event_name, type_data, type_data_, token, token_variable_name, date_from, date_to):
    """Функция для скачивания данных для одного приложения."""
    
    url = f"https://hq1.appsflyer.com/api/{type_data_}/export/app/{app_id}/{type_data}/v5?timezone=Europe%2FMoscow"
    
    # Формирование параметров запроса
    params = {
        "from": date_from,
        "to": date_to,
        "event_name": event_name
    }
    
    # Формирование заголовков запроса с токеном доступа
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    # Переменная для отслеживания числа попыток
    attempts = 0
    
    # Цикл для повторных попыток
    while attempts < 2:
        # Выполнение запроса
        response = requests.get(url, params=params, headers=headers)
        
        # Проверка статуса запроса
        if response.status_code == 200:
            # Обработка успешного ответа
            print(f"Данные успешно загружены для app_id={app_id}, event_name={event_name}")
            try:
                # Преобразование CSV данных в DataFrame
                data = pd.read_csv(StringIO(response.text))
                data['App_ID'] = app_id
                data['Event_Name'] = event_name
                # Добавляем новое поле с названием переменной токена
                data['App_ID_With_Token'] = data['App_ID'] + token_variable_name
                return data
            except Exception as e:
                print(f"Ошибка обработки данных для {app_id} ({event_name}): {e}")
                return None
        elif response.status_code == 400:
            print("Ошибка: Некорректный запрос")
            break
        elif response.status_code == 401:
            print("Ошибка: Аккаунт может быть приостановлен. Проверьте статус аккаунта.")
            break
        elif response.status_code == 404:
            print("Ошибка: Идентификатор приложения не найден.")
            break
        else:
            print(f"Ошибка при загрузке данных: {response.status_code} повтор через 60 секунд")
            time.sleep(60)  # Пауза на 60 секунд
            attempts += 1  # Увеличение числа попыток
    return None

# Основная функция для обработки всех приложений
def process_all_apps(tokens, apps_df, type_data, type_data_, date_from, date_to, output_file):
    """Функция для обработки всех приложений с различными токенами."""
    
    all_data = []  # Список для хранения всех данных

    
    # Сопоставление токенов с их названиями переменных
    token_variables = ['WA', 'IB', 'DB', 'AP', 'MO', 'DA']
    
    # Проходим по каждому токену
    for token, token_variable_name in zip(tokens, token_variables):
        print(f"Обработка токена {token_variable_name} =====================================")
        futures = []
        
        
        # Создаём отдельный пул потоков для каждого токена
        with ThreadPoolExecutor(max_workers=4) as executor:
            for _, row in apps_df.iterrows():
                app_id = row['App_ID']
                event_name = row['Event_Name']
                timezone = row['Timezone']
                # Добавляем задачу в пул потоков
                futures.append(
                    executor.submit(
                        download_app_data, app_id, event_name, type_data, type_data_, token, token_variable_name, date_from, date_to, timezone
                    )
                )
            
            # Обрабатываем завершённые задачи
            for future in as_completed(futures):
                try:
                    data = future.result()
                    if data is not None:
                        all_data.append(data)  # Добавление данных в общий список
                except Exception as e:
                    print(f"Ошибка при выполнении задачи: {e}")

    # Объединение всех данных в один DataFrame и сохранение в CSV
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        
        # combined_data = combined_data[['Partner','Media Source','Country Code', 'AppsFlyer ID', 'Platform', \
        #     'App ID', 'Is Primary Attribution', 'App_ID_With_Token', 'Event_Name', 'Install Time', 'Event Time', 'Sub Param 1']]
        
        # combined_data.rename(columns={'App ID':'App_ID_clear', 'App_ID_With_Token':'App_ID','Media Source':'Media_Source',\
        #     'Country Code':'Country_Code','AppsFlyer ID':'AppsFlyer_ID','Is Primary Attribution':'Is_Primary_Attribution',\
        #     'Install Time':'Install_Time','Event Time':'Event_Time','Sub Param 1':'Sub_Param_1'}, inplace=True)
        
        if not os.path.exists(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        combined_data.to_csv(f"/state_data/{output_file}", index=False, encoding='utf-8')
        print(f"Все данные успешно сохранены в {output_file}.")
    else:
        print("Нет данных для сохранения.")

# Параметры raw in-app-events
output_file = f"{current_datetime_m}/raw_in_app_events.csv"  # Итоговый CSV файл
date_from = current_datetime_m  # Первый день месяца
date_to = current_datetime_minus_1_day  # Вчерашняя дата
type_data = 'in_app_events_report'
type_data_ = 'raw-data'
# Вызов функции
process_all_apps(tokens, apps_df, type_data, type_data_, date_from, date_to, output_file)


# # Параметры raw in-app-events-retarget
# output_file = f"{current_datetime_m}/raw_in_app_events_retarget.csv"  # Итоговый CSV файл
# date_from = current_datetime_m  # Первый день месяца
# date_to = current_datetime_minus_1_day  # Вчерашняя дата
# type_data = 'in-app-events-retarget'
# type_data_ = "raw-data"
# # Вызов функции
# process_all_apps(tokens, apps_df_r, type_data, type_data_, date_from, date_to, output_file)


# # Параметры raw fraud-post-inapps
# output_file = f"{current_datetime_m}/fraud_fraud_post_inapps.csv"  # Итоговый CSV файл
# date_from = current_datetime_m  # Первый день месяца
# date_to = current_datetime_minus_1_day  # Вчерашняя дата
# type_data = 'fraud-post-inapps'
# type_data_ = 'raw-data'
# # Вызов функции
# process_all_apps(tokens, apps_df, type_data, type_data_, date_from, date_to, output_file)


# Параметры raw fraud_post_attribution_installs
output_file = f"{current_datetime_m}/fraud_post_attribution_installs.csv"  # Итоговый CSV файл
date_from = current_datetime_m  # Первый день месяца
date_to = current_datetime_minus_1_day  # Вчерашняя дата
type_data = 'detection'
type_data_ = "raw-data"
# Вызов функции
process_all_apps(tokens, apps_df, type_data, type_data_, date_from, date_to, output_file)


# # Параметры raw fraud_installs
# output_file = f"{current_datetime_m}/fraud_installs.csv"  # Итоговый CSV файл
# date_from = current_datetime_m  # Первый день месяца
# date_to = current_datetime_minus_1_day  # Вчерашняя дата
# type_data = 'blocked_installs_report'
# type_data_ = "raw-data"
# # Вызов функции
# process_all_apps(tokens, apps_df, type_data, type_data_, date_from, date_to, output_file)







# # Основная функция для обработки всех приложений - выгрузки из агрегированных 
# def process_all_apps(tokens, apps_df, type_data, type_data_, date_from, date_to, output_file):
#     """Функция для обработки всех приложений с различными токенами."""
    
#     all_data = []  # Список для хранения всех данных
    
#     # Сопоставление токенов с их названиями переменных
#     token_variables = ['WA', 'IB', 'DB', 'AP', 'MO', 'DA']
    
#     # Проходим по каждому токену
#     for token, token_variable_name in zip(tokens, token_variables):
#         print(f"Обработка токена {token_variable_name} =====================================")
#         futures = []
        
#         # Создаём отдельный пул потоков для каждого токена
#         with ThreadPoolExecutor(max_workers=10) as executor:
#             for _, row in apps_df.iterrows():
#                 app_id = row['App_ID']
#                 event_name = row['Event_Name']
#                 # Добавляем задачу в пул потоков
#                 futures.append(
#                     executor.submit(
#                         download_app_data, app_id, event_name, type_data, type_data_, token, token_variable_name, date_from, date_to
#                     )
#                 )
            
#             # Обрабатываем завершённые задачи
#             for future in as_completed(futures):
#                 try:
#                     data = future.result()
#                     if data is not None:
#                         all_data.append(data)  # Добавление данных в общий список
#                 except Exception as e:
#                     print(f"Ошибка при выполнении задачи: {e}")

#     # Объединение всех данных в один DataFrame и сохранение в CSV
#     if all_data:
#         combined_data = pd.concat(all_data, ignore_index=True)
#         combined_data = combined_data[['Partner','Media Source', 'Campaign', 'Country Code', 'AppsFlyer ID', 'Platform']]
        
#         combined_data.rename(columns={'App ID':'App_ID_clear'}, inplace=True)
        
#         if not os.path.exists(os.path.dirname(output_file)):
#             os.makedirs(os.path.dirname(output_file))
#         combined_data.to_csv(f"/state_data/{output_file}", index=False, encoding='utf-8')
#         print(f"Все данные успешно сохранены в {output_file}.")
#     else:
#         print("Нет данных для сохранения.")



# # Параметры agg_partners_daily
# output_file = f"{current_datetime_m}/agg_partners_daily.csv"  # Итоговый CSV файл
# date_from = current_datetime_m  # Первый день месяца
# date_to = current_datetime_minus_1_day  # Вчерашняя дата
# type_data = 'partners_by_date_report'
# type_data_ = "agg-data"
# # Вызов функции
# process_all_apps(tokens, apps_df, type_data, type_data_, date_from, date_to, output_file)


# # Параметры agg_daily
# output_file = f"{current_datetime_m}/agg_daily.csv"  # Итоговый CSV файл
# date_from = current_datetime_m  # Первый день месяца
# date_to = current_datetime_minus_1_day  # Вчерашняя дата
# type_data = 'daily_report'
# type_data_ = "agg-data"
# # Вызов функции
# process_all_apps(tokens, apps_df, type_data, type_data_, date_from, date_to, output_file)
