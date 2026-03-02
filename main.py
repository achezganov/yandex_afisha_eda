import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium", auto_download=["ipynb"])


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Исследовательский анализ "Яндекс Афиша"
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. Цели
    > **Заполнить!**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Описание
    > **Заполнить!**
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Содержимое

    - `user_id` — уникальный идентификатор пользователя, совершившего заказ;
    - `device_type_canonical` — тип устройства, с которого был оформлен заказ (`mobile` — мобильные устройства, `desktop` — стационарные);
    - `order_id` — уникальный идентификатор заказа;
    - `order_dt` — дата создания заказа (используйте данные `created_dt_msk`);
    - `order_ts` — дата и время создания заказа (используйте данные `created_ts_msk`);
    - `currency_code` — валюта оплаты;
    - `revenue` — выручка от заказа;
    - `tickets_count` — количество купленных билетов;
    - `days_since_prev` — количество дней от предыдущей покупки пользователя, для пользователей с одной покупкой — значение пропущено;
    - `event_id` — уникальный идентификатор мероприятия;
    - `service_name` — название билетного оператора;
    - `event_type_main` — основной тип мероприятия (театральная постановка, концерт и так далее);
    - `region_name` — название региона, в котором прошло мероприятие;
    - `city_name` — название города, в котором прошло мероприятие.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Загрузка данных и их предобработка
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 4.1 Импорт библиотек
    """)
    return


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from phik import phik_matrix

    return mo, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 4.2 Выгружаем датафрейм из БД (1.1)
    """)
    return


@app.cell(disabled=True, hide_code=True)
def _(pd):
    # Выгрузка из БД

    import psycopg2  # библиотека для подключения к PostgreSQL
    import os        # модуль для работы с переменными окружения
    from dotenv import load_dotenv  # функция для загрузки переменных окружения из .env файла

    # Загружаем переменные окружения из файла .env в текущую среду
    # Это позволяет не хранить логины и пароли прямо в коде
    load_dotenv()

    # Устанавливаем соединение с базой данных PostgreSQL
    # Параметры подключения берутся из переменных окружения:
    # PGHOST — хост базы данных
    # PGPORT — порт
    # PGDATABASE — имя базы данных
    # PGUSER — пользователь
    # PGPASSWORD — пароль
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
    )

    # Откатываем возможную незавершённую транзакцию
    conn.rollback()


    # Функция для загрузки SQL-запроса из файла
    # path — путь к файлу .sql
    # возвращает строку с SQL-запросом
    def load_sql(path: str) -> str:
        with open(path, "r") as f:
            return f.read()


    # Загружаем SQL-запрос из файла витрины данных
    # В данном случае — витрина покупок пользователей Афиши
    query = load_sql("sql/marts/afisha_purchases_mart.sql")


    # Выполняем SQL-запрос и загружаем результат в pandas DataFrame,
    # затем сохраняем результат в CSV файл
    # datasets/dataset.csv — файл, который будет использоваться далее для анализа
    pd.read_sql(query, conn).to_csv('datasets/dataset.csv', index=False)


    # Позже этот CSV можно загрузить обратно в DataFrame для анализа:
    # df = pd.read_csv('datasets/dataset.csv')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 4.3 Изучение общей информации о выгруженных данных (1.2)
    """)
    return


@app.cell
def _(pd):
    df_original = pd.read_csv('datasets/dataset.csv')
    df_raw = df_original.copy()
    return (df_raw,)


@app.cell
def _(df_raw):
    df_raw.info()
    return


@app.cell
def _(df_raw):
    # Выведем данные с начала и с конца датафрейма
    df_raw.head(), df_raw.tail()
    return


@app.cell(hide_code=True)
def _(pd):
    # Функция тестирования датафрейма на валидность
    def validate_dataset(df: pd.DataFrame) -> None:
        """
        Проверка корректности выгруженного датасета.
        Останавливает выполнение при обнаружении критических ошибок.
        """

        print("Запуск проверки датасета.")

        # Проверка, что датафрейм не пустой
        if df.empty:
            raise ValueError("Датасет пустой")
        print("Тест 1 пройден: датафрейм не пустой")

        # Проверка структуры (колонки)
        expected_columns = {
            'user_id',
            'device_type_canonical',
            'order_id',
            'order_dt',
            'order_ts',
            'currency_code',
            'revenue',
            'tickets_count',
            'days_since_prev',
            'event_id',
            'event_type_main',
            'service_name',
            'region_name',
            'city_name'
        }

        missing_columns = expected_columns - set(df.columns)

        if missing_columns:
            raise ValueError(f"Отсутствуют колонки: {missing_columns}")
        print("Тест 2 пройден: структура датафрейма корректна")

        # Проверка на дубликаты ключа
        duplicate_count = df['order_id'].duplicated().sum()

        if duplicate_count > 0:
            raise ValueError(f"Обнаружено {duplicate_count} дубликатов order_id")
        print("Тест 3 пройден: дубликаты order_id отсутствуют")

        # Проверка revenue
        if (df['revenue'] < 0).any():
            raise ValueError("Обнаружена отрицательная revenue")
        print("Тест 4 пройден: revenue не содержит отрицательных значений")

        # Проверка tickets_count
        if (df['tickets_count'] <= 0).any():
            raise ValueError("Обнаружен tickets_count <= 0")
        print("Тест 5 пройден: tickets_count > 0 для всех строк")

        # Проверка device_type
        valid_devices = {'mobile', 'desktop'}

        invalid_devices = set(df['device_type_canonical']) - valid_devices

        if invalid_devices:
            raise ValueError(f"Некорректные device_type: {invalid_devices}")
        print("Тест 6 пройден: device_type_canonical содержит только допустимые значения")

        # Проверка дат
        order_dt_parsed = pd.to_datetime(df['order_dt'], errors='coerce')
        order_ts_parsed = pd.to_datetime(df['order_ts'], errors='coerce')

        if order_dt_parsed.isna().any():
            raise ValueError("Некорректные значения в order_dt")
        print("Тест 7 пройден: order_dt корректно парсится в дату")

        if order_ts_parsed.isna().any():
            raise ValueError("Некорректные значения в order_ts")
        print("Тест 8 пройден: order_ts корректно парсится в datetime")

        # Проверка days_since_prev
        days_parsed = pd.to_numeric(df['days_since_prev'], errors='coerce')

        invalid_days = df['days_since_prev'].notna() & days_parsed.isna()

        if invalid_days.any():
            raise ValueError("Обнаружены некорректные значения в days_since_prev")
        print("Тест 9 пройден: days_since_prev содержит только числовые значения или NULL")

        print("Проверка датасета успешно пройдена.")

    return (validate_dataset,)


@app.cell
def _():
    # Проверим датасет на валидность 1
    #validate_dataset(df_raw)
    return


@app.cell
def _(df_raw):
    # Обработка отрицательного revenue 
    f'Доля отрицательных значений в поле revenue: {round(df_raw[df_raw['revenue'] < 0].shape[0] * 100 / df_raw.shape[0], 2)}%'
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Доля отрицательных значений в поле revenue `<5%` от общего объема и составляет `0.13%`. Следовательно, мы можем избавиться от аномальных записей.
    """)
    return


@app.cell
def _(df_raw):
    #Находим индексы строк с отрицательным revenue
    bad_idx = df_raw.index[df_raw["revenue"] < 0]

    # Удаляем их из df + сбрасываем индекс
    df_raw.drop(index=bad_idx, inplace=True)
    df_raw.reset_index(drop=True, inplace=True)
    return


@app.cell
def _(df_raw, validate_dataset):
    # Проверим датасет на валидность 2
    validate_dataset(df_raw)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Промежуточный вывод:**

    1. **Объём и структура данных.**
    Мы успешно загрузили датасет, который содержит 290 849 записей и 14 колонок (~31 mb). Структура данных соответствует ожидаемой, все необходимые признаки присутствуют. Типы данных корректны и позволяют проводить дальнейший анализ.

    2. **Качество данных.**
    В ходе первичного анализа мы обнаружили пропуски только в поле `days_since_prev`. Это ожидаемо, поскольку для пользователей с одной покупкой отсутствует предыдущая транзакция. Некорректных значений в полях `order_dt` и `order_ts` не выявлено — данные успешно приводятся к форматам даты и времени. Остальные ключевые признаки содержат допустимые значения.

    3. **Аномалии и их обработка.**
    Мы выявили небольшую долю отрицательных значений в поле `revenue` — 0.13% от общего объёма данных. Такие значения являются аномальными с точки зрения бизнес-логики, поэтому мы приняли решение удалить соответствующие записи. После удаления аномалий датасет успешно прошёл проверку валидности.

    4. **Целостность и корректность данных.**
    В процессе проверки мы убедились, что дубликаты по ключевому полю `order_id` отсутствуют. Также не выявлено критических ошибок, которые могли бы повлиять на результаты анализа. Все признаки находятся в корректном формате.

    На этапе предобработки можно расширить признаковое пространство за счет декомпозиции временных признаков. Это позволит анализировать сезонность, активность по дням недели и по времени суток. А также можно оптимизировать типы данных для уменьшения объема памяти.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 5. Предобработка данных
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 5.1 Обрабтка категориальных признаков
    """)
    return


@app.cell
def _(df_raw):
    # Проверим event_type_main на некорректные значения
    df_raw['event_type_main'].unique()
    return


@app.cell
def _(df_raw):
    # Проверим currency_code на некорректные значения
    df_raw['currency_code'].unique()
    return


@app.cell
def _(df_raw):
    # Проверим service_name на некорректные значения
    df_raw['service_name'].unique()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 5.2 Обработка временных признаков и расширение признакового пространства
    """)
    return


@app.cell
def _(df_raw, pd):
    # Приводим тип полей order_dt и order_ts к date и datetime 
    df_raw['order_dt'] = pd.to_datetime(df_raw['order_dt'], errors='coerce').dt.date
    df_raw['order_ts'] = pd.to_datetime(df_raw['order_ts'], errors='coerce')
    return


@app.cell
def _(df_raw):
    # расширим признаковое пространство, за счет декомпозиции временных признаков
    df_raw['order_year'] = df_raw['order_ts'].dt.year.astype('int16')
    df_raw['order_month'] = df_raw['order_ts'].dt.month.astype('int8')
    df_raw['order_day'] = df_raw['order_ts'].dt.day.astype('int8')
    df_raw['order_weekday'] = df_raw['order_ts'].dt.weekday.astype('int8')
    df_raw['order_hour'] = df_raw['order_ts'].dt.hour.astype('int8')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 5.3 Приведение выручки к единой валюте (2.1)
    """)
    return


@app.cell
def _(pd):
    # Загружаем датасет с курсом тенге
    tenge_rate_df = pd.read_csv('datasets/final_tickets_tenge_df.csv'
    )

    # Посмотрим структуру
    tenge_rate_df.head(), tenge_rate_df['cdx'].unique(), tenge_rate_df['nominal'].unique()
    return (tenge_rate_df,)


@app.cell
def _(df_raw, pd, tenge_rate_df):
    # Рассчитываем курс: сколько рублей за 1 тенге
    tenge_rate_df['kzt_to_rub'] = tenge_rate_df['curs'] / tenge_rate_df['nominal']

    # Приводим дату к формату date для корректного merge
    tenge_rate_df['order_dt'] = pd.to_datetime(tenge_rate_df['data']).dt.date

    # Добавляем курс в основной датафрейм
    df = df_raw.merge(tenge_rate_df[['order_dt', 'kzt_to_rub']], 
                              on='order_dt', 
                              how='left')
    return (df,)


@app.cell
def _(df):
    # Создаем новый столбец revenue_rub
    # если валюта kzt — конвертируем через курс, иначе не меняем
    df['revenue_rub'] = df['revenue']

    mask_kzt = df['currency_code'] == 'kzt'
    df.loc[mask_kzt, 'revenue_rub'] = (
        df.loc[mask_kzt, 'revenue'] * df.loc[mask_kzt, 'kzt_to_rub']
    )

    # Удаляем вспомогательный столбец курса
    df.drop(columns=['kzt_to_rub'], inplace=True)
    return


@app.cell
def _(df):
    # Проверим результат 
    df[df['currency_code'] =='kzt'].head(), df[df['currency_code'] =='rub'].head()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 5.4 Обработка числовых признаков
    """)
    return


@app.cell
def _(df):
    # Проверим статистические показатели полей, что понять, можно ли их сжать
    fields = ['tickets_count', 'days_since_prev', 'event_id', 'order_id', 'revenue_rub']
    df[fields].describe()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    В полях `tickets_count`, `days_since_prev` и `revenue_rub` обнаружены критические выбросы. Мы избавимся от них, используя 99-й процентиль.
    """)
    return


@app.cell
def _(df):
    # На основе стат. показателей выберем подходящие типы для полей
    df['days_since_prev'] = df['days_since_prev'].astype('float32')
    df['tickets_count'] = df['tickets_count'].astype('uint8')
    df[['event_id', 'order_id']] = df[['event_id', 'order_id']].astype('int32')

    # Приведение финансовых показателей
    df[['revenue', 'revenue_rub']] = df[['revenue', 'revenue_rub']].astype('float32')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Отметим, что поля `revenue` и `revenue_rub` не следует сжимать, так как они являются финансовыми показателями, и потеря точности может быть критичной для корректности денежных агрегатов. Однако для аналитических расчётов допустимо приведение к типу `float32`.
    """)
    return


@app.cell
def _(df):
    # Избавляемся от выбросов по 99 процентилю
    outliers_idx = df.index[(df["tickets_count"] > df['tickets_count'].quantile(0.99))
                          | (df["days_since_prev"] > df['days_since_prev'].quantile(0.99))
                          | (df["revenue_rub"] > df['revenue_rub'].quantile(0.99))]

    print(f'Суммарная доля выбросов в полях day_since_prev, revenue_rub и tickets_count: {round(len(outliers_idx) * 100 / df.shape[0], 2)}%')
    return (outliers_idx,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Суммарная доля выбросов и, ранее удаленных, аномальных зачений не превышает 5%. Можем избавиться от выбросов.
    """)
    return


@app.cell
def _(df, outliers_idx):
    # Удаляем их из df + сбрасываем индекс
    df.drop(index=outliers_idx, inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(f"Размер df после удаления выбросов: {df.shape}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 5.5 Нормализация, обработка явных и неявных дубликатов
    """)
    return


@app.cell(hide_code=True)
def _(pd):
    # Функция нормализации текстовых полей
    def normalize_text_column(series: pd.Series) -> pd.Series:
        """
        Нормализует строковое поле:
        - приводит к строковому типу
        - убирает лишние пробелы
        - приводит к нижнему регистру
        - заменяет множественные пробелы на один
        - убирает пробелы в начале и конце
        """
        return (
            series
            .astype("string")
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", " ", regex=True)
        )

    return (normalize_text_column,)


@app.cell
def _(df, normalize_text_column):
    # Применяем нормализацию к нужным полям
    columns_to_normalize = ['service_name', 'region_name', 'city_name']

    for col in columns_to_normalize:
        df[col] = normalize_text_column(df[col])
    return


@app.cell
def _(df):
    # Проверим на явные дубликаты
    explicit_duplicates = df.duplicated()

    print(f"Количество явных дубликатов: {explicit_duplicates.sum()}")
    print(f"Доля явных дубликатов: {round(explicit_duplicates.mean() * 100, 2)}%")
    return


@app.cell
def _(df):
    # Проверим на неявные дубликаты по ключевому бизнес-идентификатору order_id
    implicit_duplicates_order = df.duplicated(subset=['order_id'])

    print(f"Количество неявных дубликатов по order_id: {implicit_duplicates_order.sum()}")
    print(f"Доля неявных дубликатов по order_id: {round(implicit_duplicates_order.mean() * 100, 2)}%")
    return


@app.cell
def _(df):
    # Проверим на неявные дубликаты заказов одного пользователя в один момент времени
    implicit_duplicates_user_time = df.duplicated(
        subset=['user_id', 'order_ts', 'event_id', 'service_name']
    )

    print(f"Количество потенциальных дублей заказов: {implicit_duplicates_user_time.sum()}")
    print(f"Доля потенциальных дублей заказов: {round(implicit_duplicates_user_time.mean() * 100, 3)}%")

    df[implicit_duplicates_user_time].head()
    return


@app.cell
def _(df):
    # Удаляем найденные неявные дубликаты
    df.drop_duplicates(
        subset=['user_id', 'order_ts', 'event_id', 'service_name'],
        keep='first',  # оставляем первый (предположительно оригинальный) заказ
        inplace=True
    )

    df.reset_index(drop=True, inplace=True)
    return


@app.cell
def _(df):
    # Проверим, что дубликаты удалены
    implicit_duplicates_user_time_after = df.duplicated(
        subset=['user_id', 'order_ts', 'event_id', 'service_name']
    )

    print(f"Количество дублей после удаления: {implicit_duplicates_user_time_after.sum()}")
    print(f"Размер df после удаления дублей: {df.shape}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 5.6 Промежуточный вывод по предобработке
    В ходе предобработки данных мы последовательно привели датасет к состоянию, пригодному для дальнейшего исследовательского анализа и построения моделей.

    Мы обработали категориальные признаки: унифицировали текстовые значения, устранили различия в регистре, лишние пробелы и неоднородности написания.

    Расширили временные признаки на основе исходного timestamp заказа. Были выделены год, месяц, день, день недели и час заказа. Это позволило подготовить данные для анализа сезонности, поведенческих паттернов пользователей и временных закономерностей спроса.

    Также мы привели выручку к единой валюте и сформировали показатель `revenue_rub`, что устранило проблему несопоставимости денежных значений и обеспечило корректность дальнейших расчетов метрик выручки.

    В числовых признаках мы провели анализ выбросов. Суммарная доля выбросов в полях `days_since_prev`, `revenue_rub` и `tickets_count` составила 1.94%. Это небольшая доля наблюдений, поэтому их удаление не оказывает существенного влияния на репрезентативность выборки, но при этом позволяет снизить влияние аномальных значений на статистики. После удаления выбросов размер датасета составил 284 846 наблюдений.

    Выполнили нормализацию текстовых признаков, приведя их к единому формату.

    Проверка на явные дубликаты показала их отсутствие (0 наблюдений). Также мы проверили дубликаты по бизнес-ключу `order_id` и убедились, что каждая запись имеет уникальный идентификатор заказа.

    Дополнительно мы выявили потенциальные неявные дубликаты — случаи, когда у одного пользователя зафиксирован заказ с одинаковыми `order_ts`, `event_id` и `service_name`. Было обнаружено 117 таких записей (0.041% выборки). С высокой вероятностью это технические дубликаты, поэтому мы удалили их, сохранив первое вхождение. После этого финальный размер датасета составил 284 729 наблюдений, и повторная проверка подтвердила отсутствие дубликатов.

    В результате мы получили чистый датасет, в котором устранены основные проблемы качества данных, способные исказить результаты исследования.
    """)
    return


if __name__ == "__main__":
    app.run()
