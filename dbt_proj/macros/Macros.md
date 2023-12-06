### Примеры макросов 

# Актуальные таблицы
{{ make_actual_table('gp_public', 'ms_05_mst_contractors', 'id', 'updated_at') }} 

# Текущая дата +- указанное кол-во дней
{{ n_days_diff(-1) }}

# Прибавка к текущей дате выбранного диапазона
{{ dbt.dateadd(datepart = 'hour', interval = -1, from_date_or_timestamp = dbt_date.now() ) }}

# MD5
{{ dbt.hash('event_name') }}

# Перевод в unixtimestamp
{{ dbt_date.to_unixtimestamp() }}

# Перевод из unixtimestamp BQ и Kafka
{{ dbt_date.from_unixtimestamp("event_timestamp / 1000000") }}

# Последний день месяца/квартала
{{ dbt.last_day('created_at', 'month') }}

# День месяца
{{ dbt_date.day_of_month('date') }}

# Округляет метку времени до первого момента для данной даты
{{ dbt.date_trunc('day', 'b1.enddate') }} 

# Собрать строки в одну ячейку с группировкой по столбцу
{{ dbt.listagg(measure='CrossService', delimiter_text=', ', order_by_clause='order by cte2.MemberId') }}

# Разница между датами
{{ dbt.datediff('b1.StartDateMin', '(current_date - 1)', 'day') }}

# Достать текст из столбца по разделителю
{{ dbt.split_part(string_text="question", delimiter_text="'|'", part_number=1) }}

# Изменить тип столбца
{{ dbt.type_string() }}

# Позиция символа в строке
{{ dbt.position("'-'", "text_column") }}

# Соединить столбцы по разделителю
{{ dbt.concat(["LastName", "' '" , "FirstName", "' '" , "Patronymic"]) }}

# Возвращает 1 если regexp найден в source_value начиная с указанной position
{% macro regexp_instr(source_value, regexp, position = 1, occurrence = 1, is_raw = False) %} 