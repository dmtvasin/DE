# Описание структуры таблиц

manager_departments - таблица содержащая информацию о принадлежности менеджера к отделу
-	email - email адрес менеджера
-	department - отдел к которому относится менеджер
payments - таблица содержащая информацию о поступлениях 
-	id - уникальный идентификатор поступления
-	value - сумма поступления 
-	client_id - идентификатор клиента осуществивший платеж
-	client_name - название клиента осуществивший платеж
-	payment_date - дата поступления
-	manager_name - ФИО менеджера ответственного за клиента
-	manager_email - email адрес менеджера ответственного за клиента

# Задание №1

На основе таблицы с поступлениями, необходимо найти платежи с максимальными суммами в январе 2023 года. Результат должен быть представлен в виде таблицы со следующими полями:
-	Название клиента
-	Дата поступления
-	Сумма поступления

# Задание №2

Необходимо определить сколько принес денег каждый отдел. Таблица с поступлениями хранит информацию с имением и email адресе менеджера. Информация об отделе хранится в отдельной таблице-справочнике manager_departments. Связь этих двух таблиц осуществляется по полю email, но формат почты отличается. В каких-то случаях email менеджера представлен в формате n.ssssssss@domain.ru, где n это первая буква имени, ssssssss фамилия менеджера и между ними “.” (знак точки). В каких-то случаях формат имеет вид nssssssss@domain.ru, без точки между именем и фамилией. Необходимо объединить две таблицы с предварительным приведением почты к единому виду и дальнейшей агрегацией. Если для менеджера не определен отдел (указано значение NULL), то таких менеджеров необходимо отнести в группу “Отдел не определен”

# Задание №3

Необходимо дополнить таблицу с поступлениями полем “состояние клиента”. Если платеж для клиента является первым, то состояние клиента должно быть “Новый”. Для всех остальных платежей данного клиента, состояние должно быть “Действующий”.

# Задание №4

Необходимо сформировать таблицу в которой будет отражено сколько поступило денег в каждом месяце и как росла общая выручка с учетом предыдущих месяцев.