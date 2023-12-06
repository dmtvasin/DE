# Собрать образ из Dockerfile
docker build -t third_docker .

# Запустить новый образ
docker run -p 8080:8080 third_docker
docker run -p 8080:8080 --mount type=bind,source="$(pwd)"/dags,target=/root/airflow/dags airlow

docker run -p 8080:8080 --mount type=bind,source="$(pwd)"/dags,target=/root/airflow/dags -v /var/run/docker.sock:/var/run/docker.sock airflow_tutorial
docker run -it test:pandas 2022

# Запустить существующий контейнер
docker start airflow

# Какие есть созданные образы
docker images

# Какие есть запущенные docker контейнеры
docker ps

# Зайти внутрь созданного docker контейнера
docker exec -it bd55fa761b5e //bin//bash
docker exec -it bd55fa761b5e shcd

# Запустить сборку docker-compose
docker-compose up

# Остановить все контейнеры и сохранить данные
docker-compose stop

# Удалить все контейнеры
docker-compose down 

# Сохранить/загрузить docker образ в/из файла
docker save third_docker > third_docker.tar 
docker load < third_docker.tar

# Узнать IP адрес контейнера
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 2381481b7b9e

# Терминал
ls - посмотреть списокок файлов в директории
pwd - вывести полный путь до текущей рабочей директории, в которой находится пользователь
cat - посмотреть содержимое файла
echo "Hello World" - распечатать текст
mkdir - создать новую папку
cd - - назад в пред. папку
cd ai*w - переход в папку, не указывая полное название (было airflow)
wc -l output.csv - узнать кол-во строк в csv
rm - удаление файлов
exit - выйти из терминала

curl -sSL https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv > test.csv - сохранить csv по ссылке

# Узнать версию
airflow version
pip show sqlalchemy

# Запустить ноутбук
python -m notebook

# Калькулятор cron
https://crontab.guru/