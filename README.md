# **Вступительная задача в компанию PostgresPro**
### **Кандидат - Арляпов Евгений**
### **специальность - стажер-инженер по отказоустойчивости**
## **Часть первая. Создание базы данных и импорт данных из xml-файлов в базу данных**
### **Описание**
Первая часть проекта состоит из двух python-модулей и одного sql-скрипта.\
\
**1. Модуль funcs.py** - содержит функции:


```python
import psycopg2
from psycopg2 import Error
import xml.etree.ElementTree as ET


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
        database = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port
        )
        print('connection successful!')
    except Error as e:
        print(f'The error in connection: {e}')
    return connection 


def execute_query(connection, query): # функция, формирующая запрос
    with connection.cursor() as cur:
        try:
            cur.execute(query)
            connection.commit()
            print("Query executed successfully")
        except Error as e:
            print(f"Error in query: {e}")    


def executemany_query(connection, query, lst): # функция, формирующая множественный запрос
    with connection.cursor() as cur:
        try:
            cur.executemany(query, lst)
            connection.commit()
            print("Query executed successfully")
        except Error as e:
            print(f"Error in query: {e}")                 


def get_all_coloumns_list(path): # строки в xml содержат разное количество полей. эта функция ищет строку с максимальным количеством полей (то есть те, которые встречаются в файле хотя бы один раз) 
    tree = ET.parse(path)
    root = tree.getroot()

    max_n_coloumns = 0
    all_coloumns_list = [] # список со всеми полями таблицы

    for el in root.iter('row'):
        if max_n_coloumns < len(el.attrib):
            max_n_coloumns = len(el.attrib)
            all_coloumns_list = [elem for elem in el.attrib.keys()]

    return all_coloumns_list


def check_all_coloumns_list(path): # предыдущая функция не учитывает следующей ситуации:
                                 # даже в строке с максимальной длиной может происходить
                                 # потеря данных, если в нее не попадут данные из некоторого 
                                 # столбца. Чтобы этого избежать, надо написать проверку: из 
                                 # первой строки в список добавить все поля, далее пройтись
                                 # по всем строкам, и если некоторые поля будут обнаружены
                                 # впервые, добавить в список
                                 # В случае, если get_all_coloumns_list() и check_all_coloumns_list()
                                 # дадут одинаковые результаты, добавляем данные. Иначе список 
                                 # all_check_coloumns_list придется сортировать вручную, так как
                                 # поля в этот список добавляются не в том порядке, в котором они
                                 # распололжены в таблице 
    tree = ET.parse(path)
    root = tree.getroot()

    i = 0
    all_check_coloumns_list = [] # список со всеми полями таблицы

    for el in root.iter('row'):
        if i == 0:
            all_check_coloumns_list = list(el.attrib.keys())
        else:
            for elem in el.attrib.keys():
                if elem not in all_check_coloumns_list:
                    all_check_coloumns_list.append(elem)
        i+=1

    return all_check_coloumns_list


def add_value(el, d): # Функция добавляет значения из словаря  el.attrib в словарь d
    for key in list(el.attrib.keys()):
        d[key] = el.attrib[key]
    

def get_all_strings(path, all_coloumns_list): # В этой функции получаем все строки из xml файла в виде списка кортежей
                                            # Такой формат удобен для дальнейшего добавления данных в таблицу с
                                            # с помощью функции executemany()
    tree = ET.parse(path)
    root = tree.getroot()

    string_list = []

    for el in root.iter('row'):
        d = {}

        if list(el.attrib.keys()) != all_coloumns_list: # если текущая строка не содержит полный набор полей,
                                                      # полученный с помощью функции get_all_coloumns_list

            d = dict.fromkeys(all_coloumns_list) # то в этом случае формируется словарь d, содержащий набор ключей,
                                               # равный максимальному набору полей в xml. При этом все значения 
                                               # этого словаря по умолчанию становятся None

            add_value(el, d) # далее мы из текущей строки добавляем все значения по существующим ключам,
                           # а если ключа ключа из полного списка полей нет в данной строке, то значение 
                           # по этому ключу отсается None

        add_value(el, d)

        string_list.append(tuple(d.values())) # таким образом, формируется список кортежей, в каждом 
                                              # в каждом из которых по 12 элементов независимо от того,
                                              # сколько полей в строке xml-файла. Далее этот список остается 
                                              # добавить в таблицу с помощью метода executemany()
    return string_list
```

**Описание структуры:**\
    **1.1. psycopg2** - модуль для работы с СУБД PostgreSQL. Класс Error используется для обработки исключений, возникающих при взаимодействии с СУБД.\
    &ensp;**Внимание!** Модуль psycopg2 не является частью стандартной библиотеки Python (см. инструкцию по запуску)\
    \
    **1.2. xml.etree.ElementTree** - класс для работы с xml-файлами из стандартной библиотеки python\
    \
    **1.3. create_connection** - функция для подключения к базе данных db_name пользователя db_user\
    \
    **1.4. execute_query** - функция для выполнения SQL-запросов. Принимает объект соединения connection, возвращаемый функцией create_connection, а также SQL-запрос в виде строки. Изменения в транзакции фиксируются функцией                      connection.commit() (режим автокоммита не используется)\
    \
    **1.5 executemany_query** - функция для выполнения множественного SQL-запроса. Дополнительно принимает список кортежей lst, с помощью которого происходит перенос данных из xml в базу данных\
    \
    **1.6. get_all_coloumns_list** - принимает абсолютный или относительный путь к xml-файлу. При чтении xml-файлов возникает следующая проблема: в заданной таблице фиксированное число полей, например n. Для того чтобы добавить                   даннные из xml в таблицу, нужно составить список кортежей, который затем нужно передать в функцию executemany_query, причём **каждый кортеж в списке должен иметь длину n, соответствующую количеству полей в таблице**. Однако                   было определено, что **строки в xml имеют разную длину**. Это связано с тем, что поля, содержащие null, по всей видимости, **не заносились в xml**. В связи с этим было необходимо придумать алгоритм, который находит все поля таблицы в         xml, в которых хотя бы одно значение не null. \
    То есть функция находит самую длинную строку в xml-файле. Далее предполагается, что эта строка содержит полный набор полей, и по этой строке выравниваются все остальные кортежи в списке. При этом строки с меньшей длиной дополняются           недостающими полями, а в качестве значений отсутствующих полей выбирается null\
    \
    **1.7. check_all_coloumns_list** - принимает абсолютный или относительный путь к xml-файлу. При реализации алгоритма в пункте 1.6 также возникает проблема: **даже самая длинная строка в xml-файле может не содержать полного набора             данных**. Для того чтобы точно определить все поля, которые есть в xml, реализован следующий алгоритм: создается пустой список, в него добавляются все поля, найденные в первой строке xml. Далее проходимся по всем строкам xml и                добаляем поля, которых нет в списке (учитываем при этом, что данные из xml получаются в виде словаря где ключи - поля, а значения - строки. Поэтому предварительно преобразовываем ключи словаря в список). В итоге получаем список               всех уникальных полей. **Таким способом было найдено поле ContentLicense из таблицы Posts.Posts, которого не было в схеме данных, но которое присутствовало в xml**.\
    Однако здесь возникает новая проблема. Функция get_all_coloumns_list **возвращает список полей в том порядке, в котором они находятся в таблице**, в то время как функция check_all_coloumns_list **перемешивает поля**, из-за чего нужно         перед добавлением данных вручную сортировать полученные поля так, чтобы их последовательность была такая же, как в таблице. Автоматизированного способа сортировки найдено не было.\
    Для того чтобы избежать дополнительной ручной работы с каждой таблицей, было сделано следующее: список полей находился с помощью обеих функций get_all_coloumns_list и check_all_coloumns_list. Далее сравнивались длины списков, возвращаемых этими функциями: если эти длины были равны, значит функция get_all_coloumns_list верно определила набор полей и далее можно было автоматически вставлять данные функцией executemany_query. В противном слуучае поля из  списка,возвращенного функцией check_all_coloumns_list сортировались вручную и только после этого происходило добавление данных в таблицу.\
    Реализация данного алгоритма будет видна в описании модуля read_xml.py\
    \
    **1.8. add_key** - принимает два словаря. Является вспомогательной для функции get_all_strings, переносит значения по существующим ключам из первого словаря во второй словарь. Назначение - избежание дублирования кода\
    \
    **1.9. get_all_strings** - принимает путь к xml-файлу и полный список полей. Возвращает все строки xml-файла в виде списка кортежей (то есть непосредственно данные в готовой для функции executemany_query и добавления в таблицу). Алгоритм работы следующий: создаем пустой список строк. Каждую строчку xml получаем в виде словаря, ключи которого - поля, значения - строки. Для каждой строки преобразуем ключи в словаря в список и сравниваем с со списком всех найденных уникальных полей (это будет либо результат работы функции get_all_coloumns_list, либо check_all_coloumns_list в зависимости от таблицы).\
    Если списки не равны, то формируем новый словарь, набор ключей котрого равен маскимальному набору ункальных полей в списке. Так как для такого преобразования используется функция fromkeys(), то по всем ключам полученного словаря будут лежать значения None, которые соответствуют NULL в СУБД PosgreSQL.\
    Далее сравниваем ключи сформированного словаря с ключами словаря, полученного для каждой строки (этот словарь содержит весь список NOT NULL полей для текущей строки). Если ключ найден, заносим в него значение, если нет - то знаечение по ключу остается None. В конце работы алгоритма для текущей строки добавляем в пустой список, сформированный в начале, значения ключей словаря с полным набором полей, предварительно преобразованные в кортеж.\
    Если же списки равны, то сразу реализуем алгоритм со словарями без предварительного использования функции fromkeys().\
    То есть основная идея описанного алгоритма - восстановить все NULL-значения, которые не были добавлены в xml, а затем из восстановленных данных сформировать список кортежей, котроый затем уже можно будет передать в функцию executemany_query для добавления данных в таблицу\
    \
    \
    **2. SQL-скрипт create_db.sql** - создаёт базу данных pg1, подлкючается к ней, создаёт схемы и таблицы внутри БД:


```sql
\c postgres
DROP IF EXISTS DATABASE pg1;
CREATE DATABASE pg1;

\c pg1

CREATE SCHEMA IF NOT EXISTS Users;
CREATE SCHEMA IF NOT EXISTS Posts;
CREATE SCHEMA IF NOT EXISTS Tags;
CREATE SCHEMA IF NOT EXISTS History;
CREATE SCHEMA IF NOT EXISTS Votes;

-- замена типов данных: TYNIINT->SMALLINT, DATETIME->TIMESTAMP, NVARCHAR->TEXT, BYTE->BOOLEAN

CREATE TABLE IF NOT EXISTS Users.Users(
    Id INTEGER PRIMARY KEY,
    Reputation INTEGER, 
    CreationDate TIMESTAMP,
    DisplayName TEXT,
    LastAccessDate TIMESTAMP,
    WebsiteUrl TEXT,
    Location TEXT, 
    AboutMe TEXT, 
    Views INTEGER, 
    UpVotes INTEGER,
    DownVotes INTEGER,
    AccountId INTEGER
);

CREATE TABLE IF NOT EXISTS Posts.Posts(
    Id INTEGER PRIMARY KEY,
    PostTypeId SMALLINT, -- в схеме есть внешний ключ на таблицу, которой нет в файле, поэтому она не пишется здесь
    AcceptedAnswerId INTEGER, --REFERENCES Posts(Id)
    ParentId INTEGER,
    CreationDate TIMESTAMP, 
    Score INTEGER, 
    ViewCount INTEGER,
    Body TEXT,
    OwnerUserId INTEGER, --REFERENCES Users.Users(Id)
    OwnerDisplayName TEXT,
    LastEditorUserId INTEGER, --REFERENCES Users.Users(Id)
    LastEditorDisplayName TEXT,
    LastEditDate TIMESTAMP, 
    LastActivityDate TIMESTAMP,
    Title TEXT,
    Tags TEXT,
    AnswerCount INTEGER, 
    CommentCount INTEGER, 
    FavoriteCount INTEGER,
    ClosedDate TIMESTAMP,
    CommunityOwnedDate TIMESTAMP,
    ContentLicense TEXT -- в схеме такого поля нет, оно было найдено в xml-файле благодаря алгоритму поиска,
                        -- описанному выше
);

CREATE TABLE IF NOT EXISTS Users.Badges (
    Id INTEGER PRIMARY KEY,
    UserId INTEGER, --REFERENCES Users.Users(id)
    Name TEXT,   
    Date TIMESTAMP,
    Class SMALLINT,
    TagBased BOOLEAN
);

CREATE TABLE IF NOT EXISTS Posts.Comments(
    Id INTEGER PRIMARY KEY,
    PostId INTEGER, --REFERENCES Posts.Posts(Id)
    Score INTEGER, 
    Text TEXT,
    CreationDate TIMESTAMP, 
    UserDisplayName TEXT,
    UserId INTEGER
);

CREATE TABLE IF NOT EXISTS History.PostHistory (
    Id INTEGER PRIMARY KEY,
    PostHistoryTypeId SMALLINT, -- в схеме есть внешний ключ на таблицу, которой нет в файле, поэтому она не пишется здесь
    PostId INTEGER, --REFERENCES Posts.Posts(Id)
    RevisionGUID TEXT,
    CreationDate TIMESTAMP,
    UserId INTEGER, --REFERENCES Users.Users(Id)
    UserDisplayName TEXT,
    Comment TEXT,
    Text TEXT, 
    ContentLicense TEXT -- в базе данных такого поля нет, оно тоже было найдено в xml
);

CREATE TABLE IF NOT EXISTS Posts.PostLinks (
    Id INTEGER PRIMARY KEY,
    CreationDate TIMESTAMP, 
    PostId INTEGER, --REFERENCES Posts.Posts(Id)
    RelatedPostId INTEGER, --REFERENCES Posts.Posts(Id)
    LinkTypeId SMALLINT
);

CREATE TABLE IF NOT EXISTS Tags.Tags(
    Id INTEGER PRIMARY KEY, -- в схеме есть внешний ключ на таблицу, которой нет в файле, поэтому она не пишется здесь
    TagName TEXT, 
    Count INTEGER, 
    ExcerptPostId INTEGER, --REFERENCES Posts.Posts(id)
    WikiPostId INTEGER, --REFERENCES Posts.Posts(id)
    IsModeratorOnly BOOLEAN,-- новое поле обнаружено при поиске уникальных полей
    IsRequired BOOLEAN -- второе новое поле обнаружено при поиске уникальных полей
);

CREATE TABLE IF NOT EXISTS Votes.Votes(
    Id INTEGER PRIMARY KEY,
    PostId INTEGER, --REFERENCES Posts(id)
    VoteTypeId SMALLINT, -- в схеме есть внешний ключ на таблицу, которой нет в файле, поэтому она не пишется здесь
    CreationDate TIMESTAMP
);
```    

**Описание структуры:**\
**2.1 Типы данных** - в схеме БД указаны некоторые типы данных, которых нет в PostgreSQL. Они были заменены на наиболее подходящие (см. комментарий в начале скрипта)\
**2.2 Внешние ключи** - в схеме есть внешние ключи на таблицы, которые не представлены в xml-файле. Такие внешние ключи были убраны.\
Кроме того в некоторые внешние ключи ведут на поля из этой же таблицы (пример - поля AcceptedAnswerId и OwnerUserId из таблицы Posts.Posts на поле Id из этой же таблицы). Они не были добавлены при создании базы данных, поскольку может возникнуть ситуация, когда эти поля ссылаются на ещё не добавленные данные в поле Id. Поэтому было сделано сделано следующее: сначала добавлены данные в таблицу, а затем с помощью команды ALTER добавлено ограничение внешнего ключа (см. эту реализацию во втором python-скрипте).\
Кроме того, почти во всех случаях при работе с внешними ключами наблюдалась такая ситуация: в таблице, на которую ссылался внешний ключ, не было некоторых некоторых значений из ссылающегося поля, при попытке установить такой ключ возникала ошибка. Во избежание потери данных был реализован следующий алгоритм: в поле с внешним ключом найдены все уникальные значения, которых нет в поле, на которое ссылается этот ключ, и добавлены в него (при этом значения остальных полей были объявлены как NULL, где это возможно). Затем с помощью команды ALTER было добавлено соответствующее ограничение (реализацию алгоритма также см. во втором python-скрипте)
    \
    \
    **3. python-скрипт read_xml.sql** заполняет данными БД и устанавливает внешние ключи:


```python
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2 import Error
from psycopg2 import sql

from funcs import create_connection as crtcnt
from funcs import execute_query
from funcs import executemany_query
from funcs import get_all_coloumns_list
from funcs import check_all_coloumns_list
from funcs import add_key
from funcs import get_all_strings


if __name__ == '__main__':

    conn = crtcnt(
    'pg1', 'eugene', 'postgres', 'localhost', '5432'
    )

    if conn is not None:
        ###ВНИМАНИЕ! Пути к xml-файлам необходимо поменять!###
        users_path = '/home/mephist/postgresql/scripts/xml/dba.stackexchange.com/Users.xml'
        posts_path = '/home/mephist/postgresql/scripts/xml/dba.stackexchange.com/Posts.xml'
        badges_path = '/home/mephist/postgresql/scripts/xml/Badges.xml'
        comments_path = '/home/mephist/postgresql/scripts/xml/Comments.xml'
        post_history_path = '/home/mephist/postgresql/scripts/xml/PostHistory.xml'
        post_links_path = '/home/mephist/postgresql/scripts/xml/PostLinks.xml'
        tags_path = '/home/mephist/postgresql/scripts/xml/Tags.xml'
        votes_path = '/home/mephist/postgresql/scripts/xml/Votes.xml'
        
        try: # Всю работу с таблицами помещаем блок в try...except на случай, если
             # пользователь забудет поменять пути к xml-файлам
            ##### Users.xml #####
            users_coloumns_list = get_all_coloumns_list(users_path)
            users_check_coloumns_list = check_all_coloumns_list(users_path)
            users_string_list = get_all_strings(users_path, users_coloumns_list)

            if len(users_coloumns_list)==len(users_check_coloumns_list):
                print('automatic insert in table: Users.Users')
                users_fields_str = '('+(', ').join(users_coloumns_list)+')'
                users_format_str = '%s, '*len(users_coloumns_list) # чтобы вручную не прописывать много раз %s
                users_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                        fields = users_fields_str,
                        table='Users.Users',
                        form='('+users_format_str[:len(users_format_str)-2]+')' # удаление лишних символов из конца строки 
                )
                executemany_query(conn, users_query, users_string_list)
            else:
                pass # так как предыдущая проверка прошла, здесь проверка убрана в целях
                    # сокращения кода реализация блока else пропущена  


            ##### Posts.xml #####
            posts_coloumns_list = get_all_coloumns_list(posts_path)
            posts_check_coloumns_list = check_all_coloumns_list(posts_path)

            print(posts_check_coloumns_list)

            if len(posts_coloumns_list)==len(posts_check_coloumns_list):
                pass # так как предыдущая проверка прошла, здесь проверка убрана в целях
                    # сокращения кода реализация блока else пропущена  
            else:
                print('you need in a handwork with a table: Posts.Posts')

                posts_coloumns_list_true = [
                    'Id', 'PostTypeId', 'AcceptedAnswerId', 'ParentId', 'CreationDate',
                    'Score', 'ViewCount', 'Body', 'OwnerUserId', 'OwnerDisplayName',
                    'LastEditorUserId', 'LastEditorDisplayName', 'LastEditDate', 
                    'LastActivityDate', 'Title', 'Tags', 'AnswerCount', 'CommentCount',
                    'FavoriteCount', 'ClosedDate',  'CommunityOwnedDate', 'ContentLicense' 
                ] # в ходе поиска уникальных полей в xml-файле было найдено поле ContentLicense, 
                # которого нет в схеме данных. Напротив, поля DeletionDate нет в xml
                # положение поля ContentLicense в таблице Posts было найдено следующим образом:
                # всего в таблице Posts найдено 22 уникальных поля. По первой строке xml было 
                # определено 16 полей, находящихся перед полем DeletionDate. Положенеи ещё 3 полей удалось 
                # определить из схемы базы данных. Оставшиеся 2 - вручную, по файлу с помощью ctrl+f
                # В результате определено, что поле ContentLicense надо добавить в конец таблицы

                posts_string_list_true = get_all_strings(posts_path, posts_coloumns_list_true)
            
                posts_fields_str = '('+(', ').join(posts_coloumns_list_true)+')'
                posts_format_str = '%s, '*len(posts_coloumns_list_true)
                posts_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                        fields = posts_fields_str,
                        table='Posts.Posts',
                        form='('+posts_format_str[:len(posts_format_str)-2]+')' 
                )
                executemany_query(conn, posts_query, posts_string_list_true)

            posts_insert_query = '''
                INSERT INTO Posts.Posts (id) 
                SELECT DISTINCT AcceptedAnswerId FROM Posts.Posts
                WHERE AcceptedAnswerId NOT IN (SELECT id FROM Posts.Posts); 
            ''' # в поле AcceptedAnswerId таблицы Posts есть записи со значениями,
                # которых нет в поле id таблицы Posts, но при этом в поле 
                # AcceptedAnswerId есть ограничение в виде внешнего ключа на Posts.id
                # Во избежание потерь данных в Posts.id добавлены уникальные 
                # из Posts.AcceptedAnswerId, которых нет в Posts.id
            execute_query(conn, posts_insert_query)

            posts_alter_query = '''
                ALTER TABLE Posts.Posts
                ADD CONSTRAINT fk_AcceptedAnswerId 
                FOREIGN KEY(AcceptedAnswerId) REFERENCES Posts.Posts(id);
            ''' # Поле AcceptedAnswerId ссылается на поле id этой же таблицы.
                # Если прописать сразу внешний ключ, будет ошибка, так как 
                # таблица Posts еще не содержит данные и внешний ключ ссылается вникуда
                # Поэтому сначала надо заполнить таблицу Posts данными, а потом через
                # ALTER обновить поле AcceptedAnswerId, добавив ограничение в виде
                # внешнего ключа

            execute_query(conn, posts_alter_query)

            users_insert_query = '''
                INSERT INTO Users.Users (id) 
                SELECT DISTINCT OwnerUserId, LastEditorUserId FROM Posts.Posts
                WHERE OwnerUserId NOT IN (SELECT id FROM Users.Users) AND LastEditorUserId NOT IN (SELECT id FROM Users.Users); 
            ''' # в полях LastEditorUserId, OwnerUserId таблицы Posts есть записи со значениями,
                # которых нет в поле id таблицы Users, но при этом в поле 
                # LastEditorUserId, OwnerUserId есть ограничение в виде внешнего ключа на Users.id
                # Во избежание потерь данных в Users.id добавлены уникальные 
                # из Posts.OwnerUserId, которых нет в Users.id
            execute_query(conn, users_insert_query)

            users_alter_query = '''
                ALTER TABLE Posts.Posts
                ADD CONSTRAINT fk_OwnerUserId
                FOREIGN KEY(OwnerUserId) REFERENCES Users.Users(id),
                ADD CONSTRAINT fk_LastEditorUserId
                FOREIGN KEY(LastEditorUserId) REFERENCES Users.Users(id);
            ''' # Теперь после добвления данных в Users.id можно
                # можно добавить внешний ключ
            execute_query(conn, users_alter_query)


            ##### Badges.xml #####
            badges_coloumns_list = get_all_coloumns_list(badges_path)
            badges_check_coloumns_list = check_all_coloumns_list(badges_path)
            badges_string_list = get_all_strings(badges_path, badges_coloumns_list)

            if len(badges_coloumns_list)==len(badges_check_coloumns_list):
                print('automatic insert in table: Users.Badges')
                badges_fields_str = '('+(', ').join(badges_coloumns_list)+')'
                badges_format_str = '%s, '*len(badges_coloumns_list)
                badges_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                        fields = badges_fields_str,
                        table='Users.Badges',
                        form='('+badges_format_str[:len(badges_format_str)-2]+')'
                )
                executemany_query(conn, badges_query, badges_string_list)
            else:
                pass # так как предыдущая проверка прошла, здесь проверка убрана в целях
                    # сокращения кода реализация блока else пропущена   

            badges_insert_query = '''
                INSERT INTO Users.Users (id) 
                SELECT DISTINCT UserId FROM Users.Badges
                WHERE UserId NOT IN (SELECT id FROM Users.Users); 
            ''' # Комментарий аналогичен предыдущим
            execute_query(conn, badges_insert_query)

            badges_alter_query = '''
                ALTER TABLE Users.Badges
                ADD CONSTRAINT fk_UserId
                FOREIGN KEY(UserId) REFERENCES Users.Users(id);
            ''' # Комментарий аналогичен предыдущим
            execute_query(conn, badges_alter_query)


            ##### Comments.xml #####
            comments_coloumns_list = get_all_coloumns_list(comments_path)
            comments_check_coloumns_list = check_all_coloumns_list(comments_path)
            comments_string_list = get_all_strings(comments_path, comments_coloumns_list)

            if len(comments_coloumns_list)==len(comments_check_coloumns_list):
                pass # так как проверка не прошла, она убрана в целях
                    # сокращения кода. Реализация блока else представлена ниже
            else:
                print('you need in a handwork with a table: Posts.Comments')

                comments_coloumns_list_true = [
                    'Id', 'PostId', 'Score', 'Text', 'CreationDate', 
                    'UserDisplayName', 'UserId'
                ] 

                comments_string_list_true = get_all_strings(comments_path, comments_coloumns_list_true)
            
                comments_fields_str = '('+(', ').join(comments_coloumns_list_true)+')'
                comments_format_str = '%s, '*len(comments_coloumns_list_true)
                comments_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                        fields = comments_fields_str,
                        table='Posts.Comments',
                        form='('+comments_format_str[:len(comments_format_str)-2]+')' 
                )
                executemany_query(conn, comments_query, comments_string_list_true)

            comments_insert_query = '''
                INSERT INTO Posts.Posts (id) 
                SELECT DISTINCT PostId FROM Posts.Comments
                WHERE PostId NOT IN (SELECT id FROM Posts.Posts); 
            ''' # Комментарий аналогичен предыдущим
            execute_query(conn, comments_insert_query)

            badges_alter_query = '''
                ALTER TABLE Posts.Comments
                ADD CONSTRAINT fk_PostId
                FOREIGN KEY(PostId) REFERENCES Posts.Posts(id);
            ''' # Комментарий аналогичен предыдущим
            execute_query(conn, badges_alter_query)


            ##### PostHistory.xml #####
            post_history_coloumns_list = get_all_coloumns_list(post_history_path)
            post_history_check_coloumns_list = check_all_coloumns_list(post_history_path)
            post_history_string_list = get_all_strings(post_history_path, post_history_coloumns_list)

            if len(post_history_coloumns_list)==len(post_history_check_coloumns_list):
                pass
            else:
                print('you need in a handwork with a table: History.PostHistory')

                post_history_coloumns_list_true = [
                    'Id', 'PostHistoryTypeId', 'PostId', 'RevisionGUID', 'CreationDate', 
                    'UserId', 'UserDisplayName', 'Comment', 'Text', 'ContentLicense'
                ] 

                post_history_string_list_true = get_all_strings(post_history_path, post_history_coloumns_list_true)
            
                post_history_fields_str = '('+(', ').join(post_history_coloumns_list_true)+')'
                post_history_format_str = '%s, '*len(post_history_coloumns_list_true)
                post_history_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                        fields = post_history_fields_str,
                        table='History.PostHistory',
                        form='('+post_history_format_str[:len(post_history_format_str)-2]+')' 
                )
                executemany_query(conn, post_history_query, post_history_string_list_true)

            post_history_insert_query = '''
                INSERT INTO Posts.Posts (id) 
                SELECT DISTINCT PostId FROM History.PostHistory
                WHERE PostId NOT IN (SELECT id FROM Posts.Posts); 
            ''' # Комментарий аналогичен предыдущим
            execute_query(conn, post_history_insert_query)

            post_history_alter_query = '''
                ALTER TABLE History.PostHistory
                ADD CONSTRAINT fk_PostId
                FOREIGN KEY(PostId) REFERENCES Posts.Posts(id);
            ''' # Комментарий аналогичен предыдущим
            execute_query(conn, post_history_alter_query)

            post_history_insert_query_2 = '''
                INSERT INTO Users.Users (id) 
                SELECT DISTINCT UserId FROM History.PostHistory
                WHERE UserId NOT IN (SELECT id FROM Users.Users) AND UserId IS NOT NULL; 
            ''' # Комментарий аналогичен предыдущим
            execute_query(conn, post_history_insert_query_2)

            post_history_alter_query_2 = '''
                ALTER TABLE History.PostHistory
                ADD CONSTRAINT fk_UserId
                FOREIGN KEY(UserId) REFERENCES Users.Users(id);
            ''' # Комментарий аналогичен предыдущим
            execute_query(conn, post_history_alter_query_2)


            ##### PostLinks.xml #####
            post_links_coloumns_list = get_all_coloumns_list(post_links_path)
            post_links_check_coloumns_list = check_all_coloumns_list(post_links_path)
            post_links_string_list = get_all_strings(post_links_path, post_links_coloumns_list)

            if len(post_links_coloumns_list)==len(post_links_check_coloumns_list):
                print('automatic insert in table: Posts.PostLinks')
                post_links_fields_str = '('+(', ').join(post_links_coloumns_list)+')'
                post_links_format_str = '%s, '*len(post_links_coloumns_list)
                post_links_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                        fields = post_links_fields_str,
                        table='Posts.PostLinks',
                        form='('+post_links_format_str[:len(post_links_format_str)-2]+')'
                )
                executemany_query(conn, post_links_query, post_links_string_list)
            else:
                pass

            post_links_insert_query = '''
                INSERT INTO Posts.Posts (Id)
                SELECT DISTINCT PostId, RelatedPostId FROM Posts.PostLinks
                WHERE PostId NOT IN (SELECT Id FROM Posts.Posts) AND RelatedPostId NOT IN (SELECT Id FROM Posts.Posts)
            '''
            post_links_alter_query = '''
                ALTER TABLE Posts.PostLinks
                ADD CONSTRAINT fk_PostId
                FOREIGN KEY(PostId) REFERENCES Posts.Posts(Id),
                ADD CONSTRAINT fk_RelatedPostId
                FOREIGN KEY(RelatedPostId) REFERENCES Posts.Posts(Id);
            '''
            execute_query(conn, post_links_alter_query)


            ##### Tags.xml #####
            tags_coloumns_list = get_all_coloumns_list(tags_path)
            tags_check_coloumns_list = check_all_coloumns_list(tags_path)
            tags_string_list = get_all_strings(tags_path, tags_coloumns_list)

            if len(tags_coloumns_list)==len(tags_check_coloumns_list):
                pass
            else:
                print('you need in a handwork with a table: Tags.Tags')

                tags_coloumns_list_true = [
                    'Id', 'TagName', 'Count', 'ExcerptPostId', 
                    'WikiPostId', 'IsModeratorOnly', 'IsRequired' 
                ] 

                tags_string_list_true = get_all_strings(tags_path, tags_coloumns_list_true)
            
                tags_fields_str = '('+(', ').join(tags_coloumns_list_true)+')'
                tags_format_str = '%s, '*len(tags_coloumns_list_true)
                tags_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                        fields = tags_fields_str,
                        table='Tags.Tags',
                        form='('+tags_format_str[:len(tags_format_str)-2]+')' 
                )
                executemany_query(conn, tags_query, tags_string_list_true)

            tags_insert_query = '''
                INSERT INTO Posts.Posts (Id)
                SELECT DISTINCT ExcerptPostId, WikiPostId FROM Tags.Tags
                WHERE ExcerptPostId NOT IN (SELECT Id FROM Posts.Posts) AND WikiPostId NOT IN (SELECT Id FROM Posts.Posts)
            '''
            tags_alter_query = '''
                ALTER TABLE tags.tags
                ADD CONSTRAINT fk_ExcerptPostId
                FOREIGN KEY(ExcerptPostId) REFERENCES Posts.Posts(Id), 
                ADD CONSTRAINT fk_WikiPostId
                FOREIGN KEY(WikiPostId) REFERENCES Posts.Posts(Id);
            '''
            execute_query(conn, tags_alter_query)


            ##### Votes.xml ####
            votes_coloumns_list = get_all_coloumns_list(votes_path)
            votes_check_coloumns_list = check_all_coloumns_list(votes_path)
            votes_string_list = get_all_strings(votes_path, votes_coloumns_list)

            if len(votes_coloumns_list)==len(votes_check_coloumns_list):
                print('automatic insert in table: Votes.Votes')
                votes_fields_str = '('+(', ').join(votes_coloumns_list)+')'
                votes_format_str = '%s, '*len(votes_coloumns_list)
                votes_query = ('INSERT INTO {table} {fields} VALUES {form};').format(
                        fields = votes_fields_str,
                        table='Votes.Votes',
                        form='('+votes_format_str[:len(votes_format_str)-2]+')'
                )
                executemany_query(conn, votes_query, votes_string_list)
            else:
                pass

            votes_insert_query = '''
                INSERT INTO Posts.Posts (id) 
                SELECT DISTINCT PostId FROM Votes.Votes
                WHERE PostId NOT IN (SELECT id FROM Posts.Posts); 
            '''
            execute_query(conn, votes_insert_query)

            votes_alter_query = '''
                ALTER TABLE Votes.Votes
                ADD CONSTRAINT fk_PostId 
                FOREIGN KEY(PostId) REFERENCES Posts.Posts(id);
            ''' 
            execute_query(conn, votes_alter_query)
        except FileNotFoundError:
            print('Неправильный путь к xml-файлам!')
    else:
        print('Error in connection!')
        
    conn.close()
```


**Описание структуры:**\
**3.1 Общий принцип работы:** для каждой таблицы делается проверка на равенство списков, возвращённых функциями get_all_coloumns_list и check_all_coloumns_list.\
Если она проходит, то происходит автоматическая вставка данных в таблицу. При этом структура форматных строк (например, количество плейсхолдеров %s) выстраивается автоматически по длине списка с полным набором полей, чтобы не прописывать вручную.\
Если проверка не проходит, полный список полей, возвращенный функцией check_all_coloumns_list сортируется вручную в порядке, указанном в схеме данных, а затем происходит добавление даннных в таблицу.\
Далее в полях, которые должны иметь ограничение внешнего ключа, ищутся значения, которых нет в тех полях, на которые они ссылаются, и добавляются в них. А затем добавляется внешний ключ.\
&ensp;**Внимание!** в этом скрипте необходимо поменять пути к xml-файлам при запуске на своей локальной машине. На случай, если пользователь забудет это сделать, вся работа с таблицами помещена в блок try...except(отлавливается исключение FileNotFoundError)

### **Инструкция по запуску**
