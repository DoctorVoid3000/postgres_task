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