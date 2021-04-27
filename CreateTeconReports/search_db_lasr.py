import os


def find_arh_path(path):
    list_path = path.split('\\')
    return f"""{path}\\archive_{list_path[-2]}{list_path[-1]}_0000_001.arc"""


def find_last_arh(path, exception=None):
    # Получим список имен всего содержимого папки
    # и превратим их в абсолютные пути
    dir_list = [os.path.join(path, x) for x in os.listdir(path)]

    if dir_list:
        # Создадим список из путей к файлам и дат их создания.
        date_list = [[x, os.path.getctime(x)] for x in dir_list if x not in exception]

        # Отсортируем список по дате создания в обратном порядке
        sort_date_list = sorted(date_list, key=lambda x: x[1], reverse=True)

        if sort_date_list:
            return find_arh_path(sort_date_list[0][0])
        else:
            return None


def find_last_db(path, extension='gdb'):
    dir_list = [os.path.join(path, x) for x in os.listdir(path)]
    if dir_list:
        # Создадим список из путей к файлам и дат их создания.
        date_list = [[x, os.path.getmtime(x)] for x in dir_list if x.split('.')[-1].lower() == extension.lower()]

        # Отсортируем список по дате создания в обратном порядке
        sort_date_list = sorted(date_list, key=lambda x: x[1], reverse=True)

        if sort_date_list:
            # Выведем первый элемент списка. Он и будет самым последним по дате
            return sort_date_list[0][0]
        else:
            return None


find_last_arh(path='C:\\_Scada\\Scada.Archive\\Arc\\Archive\\202104', exception=[])
find_last_db(path='C:\\_Scada\\DB')
