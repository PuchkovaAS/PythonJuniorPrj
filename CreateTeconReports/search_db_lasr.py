
import os
path = "C:\_Scada\DB"
# path = 'C:\\_Scada\\Scada.Archive\\Arc\\Archive\\202104\\24' # Путь к вашей папке

# Получим список имен всего содержимого папки
# и превратим их в абсолютные пути
dir_list = [os.path.join(path, x) for x in os.listdir(path)]

if dir_list:
    # Создадим список из путей к файлам и дат их создания.
    date_list = [[x, os.path.getctime(x)] for x in dir_list]

    # Отсортируем список по дате создания в обратном порядке
    sort_date_list = sorted(date_list, key=lambda x: x[1], reverse=True)

    # Выведем первый элемент списка. Он и будет самым последним по дате
    print(sort_date_list[0][0])