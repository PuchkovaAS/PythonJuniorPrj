import os
from dataclasses import dataclass

import firebirdsql

path_db = "C:\\_Scada\\DB\\01 KS4 Nimnyrskay\\KS4\\06.02.21\\SCADABD.GDB"


class FindPath:

    def __init__(self, path_tecon_archive):
        self.path_tecon_archive = path_tecon_archive
        self.exeption_months = []
        self.exeption_days = []

    @staticmethod
    def get_arh_path(path):
        list_path = path.split('\\')
        return f"""{path}\\archive_{list_path[-2]}{list_path[-1]}_0000_001.arc"""

    def find_path_arch(self):
        month = self.find_last(self.path_tecon_archive, self.exeption_months)
        arh_path = None
        while arh_path is None:
            arh_path = self.find_last(month, self.exeption_days)
            if arh_path is None:
                self.exeption_months.append(month)
                self.exeption_days = []
                month = self.find_last(self.path_tecon_archive, self.exeption_months)
            if month is None:
                return None
        self.exeption_days.append(arh_path)
        return self.get_arh_path(arh_path)

    def find_last(self, path, exception=None, func = os.path.getctime):
        """

        :param path:
        :param exception:
        :param func: os.path.getctime - время создания os.path.getmtime - время изменения
        :return:
        """

        dir_list = [os.path.join(path, x) for x in os.listdir(path)]
        if dir_list:
            date_list = [[x, func(x)] for x in dir_list if x not in exception]
            sort_date_list = sorted(date_list, key=lambda x: x[1], reverse=True)
            if sort_date_list:
                return sort_date_list[0][0]
            else:
                return None

    @staticmethod
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


@dataclass
class Page:
    ID: int = 0
    PID: int = 0
    NAME: str = ''


class PagesStruct:

    def __init__(self, cursor):
        self.pages_dict_SQL = {}
        self.cursor = cursor
        self.get_all_pages()
        self.get_struct_pages()

    def get_all_pages(self):
        self.cursor.execute(f"""select ID, PID, NAME from GRPAGES""")
        self.pages_dict_SQL = {ID: Page(ID=ID, PID=PID, NAME=NAME) for ID, PID, NAME in self.cursor.fetchall()}

    def get_struct_pages(self):
        for page in self.pages_dict_SQL.values():
            while True:
                try:
                    parent = self.pages_dict_SQL[page.PID]
                    page.PID = parent.PID
                    page.NAME = f"{parent.NAME}//{page.NAME}"
                except:
                    break


class KlassStruct(PagesStruct):
    def get_all_pages(self):
        self.cursor.execute(f"""select ID, PID, NAME from KLASSIFIKATOR""")
        self.pages_dict_SQL = {ID: Page(ID=ID, PID=PID, NAME=NAME) for ID, PID, NAME in self.cursor.fetchall()}


class ObjectTypeInfo:
    """
    Класс для типа
    """

    def __init__(self, fbd_cur, object_type_name: str, object_type_chanels: list):
        """

        :param object_type_name:
        :param object_type_chanels:
        """
        self.object_type_name = object_type_name
        self.object_type_chanels = object_type_chanels
        self.object_type_id = self.get_obgect_type_id(fbd_cur)
        self.object_type_chanels_id = self.get_object_type_chanels_id(fbd_cur)

    def get_obgect_type_id(self, fbd_cur):
        select = f"""select ID from OBJTYPE  where name = '{self.object_type_name}'"""
        fbd_cur.execute(select)
        return fbd_cur.fetchall()[0][0]

    def get_object_type_chanels_id(self, fbd_cur):
        select = f"""select OBJTYPEPARAM.ID, OBJTYPEPARAM.DISC 
from OBJTYPE JOIN OBJTYPEPARAM 
ON OBJTYPE.ID = OBJTYPEPARAM.PID 
where OBJTYPE.NAME = '{self.object_type_name}' and OBJTYPEPARAM.NAME in ('{"', '".join(self.object_type_chanels)}')"""

        fbd_cur.execute(select)

        return {str(ID): DISC for ID, DISC in fbd_cur.fetchall()}


class QFSearch:
    """

    """

    def __init__(self, fdb_cur, object_types: list, id_klass: int):
        """

        :param object_types:
        :param object_chanels:
        :param id_klass:
        """
        self.list_of_object = self.get_all_marks(object_types, id_klass, fdb_cur)

    def get_all_marks(self, object_types, id_klass, fdb_cur):
        """

        :param object_types:
        :param id_klass:
        :return:
        """
        result = []
        for object_type in object_types:
            select = f"select ID, MARKA, NAME from CARDS  where klid = {id_klass} and objtypeid = {object_type.object_type_id} "
            fdb_cur.execute(select)
            try:
                db_list = fdb_cur.fetchall()
                for id, marka, name in db_list:
                    result.update(
                        {marka: ObjectInfo(marka=marka, name=name, id=id, type_info=object_type, fdb_cur=fdb_cur)})

            except:
                result = {}

        return result


class ObjectInfo:

    def __init__(self, marka, name, id, type_info, fdb_cur):
        self.marka = marka
        self.name = name
        self.id = id
        self.chanels = self.get_chanels(type_info, fdb_cur)

    def get_chanels(self, type_info, fdb_cur):
        select = f"""select objtypeparamid, id from cardparams  where cardid = {self.id} and  objtypeparamid in ('{"', '".join(type_info.object_type_chanels_id.keys())}')"""
        fdb_cur.execute(select)
        return {id: type_info.object_type_chanels_id[str(typeid)] for typeid, id in fdb_cur.fetchall()}


class DBResult:

    def __init__(self, data_base_path, arh_path):

        self.find_path = FindPath
        self.path_fbd = self.find_path.find_last_db(data_base_path)
        self.server = '127.0.0.1'

    def get_object_type_info(self, object_types: list, chanel_list: list) -> list:
        """

        :param chanel_list:
        :param object_types:
        :return:
        """
        result_list = []
        for type, chanels in zip(object_types, chanel_list):
            result_list.append(ObjectTypeInfo(fbd_cur=self.fdb_cur, object_type_name=type, object_type_chanels=chanels))

        return result_list

    def firebird_db_init(self):
        self.fdb_conn = firebirdsql.connect(
            host=self.server,
            database=self.path_fbd,
            port=3050,
            user='sysdba',
            password='masterkey',
            charset='utf8'
        )
        self.fdb_cur = self.fdb_conn.cursor()

        # определяем классификаторы
        klid = KlassStruct(self.fdb_cur)

        object_types_id = self.get_object_type_info(object_types=['QF_0x3', 'QF_0x2', 'BOX_VV'], chanel_list=[
            ['.MsgStOn', '.MsgStOff', '.MsgStUnc', '.MsgStDbl', '.MsgStInvalid', '.MsgEOff', '.MsgInvalidEOff'],
            ['.MsgOn', '.MsgOff', '.MsgUnc', '.MsgDbl', '.MsgInvalid'],
            ['.Msg_Q_Dbl', '.Msg_Q_DU_Dbl', '.Msg_Q_DU_Invalid', '.Msg_Q_DU_Off', '.Msg_Q_DU_On', '.Msg_Q_DU_Unc',
             '.Msg_Q_Invalid', '.Msg_Q_Off', '.Msg_Q_On', '.Msg_Q_Unc', '.Msg_QS_Dbl', '.Msg_QS_Invalid', '.Msg_QS_Off',
             '.Msg_QS_On', '.Msg_QS_Test', '.Msg_QS_Unc']])

        electricity = []
        data_firebird = {}

        for klass in klid.pages_dict_SQL.keys():
            klass_data = klid.pages_dict_SQL[klass]
            if "Электроснабжение" in klass_data.NAME:
                electricity.append(klass_data)
                list_of_object = QFSearch(fdb_cur=self.fdb_cur, id_klass=klass_data.ID,
                                          object_types=object_types_id).list_of_object
                if list_of_object:
                    data_firebird.update({klass_data.NAME: list_of_object})

        print('Выполнено')

    def get_type(self, OBJTYPEID):
        self.fdb_cur.execute(
            f"select NAME from OBJTYPE where id = {OBJTYPEID}")
        return self.fdb_cur.fetchall()[0][0]


if __name__ == '__main__':
    # new_BD = DBResult(data_base_path='C:\\_Scada\\DB\\01 KS4 Nimnyrskay\\KS4\\06.02.21',
    #                   arh_path='C:\\_Scada\\Scada.Archive\\Arc\\Archive')
    # new_BD.firebird_db_init()


    test_path = FindPath(path_tecon_archive='C:\\_Scada\\Scada.Archive\\Arc\\Archive')
    test = True
    while test:
        test = test_path.find_path_arch()
        print(test)
