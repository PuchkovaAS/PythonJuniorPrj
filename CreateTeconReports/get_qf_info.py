import os
import sqlite3
from dataclasses import dataclass

import firebirdsql

path_db = "C:\\_Scada\\DB\\01 KS4 Nimnyrskay\\KS4\\06.02.21\\SCADABD.GDB"


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
                    result.append(
                        ObjectInfo(marka=marka, name=name, id=id, type_info=object_type, fdb_cur=fdb_cur))

            except:
                result = []

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
    name_db = 'result2.db'
    path_db_res = os.path.join(os.getcwd(), name_db)

    def __init__(self, path_result, path_fbd, server):
        self.path_fbd = path_fbd
        self.server = server
        if os.path.exists(path_result):
            try:
                os.remove(path_result)
            except OSError:
                # print('База открыта')
                raise ValueError("База открыта")
                exit()

        self.conn = sqlite3.connect(path_result)
        """ Создание БД в памяти """
        # self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()
        self.create_new_table()

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

    def create_new_table(self):
        # создание таблиц
        self.cursor.executescript("""
        			BEGIN TRANSACTION;
        			CREATE TABLE "ResultTable" (
        				`id`    INTEGER PRIMARY KEY AUTOINCREMENT,
        				`id_marka`    INTEGER,   
        				`Marka`    TEXT,
        				`KLASS`    TEXT,
        				`TYPE`    TEXT,
        				`PLC_NAME`    TEXT,
        				`ISA`    BOOLEAN,
        				`Pages`    TEXT,
        				`ISA_Pages`    TEXT,
        				`ST_prog`    TEXT
        			);

        			CREATE TABLE "AdditionalTable" (
	"id_marka"	INTEGER,
	"NAME"	TEXT,
	"DISC"	TEXT,
	"OBJSIGN"	TEXT,
	"OBJNUMBER"	TEXT,
	"PLC_VARNAME"	TEXT,
	"ARH_PER"	TEXT,
	"KKS"	TEXT,
	"OBJDPARAM"	TEXT,
	"SREZCONTROL"	TEXT,
	"EVGROUP"	TEXT,
	"PLC_ADRESS"	TEXT,
	"PLC_GR"	TEXT,
	"TEMPLATE"	TEXT,
	FOREIGN KEY("id_marka") REFERENCES "ResultTable"("id_marka")
);

        			COMMIT;
        		""")

        # фиксирую коммит
        self.conn.commit()

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

        object_types_id = self.get_object_type_info(object_types=['QF_0x3', 'QF_0x2'], chanel_list=[
            ['.MsgStOn', '.MsgStOff', '.MsgStUnc', '.MsgStDbl', '.MsgStInvalid', '.MsgEOff', '.MsgInvalidEOff'],
            ['.MsgOn', '.MsgOff', '.MsgUnc', '.MsgDbl', '.MsgInvalid']])

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

    def add_addition_row(self, id_marka=0):
        self.fdb_cur.execute(
            f"select NAME, DISC, OBJSIGN, OBJNUMBER, PLC_VARNAME, ARH_PER, KKS, OBJDPARAM, SREZCONTROL, EVKLID, PLC_ADRESS, PLC_GR, OBJTYPEID from CARDS where id = {id_marka}")
        NAME, DISC, OBJSIGN, OBJNUMBER, PLC_VARNAME, ARH_PER, KKS, OBJDPARAM, SREZCONTROL, EVKLID, PLC_ADRESS, PLC_GR, OBJTYPEID = \
            self.fdb_cur.fetchall()[0]
        EVGROUP = self.evklid.pages_dict_SQL[EVKLID].NAME
        self.fdb_cur.execute(
            f"select ISAOBJ.NAME from CARDS join   OBJTYPE on CARDS.OBJTYPEID = OBJTYPE.ID join   ISAOBJ on ISAOBJ.ID = OBJTYPE.DEFPOUID where CARDS.ID = {id_marka}")

        TEMPLATE = self.fdb_cur.fetchall()[0][0]
        self.fdb_cur.execute(f"select RESOURCE_NUM from RESOURCES where id = {PLC_GR}")
        try:
            PLC_GROUP = self.fdb_cur.fetchall()[0][0]
        except:
            PLC_GROUP = PLC_GR

        self.cursor.execute(
            f"""INSERT INTO `AdditionalTable`  (id_marka, NAME, DISC, OBJSIGN, OBJNUMBER, PLC_VARNAME, ARH_PER, KKS, OBJDPARAM, SREZCONTROL, EVGROUP, PLC_ADRESS, PLC_GR, TEMPLATE)  VALUES({id_marka}, '{NAME if NAME and NAME != 'None' else ''}', '{DISC if DISC and DISC != 'None' else ''}', '{OBJSIGN if OBJSIGN and OBJSIGN != 'None' else ''}', '{OBJNUMBER if OBJNUMBER and OBJNUMBER != 'None' else ''}', '{PLC_VARNAME if PLC_VARNAME and PLC_VARNAME != 'None' else ''}', '{ARH_PER if ARH_PER and ARH_PER != 'None' else ''}', '{KKS if KKS and KKS != 'None' else ''}', '{OBJDPARAM if OBJDPARAM and OBJDPARAM != 'None' else ''}', '{SREZCONTROL if SREZCONTROL and SREZCONTROL != 'None' else ''}', '{EVGROUP if EVGROUP and EVGROUP != 'None' else ''}', '{PLC_ADRESS if PLC_ADRESS and PLC_ADRESS != 'None' else ''}', '{PLC_GROUP if PLC_GROUP and PLC_GROUP != 'None' else ''}', '{TEMPLATE if TEMPLATE and TEMPLATE != 'None' else ''}');""")
        self.conn.commit()

    def add_new_row(self, id_marka=0, Marka='', ISA=False, KLASS='', TYPE='', PLC_NAME='', Pages='', ISA_Pages='',
                    ST_prog=''):
        self.cursor.execute(
            f"""INSERT INTO `ResultTable`  (id_marka, Marka, KLASS, TYPE, PLC_NAME, ISA, Pages, ISA_Pages, ST_prog)  VALUES({id_marka},'{Marka}', '{KLASS}', '{TYPE}', '{PLC_NAME}', {ISA}, '{Pages}', '{ISA_Pages}', '{ST_prog}');""")
        self.conn.commit()

    def show_table(self):
        self.cursor.execute("""
                    SELECT 
                        *
                    FROM 
                        ResultTable 
               		""")

        print(self.cursor.fetchall())


if __name__ == '__main__':
    new_BD = DBResult(path_result='result.db', path_fbd=path_db, server='127.0.0.1')
    new_BD.firebird_db_init()
