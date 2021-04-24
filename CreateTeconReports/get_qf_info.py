import os
import sqlite3
from dataclasses import dataclass

import firebirdsql

path_db = "C:\\_Scada\\DB\\06.02.21\\SCADABD.GDB"


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


class QFSearch:
    """

    """

    def __init__(self, fdb_cur, object_types: list, object_chanels: list, id_klass: int):
        """

        :param object_types:
        :param object_chanels:
        :param id_klass:
        """
        self.fdb_cur = fdb_cur
        marks = self.get_all_marks(object_types, id_klass)
        return marks

    def get_all_marks(self, object_types, id_klass):
        """

        :param object_types:
        :param id_klass:
        :return:
        """
        select = f"select ID, MARKA, NAME from CARDS  where klid = {id_klass} and objtypeid in ({','.join(object_types)})"
        self.fdb_cur.execute(select)
        # TODO
        return self.fdb_cur.fetchall()[0]

    def get_id_texobj(self, name_texobj):
        select = f"select ID from CARDS where MARKA = '{name_texobj}' and PLC_GR in (select id from RESOURCES where name='{self.plc_res.text()}' and cardid in (select id from CARDS where marka = '{self.plc_name.text()}'))"
        self.cur.execute(select)

        try:
            id_tex = [id[0] for id in self.cur][0]
        except:
            id_tex = name_texobj
        return id_tex

    def get_id_params(self, texobj_id, param_name):
        select = f"select CARDPARAMS.ID from CARDPARAMS join OBJTYPEPARAM on CARDPARAMS.OBJTYPEPARAMID = OBJTYPEPARAM.ID " \
                 f"where CARDPARAMS.CARDID = {texobj_id} and  OBJTYPEPARAM.NAME = '{param_name}' and CARDPARAMS.PLC_GR in (select id from RESOURCES where name='{self.plc_res.text()}' and cardid in (select id from CARDS where marka = '{self.plc_name.text()}'))"
        self.cur.execute(select)
        try:
            id_tex = [id[0] for id in self.cur][0]
        except:
            id_tex = param_name
        return id_tex

    def get_id_texobj_param(self, name):
        texobj_name, param_name = name.split('.', 1)
        texobj_id = self.get_id_texobj(texobj_name)
        if texobj_id != texobj_name:
            param_id = self.get_id_params(texobj_id, param_name)
            return param_id
        return param_name


class DBResult:
    name_db = 'result2.db'
    path_db_res = os.path.join(os.getcwd(), name_db)

    # __slots__ = []

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

    def get_object_type_id(self, object_types):
        """

        :param object_types:
        :return:
        """
        select = f"""select ID from OBJTYPE  where NAME in ('{"', '".join(object_types)}')"""
        print(select)
        self.fdb_cur.execute(select)

        result = [str(id[0]) for id in self.fdb_cur.fetchall()]

        return result


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

        object_types_id = self.get_object_type_id(['QF_0x3','QF_0x2'])


        electricity = []
        for klass in klid.pages_dict_SQL.keys():
            klass_data = klid.pages_dict_SQL[klass]
            if "Электроснабжение" in klass_data.NAME:
                electricity.append(klass_data)
                qf_mar = QFSearch(fdb_cur=self.fdb_cur, id_klass=klass_data.ID, object_types=object_types_id, object_chanels=[])
                print('fsd')


        print('Выполнено')

    def get_type(self, OBJTYPEID):
        self.fdb_cur.execute(
            f"select NAME from OBJTYPE where id = {OBJTYPEID}")
        return self.fdb_cur.fetchall()[0][0]

    def find_id_plc(self, plc_name='ICore_2'):
        self.fdb_cur.execute(f"select ID from CARDS where MARKA = '{plc_name}'")
        return self.fdb_cur.fetchall()[0][0]

    def show(self):
        self.cursor.execute("""
                    SELECT 
                        name
                    FROM 
                        sqlite_master 
                    WHERE 
                        type ='table' AND 
                        name NOT LIKE 'sqlite_%';
               		""")

        print(self.cursor.fetchall())

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

    def find_plc_source(self, plc_adress=36):
        self.fdb_cur.execute(f"""select MARKA from CARDS where plc_adress = {plc_adress}""")
        return [marka[0] for marka in self.fdb_cur.fetchall()]


if __name__ == '__main__':
    new_BD = DBResult(path_result='result.db', path_fbd=path_db, server='127.0.0.1')
    new_BD.firebird_db_init()
