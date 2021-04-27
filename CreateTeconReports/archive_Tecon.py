import datetime
import sqlite3

path_archive = "C:\\_Scada\\Scada.Archive\\Arc\\Archive\\202104\\27\\archive_20210427_0000_001.arc"
conn = sqlite3.connect(path_archive)
cursor = conn.cursor()

state = {4437: 'On', 4438: 'Off', 4439: 'Unc', 4440: 'Dbl', 4441: 'Invalid'}

# [135539,135540,135541,135538]
for id in [135539,135540,135541,135538]:
    select = f"""SELECT Data FROM ArchiveData where Tagid = {id} ORDER BY StoredTime DESC LIMIT 1"""
    cursor.execute(select)
    res = cursor.fetchall()
    print(str(res[0][0]).split('\\x')[-1])

