import pyodbc
import xlrd


def get_eNodeBID(fct):
    res = list()
    workbook = xlrd.open_workbook('佛山无线中心LET工参-20151203.xlsx')
    sheet = workbook.sheets()[1]
    for r in range(1, sheet.nrows):
        if sheet.cell_value(r, 2) != fct:
            continue
        enodeb_id = ''
        cell_id = ''
        try:
            enodeb_id = str(sheet.cell_value(r, 7))
            cell_id = str(sheet.cell_value(r, 8))
        except:
            continue
        res.append('%s_%s' % (enodeb_id, cell_id))
    return res


def get_ObjectNoOfeNodeBId(fct, settingFiles):
    objectNo = set()
    eNodeBIds = get_eNodeBID(fct)
    for fName in settingFiles:
        settingInfo = ''
        sqlStr = ''
        with open(fName, 'r') as sF:
            for line in sF.readlines():
                line = line.replace('\n', '')
                line = line.replace('\r', '')
                infos = line.split(':')

                if infos[0] == 'settingInfo':
                    settingInfo = infos[1]
                elif infos[0] == 'sqlStr':
                    sqlStr = infos[1]
            sF.close()
        conn = pyodbc.connect(settingInfo)
        cursor = conn.cursor()
        cursor.execute(sqlStr)
        rows = cursor.fetchall()
        for row in rows:
            objectMemName = str(row[1])
            if 'eNodeB标识' in objectMemName:
                L = objectMemName.split(',')
                eNodeB = ''
                cellID = ''
                for l in L:
                    if 'eNodeB标识' in l:
                        eNodeB = l.split('=')[1]
                    elif '本地小区标识' in l:
                        cellID = l.split('=')[1]
                if '%s_%s' % (eNodeB, cellID) in eNodeBIds:
                    objectNo.add(row[0])
    return objectNo

def get_rrc_erab(objectNo, settingFiles):
    ids = {'rrc':{1526726659:1526726658}, 'erab':{1526727544:1526727545}}
    for fName in settingFiles:
        settingInfo = ''
        with open(fName, 'r') as sF:
            settingInfo = sF.readline()
            settingInfo = settingInfo.replace('\r', '')
            settingFiles = settingInfo.replace('\n', '')
            sF.close()
        conn = pyodbc.connect(settingInfo)
        cursor = conn.cursor()
        for key, val in ids.items():
            if key == 'rrc':
                pass
            elif key == 'erab':
                pass


conn = pyodbc.connect('DSN=pmdb_84;UID=wxit;PWD=Wxit1234*')
cursor = conn.cursor()

sqlStr = 'select ObjectTypeId, ObjectNo, ObjectMemName0 from tbl_ObjectInstance where ObjectTypeId=1526726657 and ObjectMemName0 like \'%eNodeB%\''
sqlStr = 'select id, name from sysobjects where type=\'U\' and name=\'systbl_Counters\''
sqlStr = 'select * from syscolumns where id=1877760353'
sqlStr = 'select FunctionSubSetId from systbl_Counters where CounterId=1526727544'
# sqlStr = 'select top 500 * from tbl_ObjectInstance'
cursor.execute(sqlStr)

rows = cursor.fetchall()
for row in rows:
    print(row)
# with open('result.txt', 'w') as ff:
#	for row in rows:
#		ff.write('%s\n'%(str(row)))
#	ff.close()
