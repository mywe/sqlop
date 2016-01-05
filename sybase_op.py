import pyodbc
import xlrd
import collections
import datetime

curDate = datetime.date() + datetime.timedelta(-1)
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
    objectNo2eNodeBId = collections.defaultdict(lambda : set())
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
                    objectNo2eNodeBId[row[0]].add('%s_%s' % (eNodeB, cellID))
    with open('objectNo2eNodeB.txt', 'w') as ff:
        for key, val in objectNo2eNodeBId.items():
            ff.write('%s:'%(str(key)))
            ff.write('%s\n'%(','.join(val)))
        ff.close()
    return objectNo


def get_cntM(settingInfo, Id, CntM):
    conn = pyodbc.connect(settingInfo)
    cursor = conn.cursor()
    sqlStr = 'select FunctionSubSetId from systbl_Counters where CounterId=%d'%(Id)
    cursor.execute(sqlStr)
    fcId = cursor.fetchall()
    FuncId = set()
    for fc in fcId:
        FuncId.add(fc)

    dateStr = curDate.strftime('%Y-%m-%d')
    for fc in FuncId:
        sqlStr = 'select ObjectNo, Counter_%d from tbl_Result_%d_3 where StartTime>=\'%s 00:00\' and StartTime<=\'%s 23:00\''%(Id, fc, dateStr, dateStr)
        cursor.execute(sqlStr)
        cntInfos = cursor.fetchall()
        for info in cntInfos:
            CntM[info[0]] += info[1]


def get_rrc_erab(objectNo, settingFiles):
    ids = {'rrc':{1526726659:1526726658}, 'erab':{1526727544:1526727545}}
    rcc_succM = collections.defaultdict(lambda : 0)
    rcc_connM = collections.defaultdict(lambda : 0)
    erab_succM = collections.defaultdict(lambda : 0)
    erab_connM = collections.defaultdict(lambda : 0)

    for fName in settingFiles:
        settingInfo = ''
        with open(fName, 'r') as sF:
            settingInfo = sF.readline()
            settingInfo = settingInfo.replace('\r', '')
            settingInfo = settingInfo.replace('\n', '')
            sF.close()

        for key, val in ids.items():
            if key == 'rrc':
                for k, v in val.items():
                    get_cntM(settingInfo, k, rcc_succM)
                    get_cntM(settingInfo, v, rcc_connM)
            elif key == 'erab':
                for k, v in val.items():
                    get_cntM(settingInfo, k, erab_succM)
                    get_cntM(settingInfo, v, erab_connM)
    dateStr = curDate.strftime('%Y%m%d')
    outputSuccRate('%s_rcc.txt'%(dateStr), objectNo, rcc_succM, rcc_connM)
    outputSuccRate('%s_erab.txt'%(dateStr), objectNo, erab_succM, erab_connM)


def outputSuccRate(fName, objectNo, succM, connM):
    with open(fName, 'w') as ff:
        for no in objectNo:
            rate = float(succM[no]) / connM[no] if connM[no] != 0 else 0
            ff.write('%d,%f\n'%(int(no), rate))
        ff.close()


settingFiles = list()
with open('sybase_setting', 'r') as ff:
    settingFiles = ff.readlines()
    ff.close()
nFile = len(settingFiles)
for i in range(nFile):
    settingFiles[i] = settingFiles[i].replace('\r', '')
    settingFiles[i] = settingFiles[i].replace('\n', '')
objectNo = get_ObjectNoOfeNodeBId('华为', settingFiles[:2])
get_rrc_erab(objectNo, settingFiles[2:])

'''conn = pyodbc.connect('DSN=pmdb_84;UID=wxit;PWD=Wxit1234*')
cursor = conn.cursor()

sqlStr = 'select ObjectNo, ObjectMemName0 from tbl_ObjectInstance where ObjectTypeId=1526726657 and ObjectMemName0 like \'%eNodeB%\''
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
#	ff.close()'''
