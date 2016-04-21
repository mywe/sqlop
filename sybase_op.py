import pyodbc
import xlrd
import collections
import datetime
from decimal import Decimal
from mysql import connector

def get_eNodeBID(fct):
    res = list()
    workbook = xlrd.open_workbook('佛山无线中心LTE工参.xlsx')
    sheet = workbook.sheets()[1]
    for r in range(1, sheet.nrows):
        if sheet.cell_value(r, 2) != fct:
            continue
        enodeb_id = ''
        cell_id = ''
        try:
            enodeb_id = str(int(sheet.cell_value(r, 7)))
            cell_id = str(int(sheet.cell_value(r, 8)))
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
        with open(fName.split(':')[1], 'r') as sF:
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


def get_cntM(pmcomdb_settingInfo, pmdb_settingInfo, Id, CntM, date):
    conn_pmcomdb = pyodbc.connect(pmcomdb_settingInfo)
    cursor_pmcomdb = conn_pmcomdb.cursor()
    sqlStr = 'select FunctionSubSetId from systbl_Counters where CounterId=%d'%(Id)
    try:
        cursor_pmcomdb.execute(sqlStr)
    except:
        return
    fcId = cursor_pmcomdb.fetchall()
    FuncId = set()
    for fc in fcId:
        FuncId.add(fc[0])

    conn_pmdb = pyodbc.connect(pmdb_settingInfo)
    cursor_pmdb = conn_pmdb.cursor()
    dateStr = date.strftime('%Y-%m-%d')
    cntInfosList = list()
    for fc in FuncId:
        sqlStr = 'select ObjectNo, Counter_%d, StartTime from tbl_Result_%d_3 where StartTime>=\'%s 00:00\' and StartTime<=\'%s 23:00\''%(Id, fc, dateStr, dateStr)
        cursor_pmdb.execute(sqlStr)
        cntInfos = cursor_pmdb.fetchall()
        cntInfosList.extend(cntInfos)
        for info in cntInfos:
            CntM[info[0]] += info[1] if isinstance(info[1], Decimal) else Decimal('0')
    return cntInfosList

def uploadRRc(rrcInfos):
    host = ''
    user = ''
    password = ''
    rrc_store_sqlStr = ''
    with open('setting/rrc_store_svr_setting.txt', 'r') as ff:
        host = ff.readline()
        user = ff.readline()
        password = ff.readline()
        rrc_store_sqlStr = ff.readline()
        ff.close()
        host = host.replace('\n', '')
        host = host.replace('\r', '')
        user = user.replace('\n', '')
        user = user.replace('\r', '')
        password = password.replace('\n', '')
        password = password.replace('\r', '')
        rrc_store_sqlStr = rrc_store_sqlStr.replace('\n', '')
        rrc_store_sqlStr = rrc_store_sqlStr.replace('\r', '')

    nCnt = len(rrcInfos)
    i = 0
    cnx = connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()

    while (i < nCnt):
        sqlStr = rrc_store_sqlStr
        for j in range(100):
            if i + j >= nCnt:
                break
            sqlStr += (',(\'%s\',%d,%d,%d,%d)' if j > 0 else '(\'%s\',%d,%d,%d,%d)')%(rrcInfos[i+j][0].strftime('%Y-%m-%d %H:%M'),\
                                                                              int(rrcInfos[i+j][1]),int(rrcInfos[i+j][2]),int(rrcInfos[i+j][3]),int(rrcInfos[i+j][4]))

        i += 100
        if len(sqlStr) != len(rrc_store_sqlStr):
            cursor.execute(sqlStr)
            cnx.commit()
    cursor.close()
    cnx.close()

def getObjectNo2eNodeNo():
    objectNo2eNodeNo = dict()
    with open('objectNo2eNodeB.txt', 'r') as ff:
        for line in ff.readlines():
            line = line.replace('\n', '')
            line = line.replace('\r', '')
            line = line.split(':')
            objectNo = int(line[0])
            objectNo2eNodeNo[objectNo] = dict()
            eNodeNo = line[1].split(',')[0].split('_')
            objectNo2eNodeNo[objectNo]['meid'] = int(eNodeNo[0])
            objectNo2eNodeNo[objectNo]['cellid'] = int(eNodeNo[1])
        ff.close()
    return objectNo2eNodeNo

def get_rrc(rrc_succ, rrc_total):
    objectNo2eNode = getObjectNo2eNodeNo()
    rrcInfoDict = dict()
    defVal = [None, 0, 0, 0, 0]
    for succ in rrc_succ:
        if succ[0] not in objectNo2eNode:
            continue
        if succ[0] not in rrcInfoDict:
            rrcInfoDict[succ[0]] = dict()
        h = succ[2].hour
        if h not in rrcInfoDict[succ[0]]:
            val = defVal.copy()
            val[0] = succ[2]
            eNodeInfo = objectNo2eNode[succ[0]]
            val[1] = eNodeInfo['meid']
            val[2] = eNodeInfo['cellid']
            val[3] = succ[1]
            rrcInfoDict[succ[0]][h] = val
        else:
            rrcInfoDict[succ[0]][h][3] += succ[1]

    for total in rrc_total:
        if total[0] not in objectNo2eNode:
            continue
        if total[0] not in rrcInfoDict:
            rrcInfoDict[total[0]] = dict()
        h = total[2].hour
        if h not in rrcInfoDict[total[0]]:
            val = defVal.copy()
            val[0] = total[2]
            eNodeInfo = objectNo2eNode[total[0]]
            val[1] = eNodeInfo['meid']
            val[2] = eNodeInfo['cellid']
            val[4] = total[1]
            rrcInfoDict[total[0]][h] = val
        else:
            rrcInfoDict[total[0]][h][4] += total[1]

    rrcInfoList = list()
    for key, val in rrcInfoDict.items():
        for k, v in val.items():
            rrcInfoList.append(v)

    return rrcInfoList

def get_rrc_erab(objectNo, pmcomdb_settingFiles, pmdb_settingFiles, date):
    ids = {'rrc':{1526726659:1526726658}, 'erab':{1526727544:1526727545}}
    rcc_succM = collections.defaultdict(lambda : Decimal('0'))
    rcc_connM = collections.defaultdict(lambda : Decimal('0'))
    erab_succM = collections.defaultdict(lambda : Decimal('0'))
    erab_connM = collections.defaultdict(lambda : Decimal('0'))

    for fName in pmdb_settingFiles:
        pmdb_settingInfo = ''
        with open(fName.split(':')[1], 'r') as sF:
            pmdb_settingInfo = sF.readline()
            pmdb_settingInfo = pmdb_settingInfo.replace('\r', '')
            pmdb_settingInfo = pmdb_settingInfo.replace('\n', '')
            sF.close()

        for pmcomdbFName in pmcomdb_settingFiles:
            pmcomdb_settingInfo = ''
            with open(pmcomdbFName.split(':')[1], 'r') as sF:
                pmcomdb_settingInfo = sF.readline().split(':')[1]
                pmcomdb_settingInfo = pmcomdb_settingInfo.replace('\r', '')
                pmcomdb_settingInfo = pmcomdb_settingInfo.replace('\n', '')
                sF.close()

            for key, val in ids.items():
                if key == 'rrc':
                    for k, v in val.items():
                        rrc_succ = get_cntM(pmcomdb_settingInfo, pmdb_settingInfo, k, rcc_succM, date)
                        rrc_total = get_cntM(pmcomdb_settingInfo, pmdb_settingInfo, v, rcc_connM, date)
                        rrcInfos = get_rrc(rrc_succ, rrc_total)
                        uploadRRc(rrcInfos)
                        outputRes("data/%s_rrc_h.txt"%(date.strftime('%Y%m%d')), rrcInfos)
                elif key == 'erab':
                    for k, v in val.items():
                        get_cntM(pmcomdb_settingInfo, pmdb_settingInfo, k, erab_succM, date)
                        get_cntM(pmcomdb_settingInfo, pmdb_settingInfo, v, erab_connM, date)


def outputRes(fName, res):
    with open(fName, 'a') as ff:
        for elem in res:
            ff.write('%s,%d,%d,%d,%d\n'%(elem[0].strftime('%Y-%m-%d %H:%M'), elem[1],elem[2],elem[3],elem[4]))
        ff.close()


def outputSuccRate(fName, objectNo, succM, connM):
    with open(fName, 'w') as ff:
        for no in objectNo:
            rate = float(succM[no] / connM[no]) if float(connM[no]) != 0 else 0
            ff.write('%d,%f\n'%(int(no), rate))
        ff.close()


def doAnalyze(date):
    settingFiles = list()
    with open('setting/sybase_setting', 'r') as ff:
        settingFiles = ff.readlines()
        ff.close()
    nFile = len(settingFiles)
    for i in range(nFile):
        settingFiles[i] = settingFiles[i].replace('\r', '')
        settingFiles[i] = settingFiles[i].replace('\n', '')
    objectNo = get_ObjectNoOfeNodeBId('华为', settingFiles[:4])
    get_rrc_erab(objectNo, settingFiles[:4], settingFiles[4:], date)

if __name__ == "__main__":
    curTime = datetime.datetime.today() + datetime.timedelta(-1)
    doAnalyze(curTime)
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
