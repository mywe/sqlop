import sys
import cx_Oracle
import datetime
import xlrd
from mysql import connector

def getSettingInfo(fileName):
	set = dict()
	with open(fileName, 'r') as ff:
		for line in ff.readlines():
			line = line.replace('\r', '')
			line = line.replace('\n', '')
			info = line.split(':')
			set[info[0]] = info[1]
		ff.close()
	return set

def getCellID(factory):
    workbook = xlrd.open_workbook('佛山无线中心LTE工参.xlsx')
    sheet = workbook.sheets()[1]

    cellIdInfos = list()
    for r in range(1, sheet.nrows):
        if sheet.cell_value(r, 2) != factory:
            continue
        eNodeB_Id = None
        cell_Id = None
        try:
            eNodeB_Id = int(sheet.cell_value(r, 7))
            cell_Id = int(sheet.cell_value(r,8))
        except:
            continue
        cellIdInfos.append('%d_%d'%(eNodeB_Id, cell_Id))
    return cellIdInfos

def getRRC(date):
    startTime = datetime.datetime(date.year, date.month, date.day)
    endTime = datetime.datetime(date.year, date.month, date.day, 23, 59, 59)

    dbSetting = getSettingInfo('oracle_setting')
    print(dbSetting)

    db = None
    try:
        db = cx_Oracle.connect(dbSetting['user'], dbSetting['pwd'], '%s:%s/%s'%(dbSetting['host'], dbSetting['port'], dbSetting['dbName']))
    except:
        print('wrong setting of database!')
        sys.exit()

    print(db.version)
    cursor = db.cursor()

    startT = startTime.strftime('%Y-%m-%d %H:%M:%S')
    endT = endTime.strftime('%Y-%m-%d %H:%M:%S')
    sqlStr = dbSetting['sqlStr'] + ' where BEGINTIME between to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\') and to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\')'%(startT, endT)
    print(sqlStr)
    cursor.execute(sqlStr)
    rows = cursor.fetchall()
    return rows

def uploadRRC(rrcInfos, cellID_list):
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

    i = 0
    nCount = len(rrcInfos)
    cnx = connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    while(i < nCount):
        sqlStr = rrc_store_sqlStr
        j = 0
        while (i < nCount and j < 100):
            if '%d_%d'%(rrcInfos[i][1], rrcInfos[i][2]) not in cellID_list:
                i += 1
                continue
            sqlStr += (',(\'%s\',%d,%d,%d,%d)' if j > 0 else '(\'%s\',%d,%d,%d,%d)')%(rrcInfos[i][0].strftime('%Y-%m-%d %H:%M'),\
                                                                              int(rrcInfos[i][1]),int(rrcInfos[i][2]),int(rrcInfos[i][3]),int(rrcInfos[i][4]))
            i += 1
            j += 1
        if len(sqlStr) != len(rrc_store_sqlStr):
            cursor.execute(sqlStr)
            cnx.commit()
    cursor.close()
    cnx.close()

def outputRes(fName, res, cellID_list):
    with open(fName, 'a') as ff:
        for elem in res:
            if '%d_%d'%(elem[1], elem[2]) not in cellID_list:
                continue
            ff.write('%s,%d,%d,%d,%d\n'%(elem[0].strftime('%Y-%m-%d %H:%M'), elem[1],elem[2],elem[3],elem[4]))
        ff.close()


def do_RRC_Analyze(date):
    rrcInfos = getRRC(date)
    cellID_list = getCellID("中兴")
    uploadRRC(rrcInfos, cellID_list)
    outputRes("data/%s_rrc_h.txt"%(date.strftime("%Y%m%d")), rrcInfos, cellID_list)


if __name__ == "__main":
    curTime = datetime.datetime.today() + datetime.timedelta(-1)
    do_RRC_Analyze(curTime)