import pyodbc
import mysql
import xlrd
import collections
import datetime
from decimal import Decimal
from mysql import connector

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


def getDataFromSybaseIQ(odbcInfo, sqlStr):
    conn = pyodbc.connect(odbcInfo)
    cursor = conn.cursor()
    cursor.execute(sqlStr)
    rows = cursor.fetchall()
    return rows


def getRRC(dap_modelInfo, date):
    rrc_sql = ''
    with open('setting/rrc_sqlStr.txt', 'r') as ff:
        rrc_sql = ff.readline()
        ff.close()
        rrc_sql = rrc_sql.replace('\n', '')
        rrc_sql = rrc_sql.replace('\r', '')
        if len(rrc_sql) == 0:
            return

    startDate = date
    endDate = startDate + datetime.timedelta(days=1)

    sqlStr = '%s where BEGINCOLLECTTIME >=\'%s 00:00\' and BEGINCOLLECTTIME < \'%s 00:00\''%(rrc_sql, startDate.strftime('%Y-%m-%d'), endDate.strftime('%Y-%m-%d'))
    rows = getDataFromSybaseIQ(dap_modelInfo, sqlStr)
    return rows
#    rcc = collections.defaultdict(lambda : dict())
#    for r in rows:
#        me_cell_ID = str(r[1]) + '_' + str(r[2])
#        rcc[me_cell_ID][r[0].hour] = {'succ_conn':int(r[3]), 'total_conn':int(r[4])}
#
#    for (key, val) in rcc.items():
#        print('%s:%s'%(str(key), str(val)))


def do_RRC_Analyze(date):
    dap_modelInfo = ''
    with open('setting/dap_model_setting.txt', 'r') as ff:
        dap_modelInfo = ff.readline()
        ff.close()
        dap_modelInfo = dap_modelInfo.replace('\n', '')
        dap_modelInfo = dap_modelInfo.replace('\r', '')

    rrcInfos = getRRC(dap_modelInfo, date)
    cellID_list = getCellID('中兴')
    uploadRRC(rrcInfos, cellID_list)
    outputRes("data/%s_rrc_h.txt"%(date.strftime("%Y%m%d")), rrcInfos, cellID_list)


def outputRes(fName, res, cellID_list):
    with open(fName, 'a') as ff:
        for elem in res:
            if '%d_%d'%(elem[1], elem[2]) not in cellID_list:
                continue
            ff.write('%s,%d,%d,%d,%d\n'%(elem[0].strftime('%Y-%m-%d %H:%M'), elem[1],elem[2],elem[3],elem[4]))
        ff.close()


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

if __name__ == "__main__":
    curTime = datetime.datetime.today() + datetime.timedelta(-1)
    do_RRC_Analyze(curTime)