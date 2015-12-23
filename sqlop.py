import os
import sys
import cx_Oracle
import datetime
import xlrd

def getDBSetting():
	set = dict()
	with open('oracle_setting', 'r') as ff:
		for line in ff.readlines():
			line = line.replace('\r', '')
			line = line.replace('\n', '')
			info = line.split(':')
			set[info[0]] = info[1]
		ff.close()
	return set

def writeFields(fileName, fields):
	with open(fileName, 'w') as ff:
		ff.write('%s\n'%fields)
		ff.close()

curTime = datetime.datetime.today() + datetime.timedelta(-1)
startTime = datetime.datetime(curTime.year, curTime.month, curTime.day)
endTime = datetime.datetime(curTime.year, curTime.month, curTime.day, 23, 59, 59)
CQI_fstr = startTime.strftime('%Y-%m-%d') + "_CQI.txt"
ERBA_fstr = startTime.strftime('%Y-%m-%d') + "_ERBA.txt"
rcc_fstr = startTime.strftime('%Y-%m-%d') + "_RCC.txt"

writeFields(CQI_fstr, 'BEGINTIME,MEID,CELLID,CQI0,CQI1,CQI2,CQI3,CQI4,CQI5,CQI6,CQI7,CQI8,CQI9,CQI10,CQI11,CQI12,CQI13,CQI14,CQI15')
writeFields(ERBA_fstr, 'BEGINTIME,MEID,CELLID,succEstab1,succEstab2,attEstab1,attEstab2')
writeFields(rcc_fstr, 'BEGINTIME,MEID,CELLID,succEstab1,succEstab2,succEstab3,succEstab4,succEstab5,succEstab6,attEstab1,attEstab2,attEstab3,attEstab4,attEstab5,attEstab6')

dbSetting = getDBSetting()
print(dbSetting)

try:
	db = cx_Oracle.connect(dbSetting['user'], dbSetting['pwd'], '%s:%s/%s'%(dbSetting['host'], dbSetting['port'], dbSetting['dbName']))
except:
	print('wrong setting of database!')
	sys.exit()

print(db.version)
cursor = db.cursor()
workbook = xlrd.open_workbook('佛山无线中心LTE工参-20151203.xlsx')
sheet = workbook.sheets()[1]

for r in range(1, sheet.nrows):
	if sheet.cell_value(r, 2) == '华为':
		continue
	eNodeB_Id = 0
	cell_Id = 0
	try:
		eNodeB_Id = int(sheet.cell_value(r, 7))
		cell_Id = int(sheet.cell_value(r,8))
	except:
		continue
	print(eNodeB_Id, cell_Id)
	startT = startTime.strftime('%Y-%m-%d %H:%M:%S')
	endT = endTime.strftime('%Y-%m-%d %H:%M:%S')
	with open('cqi_table_name.txt', 'r') as tbf:
		for tbn in tbf.readlines():
			tbn = tbn.replace('\n', '')
			tbn = tbn.replace('\r', '')
			fields = ''
			with open('%s.txt'%(tbn), 'r') as field_ff:
				fields += field_ff.readline()
				for line in field_ff.readlines():
					fields += ',' + line
				field_ff.close()
				fields = fields.replace('\n', '')
				fields = fields.replace('\r', '')
			
			sqlStr = 'select %s from MINOS_PM.%s\
			where CELLID=%d and MEID=%d and\
			BEGINTIME between to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\') and to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\')'\
			%(fields, tbn, cell_Id, eNodeB_Id, startT, endT)
			cursor.execute(sqlStr)
			rows = cursor.fetchall()
			
			if len(rows) == 0:
				continue
			with open(CQI_fstr, 'a') as ff:
				total = list(rows[0])
				nRow = len(rows)
				n = len(total)
				for r in range(1, nRow):
					for i in range(n):
						if i > 2:
							total[i] += rows[r][i]
				for i in range(n):
					ff.write('%s'%((',' + str(total[i])) if i != 0 else str(total[i])))
				ff.write('\n')
				ff.close()
			break
		tbf.close()
	
	with open('ERBA.txt', 'r') as ff:
		infos = dict()
		for line in ff.readlines():
			line = line.replace('\r', '')
			line = line.replace('\n', '')
			info = line.split(':')
			infos[info[0]] = info[1]
		
		sqlStr = 'select BEGINTIME,MEID,CELLID,%s,%s from MINOS_PM.%s\
					where CELLID=%d and MEID=%d and\
					BEGINTIME between to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\') and to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\')'\
					%(infos['NbrSuccEstab'], infos['NbrAttEstab'], infos['table_name'], cell_Id, eNodeB_Id, startT, endT)
		cursor.execute(sqlStr)
		rows = cursor.fetchall()
		
		if len(rows) != 0:
			with open(ERBA_fstr, 'a') as resff:
				total = list(rows[0])
				nRow = len(rows)
				n = len(total)
				for r in range(1, nRow):
					for i in range(n):
						if i > 2:
							total[i] += rows[r][i]
				for i in range(n):
					resff.write('%s'%((',' + str(total[i])) if i != 0 else str(total[i])))
				resff.write('\n')
				resff.close()
		
		ff.close()
		
	with open('rrc_connect.txt', 'r') as ff:
		infos = dict()
		for line in ff.readlines():
			line = line.replace('\r', '')
			line = line.replace('\n', '')
			info = line.split(':')
			infos[info[0]] = info[1]
		
		sqlStr = 'select BEGINTIME,MEID,CELLID,%s,%s from MINOS_PM.%s\
					where CELLID=%d and MEID=%d and\
					BEGINTIME between to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\') and to_date(\'%s\',\'yyyy-mm-dd hh24:mi:ss\')'\
					%(infos['SuccConnEstab'], infos['AttConnEstab'], infos['table_name'], cell_Id, eNodeB_Id, startT, endT)
		cursor.execute(sqlStr)
		rows = cursor.fetchall()
		
		if len(rows) != 0:
			with open(rcc_fstr, 'a') as resff:
				total = list(rows[0])
				nRow = len(rows)
				n = len(total)
				for r in range(1, nRow):
					for i in range(n):
						if i > 2:
							total[i] += rows[r][i]
				for i in range(n):
					resff.write('%s'%((',' + str(total[i])) if i != 0 else str(total[i])))
				resff.write('\n')
				resff.close()
		
		ff.close()