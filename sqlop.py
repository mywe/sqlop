import os
import cx_Oracle
import datetime
import xlrd

db = cx_Oracle.connect('user', 'key', 'ip')
print(db.version)
cursor = db.cursor()
curTime = datetime.datetime.today() + datetime.timedelta(-1)
startTime = datetime.datetime(curTime.year, curTime.month, curTime.day)
endTime = datetime.datetime(curTime.year, curTime.month, curTime.day, 23, 59, 59)
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
			%(fields, tbn, cell_Id, eNodeB_Id, startTime.strftime('%Y-%m-%d %H:%M:%S'), endTime.strftime('%Y-%m-%d %H:%M:%S'))
			print(sqlStr)
			cursor.execute(sqlStr)
			rows = cursor.fetchall()
			
			if len(rows) == 0:
				continue
			with open('%s_CQI.txt'%(tbn), 'a') as ff:
				for row in rows:
					n = len(row)
					for i in range(n):
						ff.write('%s'%((',' + str(row[i])) if i != 0 else str(row[i])))
					ff.write('\n')
				ff.close()
			break
		tbf.close()