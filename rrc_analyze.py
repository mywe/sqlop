import sybase_op
import sybaseiq_op
import rrc_oracle_op
import sys
import datetime

def do_analyze(date):
    sybase_op.doAnalyze(date)
    sybaseiq_op.do_RRC_Analyze(date)
    rrc_oracle_op.do_RRC_Analyze(date)

    res = list()
    with open('data/%s_rrc_h.txt'%(date.strftime('%Y%m%d')), 'r') as ff:
        for line in ff.readlines():
            line = line.replace('\r', '')
            line = line.replace('\n', '')
            res.append(line.split(','))
        ff.close()

    defVal = ['', 0, 0]
    data = dict()
    for r in res:
		cellId = '%d_%d'%(int(r[1]), int(r[2]))
		if cellId not in data:
			data[cellId] = defVal.copy()
			data[cellId][0] = cellId
		data[cellId][1] += int(r[3])
		data[cellId][2] += int(r[4])

    with open('data/%s_rrc_d.txt'%(date.strftime('%Y%m%d')), 'w') as ff:
        for key, val in data.items():
            ff.write('%s,%d,%d'%(val[0], val[1], val[2]))
        ff.close()

if __name__ == '__main__':
#    date = None
#    try:
#        date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')
#        do_analyze(date)
#    except:
#        try:
#            date = datetime.datetime.strptime(sys.argv[1], '%Y/%m/%d')
#            do_analyze(date)
#        except:
#            print("enter date format \'yyyy-mm-dd\' or \'yyyy/mm/dd\'!!!")
    startDate = datetime.datetime(2015, 4, 9)
    for delta in range(5):
        date = startDate + datetime.timedelta(delta)
        do_analyze(date)
    print('analyze finish!')

