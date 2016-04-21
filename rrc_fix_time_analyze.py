import threading
import datetime
import rrc_analyze

curTime = datetime.datetime.today()
planTime = datetime.datetime(curTime.year, curTime.month, curTime.day, 2)
planTime = planTime + datetime.timedelta(days=1)

def fix_time_analyze():
    rrc_analyze.do_analyze()
    global planTime
    planTime = planTime + datetime.timedelta(days=1)
    timer = threading.Timer((planTime - datetime.datetime.today()).total_seconds(), fix_time_analyze)
    timer.start()


timer = threading.Timer((planTime - datetime.datetime.today()).total_seconds(), fix_time_analyze)
timer.start()