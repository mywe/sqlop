import pyodbc

conn = pyodbc.connect('Driver={Adaptive Server Enterprise};HOST=132.122.151.69:4100;DATABASE=pmcomdb;UID=wxzx;PWD=Wxzx1234')
cursor = conn.cursor()