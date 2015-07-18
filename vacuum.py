import sqlite3

conn = sqlite3.connect('alerts.db')
conn.execute("VACUUM")
conn.close()
