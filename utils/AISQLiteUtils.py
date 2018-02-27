import sqlite3


class CMySQLConnection:
    def __init__(self, DBName):
        self.connection = sqlite3.connect(DBName)

    def getCursor(self):
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()
