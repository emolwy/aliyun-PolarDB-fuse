# coding=utf-8

import mysql.connector


class mysqlClient(object):

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

        self.connect = None
        self.cursor = None

    def dbconnect(self):
        self.connect = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database)
        return self

    def dbcursor(self):
        if self.connect is None:
            self.connect = self.dbconnect()

        if not self.connect.is_connected():
            self.connect = self.dbconnect()
        self.cursor = self.connect.cursor()
        return self

    def dbquery(self, sql):
        try:
            self.cursor.execute(sql)
        except Exception:
            return '查询异常...'
        return self.cursor.fetchall()

    def dbexecute(self, sql):
        try:
            self.cursor.execute(sql)
        except Exception:
            return '执行异常...'
        self.connect.commit()

    def dbclose(self):
        if self.connect:
            self.connect.close()
        if self.cursor:
            self.cursor.close()
