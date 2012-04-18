#!/usr/bin/env python
# postgresql database wrapper for evaluation of db inserts
import sqlite3
import timeit

from db_wrapper import DBWrapper

#===============================================================================
class SQLiteWrapper(DBWrapper):

    #---------------------------------------------------------------------------
    def connect(self):
        self.conn = sqlite3.connect('/tmp/{db_name}'.format(
            db_name=self.db.get('name'))
        )
        self.cur = self.conn.cursor()
        self.setup()

    #---------------------------------------------------------------------------
    def setup(self):
        self.cur.execute(
            'CREATE TABLE IF NOT EXISTS {db_table} (id REAL, num REAL, data TEXT)'.format(
                db_table=self.db.get('table')
            )
        )

    #---------------------------------------------------------------------------
    def single_query(self):
        """Use single query for each insert"""
        print "Performing single query inserts..."
        def timewrapper():
            for i in xrange(self.number_inserts):
                self.cur.execute("INSERT INTO {db_table} (num, data) VALUES (200, 'world');".format(
                    db_table=self.db.get('table', '')
                ))
        return timeit.Timer(timewrapper)
     
        
if __name__ == "__main__":
    SQLiteWrapper()