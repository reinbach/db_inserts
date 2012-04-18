#!/usr/bin/env python
# postgresql database wrapper for evaluation of db inserts
import psycopg2
import timeit

from db_wrapper import DBWrapper

#===============================================================================
class PostgreSQLWrapper(DBWrapper):

    #---------------------------------------------------------------------------
    def connect(self):
        self.conn = psycopg2.connect("dbname={db_name} user={db_user} password={db_pass}".format(
            db_name=self.db.get('name', ''),
            db_user=self.db.get('user', ''),
            db_pass=self.db.get('pass', '')
        ))
        self.cur = self.conn.cursor()
        self.setup()

    #---------------------------------------------------------------------------
    def setup(self):
        self.cur.execute(
            'CREATE TABLE IF NOT EXISTS {db_table} (id SERIAL, num INTEGER, data VARCHAR)'.format(
                db_table=self.db.get('table')
            )
        )

    #---------------------------------------------------------------------------
    def reset(self):
        super(PostgreSQLWrapper, self).reset()
        self.conn.commit()
        
    #---------------------------------------------------------------------------
    def single_query(self):
        """Use single query for each insert"""
        print "Performing single query inserts..."
        def timewrapper():
            for i in xrange(self.number_inserts):
                self.cur.execute("INSERT INTO {db_table} (num, data) VALUES (200, 'world');".format(
                    db_table=self.db.get('table', '')
                ))
            self.conn.commit()
        return timeit.Timer(timewrapper)
     
    #---------------------------------------------------------------------------
    def bulk_query(self):
        """Use a bulk query to insert data"""
        print "Performing bulk query insert..."
        def timewrapper():
            query = "INSERT INTO {db_table} (num, data) VALUES ".format(db_table=self.db.get('table', ''))
            for i in xrange(self.number_inserts):
                query += "(200, 'world'), "
            self.cur.execute(query[:-2])
            self.conn.commit()
        return timeit.Timer(timewrapper)
     
    #---------------------------------------------------------------------------
    def file_query(self):
        """Use 'COPY' to insert data

        File needs to be created and then COPY'd from
        """
        print "Performing 'COPY' query insert..."
        def timewrapper():
            with open(self.db.get('file'), "w") as f:
                for i in xrange(self.number_inserts):
                    f.write("200\tWorld\n")
            # call load data file query
            with open(self.db.get('file'), "r") as f:
                self.cur.copy_from(f, self.db.get('table'), columns=('num', 'data'))
            self.conn.commit()
        return timeit.Timer(timewrapper)

        
if __name__ == "__main__":
    PostgreSQLWrapper()