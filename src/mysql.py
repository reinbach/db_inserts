#!/usr/bin/env python
# mysql database wrapper for evaluating db inserts
import MySQLdb
import timeit

from db_wrapper import DBWrapper

#===============================================================================
class MySQLWrapper(DBWrapper):

    #---------------------------------------------------------------------------
    def connect(self):
        self.conn = MySQLdb.connect(
            db=self.db.get("name"),
            user=self.db.get("user"),
            passwd=self.db.get("pass"),
        )
        self.cur = self.conn.cursor()
        self.setup()

    #---------------------------------------------------------------------------
    def setup(self):
        self.cur.execute(
            'CREATE TABLE IF NOT EXISTS {db_table} (id INT(11) AUTO_INCREMENT, num INT(11), data VARCHAR(100), PRIMARY KEY (id))'.format(
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

    #---------------------------------------------------------------------------
    def bulk_query(self):
        """Use a bulk query to insert data"""
        print "Performing bulk query insert..."
        def timewrapper():
            query = "INSERT INTO {db_table} (num, data) VALUES ".format(db_table=self.db.get('table', ''))
            for i in xrange(self.number_inserts):
                query += "(200, 'world'), "
            self.cur.execute(query[:-2])
        return timeit.Timer(timewrapper)

    #---------------------------------------------------------------------------
    def file_query(self):
        """Use 'LOAD DATA' to insert data

        File needs to be created and then LOAD DATA'd from
        """
        print "Performing 'LOAD DATA' query insert..."
        def timewrapper():
            with open(self.db.get('file'), "w") as f:
                for i in xrange(self.number_inserts):
                    f.write("200\tWorld\n")
            # call load data file query
            self.cur.execute("LOAD DATA LOCAL INFILE '{db_file}' INTO TABLE {db_table} (num, data)".format(
                db_file=self.db.get('file'),
                db_table=self.db.get('table')
            ))
        return timeit.Timer(timewrapper)
