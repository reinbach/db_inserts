#!/usr/bin/env python
#
# scripts tests various ways of bulk inserting to various databases
#

from psql import PostgreSQLWrapper as psql
from mysql import MySQLWrapper as mysql
from sqlite import SQLiteWrapper as sqlite

DB_NAME = "evaluate_insert"
DB_USER = "webuser"
DB_PASS = "webpass"
DB_TABLE = "sample"
DB_FILE = "/tmp/sample"

# comment out the databases you do not want to evaluate
EVALUATE_DB = [
    ('psql', psql),
    ('mysql', mysql),
    ('sqlite', sqlite),
]

NUMBER_INSERTS = [
    1000,
    10000,
#    100000
]

#===============================================================================
class EvaluateDB(object):
    """Test various ways of bulk inserting"""

    #---------------------------------------------------------------------------
    def __init__(self):
        """Setup db params to be available for wrappers"""
        self.db_params = {
            'name': DB_NAME,
            'user': DB_USER,
            'pass': DB_PASS,
            'table': DB_TABLE,
            'file': DB_FILE
        }
        self.number_inserts = NUMBER_INSERTS
        self.results = []
        self.run()
    
    #---------------------------------------------------------------------------
    def run(self):
        print "Tests are running...."
        for label, run_command  in EVALUATE_DB:
            res = []
            for num in self.number_inserts:
                res.append((
                    num,
                    run_command(self.db_params, num, label).run()
                ))
            self.results.append((label, res))
        self.display_results()

    #---------------------------------------------------------------------------
    def display_results(self):
        print "\n\nResults"
        for env, num_data in self.results:
            print "\n::{env}::".format(env=env)
            for num, data in num_data:
                print "\n{num} inserts".format(num=num)
                for test_type, time_value in data:
                    print "{test_type}\t{time_value}".format(
                        test_type=test_type,
                        time_value=time_value
                    )

if __name__ == "__main__":
    print "Start query tests"
    EvaluateDB()
