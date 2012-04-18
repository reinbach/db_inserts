#===============================================================================
class DBWrapper(object):

    #---------------------------------------------------------------------------
    def __init__(self, db, number_inserts, label):
        self.db = db
        self.number_inserts = number_inserts
        self.wrapper_results = []
        self.label = label

    #---------------------------------------------------------------------------
    def close(self):
        self.cur.close()
        self.conn.close()

    #---------------------------------------------------------------------------
    def reset(self):
        self.cur.execute("DELETE FROM {db_table}".format(db_table=self.db.get('table', '')))

    #---------------------------------------------------------------------------
    def run(self):
        """Perform each of the tests"""
        print "\nRunning evaluation of {number_inserts} for {label}".format(
            number_inserts=self.number_inserts,
            label=self.label
        )
        self.connect()

        evaluation_list = [
            ("Single", "single_query"),
            ("Bulk", "bulk_query"),
            ("File", "file_query"),
        ]

        for label, evaluation in evaluation_list:
            if hasattr(self, evaluation):
                self.reset()
                self.run_evaluation(label, getattr(self, evaluation))

        self.close()

        return self.wrapper_results

    #---------------------------------------------------------------------------
    def run_evaluation(self, label, evaluation):
        self.wrapper_results.append((
            label,
            self.timer(evaluation())
        ))
        
    #---------------------------------------------------------------------------
    def timer(self, timeObj):
        time_taken = timeObj.timeit(number=1)
        print "{0}".format(time_taken)
        return time_taken
