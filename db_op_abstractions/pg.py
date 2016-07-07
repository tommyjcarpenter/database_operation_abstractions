from __future__ import division
import sys, traceback
import psycopg2
import psycopg2.errorcodes
from math import ceil
from contexttimer import Timer

from db_op_abstractions import get_module_logger

logger = get_module_logger(__name__)

"""
see: https://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL
"""

class PGConnCM(object):
    """
    Connection to pg as a context manager for a one time operation (use above for continuous connection)

    From the docs:
    In Psycopg transactions are handled by the connection class. By default, the first time a command is sent to the database (using one of the cursors created by the connection), a new transaction is created. The following database commands will be executed in the context of the same transaction – not only the commands issued by the first cursor, but the ones issued by all the cursors created by the same connection. Should any command fail, the transaction will be aborted and no further command will be executed until a call to the rollback() method.

    The connection is responsible for terminating its transaction, calling either the commit() or rollback() method.
    """
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.connection.close()

    def __init__(self, database, user, password, host, port):
        self.connection = psycopg2.connect(database=database, user=user, password=password, port=int(port), host=host)
        self.cursor = self.connection.cursor()
                                                                                       
    def select_generator(self,stmt):
        """execute a select query and return results as a generator"""
        data = []    
        try:
            self.cursor.execute(stmt)
            #change if RAM not plentiful..currently defeats the purpose of a generator
            data = self.cursor.fetchall()
            for row in data:
                yield row
        except Exception as msg:
            logger.error("psycopg2 SELECT ERROR!: {0}".format(msg))
    
    def exec_query_list(self, Q):
        """
        execute a bunch of sql queries, commiting after each one. An error on query i does not halt query i+1

        Q is a list of fully formed statements e.g., "CREATE TABLE ....;"
        """
        succ_exec = 0
        for q in Q:
            try: 
                self.cursor.execute(q)
                self.connection.commit()
                succ_exec+=1
            except psycopg2.ProgrammingError as msg:
                logger.error("Failed pg execution: {0}, error: {1}".format(q,msg))
                self.connection.rollback()
            except Exception as msg:
                logger.error("Unknown error on: {0}, error: {1}".format(q,msg))
                self.connection.rollback()
        logger.info("Sucessfuly executed {0} out of {1} operations".format(succ_exec, len(Q)))

    def bulk_insert_chunks(self, table_name, values, chunk_size=1000):
        """
        Bulk insert via batches., commiting after each batch. If any of the batches fails, insertions continue by
        attempting the next batch. Unexpected failures are only logged.

        Returns the total number of records sucessfully inserted. 
        """
        logger.info("Insert: {0}records, {1}records/chunk".format(len(values), chunk_size))
    
        def chunks(src, chunk_size):
            chunk_count = int(len(src)/chunk_size)
            chunk_count = 1 if chunk_count == 0 else chunk_count
            for i in range(0, chunk_count):
                yield src[i*chunk_size:(i+1)*chunk_size] \
                    if chunk_count-1 > i else src[i*chunk_size:]
    
        with Timer() as ct:
            num_records = [self.bulk_insert(table_name, chunk) for chunk in chunks(values, chunk_size) ]
            logger.info("Done inserting: {0}records inserted, {1}s".format(sum(num_records), ct.elapsed))
    
        return sum(num_records)
    
    def bulk_insert(self, table_name, values):
        """
        Bulk insert

        values is a list of tuples to insert

        Upon an exception, the entire batch fails and the progress is rolled back.
        If you want to execute smaller batches where successive batches are tried 
        regardless of previous failures, use bulk_insert_batches instead of this function directly. 

        Returns the number of records inserted, or 0 if there was a logged exception. 
        """
    
        num_fields = len(values[0])
        params = ','.join(['%s' for i in range(0, num_fields)])
        params = "({0})".format(params)
        try:
                    # In python3.4 version of psycopg2, mogrify returns a byte string.
                    # while 2.7 version returns a string.
                    # Thus, always write the values into byte string first then convert
                    # to unicode
                    str_values = b','.join([self.cursor.mogrify(params, x) for x in values])
                    str_values = str_values.decode('utf-8')
                    query = u"INSERT INTO {0} VALUES {1}".format(table_name, str_values)
                    self.cursor.execute(query)
                    self.connection.commit()
                    return len(values)
        except psycopg2.ProgrammingError as msg:
                logger.error("Failed pg bulk_insert: error: {0}".format(msg))
                self.connection.rollback()
                return 0
        except Exception as msg:
                logger.error("Unknown error: {1}".format(msg))
                self.connection.rollback()
                return 0
    
    
