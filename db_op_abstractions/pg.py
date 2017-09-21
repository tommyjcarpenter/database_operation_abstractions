# -*- coding: utf-8 -*-

from __future__ import division
import sys, traceback
import psycopg2
import psycopg2.errorcodes
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
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

    def __init__(self, database, user, password, host, port, create_database_on_init = False):
        """
        if create_database_on_init is True, then this connects to the Postgres database by default
        and tries to create the database `database`. The passed in User must have perms to do this
        This function keeps going if the database already exists.
        """
        if create_database_on_init:
            try:
                #stolen from: http://stackoverflow.com/questions/19426448/creating-a-postgresql-db-using-psycopg2
                con = psycopg2.connect(database='postgres', user=user, password=password, port=int(port), host=host)
                con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cur = con.cursor()
                cur.execute('CREATE DATABASE ' + database)
                con.commit()
                cur.close()
                con.close()
            except psycopg2.ProgrammingError:
                pass #database already exists, keep going

        self.connection = psycopg2.connect(database=database, user=user, password=password, port=int(port), host=host)
        self.cursor = self.connection.cursor()

    def select_generator(self,stmt, tup = ()):
        """execute a select query and return results as a generator
        tup: a safe pg statement is of the form
               cur.execute("SELECT * FROM data WHERE id = %s;", (ids,))
            tup is (ids,)
        """
        data = []
        try:
            self.cursor.execute(stmt, tup)
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

    def exec_query_list_rollback_on_error(self, Q):
        for q in Q:
            try:
                self.cursor.execute(q)
            except psycopg2.ProgrammingError as msg:
                logger.error("Failed pg execution: {0}, error: {1}".format(q,msg))
                self.connection.rollback() #rollback and reraise
                raise msg
            except Exception as msg:
                logger.error("Unknown error on: {0}, error: {1}".format(q,msg))
                self.connection.rollback() #rollback and reraise
                raise msg
        self.connection.commit() #commit only if no errors
        logger.info("Sucessfuly executed all operations")

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

        Returns the number of records inserted, or re-raises the exception after a rollback
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
        except psycopg2.ProgrammingError as e:
            logger.error("Failed pg bulk_insert: error: {0}".format(e))
            self.connection.rollback()
            raise e
        except Exception as e:
            logger.error("Unknown error: {0}".format(e))
            self.connection.rollback()
            raise e

    def get_all_tables(self):
        """
        No easy way to get the equivelent of \dt in SQL form.
        This just does the answer from: http://stackoverflow.com/questions/14730228/postgres-query-to-list-all-table-names
        """
        stmt = """
        SELECT table_name
          FROM information_schema.tables
           WHERE table_schema='public'
              AND table_type='BASE TABLE';
        """
        self.cursor.execute(stmt)
        return [i[0] for i in self.cursor.fetchall()]

    def row_count_all_tables(self):
        """
        There is no way to easily get the EXACT row count of all tables in a database:
        http://stackoverflow.com/questions/2596670/how-do-you-find-the-row-count-for-all-your-tables-in-postgres

        If you can't rely on one of the estimates there, this function computes it exactly.

        Returns a list of tuples [(table_name, row_count)]
        """
        tables = self.get_all_tables()
        ret = []
        for t in tables:
            self.cursor.execute("SELECT COUNT(*) FROM {0}".format(t))
            ret.append((t, int(self.cursor.fetchone()[0])))
        return ret



