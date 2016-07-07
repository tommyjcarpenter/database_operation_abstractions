"""
This module represents a connection to MySQL with some higher level functions than pymysql provides
"""

import pymysql as MySQLdb
from pymysql.converters import escape_string

class MysqlConnCM(object):
    """Context manager inheriting from MysqlConn"""
    def __init__(self, databaseName, usr, password, mysql_host_ip, prt):
        try:       
            self.myDB = MySQLdb.connect(host=mysql_host_ip, port=int(prt), user=usr, passwd=password.replace("\"",""), db=databaseName, charset='utf8mb4')   
            self.conn = self.myDB.cursor() 
        except MySQLdb.Error as msg:
            print("MYSQL ERROR!: {0}".format(msg))
    
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.conn.close()
    
    #public functions----------------------------------------------------------------
    def escape(self, stmt):
        """Removes single quotes and escapes back slashes because they mess up SQL"""
        return escape_string(stmt).strip() if stmt else None

    def close(self):
        """closes mysql connection"""
        self.conn.close() 
        
    def select_generator(self, stmt):
        """execute a select query and return results as a generator
        
        TODO: This should take args, and then call self.conn.execute(stmt, args), stmt should have %s
        """
        try:   
            data = []    
            self.conn.execute(stmt)
            """TODO: currently defeats the purpose of a generator, fix this to fetch_one"""
            data = self.conn.fetchall() 
            for row in data:
                yield row
        except MySQLdb.Error as msg:
            print("MYSQL QUERY ERROR!: {0}".format(msg))
                
    def exec_query_list(self, Q):
        """execute a bunch of sql queries, commits at end. An error on query i does not halt query i+1
        
        *Callers of this function are required PyMySQLs proper escape method before passing in Q*  
        
        This function exists because PyMySQLs execute_many does not perform batch inserts. Batch inserts are much faster. 
        Additionally, MySQL's "INSERT IGNORE" functionality ensures that if insert K as part of a batch of N inserts 
        where k < N blows up, k+1 -> N are still (attemptedly) executed. 
        """
        for q in Q:
            try: 
                self.conn.execute(q)
            except MySQLdb.Error as msg:
                print("{1} MYSQL QUERY ERROR!: {0}".format(msg, q))
        self.myDB.commit()
    
    def bulk_insert(self, rows, insert_clause, update_clause):
        """rows: is a list of tuples with C elements where C is the number of columns
           insert_clause: the insert query with table name and columns. For example: INSERT INTO table (C1, C2)
           update_clause: a MySQL "ON DUPLICATE KEY" clause. For example: ON DUPLICATE KEY UPDATE c1 = VALUES(c2),
        """
        #http://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html
        #bulk insert limit is 50000
        insert_queue = []  
        insert_vals = "" 
        count_this_run = 0 
        total_count = 0
        for i in range(0, len(rows)): 
            total_count += 1
            insert_vals += "(" + ",".join(["'{0}'".format(k) for k in [self.escape(j) if type(j) == str else j for j in rows[i]]]) + "),"                                                                                                                               
            if total_count % 10000 == 0 or total_count == len(rows): #start a new bulk insert every 5K rows
                insert_queue.append(insert_clause + " VALUES " + insert_vals[0:-1].replace("'None'","NULL") + " " + update_clause + ";") #REPLACE Python Nones with proper mysql NULLs
                insert_vals = "" #reset values
        self.exec_query_list(insert_queue)        

