
#
# Assignment3 Interface
#
# Ziming Dong
# CSE 512
#
import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    # #Implement ParallelSort Here.
    # #Remove this once you are done with implementation
    cur = openconnection.cursor()
    getMinMax = " select max (" + SortingColumnName + "), min(" + SortingColumnName + ") from " + InputTable + ";"
    cur.execute(getMinMax)
    end_value, start_value = cur.fetchone()
    num_threads = 5
    threads = [0] * num_threads
    cur.execute(" create table " + OutputTable + " (like " + InputTable + " including all); ")
    delta=float(end_value-start_value)/5.0
    for i in range(num_threads):
        start = start_value + i*delta
        end = start + delta
        threads[i] = threading.Thread(target=IndividualSortTable, args=(InputTable, OutputTable, SortingColumnName, start, end, i, openconnection))
        threads[i].start()
    #Remove this once you are done with implementation
    openconnection.commit()

def IndividualSortTable(InputTable, OutputTable, SortingColumnName, start, end, i, openconnection):
    cur = openconnection.cursor()
    if i == 0:
        cur.execute("insert into " + OutputTable + " select * from " + InputTable + " where " + SortingColumnName + " >= " + str(start) + " and " + SortingColumnName + "<=" + str(end) + " order by " + SortingColumnName + " asc; ")
    else:
        cur.execute("insert into " + OutputTable + " select * from " + InputTable + " where " + SortingColumnName + " > " + str(start) + " and " + SortingColumnName + "<=" + str(end) + " order by " + SortingColumnName + " asc; ")


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    # Remove this once you are done with implementation
    cur = openconnection.cursor()
    getMinMax = " select max (" + Table1JoinColumn + "), min(" + Table1JoinColumn + ") from " + InputTable1 + ";"
    cur.execute(getMinMax)
    end_value, start_value = cur.fetchone()
    num_threads = 5
    threads = [0] * num_threads
    cur.execute(" create table " + OutputTable + " as select * from " + InputTable1 + " join " + InputTable2 + " on " + Table1JoinColumn + " = " + Table2JoinColumn +";")
    #cur.execute('truncate ' + OutputTable + ';')
    delta = float(end_value - start_value) / 5.0

    for i in range(num_threads):
        start = start_value + i * delta
        end = start + delta
        threads[i] = threading.Thread(target=IndividualJoinTable, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, start, end, i))
        threads[i].start()
    # Remove this once you are done with implementation
    openconnection.commit()


def IndividualJoinTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, start, end, i):
    cur=openconnection.cursor()
    if i == 0:
        cur.execute("insert into " + OutputTable + " select * from " + InputTable1 + ", " + InputTable2 + " where " +Table1JoinColumn + " = " + Table2JoinColumn + " and " + Table1JoinColumn + " >= " + str(start)+ " and " + Table1JoinColumn + " <= " + str(end) + ";")
    else:
        cur.execute("insert into " + OutputTable + " select * from " + InputTable1 + ", " + InputTable2 + " where " + Table1JoinColumn + " = " + Table2JoinColumn + " and " + Table1JoinColumn + " > " + str(start) + " and " + Table1JoinColumn + " <= " + str(end) + ";")

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


