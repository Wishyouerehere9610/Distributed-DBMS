#Ziming Dong
#2/8/2020
#Assignment 1

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    #Implement a Python function loadRatings() that takes a file system path that
    # contains the rating file as input. loadRatings() then load all ratings into
    # a table (saved in PostgreSQL) named ratings that has the following schema
    #userid(int) – movieid(int) – rating(float)
    cur=openconnection.cursor()
    cur.execute("create table "+ ratingstablename + " (userid int, col1 text, movieid int, col2 text, rating float, col3 text, timestamp bigint);")
    cur.copy_from(open(ratingsfilepath),ratingstablename, sep=":")
    cur.execute("alter table " + ratingstablename + " drop column col1, drop column col2, drop column col3")
    cur.close()



def rangePartition(ratingstablename, numberofpartitions, openconnection):
    # Implement a Python function rangePartition() that takes as input: (1) the
    # Ratings table stored in PostgreSQL and (2) an integer value N; that represents
    # the number of partitions. rangePartition() then generates N horizontal fragments
    # of the ratings table and store them in PostgreSQL.
    # The algorithm should partition the ratings table based on N uniform ranges of the rating attribute.
    cur=openconnection.cursor()
    gap= float( 5/ numberofpartitions)
    for i in range(0, numberofpartitions):
        RANGE_TABLE_PREFIX = 'range_part'+str(i)
        start=i*gap
        end=gap+start
        cur.execute("create table " + RANGE_TABLE_PREFIX + "(userid int, movieid int, rating float, timestamp bigint);")
        if i==0:
            cur.execute("insert into " + RANGE_TABLE_PREFIX + " select * from " + ratingstablename + " where rating >= " + str(start) + " and rating <= " + str(end) + ";")
        else:
            cur.execute("insert into " + RANGE_TABLE_PREFIX + " select * from " + ratingstablename + " where rating > " + str(start) + " and rating <= " + str(end) + ";")
    cur.close()



def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    # Implement a Python function roundRobinPartition() that takes as input:
    # (1) the ratings table stored in PostgreSQL and
    # (2) an integer value N; that represents the number of partitions.
    # The function then generates N horizontal fragments of the ratings table
    # and stores them in PostgreSQL. The algorithm should partition the ratings
    # table using the round robin partitioning approach (explained in class).
    cur=openconnection.cursor()
    for i in range(0, numberofpartitions):
        RROBIN_TABLE_PREFIX = 'rrobin_part' + str(i)
        cur.execute("create table " + RROBIN_TABLE_PREFIX + "(userid int, movieid int, rating float, timestamp bigint);")
        cur.execute("insert into " + RROBIN_TABLE_PREFIX + "(userid, movieid, rating) select userid, movieid, rating from (select userid, movieid, rating, row_number() over() as temp from " +ratingstablename +") as temp where mod(temp-1, 5) = " + str(i) + ";")
    cur.close()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    # Implement a Python function roundRobinInsert() that takes as input:
    # (1) ratings table stored in PostgreSQL, (2) userid, (3) itemid, (4) rating.
    # roundRobinInsert() then inserts a new tuple to the ratings table and the
    # right fragment based on the round robin approach.
    cur = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.execute("select count(*) from " + ratingstablename + ";");
    total = (cur.fetchall())[0][0]
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + RROBIN_TABLE_PREFIX + "%';")
    numberofpartitions = cur.fetchone()[0]
    i = (total - 1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(i)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    #Implement a Python function rangeInsert() that takes as input:
    # (1) ratings table stored in PostgreSQL (2) userid, (3) itemid,
    # (4) rating. rangeInsert() then inserts a new tuple to the ratings table and
    # the correct fragment (of the partitioned ratings table) based upon the rating value.
    cur= openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name like '%range_part%'; ")
    count = cur.fetchall()
    gap = float(5) / len(count)
    min = float(0)
    max = float(gap)
    for i in range(0, len(count)):
        if i==0:
            if rating >= min and rating <= max:
                cur.execute("insert into " + count[i][0] + "(userid, movieid, rating) values ( " + str(userid) + " , " + str(itemid) + " , " + str(rating) + ")")
        else:
            if rating > min and rating <= max:
                cur.execute("insert into " + count[i][0] + "(userid, movieid, rating) values( " + str(userid) + " , " + str(itemid) + " , " + str(rating) + ")")
        min = float(min + gap)
        max = float(min + gap)
        i += 1
    cur.execute("insert into " + ratingstablename + " (userid, movieid, rating) values ( " + str(userid) + " , " + str(itemid) + " , " + str(rating) + ")")
    cur.close()

def createDB(dbname='dds_assignment1'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
