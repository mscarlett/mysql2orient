from collections import defaultdict
import errno
import os
import sys

import MySQLdb

def generate_json(mysql_url, mysql_username, mysql_password, mysql_table_name, orientdb_class, orientdb_url, orientdb_username, orientdb_password, foreign_keys=None):
    edge_list = []
    
    if foreign_keys:
        for field, ref in foreign_keys:
            orientdb_edge = '''
            {
                "edge": {
                    "class": "%s",
                    "direction": "out",
                    "joinFieldName": "%s",
                    "lookup": "%s",
                    "unresolvedLinkAction": "CREATE",
                    "if": "%s is not null"
                }
            }
            ''' % (field, field, ref, ref)
            edge_list.append(orientdb_edge)
    
    orientdb_edges = ',\n'.join(edge_list)
    
    if orientdb_edges:
        orientdb_edges = ',' + orientdb_edges
        
    return  '''{
    "config": {
        "verbose": true,
        "log": "debug"
    },
    "extractor" : {
        "jdbc": {
            "driver": "com.mysql.jdbc.Driver",
            "url": "%s",
            "userName": "%s",
            "userPassword": "%s",
            "query": "select * from %s" }
    },
    "transformers": [
        { "vertex": { "class": "%s", "extends": "V" } }
        %s
    ],
    "loader": {
        "orientdb": {
            "dbURL": "%s",
            "dbUser": "%s",
            "dbPassword": "%s",
            "dbAutoCreate": true,
            "tx": false,
            "batchCommit": 1000,
            "dbType": "graph"
        }
    }
}''' % (mysql_url, mysql_username, mysql_password, mysql_table_name, orientdb_class, orientdb_edges, orientdb_url, orientdb_username, orientdb_password)
    
def mysql_tables(cursor):
    cursor.execute('SHOW TABLES')
    return [row[0] for row in cursor.fetchall()]
    
def mysql_foreign_keys(cursor): # Assumes that foreign key is in same database
    cursor.execute('''
    SELECT k.TABLE_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME
    FROM information_schema.TABLE_CONSTRAINTS i
    LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME
    WHERE i.CONSTRAINT_TYPE = 'FOREIGN KEY'
    AND i.TABLE_SCHEMA = DATABASE();
    ''')
    
    foreign_keys = defaultdict(list)
    
    for (key_table, key_field, ref_table, ref_field) in cursor.fetchall():        
        foreign_keys[key_table].append((key_field, '%s.%s' % (ref_table, ref_field)))
        
    return foreign_keys
    
def main(argv):
    arguments = defaultdict(list)
    keyword = None
    
    for arg in argv[1:]:
        if arg.startswith("--"):
            keyword, value = arg[2:].split('=')
            if value:
                arguments[keyword].append(value)
        else:
            if keyword:
                arguments[keyword].append(arg)
            else:
                raise ValueError('Invalid input argument: %s' % arg)
    
    for k, v in arguments.iteritems():
        arguments[k] = ' '.join(v)
    
    mysql_hostname = arguments.get('mysql_hostname', 'localhost')
    mysql_username = arguments.get('mysql_username', 'root')
    mysql_password = arguments.get('mysql_password', '')
    mysql_database = arguments.get('mysql_database', None)
    
    connection = MySQLdb.connect(
                    host = mysql_hostname,
                    user = mysql_username,
                    passwd = mysql_password)
    
    cursor = connection.cursor()
    
    if not mysql_database:
        cursor.execute('show databases')
        dbs = cursor.fetchall()
    else:
        dbs = [mysql_database]
    
    orientdb_url = arguments.get('orient_database', None)
    orientdb_username = arguments.get('orient_username', 'admin')
    orientdb_password = arguments.get('orient_password', 'admin')
    outdir = arguments.get('orient_file_dir', None)
    
    for db in dbs:
        cursor.execute('use %s' % db)
        
        tables = arguments.get('mysql_table', mysql_tables(cursor))
        foreign_keys = mysql_foreign_keys(cursor)
        
        mysql_url = 'jdbc:mysql://%s/%s' % (mysql_hostname, db)
        
        if len(dbs) > 1:
            if not outdir:
                outdir = 'orient'
            myoutdir = "%s-%s" % (db, outdir)
        else:
            if not outdir:
                outdir = db
            myoutdir = outdir
        try:
            os.makedirs(myoutdir)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
    
        for table in tables:
            with open(os.path.join(myoutdir, table + '.json'), 'w') as f:
                json = generate_json(mysql_url, mysql_username, mysql_password, table, table, orientdb_url, orientdb_username, orientdb_password, foreign_keys.get(table, None))
                f.write(json)
    
if __name__ == '__main__':
    main(sys.argv)
