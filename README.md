# mysql2orient
A command-line tool to transform a MySQL database into OrientDB by dynamically generating JSON configuration files and using the OrientDB-ETL.

The python script mysql2orient.py reads the tables of the MySQL database to generate an output folder of JSON files.

    Usage: ./mysql2orient.py
    --orient_database
    --orient_username <default: admin>
    --orient_password <default: admin>
    --mysql_hostname <default: localhost>
    --mysql_username <default: root>
    --mysql_password <default: "">
    --mysql_database <default: read all databases>

Once the JSON configuration files are generated, you can use the OrientDB ETL program to load MySQL data into OrientDB directly. Please read https://github.com/orientechnologies/orientdb-etl for more information about OrientDB-ETL.
