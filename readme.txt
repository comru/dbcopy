Usage: dbcopy
 -dstMeta             don't copy, just print destination database metadata
 -dstPassword <arg>   destination database password
 -dstUrl <arg>        destination database URL
 -dstUser <arg>       destination database user
 -e <arg>             comma-separated list of tables to exclude
 -srcMeta             don't copy, just print source database metadata
 -srcPassword <arg>   source database password
 -srcUrl <arg>        source database URL
 -srcUser <arg>       source database user
 -v                   verbose output
 -oracleSchema        print database schema for Oracle
 -namespace           namespace for which to print database schema for Oracle

Sample command line to copy from PostgreSQL to MS SQLServer excluding table SYS_DB_CHANGELOG:
dbcopy -e SYS_DB_CHANGELOG -srcUrl jdbc:postgresql://localhost/thesis -srcUser root -srcPassword root -dstUrl jdbc:jtds:sqlserver://localhost/thesis -dstUser sa -dstPassword saPass1

Sample command line to print Oracle schema from existing PostgreSQL DB for "REF_" namespace:
dbcopy -oracleSchema -namespace ref_ -srcUrl jdbc:postgresql://localhost/refapp -srcUser root -srcPassword root
