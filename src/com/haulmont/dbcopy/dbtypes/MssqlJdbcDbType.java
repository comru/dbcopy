package com.haulmont.dbcopy.dbtypes;

public class MssqlJdbcDbType extends MssqlDbType {
    @Override
    public String getDriver() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }
}
