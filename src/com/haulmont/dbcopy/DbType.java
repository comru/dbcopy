/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy;

import com.haulmont.dbcopy.dbtypes.MssqlDbType;
import com.haulmont.dbcopy.dbtypes.MssqlJdbcDbType;
import com.haulmont.dbcopy.dbtypes.PostgresDbType;

/**
 * <p>$Id: DbType.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public abstract class DbType {

    public static DbType fromUrl(String url) {
        if (url.contains(":postgresql:"))
            return new PostgresDbType();
        else if (url.contains(":jtds:sqlserver:"))
            return new MssqlDbType();
        else if (url.contains("jdbc:sqlserver:")) {
            return new MssqlJdbcDbType();
        }
        else
            throw new UnsupportedOperationException("Unknown URL format: " + url);
    }

    public abstract String getDriver();

    public abstract Column createColumn(String name, int sqlType, String typeName);
}
