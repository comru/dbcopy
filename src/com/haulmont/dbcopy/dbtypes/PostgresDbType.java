/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy.dbtypes;

import com.haulmont.dbcopy.Column;
import com.haulmont.dbcopy.DbType;
import com.haulmont.dbcopy.columns.*;

import java.sql.Types;

/**
 * <p>$Id: PostgresDbType.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public class PostgresDbType extends DbType {

    @Override
    public String getDriver() {
        return "org.postgresql.Driver";
    }

    @Override
    public Column createColumn(String name, int sqlType, String typeName) {
        switch (sqlType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return new BooleanColumn(name, sqlType);
            case Types.TINYINT:
                return new ByteColumn(name, sqlType);
            case Types.INTEGER:
            case Types.SMALLINT:
                return new IntegerColumn(name, sqlType);
            case Types.BIGINT:
                if (typeName.equals("oid"))
                    return new PostgresOidColumn(name, sqlType);
                else
                    return new LongColumn(name, sqlType);
            case Types.FLOAT:
            case Types.DOUBLE:
                return new DoubleColumn(name, sqlType);
            case Types.NUMERIC:
                return new BigDecimalColumn(name, sqlType);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                return new StringColumn(name, sqlType);
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return new DateColumn(name, sqlType);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return new ByteArrayColumn(name, sqlType);
            case Types.OTHER:
                if ("uuid".equals(typeName))
                    return new PostgresUuidColumn(name, sqlType);
                else
                    return new OtherColumn(name, sqlType);
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported SQL type %d for column %s", sqlType, name));
        }
    }
}
