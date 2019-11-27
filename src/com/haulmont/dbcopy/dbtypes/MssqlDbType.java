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
 * <p>$Id: MssqlDbType.java 23049 2015-09-01 11:37:56Z gaslov $</p>
 *
 * @author krivopustov
 */
public class MssqlDbType extends DbType {

    @Override
    public String getDriver() {
        return "net.sourceforge.jtds.jdbc.Driver";
    }

    @Override
    public Column createColumn(String name, int sqlType, String typeName) {
        switch (sqlType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return new BooleanColumn(name, sqlType);
            case Types.TINYINT:
                return new ByteColumn(name, sqlType);
            case Types.SMALLINT:
            case Types.INTEGER:
                return new IntegerColumn(name, sqlType);
            case Types.BIGINT:
                return new LongColumn(name, sqlType);
            case Types.FLOAT:
            case Types.DOUBLE:
                return new DoubleColumn(name, sqlType);
            case Types.NUMERIC:
            case Types.DECIMAL:
                return new BigDecimalColumn(name, sqlType);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.NVARCHAR:
                if ("uniqueidentifier".equals(typeName))
                    return new UuidColumn(name, sqlType);
                else
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
                return new OtherColumn(name, sqlType);
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported SQL type %d for column %s", sqlType, name));
        }
    }
}
