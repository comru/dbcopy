/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy.columns;

import com.haulmont.dbcopy.Column;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ByteColumn extends Column<Byte>
{
    public ByteColumn(String name, int sqlType) {
        super(name, sqlType);
    }

    public Byte readValue(ResultSet rs) throws SQLException {
        byte b = rs.getByte(name);
        return rs.wasNull() ? null : b;
    }

    public void assignParameter(PreparedStatement st, int idx, Object value) throws SQLException {
        if (value == null)
            st.setNull(idx+1, sqlType);
        else {
            byte b;
            if (value instanceof Number)
                b = ((Number) value).byteValue();
            else if (value instanceof Boolean)
                b = (byte) (((Boolean) value) ? 1 : 0);
            else
                throw new UnsupportedOperationException("Unsupported value type: " + value.getClass());

            st.setByte(idx+1, b);
        }
    }
}
