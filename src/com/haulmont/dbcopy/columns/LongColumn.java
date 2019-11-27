/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy.columns;

import com.haulmont.dbcopy.Column;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;

public class LongColumn extends Column<Long>
{
    public LongColumn(String name, int sqlType) {
        super(name, sqlType);
    }

    public Long readValue(ResultSet rs) throws SQLException {
        long l = rs.getLong(name);
        return rs.wasNull() ? null : l;
    }

    public void assignParameter(PreparedStatement st, int idx, Object value) throws SQLException {
        if (value == null)
            st.setNull(idx+1, sqlType);
        else
            st.setLong(idx+1, (Long) value);
    }
}
