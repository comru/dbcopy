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

public class IntegerColumn extends Column<Integer>
{
    public IntegerColumn(String name, int sqlType) {
        super(name, sqlType);
    }

    public Integer readValue(ResultSet rs) throws SQLException {
        int i = rs.getInt(name);
        return rs.wasNull() ? null : i;
    }

    public void assignParameter(PreparedStatement st, int idx, Object value) throws SQLException {
        if (value == null)
            st.setNull(idx+1, sqlType);
        else
            st.setInt(idx+1, (Integer) value);
    }
}
