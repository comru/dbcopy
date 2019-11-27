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

/**
 * <p>$Id: BooleanColumn.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public class BooleanColumn extends Column<Boolean> {

    public BooleanColumn(String name, int sqlType) {
        super(name, sqlType);
    }

    @Override
    public Boolean readValue(ResultSet rs) throws SQLException {
        boolean b = rs.getBoolean(name);
        return rs.wasNull() ? null : b;
    }

    @Override
    public void assignParameter(PreparedStatement st, int idx, Object value) throws SQLException {
        if (value == null)
            st.setNull(idx+1, sqlType);
        else {
            boolean b;
            if (value instanceof Boolean)
                b = ((Boolean) value);
            else if (value instanceof Number)
                b = ((Number) value).longValue() != 0;
            else
                throw new UnsupportedOperationException("Unsupported value type: " + value.getClass());

            st.setBoolean(idx+1, b);
        }
    }
}
