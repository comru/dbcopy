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
import java.math.BigDecimal;

public class BigDecimalColumn extends Column<BigDecimal>
{
    public BigDecimalColumn(String name, int sqlType) {
        super(name, sqlType);
    }

    public BigDecimal readValue(ResultSet rs) throws SQLException {
        return rs.getBigDecimal(name);
    }

    public void assignParameter(PreparedStatement st, int idx, Object value) throws SQLException {
        if (value == null)
            st.setNull(idx+1, sqlType);
        else
            st.setBigDecimal(idx+1, (BigDecimal) value);
    }
}
