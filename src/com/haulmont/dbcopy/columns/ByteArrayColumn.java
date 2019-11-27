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

public class ByteArrayColumn extends Column<byte[]>
{
    public ByteArrayColumn(String name, int sqlType) {
        super(name, sqlType);
    }

    public byte[] readValue(ResultSet rs) throws SQLException {
        return rs.getBytes(name);
    }

    public void assignParameter(PreparedStatement st, int idx, Object value) throws SQLException {
        if (value == null)
            st.setNull(idx+1, sqlType);
        else
            st.setBytes(idx+1, (byte[]) value);
    }
}
