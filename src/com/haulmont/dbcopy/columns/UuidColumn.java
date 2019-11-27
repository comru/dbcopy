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
import java.util.UUID;

/**
 * <p>$Id: UuidColumn.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public class UuidColumn extends Column<UUID> {

    public UuidColumn(String name, int sqlType) {
        super(name, sqlType);
    }

    @Override
    public UUID readValue(ResultSet rs) throws SQLException {
        Object object = rs.getObject(name);
        return object == null ? null : UUID.fromString(object.toString());
    }

    @Override
    public void assignParameter(PreparedStatement st, int idx, Object value) throws SQLException {
        if (value == null)
            st.setNull(idx+1, sqlType);
        else
            st.setObject(idx + 1, value.toString());
    }
}
