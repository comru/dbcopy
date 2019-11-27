/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy.columns;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

/**
 * <p>$Id: PostgresUuidColumn.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public class PostgresUuidColumn extends UuidColumn {

    private static class PostgresUUID extends org.postgresql.util.PGobject
    {
        private static final long serialVersionUID = -8115115840321643248L;

        public PostgresUUID(UUID uuid) throws SQLException {
            super();
            this.setType("uuid");
            this.setValue(uuid.toString());
        }
    }

    public PostgresUuidColumn(String name, int sqlType) {
        super(name, sqlType);
    }

    @Override
    public void assignParameter(PreparedStatement st, int idx, Object value) throws SQLException {
        if (value == null)
            st.setNull(idx+1, sqlType);
        else {
            st.setObject(idx + 1, new PostgresUUID((UUID) value));
        }
    }
}
