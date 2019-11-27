/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy.columns;

import com.haulmont.dbcopy.Column;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>$Id: PostgresOidColumn.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public class PostgresOidColumn extends Column<byte[]> {

    public PostgresOidColumn(String name, int sqlType) {
        super(name, sqlType);
    }

    @Override
    public byte[] readValue(ResultSet rs) throws SQLException {
        Blob blob = rs.getBlob(name);
        return blob == null ? null : blob.getBytes(1, (int) blob.length());
    }

    @Override
    public void assignParameter(PreparedStatement st, int idx, Object value) throws SQLException {
        if (value == null)
            st.setNull(idx+1, sqlType);
        else {
            LargeObjectManager lobj = ((org.postgresql.PGConnection) st.getConnection()).getLargeObjectAPI();
            long oid = lobj.createLO(LargeObjectManager.READ | LargeObjectManager.WRITE);
            LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE);
            obj.write((byte[]) value, 0, ((byte[]) value).length);
            obj.close();

            st.setLong(idx + 1, oid);
        }
    }
}
