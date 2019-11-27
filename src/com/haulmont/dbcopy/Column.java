/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>$Id: Column.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public abstract class Column<T> {

    public static final String NULL_VAL = "null";

    protected final String name;
    protected final int sqlType;
    protected int precision;
    protected int scale;

    public Column(String name, int sqlType) {
        this.name = name;
        this.sqlType = sqlType;
    }

    public String getName() {
        return name;
    }

    public abstract T readValue(ResultSet rs) throws SQLException;

    public abstract void assignParameter(PreparedStatement st, int idx, Object value) throws SQLException;

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    @Override
    public String toString() {
        return name;
    }
}
