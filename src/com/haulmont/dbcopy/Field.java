/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy;

/**
 * <p>$Id: Field.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public class Field {

    private Column column;
    private Object value;

    public Field(Column column, Object value) {
        this.column = column;
        this.value = value;
    }

    public Column getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return column.getName() + "=" + value;
    }
}
