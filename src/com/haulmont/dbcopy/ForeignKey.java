/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>$Id: ForeignKey.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public class ForeignKey {

    private String name;
    protected String pkTable;
    private int updateRule;
    private int deleteRule;
    protected List<String> pkColumns = new ArrayList<String>();
    private List<String> fkColumns = new ArrayList<String>();

    public ForeignKey(String name, String pkTable, int updateRule, int deleteRule) {
        this.name = name;
        this.pkTable = pkTable;
        this.updateRule = updateRule;
        this.deleteRule = deleteRule;
    }

    public String getName() {
        return name;
    }

    public String getPkTable() {
        return pkTable;
    }

    public List<String> getPkColumns() {
        return pkColumns;
    }

    public List<String> getFkColumns() {
        return fkColumns;
    }

    public int getUpdateRule() {
        return updateRule;
    }

    public int getDeleteRule() {
        return deleteRule;
    }

    public String getCreateSql(String table) {
        StringBuilder sb = new StringBuilder();

        sb.append("alter table ").append(table).append(" add constraint ").append(name)
                .append(" foreign key (");
        append(sb, fkColumns);
        sb.append(") references ").append(pkTable).append(" (");
        append(sb, pkColumns);
        sb.append(")");

        if (updateRule == DatabaseMetaData.importedKeyCascade) {
            sb.append(" on update cascade");
        } else if (updateRule == DatabaseMetaData.importedKeySetNull) {
            sb.append(" on update set null");
        }

        if (deleteRule == DatabaseMetaData.importedKeyCascade) {
            sb.append(" on delete cascade");
        } else if (deleteRule == DatabaseMetaData.importedKeySetNull) {
            sb.append(" on delete set null");
        }

        return sb.toString();
    }

    public String getDropSql(String table) {
        return "alter table " + table + " drop constraint " + name;
    }

    private void append(StringBuilder sb, List<String> strings) {
        for (int i = 0; i < strings.size(); i++) {
            sb.append(strings.get(i));
            if (i < strings.size()-1)
                sb.append(",");
        }
    }

    @Override
    public String toString() {
        return "{" +
                "pkTable='" + pkTable + '\'' +
                ", pkColumns='" + pkColumns + '\'' +
                ", fkColumns=" + fkColumns +
                '}';
    }
}

