/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * <p>$Id: Table.java 23061 2015-09-02 10:39:20Z gaslov $</p>
 *
 * @author krivopustov
 */
public class Table {

    private Db db;
    private String name;
    private List<Column> columns = new ArrayList<>();
    private List<String> pkeys = new ArrayList<>(1);
    private List<ForeignKey> fkeys = new ArrayList<>(5);
    private List<String> calculatedColumns;

    public Table(Db db, String name, List<String> calculatedColumns) {
        this.db = db;
        this.name = name;
        this.calculatedColumns = calculatedColumns;
    }

    public void initializeMetadata() {
        Log.debug("Initializing " + name);
        try {
            DatabaseMetaData dbMeta = db.getConnection().getMetaData();

            fillColumns(dbMeta);

            try (ResultSet pkeysRs = dbMeta.getPrimaryKeys(null, null, name)) {
                while (pkeysRs.next()) {
                    pkeys.add(pkeysRs.getString("COLUMN_NAME"));
                }
            }

            try (ResultSet fkeysRs = dbMeta.getImportedKeys(null, null, name)) {
                while (fkeysRs.next()) {
                    addForeignKey(
                            fkeysRs.getString("FK_NAME"),
                            fkeysRs.getString("PKTABLE_NAME"),
                            fkeysRs.getString("PKCOLUMN_NAME"),
                            fkeysRs.getString("FKCOLUMN_NAME"),
                            fkeysRs.getInt("UPDATE_RULE"),
                            fkeysRs.getInt("DELETE_RULE"),
                            fkeysRs.getInt("KEY_SEQ")
                    );
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException("Unable to read DB metadata for table " + name, e);
        }
    }

    private void fillColumns(DatabaseMetaData dbMeta) throws SQLException {
        columns.clear();
        try (ResultSet rs = dbMeta.getColumns(null, null, name, null)) {
            while (rs.next()) {
                Column column = db.getDbType().createColumn(
                        rs.getString("COLUMN_NAME"),
                        rs.getInt("DATA_TYPE"),
                        rs.getString("TYPE_NAME")
                );
                column.setPrecision(rs.getInt("COLUMN_SIZE"));
                column.setScale(rs.getInt("DECIMAL_DIGITS"));
                if (calculatedColumns == null || !calculatedColumns.contains(column.getName().toUpperCase())) {
                    columns.add(column);
                }
            }
        }
    }

    private void addForeignKey(String name, String pkTable, String pkColumn, String fkColumn,
                               int updateRule, int deleteRule, int keySeq) {
        if (keySeq > 1) {
            for (ForeignKey fk : fkeys) {
                if (fk.pkTable.equals(pkTable)) {
                    fk.getPkColumns().add(pkColumn);
                    fk.getFkColumns().add(fkColumn);
                    return;
                }
            }
        }
        ForeignKey fk = new ForeignKey(name, pkTable, updateRule, deleteRule);
        fk.getPkColumns().add(pkColumn);
        fk.getFkColumns().add(fkColumn);
        fkeys.add(fk);
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<String> getPkeys() {
        return pkeys;
    }

    public List<ForeignKey> getFkeys() {
        return fkeys;
    }

    public String getSelectSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        for (Iterator it = getColumns().iterator(); it.hasNext();) {
            Column column = (Column) it.next();
            sb./*append("[").*/append(column.getName())/*.append("]")*/;
            if (it.hasNext())
                sb.append(",");
        }
        sb.append(" from ").append(name);
        return sb.toString();
    }

    public String getInsertSql() {
        StringBuilder namesSb = new StringBuilder();
        StringBuilder valuesSb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            namesSb./*append("[").*/append(column.getName())/*.append("]")*/;
            valuesSb.append("?");
            if (i < columns.size()-1) {
                namesSb.append(",");
                valuesSb.append(",");
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(name)
                .append(" (").append(namesSb).append(") values (").append(valuesSb).append(")");
        return sb.toString();
    }

    public String toString() {
        return super.toString() + "{" + name + "}";
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Table table = (Table) o;

        return name.equals(table.name);
    }

    public int hashCode() {
        return name.hashCode();
    }

    public List<Field[]> read() throws SQLException {
        Log.info("Reading " + name);

        List<Field[]> rows = new ArrayList<>();

        String sql = getSelectSql();
        PreparedStatement statement = db.getConnection().prepareStatement(sql);
        ResultSet rs;
        try {
            rs = statement.executeQuery();
        } catch (SQLException e) {
            Log.error("Unable to execute query: " + sql);
            throw e;
        }
        while (rs.next()) {
            Field[] row = new Field[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                try {
                    row[i] = new Field(column, column.readValue(rs));
                } catch (SQLException e) {
                    Log.error("Unable to read field " + column + " of row " + Arrays.toString(row));
                    throw e;
                }
            }
            rows.add(row);
        }
        rs.close();
        db.getConnection().commit();
        return rows;
    }

    public void write(List<Field[]> rows) throws SQLException {
        Log.info("Writing " + name);

        String sql = getInsertSql();
        PreparedStatement statement = db.getConnection().prepareStatement(sql);
        for (Field[] row : rows) {
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                try {
                    for (Field field : row) {
                        if (field.getColumn().getName().equalsIgnoreCase(column.getName())) {
                            column.assignParameter(statement, i, field.getValue());
                            break;
                        }
                    }
                } catch (SQLException e) {
                    Log.error("Unable to assign value to field " + column);
                    throw e;
                }
            }
            try {
                statement.execute();
                db.getConnection().commit();
            } catch (SQLException e) {
                Log.error("Unable to execute: " + sql);
                throw e;
            }
        }
    }

    public void deleteAll() throws SQLException {
        Log.info("Deleting all from " + name);
        Statement statement = db.getConnection().createStatement();
        statement.execute("delete from " + name);
        statement.close();
        db.getConnection().commit();
    }

    public void dropConstraints() throws SQLException {
        if (fkeys.isEmpty())
            return;

        Log.info("Dropping constraints for " + name);
        for (ForeignKey fkey : fkeys) {
            Statement statement = db.getConnection().createStatement();
            String sql = fkey.getDropSql(name);
            try {
                statement.execute(sql);
            } catch (SQLException e) {
                Log.error("Unable to execute: " + sql);
                throw e;
            }
            statement.close();
            db.getConnection().commit();
        }
    }

    public void createConstraints() throws SQLException {
        if (fkeys.isEmpty())
            return;

        Log.info("Creating constraints for " + name);
        for (ForeignKey fkey : fkeys) {
            String sql = fkey.getCreateSql(name);
            Log.debug(sql);
            Statement statement = db.getConnection().createStatement();
            try {
                statement.execute(sql);
            } catch (SQLException e) {
                Log.error("Unable to execute: " + sql);
                throw e;
            }
            statement.close();
            db.getConnection().commit();
        }
    }
}
