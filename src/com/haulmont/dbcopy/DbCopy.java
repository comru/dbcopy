/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy;

import com.haulmont.dbcopy.dbtypes.MssqlDbType;
import org.apache.commons.cli.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>$Id: DbCopy.java 23061 2015-09-02 10:39:20Z gaslov $</p>
 *
 * @author krivopustov
 */
public class DbCopy {

    public static final String SRC_URL_OPT = "srcUrl";
    public static final String SRC_USER_OPT = "srcUser";
    public static final String SRC_PASSWORD_OPT = "srcPassword";
    public static final String DST_URL_OPT = "dstUrl";
    public static final String DST_USER_OPT = "dstUser";
    public static final String DST_PASSWORD_OPT = "dstPassword";
    public static final String EXCLUDE_OPT = "e";
    public static final String VERBOSE_OPT = "v";
    public static final String DIR_AVAILABLE_FOR_SQL_SERVER = "mssqlDir";

    private Db srcDb;
    private Db dstDb;
    private List<String> excludeTables = new ArrayList<>();
    private List<Table> srcTables = new ArrayList<>();
    private List<Table> dstTables = new ArrayList<>();

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(SRC_URL_OPT, true, "source database URL");
        options.addOption(SRC_USER_OPT, true, "source database user");
        options.addOption(SRC_PASSWORD_OPT, true, "source database password");
        options.addOption(DST_URL_OPT, true, "destination database URL");
        options.addOption(DST_USER_OPT, true, "destination database user");
        options.addOption(DST_PASSWORD_OPT, true, "destination database password");
        options.addOption(EXCLUDE_OPT, true, "comma-separated list of tables to exclude");
        options.addOption(DIR_AVAILABLE_FOR_SQL_SERVER, true, "Directory available for executing in ms sql server");
        options.addOption(VERBOSE_OPT, false, "verbose output");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);

            if (!cmd.hasOption(SRC_URL_OPT) || !cmd.hasOption(DST_URL_OPT)
                    || !cmd.hasOption(SRC_USER_OPT) || !cmd.hasOption(DST_USER_OPT)
                    || !cmd.hasOption(SRC_PASSWORD_OPT) || !cmd.hasOption(DST_PASSWORD_OPT)
                    || !cmd.hasOption(DIR_AVAILABLE_FOR_SQL_SERVER)) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("dbcopy", options);
                System.exit(-1);
            }
            File mssqlDir = new File(cmd.getOptionValue(DIR_AVAILABLE_FOR_SQL_SERVER));
            if (!mssqlDir.exists()) {
                Log.info("Directory available for executing in ms sql server NOT EXIST!!!");
                System.exit(-1);
            }

            Log.init(cmd.hasOption(VERBOSE_OPT));


            Db srcDb = null;
            Db dstDb = null;
            try {
                srcDb = new Db(cmd.getOptionValue(SRC_URL_OPT), cmd.getOptionValue(SRC_USER_OPT), cmd.getOptionValue(SRC_PASSWORD_OPT));
                srcDb.connect();

                dstDb = new Db(cmd.getOptionValue(DST_URL_OPT), cmd.getOptionValue(DST_USER_OPT), cmd.getOptionValue(DST_PASSWORD_OPT));
                dstDb.connect();

                DbCopy dbCopy = new DbCopy(srcDb, dstDb, cmd.getOptionValue(EXCLUDE_OPT));
                dbCopy.init();


                dbCopy.copy(mssqlDir);
            } finally {
                if (srcDb != null) {
                    srcDb.disconnect();
                }
                if (dstDb != null) {
                    dstDb.disconnect();
                }
            }


        } catch (Throwable e) {
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public DbCopy(Db srcDb, Db dstDb, String excludeTables) {
        this.srcDb = srcDb;
        this.dstDb = dstDb;
        if (excludeTables != null) {
            String[] strings = excludeTables.split(",");
            for (String string : strings) {
                this.excludeTables.add(string.trim());
            }
        }
    }

    private boolean isExcluded(String table) {
        for (String excludeTable : excludeTables) {
            if (table.equalsIgnoreCase(excludeTable))
                return true;
        }
        return false;
    }

    private Map<String, List<String>> getCalculatedColumns() throws SQLException {
        if (dstDb.getDbType() instanceof MssqlDbType) {
            String sql = "select c.name as COLUMN_NAME, t.name as TABLE_NAME\n" +
                    "from sys.computed_columns c\n" +
                    "join sys.tables t on c.object_id = t.object_id";
            PreparedStatement statement = dstDb.getConnection().prepareStatement(sql);
            statement.execute();
            ResultSet rs = statement.getResultSet();
            Map<String, List<String>> result = new HashMap<>();
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME").toUpperCase();
                String columnName = rs.getString("COLUMN_NAME").toUpperCase();
                if (result.containsKey(tableName)) {
                    result.get(tableName).add(columnName);
                } else {
                    List<String> list = new ArrayList<>();
                    list.add(columnName);
                    result.put(tableName, list);
                }
            }
            rs.close();
            return result;
        }
        return null;
    }

    private void init() throws SQLException {
        Log.info("Initializing source tables metadata");
        DatabaseMetaData srcMetaData = srcDb.getConnection().getMetaData();
        try (ResultSet srcTablesRs = srcMetaData.getTables(null, null, null, new String[]{"TABLE"})) {
            while (srcTablesRs.next()) {
                String tableName = srcTablesRs.getString("TABLE_NAME");
                if (!isExcluded(tableName)) {
                    Table table = new Table(srcDb, tableName, null);
                    srcTables.add(table);
                }
            }
        }

        Log.info("Initializing destination tables metadata");
        Map<String, List<String>> calculatedColumns = getCalculatedColumns();
        DatabaseMetaData dstMetaData = dstDb.getConnection().getMetaData();
        try (ResultSet dstTablesRs = dstMetaData.getTables(null, null, null, new String[]{"TABLE"})) {
            while (dstTablesRs.next()) {
                String tableName = dstTablesRs.getString("TABLE_NAME");
                final Table srcTable = getTable(srcTables, tableName);
                if (srcTable == null) {
                    continue;
                }
                if (!isExcluded(tableName)) {
                    List<String> tableCalculatedColumns =
                            calculatedColumns != null ? calculatedColumns.get(tableName.toUpperCase()) : null;
                    Table table = new Table(dstDb, tableName, tableCalculatedColumns);
                    dstTables.add(table);
                    table.initializeMetadata();
                }
            }
        }

        final List<Table> toRemove = srcTables.stream()
                .filter(table -> getTable(dstTables, table.getName()) == null).collect(Collectors.toList());
        srcTables.removeAll(toRemove);
        srcTables.forEach(Table::initializeMetadata);
    }

    private interface TableProcessor {
        void run(Table srcTable, Table dstTable) throws SQLException, ClassNotFoundException;
    }

    private void apply(TableProcessor processor) throws SQLException, ClassNotFoundException {
        for (Table srcTable : srcTables) {
            Table dstTable = getTable(dstTables, srcTable.getName());
            if (dstTable != null) {
                processor.run(srcTable, dstTable);
            }
        }
    }

    private void copy(File mssqlDir) throws SQLException, ClassNotFoundException {
        apply((srcTable, dstTable) -> dstTable.dropConstraints());

        apply((srcTable, dstTable) -> {
            Log.info("Start copy process for: " + srcTable.getName());
            File importDataFile;
            srcDb.connect();
            try (Connection connection = srcDb.getConnection()) {
                CopyManager copyManager = new CopyManager((BaseConnection) connection);
                try {
                    final long start = System.currentTimeMillis();
                    final String fileName = "import-" + start + "-" + srcTable.getName() + ".csv";
                    File possibleImportData = new File(mssqlDir, fileName);
                    final boolean isFileCreated = possibleImportData.createNewFile();
                    importDataFile = possibleImportData;
                    Log.info("File " + importDataFile.getAbsolutePath() + " created. File created state = " + isFileCreated);
                    FileOutputStream fileOutputStream = new FileOutputStream(importDataFile);
                    //and finally execute the COPY command to the file with this method:
                    copyManager.copyOut(
                            "COPY (" + dstTable.getSelectSql() + ") TO STDOUT (FORMAT CSV, HEADER, DELIMITER ',', FORCE_QUOTE *)",
                            fileOutputStream);
                    Log.info("CSV file was created in " + (System.currentTimeMillis() - start) + "ms");
                    fileOutputStream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }

            dstDb.connect();
            try (Connection dstConn = dstDb.getConnection();
                 final Statement stmt = dstConn.createStatement()) {
                long start = System.currentTimeMillis();
                try (Statement statement = dstConn.createStatement()) {
                    // Empty the destination table.
                    statement.execute("TRUNCATE TABLE " + dstTable.getName());
                }
                Log.info("Data was deleted from destination server table in "
                        + (System.currentTimeMillis() - start) + "ms");

                start = System.currentTimeMillis();
                final int updatedRow = stmt.executeUpdate("BULK INSERT " + dstTable.getName() +
                        " from '" + importDataFile.getAbsolutePath() + "'" +
                        " WITH ( FORMAT='CSV', FIRSTROW=2,ROWTERMINATOR = '0x0a')");
                Log.info("Data was imported (" + updatedRow + " updated rows) in destination server in " + (System.currentTimeMillis() - start) + "ms");

                dstConn.commit();
            }
            boolean isFileDelete = importDataFile.delete();
            Log.info("File " + importDataFile.getAbsolutePath() + " deleted. File delete state: " + isFileDelete + "\n\n");
        });

        apply((srcTable, dstTable) -> dstTable.createConstraints());
    }

    private Table getTable(List<Table> tables, String name) {
        for (Table table : tables) {
            if (table.getName().equalsIgnoreCase(name))
                return table;
        }
        return null;
    }
}
