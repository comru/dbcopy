/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * <p>$Id: Db.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public class Db {

    private String url;
    private String user;
    private String password;
    private DbType dbType;

    private Connection connection;

    public Db(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
        dbType = DbType.fromUrl(url);

    }

    public void connect() throws SQLException, ClassNotFoundException {
        Class.forName(dbType.getDriver());
        connection = DriverManager.getConnection(url, user, password);
        connection.setAutoCommit(false);
    }

    public void disconnect() {
        if (connection != null)
            try {
                connection.close();
            } catch (SQLException e) {
                //
            }
    }

    public Connection getConnection() {
        return connection;
    }

    public DbType getDbType() {
        return dbType;
    }
}
