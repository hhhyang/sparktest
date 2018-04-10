package com.sparktest.streamingnetflow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SqlConnectionManager {
    private Connection conn;
    private PreparedStatement statement;

    private String url;
    private String user;
    private String passwd;

    public SqlConnectionManager(final String url, final String user, final String passwd) {
        this.url = url;
        this.user = user;
        this.passwd = passwd;

        try {
            conn = DriverManager.getConnection(url, user, passwd);
        }
        catch (SQLException e) {
            this.url = null;
            this.user = null;
            this.passwd = null;
            conn = null;
            statement = null;
        }
    }

    public Connection getConnection() {
        return conn;
    }

    public PreparedStatement setPrepareStatement(final String sql) {
        try {
            if (conn != null) {
                statement = conn.prepareStatement(sql);
            }
        }
        catch (SQLException e) {
            statement = null;
        }
        return statement;
    }

    public void close() {
        try {
            if (statement != null) {
                statement.close();
            }
        }
        catch (SQLException e) {
            System.out.println("SQLException message: " + e.getMessage());
        }

        try {
            if (conn != null) {
                conn.close();
            }
        }
        catch (SQLException e) {
            System.out.println("SQLException message: " + e.getMessage());
        }
    }

}

