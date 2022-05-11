package de.swingbe.ifleet.controller;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PgConnection {
    private final String host;
    private final String port;
    private final String db;
    private final String url;
    private final String usr;
    private final String key;
    private Connection connection = null;

    public PgConnection(String host, String port, String db, String usr, String key) {
        this.host = Objects.requireNonNull(host, "host must not be null");
        this.port = Objects.requireNonNull(port, "port must not be null");
        this.db = Objects.requireNonNull(db, "db must not be null");
        this.url = "jdbc:postgresql://" + host + ":" + port + "/" + db;
        this.usr = Objects.requireNonNull(usr, "usr must not be null");
        this.key = Objects.requireNonNull(key, "key must not be null");
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getDb() {
        return db;
    }

    public String getUrl() {
        return url;
    }

    public String getUsr() {
        return usr;
    }

    public String getKey() {
        return key;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection() {
        //TODO switch to logger
        System.out.println("setConnection() start...");
        if (connection == null) {
            try {
                connection = DriverManager.getConnection(url, usr, key);
            } catch (SQLException e) {
                //TODO handle logging properly
                Logger lgr = Logger.getLogger(PgConnection.class.getName());
                lgr.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        System.out.println("setConnection() done.");
    }
}
