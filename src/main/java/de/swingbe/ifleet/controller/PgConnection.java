package de.swingbe.ifleet.controller;

import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

public class PgConnection {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PgConnection.class);

    private final String host;
    private final String port;
    private final String db;
    private final String url;
    private final String usr;
    private final String key;
    private Connection connection = null;

    public PgConnection(String host, String port, String db, String usr, String key) {
        this.host = Objects.requireNonNull(host, "arg must not be null");
        this.port = Objects.requireNonNull(port, "arg must not be null");
        this.db = Objects.requireNonNull(db, "arg must not be null");
        this.url = "jdbc:postgresql://" + host + ":" + port + "/" + db;
        this.usr = Objects.requireNonNull(usr, "arg must not be null");
        this.key = Objects.requireNonNull(key, "arg must not be null");
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
        LOG.debug("setConnection() start...");

        if (connection == null) {
            try {
                connection = DriverManager.getConnection(url, usr, key);
            } catch (SQLException e) {
                LOG.error("setConnection() can not connect to : " + url + " with user: " + usr + " cos of: " + e.getMessage());
            }
        }
        LOG.debug("setConnection() done.");
    }
}
