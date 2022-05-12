package de.swingbe.ifleet.controller;

import de.swingbe.ifleet.model.LctMsg;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public class PgPrepStatement {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PgPrepStatement.class);
    private final PgConnection pgCon;

    public PgPrepStatement(PgConnection connection) {
        this.pgCon = connection;
    }

    public void createTable(String table) {
        LOG.debug("createTable() start...");
        Objects.requireNonNull(table, "table must not be null");

        try (Statement st = pgCon.getConnection().createStatement()) {

            //autocommit should always be turned off when doing batch updates
            pgCon.getConnection().setAutoCommit(false);

            //sql query
            String sqlDrop = "DROP TABLE IF EXISTS " + table;
            String sqlCreate = "CREATE TABLE " + table + "(bs_id bigserial PRIMARY KEY NOT NULL,vc_trip VARCHAR(20) NOT NULL,vc_route VARCHAR(20),vc_tenant VARCHAR(20),vc_date VARCHAR(20) NOT NULL,vc_time VARCHAR(20) NOT NULL,vc_lat VARCHAR(20) NOT NULL,vc_lon VARCHAR(20) NOT NULL)";

            //create a new table
            st.addBatch(sqlDrop);
            st.addBatch(sqlCreate);

            //method returns an array of committed changes
            int[] counts = st.executeBatch();

            pgCon.getConnection().commit();

            LOG.debug("createTable() Committed " + counts.length + " updates");

        } catch (SQLException ex) {

            if (pgCon != null) {
                try {
                    pgCon.getConnection().rollback();
                } catch (SQLException ex1) {
                    LOG.error("createTable() can not rollback connection" + ex.getMessage());
                }
            }
            LOG.error("createTable() can not create table: " + table + " cos of: " + ex.getMessage());
        }

        LOG.debug("createTable() done.");
    }

    public void insert(LctMsg lctMsg, String table) {
        LOG.debug("insert() start...");
        Objects.requireNonNull(lctMsg, "lctMsg must not be null");
        Objects.requireNonNull(table, "table     must not be null");

        try (Statement st = pgCon.getConnection().createStatement()) {

            //autocommit should always be turned off when doing batch updates
            pgCon.getConnection().setAutoCommit(false);

            //sql query
            String sql = "INSERT INTO " + table + "(vc_trip,vc_route,vc_tenant,vc_date,vc_time,vc_lat,vc_lon) VALUES ('" + lctMsg.getTrip() + "','" + lctMsg.getRoute() + "','" + lctMsg.getTenant() + "','" + lctMsg.getDate() + "','" + lctMsg.getTime() + "','" + lctMsg.getLat() + "','" + lctMsg.getLon() + "')";

            //insert lct
            st.addBatch(sql);

            //method returns an array of committed changes
            int[] counts = st.executeBatch();

            pgCon.getConnection().commit();

            LOG.debug("insert() Committed " + counts.length + " updates");

        } catch (SQLException ex) {

            if (pgCon != null) {
                try {
                    pgCon.getConnection().rollback();
                } catch (SQLException ex1) {
                    LOG.error("insert() can not rollback connection" + ex.getMessage());
                }
            }
            LOG.error("insert() can not insert into table: " + table + " cos of: " + ex.getMessage());
        }

        LOG.debug("insert() done.");
    }

    public boolean hasTable(String table, String schema) {
        LOG.debug("hasTable() start...");
        Objects.requireNonNull(table, "table must not be null");
        Objects.requireNonNull(schema, "schema must not be null");

        String query = "SELECT CASE WHEN EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_NAME = '" + table + "') THEN 'true' ELSE 'false' END;";

        String result = null;
        //create prepared statement using placeholders instead of directly writing values
        try (PreparedStatement pst = pgCon.getConnection().prepareStatement(query); ResultSet rs = pst.executeQuery()) {

            //advance cursor to the next record
            //return false if there are no more records in the result set
            while (rs.next()) {
                result = rs.getString(1);
            }

        } catch (SQLException ex) {
            LOG.error("hasTable() can not execute query for: " + table + " cos of: " + ex.getMessage());
        }

        LOG.debug("hasTable() result: " + result);
        if (result != null && result.equals("true")) {
            LOG.debug("hasTable() result: " + result + " equals true");
            return true;
        } else {
            LOG.debug("hasTable() result: " + result + " equals true NOT");
        }
        LOG.debug("hasTable() done.");
        return false;
    }

    public boolean hasLctMsg(LctMsg lctMsg, String table) {
        LOG.debug("hasLctMsg() start...");
        Objects.requireNonNull(lctMsg, "lctMsg must not be null");
        Objects.requireNonNull(table, "table must not be null");

        String query = "SELECT CASE WHEN EXISTS (SELECT * FROM " + table + " where vc_date='" + lctMsg.getDate() + "' AND vc_trip='" + lctMsg.getTrip() + "') THEN 'true' ELSE 'false' END;";

        String result = null;
        //create prepared statement using placeholders instead of directly writing values
        try (PreparedStatement pst = pgCon.getConnection().prepareStatement(query); ResultSet rs = pst.executeQuery()) {

            //advance cursor to the next record
            //return false if there are no more records in the result set
            while (rs.next()) {
                result = rs.getString(1);
            }

        } catch (SQLException ex) {

            LOG.error("hasTable() can not execute query for: " + table + " cos of: " + ex.getMessage());
        }

        LOG.debug("hasLctMsg() result: " + result);
        if (result != null && result.equals("true")) {
            LOG.debug("hasLctMsg() result: " + result + " equals true");
            return true;
        } else {
            LOG.debug("hasLctMsg() result: " + result + " equals true NOT");
        }
        LOG.debug("hasLctMsg() done.");
        return false;
    }
}
