package de.swingbe.ifleet.controller;

import de.swingbe.ifleet.model.LctMsg;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PgPrepStatement {
    private final PgConnection pgCon;

    public PgPrepStatement(PgConnection connection) {
        this.pgCon = connection;
    }

    public void createTable(String table) {
        System.out.println("createTable() start...");
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

            System.out.println("Committed " + counts.length + " updates");

        } catch (SQLException ex) {

            if (pgCon != null) {
                try {
                    pgCon.getConnection().rollback();
                } catch (SQLException ex1) {
                    Logger lgr = Logger.getLogger(PgPrepStatement.class.getName());
                    lgr.log(Level.WARNING, ex1.getMessage(), ex1);
                }
            }

            Logger lgr = Logger.getLogger(PgPrepStatement.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }

        System.out.println("createTable() done.");
    }

    public void insert(LctMsg lctMsg, String table) {
        System.out.println("insert() start...");
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

            System.out.println("Committed " + counts.length + " updates");

        } catch (SQLException ex) {

            if (pgCon != null) {
                try {
                    pgCon.getConnection().rollback();
                } catch (SQLException ex1) {
                    Logger lgr = Logger.getLogger(PgPrepStatement.class.getName());
                    lgr.log(Level.WARNING, ex1.getMessage(), ex1);
                }
            }

            Logger lgr = Logger.getLogger(PgPrepStatement.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }

        System.out.println("insert() done.");
    }

    public boolean hasTable(String table, String schema) {
        System.out.println("hasTable() start...");
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

            Logger lgr = Logger.getLogger(PgPrepStatement.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }

        System.out.println("result: " + result);
        if (result != null && result.equals("true")) {
            System.out.println("result: " + result + " equals true");
            return true;
        } else {
            System.out.println("result: " + result + " equals true NOT");
        }
        System.out.println("hasTable() done.");
        return false;
    }

    public boolean hasLctMsg(LctMsg lctMsg, String table) {
        System.out.println("hasLct() start...");
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

            Logger lgr = Logger.getLogger(PgPrepStatement.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }

        System.out.println("result: " + result);
        if (result != null && result.equals("true")) {
            System.out.println("result: " + result + " equals true");
            return true;
        } else {
            System.out.println("result: " + result + " equals true NOT");
        }
        System.out.println("hasLct() done.");
        return false;
    }
}
