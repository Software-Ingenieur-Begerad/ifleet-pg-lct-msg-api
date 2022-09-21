package de.swingbe.ifleet.controller;

import de.swingbe.ifleet.model.LctMsg;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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

    public void update(LctMsg lctMsg, String table) {
        LOG.debug("update() start...");
        Objects.requireNonNull(lctMsg, "arg must not be null");
        Objects.requireNonNull(table, "arg must not be null");

        try (Statement st = pgCon.getConnection().createStatement()) {

            //autocommit should always be turned off when doing batch updates
            pgCon.getConnection().setAutoCommit(false);

            //sql query
            LOG.debug("update() trip: " + lctMsg.getTrip());
            String sql = "UPDATE " + table + "SET vc_route = " + lctMsg.getRoute() + ", vc_tenant = " + lctMsg.getTenant() + ", vc_date = " + lctMsg.getDate() + ", vc_time = " + lctMsg.getTime() + ", vc_lat = " + lctMsg.getLat() + ", vc_lon = " + lctMsg.getLon() + " WHERE vc_trip = '" + lctMsg.getTrip() + "';";

            //insert lct
            st.addBatch(sql);

            //method returns an array of committed changes
            int[] counts = st.executeBatch();

            pgCon.getConnection().commit();

            LOG.debug("update() Committed " + counts.length + " updates");

        } catch (SQLException ex) {

            if (pgCon != null) {
                try {
                    pgCon.getConnection().rollback();
                } catch (SQLException ex1) {
                    LOG.error("update() can not rollback connection" + ex.getMessage());
                }
            }
            LOG.error("update() can not update table: " + table + " cos of: " + ex.getMessage());
        }

        LOG.debug("update() done.");
    }

    public void insert(LctMsg lctMsg, String table) {
        LOG.debug("insert() start...");
        Objects.requireNonNull(lctMsg, "arg must not be null");
        Objects.requireNonNull(table, "arg must not be null");

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

    public ArrayList<ArrayList<String>> get(String date, String tenant, String time, String trip, String route) {
        LOG.debug("get() start...");
        Objects.requireNonNull(date, "arg must not be null");
        Objects.requireNonNull(tenant, "arg must not be null");
        Objects.requireNonNull(time, "arg must not be null");
        Objects.requireNonNull(trip, "arg must not be null");
        Objects.requireNonNull(route, "arg must not be null");

        String query = "SELECT vc_trip,vc_route,vc_tenant,vc_date,vc_time,vc_lat,vc_lon from lct_msg where vc_date like '" + date + "' and vc_tenant like '" + tenant + "' and vc_time like '" + time + "' and vc_trip like '" + trip + "';";

        ArrayList<ArrayList<String>> aryResult = null;
        //create prepared statement using placeholders instead of directly writing values
        try (PreparedStatement pst = pgCon.getConnection().prepareStatement(query); ResultSet rs = pst.executeQuery()) {

            //advance cursor to the next record
            //return false if there are no more records in the aryRecord set
            while (rs.next()) {
                ArrayList<String> aryRecord = new ArrayList<>();
                aryRecord.add(rs.getString(1));
                aryRecord.add(rs.getString(2));
                aryRecord.add(rs.getString(3));
                aryRecord.add(rs.getString(4));
                aryRecord.add(rs.getString(5));
                aryRecord.add(rs.getString(6));
                aryRecord.add(rs.getString(7));
                if (aryResult == null) {
                    aryResult = new ArrayList<>();
                }
                aryResult.add(aryRecord);
            }
        } catch (SQLException ex) {
            LOG.error("get() can not execute query cos of: " + ex.getMessage());
        }

        LOG.debug("get() done.");
        return aryResult;
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
        Objects.requireNonNull(lctMsg, "arg must not be null");
        Objects.requireNonNull(table, "arg must not be null");

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

    public boolean hasLctMsgAlb(LctMsg lctMsg, String table) {
        LOG.debug("hasLctMsgAlb() start...");
        Objects.requireNonNull(lctMsg, "arg must not be null");
        Objects.requireNonNull(table, "arg must not be null");

        String query = "SELECT CASE WHEN EXISTS (SELECT * FROM " + table + " WHERE vc_trip='" + lctMsg.getTrip() + "') THEN 'true' ELSE 'false' END;";

        String result = null;
        //create prepared statement using placeholders instead of directly writing values
        try (PreparedStatement pst = pgCon.getConnection().prepareStatement(query); ResultSet rs = pst.executeQuery()) {

            //advance cursor to the next record
            //return false if there are no more records in the result set
            while (rs.next()) {
                result = rs.getString(1);
            }

        } catch (SQLException ex) {

            LOG.error("hasLctMsgAlb() can not execute query for: " + table + " cos of: " + ex.getMessage());
        }

        LOG.debug("hasLctMsgAlb() result: " + result);
        if (result != null && result.equals("true")) {
            LOG.debug("hasLctMsgAlb() result: " + result + " equals true");
            return true;
        } else {
            LOG.debug("hasLctMsgAlb() result: " + result + " equals true NOT");
        }
        LOG.debug("hasLctMsgAlb() done.");
        return false;
    }
}
