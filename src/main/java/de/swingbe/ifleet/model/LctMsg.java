package de.swingbe.ifleet.model;

import java.util.Objects;

public class LctMsg {
    private final String trip;
    private final String lat;
    private final String lon;
    private final String date;
    private final String time;
    private String tenant;
    private String route;

    public LctMsg(String trip, String lat, String lon, String date, String time) {
        //TODO alternative null checking could happen in setters if params are not final
        this.trip = Objects.requireNonNull(trip, "trip must not be null");
        this.lat = Objects.requireNonNull(lat, "lat must not be null");
        this.lon = Objects.requireNonNull(lon, "lon must not be null");
        this.date = Objects.requireNonNull(date, "date must not be null");
        this.time = Objects.requireNonNull(time, "time must not be null");
    }

    @Override
    public String toString() {
        return "LctMsg{" + "trip='" + trip + '\'' + ", lat='" + lat + '\'' + ", lon='" + lon + '\'' + ", date='" + date + '\'' + ", time='" + time + '\'' + ", tenant='" + tenant + '\'' + ", route='" + route + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LctMsg lctMsg = (LctMsg) o;
        return trip.equals(lctMsg.trip) && lat.equals(lctMsg.lat) && lon.equals(lctMsg.lon) && date.equals(lctMsg.date) && time.equals(lctMsg.time) && Objects.equals(tenant, lctMsg.tenant) && Objects.equals(route, lctMsg.route);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trip, lat, lon, date, time, tenant, route);
    }

    public String getTrip() {
        return trip;
    }

    public String getLat() {
        return lat;
    }

    public String getLon() {
        return lon;
    }

    public String getDate() {
        return date;
    }

    public String getTime() {
        return time;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getRoute() {
        return route;
    }

    public void setRoute(String route) {
        this.route = route;
    }
}
