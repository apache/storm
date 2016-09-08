package org.apache.storm.pacemaker;

public class PacemakerConnectionException extends Exception {
    public PacemakerConnectionException(String err) {
        super(err);
    }
}
