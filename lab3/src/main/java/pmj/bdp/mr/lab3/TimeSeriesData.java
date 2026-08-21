package pmj.bdp.mr.lab3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * (timestamp, value) pair for a time series point.
 * From Parsian, Data Algorithms, Chapter 6.
 */
public class TimeSeriesData implements Writable, Comparable<TimeSeriesData> {

    private long timestamp;
    private double value;

    public TimeSeriesData() {
    }

    public TimeSeriesData(long timestamp, double value) {
        set(timestamp, value);
    }

    public void set(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public static TimeSeriesData copy(TimeSeriesData other) {
        return new TimeSeriesData(other.timestamp, other.value);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        timestamp = in.readLong();
        value = in.readDouble();
    }

    @Override
    public int compareTo(TimeSeriesData other) {
        return Long.compare(this.timestamp, other.timestamp);
    }

    @Override
    public String toString() {
        return "(" + timestamp + "," + value + ")";
    }
}
