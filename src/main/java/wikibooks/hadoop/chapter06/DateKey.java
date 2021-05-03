package wikibooks.hadoop.chapter06;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateKey implements WritableComparable<DateKey> {

    private String year;
    private Integer month;

    public DateKey() {
    }

    public DateKey(String year, Integer month) {
        this.year = year;
        this.month = month;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    @Override
    public String toString() {
        return (new StringBuilder()).append(year).append(",").append(month).toString();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = WritableUtils.readString(dataInput);
        month = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, year);
        dataOutput.writeInt(month);
    }

    @Override
    public int compareTo(DateKey key) {
        int result = year.compareTo(key.year);
        if (0 == result) {
            result = month.compareTo(key.month);
        }
        return result;
    }
}
