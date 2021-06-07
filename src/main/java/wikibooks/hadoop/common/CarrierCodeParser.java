package wikibooks.hadoop.common;

import org.apache.hadoop.io.Text;

public class CarrierCodeParser {
    private String carrierCode;
    private String carrierName;

    public CarrierCodeParser(Text value) {
        this(value.toString());
    }

    public CarrierCodeParser(String value) {
        try {
            String[] columns = value.split(",");
            if (columns != null && columns.length > 0) {
                carrierCode = columns[0];
                carrierName = columns[1];
            }
        } catch (Exception e) {
            System.out.println("Error parsing a record :" + e.getMessage());
        }
    }

    public String getCarrierCode() {
        return carrierCode;
    }

    public String getCarrierName() {
        return carrierName;
    }
}
