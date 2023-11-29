package vn.edu.uit.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import vn.edu.uit.util.MonthUtil;

import java.io.IOException;

public class CustomerCountAndTotalMapper extends Mapper<Object, Text, Text, Text> {
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        final String line = value.toString().trim();
        final String[] parts = line.split(",");
        final String month = MonthUtil.getMonth(parts[1]).orElse("unknown");
        double cost;
        try {
            cost = Double.parseDouble(parts[3]);
        } catch (NumberFormatException | NullPointerException e) {
            cost = 0;
        }
        final String customerId = parts[2];
        context.write(new Text(month), new Text(customerId + "," + cost));
    }

}
