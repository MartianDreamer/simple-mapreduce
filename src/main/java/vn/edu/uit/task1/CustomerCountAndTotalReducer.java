package vn.edu.uit.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CustomerCountAndTotalReducer extends Reducer<Text, Text, Text, Text> {

    private double maxCost = -1;
    private String maxMonth;


    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> customerIds = new HashSet<>();
        double cost = 0;
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            customerIds.add(parts[0]);
            cost += Double.parseDouble(parts[1]);

        }
        if (maxCost < cost) {
            maxMonth = key.toString();
            maxCost = cost;
        }
        context.write(key, new Text(customerIds.size() + "\t" + cost));
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text(maxMonth), new Text(" is the month with highest cost (" + maxCost + ")"));
    }
}
