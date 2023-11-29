package vn.edu.uit.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CustomerCountAndTotalReducer extends Reducer<Text, CustomerCountAndCost, Text, Text> {

    private double maxCost = -1;
    private String maxMonth;


    @Override
    protected void reduce(Text key, Iterable<CustomerCountAndCost> values, Reducer<Text, CustomerCountAndCost, Text, Text>.Context context) throws IOException, InterruptedException {
        int customerCount = 0;
        double cost = 0;
        for (CustomerCountAndCost value : values) {
            customerCount += value.customerCount();
            cost += value.cost();

        }
        if (maxCost < cost) {
            maxMonth = key.toString();
            maxCost = cost;
        }
        context.write(key, new Text(customerCount + "\t" + cost));
    }

    @Override
    protected void cleanup(Reducer<Text, CustomerCountAndCost, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text(maxMonth), new Text(" is the month with highest cost (" + maxCost + ")"));
    }
}
