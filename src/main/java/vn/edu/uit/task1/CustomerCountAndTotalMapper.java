package vn.edu.uit.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class CustomerCountAndTotalMapper extends Mapper<Text, Text, Text, CustomerCountAndCost> {

    private final Set<String> customerIds = new HashSet<>();

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, CustomerCountAndCost>.Context context) throws IOException, InterruptedException {
        final String line = value.toString().trim();
        final String[] parts = line.split(",");
        final String month = getMonth(parts[1]).orElse("unknown");
        double cost;
        try {
            cost = Double.parseDouble(parts[3]);
        } catch (NumberFormatException | NullPointerException e) {
            cost = 0;
        }
        int customerCount = 0;
        if (!customerIds.contains(parts[2])) {
            customerCount = 1;
            customerIds.add(parts[2]);
        }
        context.write(new Text(month), new CustomerCountAndCost(customerCount, cost));
    }

    private Optional<String> getMonth(String string) {
        final String[] parts = string.split("-");
        if (parts.length > 0) {
            return Optional.of(parts[0]);
        }
        return Optional.empty();
    }
}
