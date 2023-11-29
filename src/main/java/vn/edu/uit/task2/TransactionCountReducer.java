package vn.edu.uit.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class TransactionCountReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Map<String, Integer> nameCounter = new HashMap<>();
        values.forEach(e -> {
            String line = e.toString();
            int count = nameCounter.computeIfAbsent(line, (k) -> 0);
            nameCounter.put(line, count + 1);
        });
        String reducerValue = nameCounter.entrySet().stream().map(e -> e.getKey()+"-"+e.getValue()).collect(Collectors.joining(" "));
        context.write(key, new Text(reducerValue));
    }
}
