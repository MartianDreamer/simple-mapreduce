package vn.edu.uit.task2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import vn.edu.uit.util.MonthUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TransactionCountMapper extends Mapper<Object, Text, Text, Text> {
    private static final Map<String, String> names = new HashMap<>();
    @Override
    protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        for (URI cacheFile: cacheFiles) {
            loadNames(cacheFile);
        }
        super.setup(context);
    }

    private void loadNames(URI uri) throws IOException {
        Path path = new Path(uri);
        try (BufferedReader bReader = new BufferedReader(new FileReader(path.toString()));) {
            String line = bReader.readLine();
            while (line != null) {
                line = line.trim();
                String[] parts = line.split(",");
                names.put(parts[0], parts[1]);
                line = bReader.readLine();
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        final String line = value.toString().trim();
        final String[] parts = line.split(",");
        final String month = MonthUtil.getMonth(parts[1]).orElse("unknown");
        context.write(new Text(month), new Text(names.get(parts[2])));
    }
}
