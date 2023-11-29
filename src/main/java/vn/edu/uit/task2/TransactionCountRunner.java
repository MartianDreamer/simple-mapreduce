package vn.edu.uit.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TransactionCountRunner {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        if (args.length < 3) {
            throw new IllegalArgumentException("output file is missing");
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJobName("PlayerTransactionCount");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(TransactionCountMapper.class);
        job.setReducerClass(TransactionCountReducer.class);
        job.setJarByClass(TransactionCountRunner.class);
        job.addCacheFile(new URI(args[args.length-2]));
        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
        for (int i = 0; i < args.length - 2; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
