package vn.edu.uit.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomerCountAndTotalRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 2) {
            throw new IllegalArgumentException("output file is missing");
        }
        final String output = args[args.length - 1];
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJobName("MonthlyCustomerCountAndTotalCost");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(CustomerCountAndTotalMapper.class);
        job.setReducerClass(CustomerCountAndTotalReducer.class);
        job.setJarByClass(CustomerCountAndTotalRunner.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}