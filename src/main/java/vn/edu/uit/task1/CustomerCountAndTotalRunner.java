package vn.edu.uit.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class CustomerCountAndTotalRunner {
    public static void run(String output, String ...inputs) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJobName("MonthlyCustomerCountAndTotalCost");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomerCountAndTotalRunner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(CustomerCountAndTotalMapper.class);
        job.setReducerClass(CustomerCountAndTotalReducer.class);
        job.setJarByClass(CustomerCountAndTotalRunner.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        for (String input: inputs) {
            FileInputFormat.addInputPath(job, new Path(input));
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
