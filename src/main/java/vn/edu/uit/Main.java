package vn.edu.uit;

import vn.edu.uit.task1.CustomerCountAndTotalRunner;

import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 2) {
            throw new IllegalArgumentException("output file is missing");
        }
        final String[] inputs = Arrays.copyOf(args, args.length - 1);
        final String output = args[args.length - 1];
        CustomerCountAndTotalRunner.run(output, inputs);
    }
}