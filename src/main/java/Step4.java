import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;

public class Step4 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (fields.length != 4) { // Expecting: W1 W2 W3 prob
                System.err.println("Invalid input: " + value.toString());
                return; // Skip this line
            } else {
                Text outputKey = new Text(String.format("%s %s %s", fields[0], fields[1], fields[2]));
                Text outputValue = new Text(String.format("%s", fields[3]));
                context.write(outputKey, outputValue);
            }
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        }
    }

//    public static class PartitionerClass extends Partitioner<Text, Text> {
//        @Override
//        public int getPartition(Text key, Step2.TaggedValue value, int numPartitions) {
//
//            return Math.abs(key.toString().hashCode() % numPartitions);
//        }
//    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step4");

        job.setJarByClass(Step4.class);
        job.setMapperClass(Step4.MapperClass.class);
        job.setReducerClass(Step4.ReducerClass.class);
//        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(CombinerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);

//        job.setNumReduceTasks(4);


        FileInputFormat.addInputPath(job, new Path("s3://jarbucket1012/step3_output/"));
//        FileInputFormat.addInputPath(job, new Path("/user/local/step1_output/"));
        FileOutputFormat.setOutputPath(job, new Path("s3://jarbucket1012/step4_output/"));
//        FileOutputFormat.setOutputPath(job, new Path("/user/local/step2_output/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
