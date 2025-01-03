import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;

import java.net.URI;

import java.util.Comparator;

public class Step3 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
//            output from step2: :
//            context.write(new Text(nGram), new Text(String.format("C0:%s C1:%d C2:%d N3:%d", C0, NorC1, NorC2, count)));

            String[] fields = value.toString().split("\\s+");
            if (fields.length != 7) {// Expecting: W1 W2 W3 C0 C1/N1 C2/N2 N3
                System.err.println("Invalid input: " + value.toString());
                return; // Skip this line
            } else {
                Text outputKey = new Text(String.format("%s %s %s", fields[0], fields[1], fields[2]));
                Text outputValue = new Text(String.format("%s %s %s %s", fields[3], fields[4], fields[5], fields[6]));
                context.write(outputKey, outputValue);
            }
        }

    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double C0 = 0;
            double C1 = 0;
            double C2 = 0;
            double N1 = 0;
            double N2 = 0;
            double N3 = 0;
            for (Text value: values){

                String[] fields = value.toString().split("\\s+");

                for (String field : fields){
                    String[] parts = field.split(":");
                    String name = parts[0];
                    double val = Double.parseDouble(parts[1]);

                    switch (name) {
                        case "C0":
                            C0 = val;
                            break;
                        case "C1":
                            C1 = val;
                            break;
                        case "C2":
                            C2 = val;
                            break;
                        case "N1":
                            N1 = val;
                            break;
                        case "N2":
                            N2 = val;
                            break;
                        case "N3":
                            N3 = val;
                            break;
                    }
                }

            }
            if (C0 == 0 || C1 == 0 || C2 == 0 ||
                    N1 == 0 || N2 == 0 || N3 == 0) {
                System.err.println("Invalid input: " + key.toString());
            }
            else { // Calculate the probability
                double k2 = (Math.log10(N2 + 1) + 1) / (Math.log10(N2 + 2) + 2);
                double k3 = (Math.log10(N3 + 1) + 1) / (Math.log10(N3 + 2) + 2);
                double prob = (k3 * (N3/C2)) + ((1 - k3)*k2*(N2/C1)) + ((1 - k3)*(1 - k2)*(N1/C0));
                // context.write(new Text(String.format("C0:%.1f C1:%.1f C2:%.1f N1:%.1f N2:%.1f N3:%.1f", C0, C1, C2, N1, N2, N3)), new Text(""));
                context.write(key, new Text(String.format("%.5f", prob)));
            }
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
        System.out.println("[DEBUG] STEP 3 started!");
//        System.out.println(args.length > 0 ? args[0] : "no args");

//        String jarBucketName = "jarbucket1012";
        String jarBucketName = "hadoop-map-reduce-bucket";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step3");

        job.setJarByClass(Step3.class);
        job.setMapperClass(Step3.MapperClass.class);
        job.setReducerClass(Step3.ReducerClass.class);
//        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(CombinerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        job.setInputFormatClass(SequenceFileInputFormat.class);  // For S3 N-gram data
        job.setInputFormatClass(TextInputFormat.class);

//        job.setNumReduceTasks(4);

//        FileInputFormat.addInputPath(job,
//                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
//        FileInputFormat.addInputPath(job, new Path("s3://" + jarBucketName + "/input/3gram_sample_100"));
//        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/output_word_count"));


        FileInputFormat.addInputPath(job, new Path("s3://" + jarBucketName + "/step2_output/"));
//        FileInputFormat.addInputPath(job, new Path("/user/local/step1_output/"));
        FileOutputFormat.setOutputPath(job, new Path("s3://" + jarBucketName + "/step3_output/"));
//        FileOutputFormat.setOutputPath(job, new Path("/user/local/step2_output/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
