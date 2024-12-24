import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class Step2
{
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        public static int C0 = -1;

        @Override
        protected void setup(Context context) {
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length != 2) return;

            String nGram = parts[0].trim(); // e.g., "* w *" or "w q s"
            String countInfo = parts[1].trim(); // Count value

            String[] words = nGram.split(" ");

            if (nGram.equals("* C0 *")) {
                // Corpus size
                context.write(new Text("BROADCAST"), new Text("C0:" + countInfo));
            } else if (nGram.startsWith("*") && nGram.endsWith("*")) {
                // Unigram count for w
                context.write(new Text(words[1] + ":UNIGRAM"), new Text("C1:" + countInfo));
            } else if (nGram.endsWith("*")) {
                // Bigram count <w, q>
                context.write(new Text(words[0] + "," + words[1] + ":BIGRAM"), new Text("C2:" + countInfo));
            } else if (nGram.startsWith("*")) {
                // Bigram count <q, s>
                context.write(new Text(words[1] + "," + words[2] + ":BIGRAM"), new Text("N2:" + countInfo));
            } else if (words.length == 3) {
                // Trigram count <w, q, s>
                context.write(new Text(words[0] + "," + words[1] + "," + words[2]), new Text("N3:" + countInfo));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int C0 = 0, C1 = 0, C2 = 0, N1 = 0, N2 = 0, N3 = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(":");
                String countType = parts[0];
                int count = Integer.parseInt(parts[1]);

                switch (countType) {
                    case "C0":
                        C0 = count;
                        break;
                    case "C1":
                        C1 = count;
                        break;
                    case "C2":
                        C2 = count;
                        break;
                    case "N1":
                        N1 = count;
                        break;
                    case "N2":
                        N2 = count;
                        break;
                    case "N3":
                        N3 = count;
                        break;
                }
            }

            // Emit trigram and its associated counts
            context.write(key, new Text("C0:" + C0 + " C1:" + C1 + " C2:" + C2 + " N1:" + N1 + " N2:" + N2 + " N3:" + N3));
        }
    }

//    public static class CombinerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

//        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable value : values) {
//                sum += value.get();
//            }
//            context.write(key, new IntWritable(sum));
//
//        }
//    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String baseKey;

            // Handle special cases
            if (key.toString().startsWith("BROADCAST")) {
                baseKey = "BROADCAST";
            } else {
                // Use only the base trigram key (strip type suffixes like :UNIGRAM, :BIGRAM)
                baseKey = key.toString().split(":")[0];
            }

            // Hash the base key and ensure it maps to one of the reducers
            return (baseKey.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2");

        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(CombinerClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

//        FileInputFormat.addInputPath(job,
//                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
//        FileInputFormat.addInputPath(job, new Path("s3://bucketforjars/input/3gram_sample_100"));
//        FileOutputFormat.setOutputPath(job, new Path("s3://bucketforjars/output_word_count"));

//        conf.set("mapreduce.input.fileinputformat.split.maxsize", "128000000");

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);

        // if (args.length != 2) {
        // System.err.println("Usage: WordCount <input path> <output path>");
        // System.exit(-1);
        // }

        // Configuration conf = new Configuration();
        // Job job = Job.getInstance(conf, "Local WordCount");

        // job.setJarByClass(WordCount.class);
        // job.setMapperClass(MapperClass.class);
        // job.setReducerClass(ReducerClass.class);
        // job.setCombinerClass(CombinerClass.class);
        // job.setPartitionerClass(PartitionerClass.class);

        // // job.setInputFormatClass(TextInputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class);

        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        // job.setOutputFormatClass(TextOutputFormat.class);

        // // Set the input and output paths from command line arguments
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
