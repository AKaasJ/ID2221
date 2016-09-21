package sics;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;





public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, NullWritable, Text> {
        //log handler for logging
        private static final Log log = LogFactory.getLog(TokenizerMapper.class);
        TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();


        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text output = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //Initialize regex and convert value to strings
            Pattern paccount = Pattern.compile("DisplayName=\"(.*?)\"");
            Pattern preputation = Pattern.compile("Reputation=\"(.*?)\"");
            Matcher maccount = paccount.matcher(value.toString());
            Matcher mreputation = preputation.matcher(value.toString());

            //Find Reputation and displayname in row, put them in tree as key and value.
            if (maccount.find() && mreputation.find()) {
                word.set(maccount.group(1));
                repToRecordMap.put(Integer.parseInt(mreputation.group(1)), new Text(word));
            }
            //Check if size is greater than 10, remove lowest key
            if (repToRecordMap.size() > 10) { // works!
                repToRecordMap.remove(repToRecordMap.firstKey()); //removes first key if size > 10
            }
            //

        }
        //outputs a string (later Text) as a value with format Reputation + Username for each Key in the treemap.
        String preout = ""; //initialize string
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our ten records to the reducers with a null key
            for (Integer t : repToRecordMap.keySet()) {
                //log.info(t + " " + repToRecordMap.get(t));
                preout = t + " " + repToRecordMap.get(t).toString();
                context.write(NullWritable.get(), new Text(preout));
            }
        }
    }

    public static class IntSumReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        //private IntWritable result = new IntWritable();
        private static final Log log = LogFactory.getLog(IntSumReducer.class);
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
        private Text output = new Text();


        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //take values from mapper, split, parse and put in tree
            for (Text value : values) {
                String string = value.toString();
                String[] elements = string.split("\\s+");
                //log.info(elements[0] + " " + elements[1]);
                repToRecordMap.put(Integer.parseInt(elements[0]), new Text(elements[1]));

                //check if tree is too big
                if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
            //output in the same fashion mapper does, as a string in the following format: reputation + username
            String preout = "";
            for (Integer t : repToRecordMap.keySet()) {
                preout = t + " " + repToRecordMap.get(t).toString();
                context.write(NullWritable.get(), new Text(preout));
            }
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class); //not necessary
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}