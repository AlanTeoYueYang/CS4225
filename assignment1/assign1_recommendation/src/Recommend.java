import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.net.URI;
import java.io.BufferedReader;;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Recommend {
    public static class UserItemScoreMapper
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        private Text item_score = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] temp = itr.nextToken().split(",");
                int user = Integer.parseInt(temp[0]);
                item_score.set(temp[1]+":"+temp[2]);
                context.write(new IntWritable(user), item_score);
            }
        }
    }

    public static class UserItemScoreReducer
            extends Reducer<IntWritable,Text,IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            for (Text val : values) {
                builder.append(val + ";");
            }
            context.write(key, new Text(builder.toString()));
        }
    }

    public static class ItemCooccurrenceMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private Text item_item = new Text();
        private Set<String> all_items = new HashSet<String>();
        private Set<String> item_item_set = new HashSet<String>();

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] temp = value.toString().split("\t")[1].split(";");
            for (int i = 0; i < temp.length; i++){
                String item_i = temp[i].split(":")[0];
                if (!all_items.contains(item_i)){
                    all_items.add(item_i);
                }
                for (int j = 0; j < temp.length; j++){
                    String item_j = temp[j].split(":")[0];
                    String item_item_str = item_i+"-"+item_j;
                    if (!item_item_set.contains(item_item_str)){
                        item_item_set.add(item_item_str);
                    }
                    item_item.set(item_item_str);
                    if (i!=j){
                        context.write(item_item, one);
                    }
                    else{
                        context.write(item_item, zero);
                    }
                }
            }

            for (String item_i : all_items){
                for (String item_j : all_items){
                    String item_item_str = item_i+"-"+item_j;
                    if (!item_item_set.contains(item_item_str)){
                        item_item.set(item_item_str);
                        context.write(item_item, zero);
                    }
                }
            }
        }
    }

    public static class ItemCooccurrenceReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class CooccurrenceMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        Text item_i = new Text();
        Text item_j_co = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] temp0 = value.toString().split("\t");
            String[] temp1 = temp0[0].split("-");
            item_i.set(temp1[0]);
            item_j_co.set(temp1[1]+":"+temp0[1]);
            context.write(item_i, item_j_co);
        }
    }

    public static class ScoreMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        Text item_i = new Text();
        Text user_score = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] temp = itr.nextToken().split(",");
                item_i.set(temp[1]);
                user_score.set(temp[0] + "=" + temp[2]);
                context.write(item_i, user_score);
            }
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> CoMap = new HashMap<String, Integer>();
            Map<String, Double> ScoreMap = new HashMap<String, Double>();
            Set<String> users = new HashSet<String>();

            for (Text val:values){
                String temp = val.toString();
                if (temp.contains(":")){
                    String[] item_co = temp.split(":");
                    CoMap.put(item_co[0], Integer.parseInt(item_co[1]));
                }
                else if (temp.contains("=")){
                    String[] user_score = temp.toString().split("=");
                    ScoreMap.put(user_score[0], Double.parseDouble(user_score[1]));
                }
            }

            Text user_item = new Text();

            for(Map.Entry<String, Integer> entry1: CoMap.entrySet()){
                String item_j = entry1.getKey();
                Integer co = entry1.getValue();
                for (Map.Entry<String, Double> entry2: ScoreMap.entrySet()){
                    String user = entry2.getKey();
                    Double score = entry2.getValue();
                    user_item.set(user+"-"+item_j);
                    context.write(user_item, new DoubleWritable(co*score));
                }
            }
        }
    }

    public static class SumMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text user_item = new Text();
        Text score = new Text();
        private Set omit_data = new HashSet();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());
                try{
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String temp0 = null;
                    while((temp0 = bufferedReader.readLine()) != null) {
                        String[] temp1 = temp0.split(",");
                        String user_item = temp1[0]+"-"+temp1[1];
                        omit_data.add(user_item);
                    }
                } catch(IOException ex) {
                    System.err.println("Exception while reading omit data: " + ex.getMessage());
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split("\t");
            if (!omit_data.contains(temp[0])){
                user_item.set(temp[0]);
                score.set(temp[1]);
                context.write(user_item, score);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            Text user = new Text();
            Text item_score = new Text();
            for (Text val: values) {
                Double value = Double.parseDouble(val.toString());
                sum += value;
            }
            String[] temp = key.toString().split("-");
            user.set(temp[0]);
            item_score.set(temp[1]+","+Double.toString(sum));
            context.write(user, item_score);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path("temp_output1"))){
            fs.delete(new Path("temp_output1"));
        }

        if (fs.exists(new Path("temp_output2"))){
            fs.delete(new Path("temp_output2"));
        }

        if (fs.exists(new Path("temp_output3"))){
            fs.delete(new Path("temp_output3"));
        }

        Job job1 = Job.getInstance(conf, "Map user");
        job1.setJarByClass(Recommend.class);
        job1.setMapperClass(UserItemScoreMapper.class);
        job1.setReducerClass(UserItemScoreReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp_output1"));
//        System.exit(job1.waitForCompletion(true) ? 0 : 1);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Map items");
        job2.setJarByClass(Recommend.class);
        job2.setMapperClass(ItemCooccurrenceMapper.class);
        job2.setReducerClass(ItemCooccurrenceReducer.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("temp_output1"));
        FileOutputFormat.setOutputPath(job2, new Path("temp_output2"));
        job2.waitForCompletion(true);
        fs.delete(new Path("temp_output1"));

        Job job3 = Job.getInstance(conf, "Multiplication");
        job3.setJarByClass(Recommend.class);
        ChainMapper.addMapper(job3, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job3, ScoreMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
        job3.setMapperClass(CooccurrenceMapper.class);
        job3.setMapperClass(ScoreMapper.class);
        job3.setReducerClass(MultiplicationReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job3, new Path("temp_output2"), TextInputFormat.class, CooccurrenceMapper.class);
        MultipleInputs.addInputPath(job3, new Path(args[0]), TextInputFormat.class, ScoreMapper.class);
        TextOutputFormat.setOutputPath(job3, new Path("temp_output3"));
        job3.waitForCompletion(true);
        fs.delete(new Path("temp_output2"));

        Job job4 = Job.getInstance(conf, "Sum");
        job4.setJarByClass(Recommend.class);
        job4.setMapperClass(SumMapper.class);
        job4.setReducerClass(SumReducer.class);
        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(job4, new Path("temp_output3"));
        TextOutputFormat.setOutputPath(job4, new Path(args[1]));
        job4.addCacheFile(new URI(args[0]));
        job4.waitForCompletion(true);
        fs.delete(new Path("temp_output3"));

        System.exit(0);
    }
}
