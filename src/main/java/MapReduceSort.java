import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

public class MapReduceSort {

    // map将输入中的value化成LongWritable类型, 作为输出的key
    public static class Map extends Mapper<Object, Text, LongWritable, LongWritable> {
        private static final LongWritable data = new LongWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line != null && line.length() != 0) {
                data.set(Integer.parseInt(line));
                context.write(data, new LongWritable(1));
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        private static LongWritable linenum = new LongWritable(1);

        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable val : values) {
                context.write(linenum, key);
                linenum = new LongWritable(linenum.get() + 1);
            }
        }
    }

    public static class MyPartition extends Partitioner<LongWritable, LongWritable> {
        @Override
        public int getPartition(LongWritable key, LongWritable value, int numTaskReduce) {
            int keyNum = (int) key.get();
            return Math.floorMod(keyNum, numTaskReduce);
        }
    }


    public static void main(String[] args) throws Exception {

        Generator.run(5, 500);
        FileUtil.deleteDir("D:\\COURSE\\program\\java\\hadooptest\\output");

        if (args.length == 0) {
            args = new String[]{
                    "D:\\COURSE\\program\\java\\hadooptest\\input",
                    "D:\\COURSE\\program\\java\\hadooptest\\output",
                    "D:\\COURSE\\program\\java\\hadooptest\\partition\\partition"
            };
        }

        Configuration conf = new Configuration();
//        conf.setBoolean("dfs.client.use.datanode.hostname", true);
//        conf.setBoolean("dfs.datanode.use.datanode.hostname", true);
//        System.setProperty("HADOOP_USER_NAME", "root");
//        conf.set("fs.defaultFS", "hdfs://81.70.102.186:9000");

        Job job = Job.getInstance(conf, "sort");

        job.setJarByClass(MapReduceSort.class); // main Class

        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入数据的目录
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出数据的目录

//        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[2]));
//        InputSampler.Sampler<LongWritable, LongWritable> sampler = new InputSampler.RandomSampler<LongWritable, LongWritable>(0.5, 50, 3);
//       InputSampler.writePartitionFile(job, sampler);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // 这里换成了TotalOrderPartitioner
        //job.setPartitionerClass(TotalOrderPartitioner.class);

        job.setPartitionerClass(MyPartition.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}