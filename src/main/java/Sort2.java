import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sort2 {


    // map将输入中的value化成IntWritable类型, 作为输出的key
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

        private static final IntWritable data = new IntWritable();

        // 实现map函数
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line != null && line.length() != 0 && !"".equals(line)) {
                data.set(Integer.parseInt(line));
                context.write(data, new IntWritable(1));
            }
        }
    }

    // reduce会对map传进来的值进行排序
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private static IntWritable linenum = new IntWritable(1);

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(linenum, key);
                linenum = new IntWritable(linenum.get() + 1);
            }
        }
    }

    public static class MyPartition extends Partitioner<IntWritable, IntWritable> {

        @Override
        public int getPartition(IntWritable key, IntWritable value, int numTaskReduce) {

            int maxNumber = 65223;    // 样本数据中的最大值
            int part = maxNumber / numTaskReduce + 1;
            int keyNum = key.get();
            for (int i = 0; i < numTaskReduce; i++) {

                if (keyNum >= part * i && keyNum <= part * (i + 1)) {
                    return i;
                }
            }
            return -1;     // 如果没有出现在分区里面就会返回-1, 如果返回-1肯定是要报错的
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            args = new String[]{
                    "D:\\COURSE\\program\\java\\hadooptest\\input",
                    "D:\\COURSE\\program\\java\\hadooptest\\ouput",
            };
        }

        Job job = new Job();
        job.setJarByClass(Sort2.class); // main Class
        job.setJobName("Sort");

        FileInputFormat.addInputPath(job, new Path(args[0]));   // 输入数据的目录
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出数据的目录

        job.setNumReduceTasks(3);                     // Reduce任务数为3
        job.setPartitionerClass(MyPartition.class);   // 设置分区的类

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}