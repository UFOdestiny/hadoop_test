import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsLearn {
    public static void main(String[] args) {
        try {
            String filename = "hdfs://81.70.102.186:9000/entrypoint.sh";
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://81.70.102.186:9000");
            FileSystem fs = FileSystem.get(conf);
            System.out.println(fs);
            if (fs.exists(new Path(filename))) {
                System.out.println("文件存在");
            } else {
                System.out.println("文件不存在");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}