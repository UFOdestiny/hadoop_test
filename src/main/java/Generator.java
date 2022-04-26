import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Generator {
    private static final String path = "input/";
    private static final Random random = new Random();
    private static final List<Integer> lst = new ArrayList<>();
    private static final int file_num = 5;
    private static final int int_num = 500;

    public static void run(int file_num, int int_num) throws IOException {

        FileUtil.deleteDir(path);
        File input = new File(path);
        input.mkdir();

        for (int i = 0; i < int_num; i++) //-10亿~10亿
            lst.add(random.nextInt(2000000000) - 1000000000);

        List<List<Integer>> lists = Lists.partition(lst, int_num / file_num);

        for (int i = 0; i < file_num; i++) {
            String filename = path + i + ".txt";
            File file = new File(filename);
            if (file.exists()) {
                file.delete();
            }
            FileWriter writer = new FileWriter(file, true);
            for (int j : lists.get(i)) {
                writer.write(j + "\r\n");
            }
            writer.close();

        }
    }


    public static void main(String[] args) throws IOException {
        run(file_num, int_num);
    }

}
