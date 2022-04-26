import java.io.File;
import java.util.Objects;


public class FileUtil {

    public static boolean deleteDir(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            for (File f : Objects.requireNonNull(dir.listFiles())) {
                if (f.isDirectory()) {
                    deleteDir(f.getName());
                } else {
                    f.delete();
                }
            }
            dir.delete();
            return true;
        } else {
            System.out.println("文件(夹)不存在!");
            return false;
        }
    }

}
