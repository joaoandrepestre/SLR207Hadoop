package distributed.utils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Local {

    public static void createDir(String dirname) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", dirname);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);
    }

    public static int positiveHash(String s){
        return s.hashCode() & 0xfffffff;
    }

}