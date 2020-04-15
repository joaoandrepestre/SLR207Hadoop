package distributed.utils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Local {

    public static void createDir(String dirname, int verbose) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", dirname);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);
    }

    public static void zipFile(String filename, String zipname, int verbose) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("zip", zipname, filename);
        pb.redirectErrorStream(true);
        if(verbose == 1) pb.inheritIO();
        Process p = pb.start();

        p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);
    }

    public static int positiveHash(String s){
        return s.hashCode() & 0xfffffff;
    }

}