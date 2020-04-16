package distributed.utils;

import java.io.IOException;

public class Local {

    public static void createDir(String dirname, int verbose) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", dirname);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        p.waitFor();
    }

    public static void zipFile(String filename, String zipname, int verbose) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("zip", zipname, filename);
        pb.redirectErrorStream(true);
        if(verbose == 1) pb.inheritIO();
        Process p = pb.start();

        p.waitFor();
    }

    public static int positiveHash(String s){
        return s.hashCode() & 0xfffffff;
    }

}