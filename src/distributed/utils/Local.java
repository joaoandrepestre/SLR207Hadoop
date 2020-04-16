package distributed.utils;

import java.io.IOException;

/* 
* The class Local implements functions used locally by different parts of the system.
 */
public class Local {

    /* 
    * Creates a directory in the local machine.
    * @param dirname Name of the directory to be created
     */
    public static void createDir(String dirname) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", dirname);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        p.waitFor();
    }

    /* 
    * Zips the file into the zip file.
    * @param filename Name of the file to be zipped
    * @param zipname Name of the zip target file
    * @param verbose if 1, the execution will log more details
     */
    public static void zipFile(String filename, String zipname, int verbose) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("zip", zipname, filename);
        pb.redirectErrorStream(true);
        if(verbose == 1) pb.inheritIO();
        Process p = pb.start();

        p.waitFor();
    }

    /* 
    * Calculates a strictly positive hash for the string.
    * @param s String to be hashed
    * @return The positive hash of the string
     */
    public static int positiveHash(String s){
        return s.hashCode() & 0xfffffff;
    }

}