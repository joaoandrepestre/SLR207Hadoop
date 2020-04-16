package distributed.utils;

import java.io.IOException;

/* 
* The class DeleteDir deletes the directory (or file) in the target machine
 */
public class DeleteDir extends Thread {

    private String machine; /* name of the target machine */
    private String dirname; /* name of the directory (or file) to be deleted */
    private int verbose; /* if 1, the execution will log more details */

    /* 
    * Constructor. Initializes the variables.
     */
    public DeleteDir(String machine, String dirname, int verbose) {
        this.machine = machine;
        this.dirname = dirname;
        this.verbose = verbose;
    }

    /* 
    * Deletes the directory in the target machine.
     */
    private void deleteDir() throws InterruptedException, IOException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "rm", "-rf", dirname);
        pb.redirectErrorStream(true);
        if (verbose == 1)
            pb.inheritIO();
        Process p = pb.start();

        p.waitFor();
    }

    /* 
    * Main thread method. Deletes the directory.
     */
    public void run() {
        try {
            deleteDir();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

}