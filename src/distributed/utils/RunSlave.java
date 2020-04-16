package distributed.utils;

import java.io.IOException;

/* 
* The class RunSlave executes the slave.jar file in the target machine
 */
public class RunSlave extends Thread {

    private String machine; /* name of the target machine */
    private int mode; /* mode of execution of the slave. 0 - map; 1 - shuffle; 2 - reduce */
    private String filename; /* name of the file to be passed as argument to the slave */
    private int verbose; /* if 1, the execution will log more details */

    /* 
    * Constructor. Initializes the variables
     */
    public RunSlave(String machine, int mode, String filename, int verbose) {
        this.machine = machine;
        this.mode = mode;
        this.filename = filename;
        this.verbose = verbose;
    }

    public RunSlave(String machine, int mode, int verbose) {
        this.machine = machine;
        this.mode = mode;
        this.filename = "";
        this.verbose = verbose;
    }

    /* 
    * Runs the slave.
     */
    private void runSlave() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "java", "-jar", Constants.BASEDIR + "/slave.jar",
                "" + mode, filename, ""+verbose);

        Process p = pb.inheritIO().start();
        p.waitFor();
    }

    /* 
    * Main thread method. Runs the slave.
     */
    public void run() {
        try {
            runSlave();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}