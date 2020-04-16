package distributed.utils;

import java.io.IOException;

/* 
* The class CopyFile copies the file to the target machine.
 */
public class CopyFile extends Thread {

    private String filename; /* name of the file to be copied */
    private String machine; /* name of the target machine */
    private String destinationDir; /* directory to which the file will be copied */
    private int verbose; /* if 1, the execution will log more details */

    /*
     * Constructor. Initializes the variables.
     */
    public CopyFile(String filename, String machine, String destinationDir, int verbose) {
        this.filename = filename;
        this.machine = machine;
        this.destinationDir = destinationDir;
        this.verbose = verbose;
    }

    /*
     * Create the destination directory before copying to it.
     */
    private void createDestDir() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "mkdir", "-p", destinationDir);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        p.waitFor();
    }

    /*
     * Copies the file to the target machine.
     */
    private void copyFile() throws IOException, InterruptedException {
        createDestDir();
        ProcessBuilder pb = new ProcessBuilder("scp", filename, machine + ":" + destinationDir);
        pb.redirectErrorStream(true);
        if (verbose == 1)
            pb.inheritIO();
        Process p = pb.start();

        p.waitFor();
    }

    /* 
    * Main thread method. Copies the file.
     */
    public void run() {
        try {
            copyFile();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}