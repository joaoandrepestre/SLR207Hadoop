package distributed.utils;

import java.io.IOException;

/* 
* The class CopyAndUnzip copies the zip file to the target machine and unzips it.
 */
public class CopyAndUnzip extends Thread {

    private String sourceDir; /* directory where the zip file is */
    private String filename; /* name of the zip file */
    private String machine; /* name of the target machine */
    private String destinationDir; /* diretory to which the file will be copied */
    private int verbose; /* if 1, the execution will log more details */

    /* 
    * Constructor. Initializes the variables.
     */
    public CopyAndUnzip(String sourceDir, String filename, String machine, String destinationDir, int verbose) {
        this.sourceDir = sourceDir;
        this.filename = filename;
        this.machine = machine;
        this.destinationDir = destinationDir;
        this.verbose = verbose;
    }

    /* 
    * Unzips the file inside the destination directory.
     */
    private void unzip() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "unzip", "-o", "-d", destinationDir,
                destinationDir + "/" + filename);
        pb.redirectErrorStream(true);
        if (verbose == 1)
            pb.inheritIO();
        Process p = pb.start();

        p.waitFor();
    }

    /* 
    * Main thread method. Calls the CopyFile class to copy the file, unzips it 
    * and then calls the DeleteDir class to delete the zip file. 
     */
    public void run() {
        new CopyFile(sourceDir + filename, machine, destinationDir, verbose).run();
        try {
            unzip();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        new DeleteDir(machine, destinationDir + "/" + filename, verbose).run();
    }

}