package distributed.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/* 
* The class GetFilesInDir copies all the files in the directory from the target machine to this machine.
 */
public class GetFilesInDir extends Thread {

    private String machine; /* name of the target machine */
    private String dirname; /* directory from which to copie files */
    private String destinationDir; /* directory to which files will be copied */
    private int verbose; /* if 1, the execution will log more details */

    /* 
    * Constructor. Initializes the variables.
     */
    public GetFilesInDir(String machine, String dirname, String destinationDir, int verbose) {
        this.machine = machine;
        this.dirname = dirname;
        this.destinationDir = destinationDir;
        this.verbose = verbose;
    }

    /* 
    * Copies the specified file from the machine to this machine.
    * @param filename Name of the file to be copied
     */
    private void getFile(String filename) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("scp", machine + ":" + dirname + "/" + filename, destinationDir);
        pb.redirectErrorStream(true);
        if(verbose == 1) pb.inheritIO();
        Process p = pb.start();

        p.waitFor();
    }

    /* 
    * Lists files the directory and calls getFile to each entry.
     */
    private void getAllFiles() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "ls", dirname);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

        p.waitFor();

        String filename;
        while((filename = reader.readLine()) != null){
            getFile(filename);
        }
    }

    /* 
    * Main thread method. Gets all files.
    */
    public void run() {
        try {
            getAllFiles();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}