package distributed.utils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class CopyFile extends Thread {

    private String filename;
    private String machine;
    private String destinationDir;
    private int verbose;

    public CopyFile(String filename, String machine, String destinationDir, int verbose) {
        this.filename = filename;
        this.machine = machine;
        this.destinationDir = destinationDir;
        this.verbose = verbose;
    }

    private void createDestDir() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "mkdir", "-p", destinationDir);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);
    }

    private void copyFile() throws IOException, InterruptedException {
        createDestDir();
        ProcessBuilder pb = new ProcessBuilder("scp", filename, machine + ":" + destinationDir);
        pb.redirectErrorStream(true);
        if(verbose == 1) pb.inheritIO();
        Process p = pb.start();

        p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);
    }

    public void run() {
        try {
            copyFile();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}