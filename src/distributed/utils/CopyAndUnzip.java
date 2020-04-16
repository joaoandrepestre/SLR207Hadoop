package distributed.utils;

import java.io.IOException;

public class CopyAndUnzip extends Thread {

    private String sourceDir;
    private String filename;
    private String machine;
    private String destinationDir;
    private int verbose;

    public CopyAndUnzip(String sourceDir, String filename, String machine, String destinationDir, int verbose) {
        this.sourceDir = sourceDir;
        this.filename = filename;
        this.machine = machine;
        this.destinationDir = destinationDir;
        this.verbose = verbose;
    }

    private void unzip() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "unzip", "-o", "-d", destinationDir, destinationDir + "/" + filename);
        pb.redirectErrorStream(true);
        if(verbose == 1) pb.inheritIO();
        Process p = pb.start();

        p.waitFor();
    }

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