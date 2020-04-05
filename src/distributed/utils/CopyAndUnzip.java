package distributed.utils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class CopyAndUnzip extends Thread {

    private String sourceDir;
    private String filename;
    private String machine;
    private String destinationDir;

    public CopyAndUnzip(String sourceDir, String filename, String machine, String destinationDir) {
        this.sourceDir = sourceDir;
        this.filename = filename;
        this.machine = machine;
        this.destinationDir = destinationDir;
    }

    private void unzip() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "unzip", "-d", destinationDir, destinationDir + "/" + filename);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);
    }

    public void run() {
        new CopyFile(sourceDir + filename, machine, destinationDir).run();
        try {
            unzip();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        new DeleteDir(machine, destinationDir + "/" + filename).run();
    }

}