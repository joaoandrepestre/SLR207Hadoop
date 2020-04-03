package distributed.utils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DeleteDir extends Thread {

    private String machine;
    private String dirname;

    public DeleteDir(String machine, String dirname) {
        this.machine = machine;
        this.dirname = dirname;
    }

    private void deleteDir() throws InterruptedException, IOException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "rm", "-rf", dirname);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);
    }

    public void run() {
        try {
            deleteDir();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

}