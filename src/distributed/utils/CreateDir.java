package distributed.utils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class CreateDir extends Thread {

    private String machine;
    private String dirname;

    public CreateDir(String machine, String dirname) {
        this.machine = machine;
        this.dirname = dirname;
    }

    private void createDir() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "mkdir", "-p", dirname);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);
    }

    public void run() {
        try {
            createDir();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}