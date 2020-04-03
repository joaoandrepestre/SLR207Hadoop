package distributed.utils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RunSlave extends Thread {

    private String machine;
    private int mode;
    private String filename;

    public RunSlave(String machine, int mode, String filename) {
        this.machine = machine;
        this.mode = mode;
        this.filename = filename;
    }

    public RunSlave(String machine, int mode) {
        this.machine = machine;
        this.mode = mode;
        this.filename = "";
    }

    private void runSlave() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "java", "-jar", Constants.BASEDIR + "/slave.jar",
                "" + mode, filename);

        Process p = pb.inheritIO().start();
        p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);
    }

    public void run() {
        try {
            runSlave();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}