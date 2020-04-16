package distributed.utils;

import java.io.IOException;

public class RunSlave extends Thread {

    private String machine;
    private int mode;
    private String filename;
    private int verbose;

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

    private void runSlave() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "java", "-jar", Constants.BASEDIR + "/slave.jar",
                "" + mode, filename, ""+verbose);

        Process p = pb.inheritIO().start();
        p.waitFor();
    }

    public void run() {
        try {
            runSlave();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}