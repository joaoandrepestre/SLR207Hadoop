package distributed.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

public class GetFilesInDir extends Thread {

    private String machine;
    private String dirname;
    private String destinationDir;

    public GetFilesInDir(String machine, String dirname, String destinationDir) {
        this.machine = machine;
        this.dirname = dirname;
        this.destinationDir = destinationDir;
    }

    private void getFile(String filename) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("scp", machine + ":" + dirname + "/" + filename, destinationDir);
        pb.redirectErrorStream(true);
        Process p = pb.start();

        p.waitFor();
    }

    private void getAllFiles() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("ssh", machine, "ls", dirname);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

        p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);

        String filename;
        while((filename = reader.readLine()) != null){
            getFile(filename);
        }
    }

    public void run() {
        try {
            getAllFiles();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}