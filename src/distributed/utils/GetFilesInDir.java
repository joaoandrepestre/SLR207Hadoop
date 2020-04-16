package distributed.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class GetFilesInDir extends Thread {

    private String machine;
    private String dirname;
    private String destinationDir;
    private int verbose;

    public GetFilesInDir(String machine, String dirname, String destinationDir, int verbose) {
        this.machine = machine;
        this.dirname = dirname;
        this.destinationDir = destinationDir;
        this.verbose = verbose;
    }

    private void getFile(String filename) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("scp", machine + ":" + dirname + "/" + filename, destinationDir);
        pb.redirectErrorStream(true);
        if(verbose == 1) pb.inheritIO();
        Process p = pb.start();

        p.waitFor();
    }

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

    public void run() {
        try {
            getAllFiles();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}