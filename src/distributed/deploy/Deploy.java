package distributed.deploy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import distributed.utils.*;

public class Deploy {

	ConcurrentLinkedQueue<String> machinesOn;

	BufferedReader in = null;

	public Deploy(String filename) throws FileNotFoundException {
		in = new BufferedReader(new FileReader(filename));
		machinesOn = new ConcurrentLinkedQueue<String>();
	}

	public static void compileSlave() throws IOException, InterruptedException {
		ProcessBuilder pb = new ProcessBuilder("javac", "-d", ".", "src/distributed/Slave.java", 
																   "src/distributed/utils/CopyFile.java",
																   "src/distributed/utils/Constants.java",
																   "src/distributed/utils/Local.java",
																   "src/distributed/utils/CopyAndUnzip.java",
																   "src/distributed/utils/DeleteDir.java");
		pb.redirectErrorStream(true);
		Process p = pb.start();

		p.waitFor();
	}

	public static void createJar() throws InterruptedException, IOException {
		ProcessBuilder pb = new ProcessBuilder("jar", "cfm", "slave.jar", "manifest.mf", "distributed");
		pb.redirectErrorStream(true);
		Process p = pb.start();

		p.waitFor();
	}

	public static void cleanJar() throws IOException, InterruptedException {
		ProcessBuilder pb = new ProcessBuilder("rm", "-rf", "distributed", "slave.jar");
		pb.redirectErrorStream(true);
		Process p = pb.start();

		p.waitFor();
	}

	public void copyToAll() throws InterruptedException, IOException {
		ArrayList<Thread> threads = new ArrayList<Thread>();
		
		compileSlave();
		createJar();
		for (String machine : machinesOn) {
			CopyFile cp = new CopyFile("slave.jar", machine, Constants.BASEDIR, 0);
			threads.add(cp);
			cp.start();
		}

		for (Thread t : threads) {
			t.join();
		}

		cleanJar();
	}

	public void checkAll() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		String line;
		String room;
		int nbMachines;
		String machine;

		while ((line = in.readLine()) != null) {
			String[] split = line.split(" ");
			room = split[0];
			nbMachines = Integer.parseInt(split[1]);
			for (int i = 1; i <= nbMachines; i++) {
				String si = i < 10 ? "-0" + i : "-" + i;
				machine = "tp-" + room + si;

				Check c = new Check(machine, machinesOn);
				threads.add(c);
				c.start();

			}
		}

		for (Thread t : threads) {
			t.join();
		}

		BufferedWriter outFile = new BufferedWriter(new FileWriter("machinesOn"));
		outFile.write("" + machinesOn.size() + "\n");
		for (String m : machinesOn) {
			outFile.write(m + "\n");
		}

		outFile.close();
	}

	public static void main(String args[]) throws IOException, InterruptedException {
		String targets = args[0];

		Deploy d = new Deploy(targets);
		System.out.println("Checking machines...");
		d.checkAll();
		System.out.println("Finished check.");
		System.out.println("Copying slave to all machines...");
		d.copyToAll();
		System.out.println("Finished copying.");

	}

}
