package distributed.deploy;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import distributed.utils.*;

public class Clean {

	ConcurrentLinkedQueue<String> machinesOn;

	BufferedReader in = null;

	public Clean(String filename) throws FileNotFoundException {
		in = new BufferedReader(new FileReader(filename));
		machinesOn = new ConcurrentLinkedQueue<String>();
	}
	
	public void cleanAll() throws InterruptedException, IOException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		for (String machine : machinesOn) {
			DeleteDir d = new DeleteDir(machine, Constants.BASEDIR, 0);
			threads.add(d);
			d.start();
		}

		for (Thread t : threads) {
			t.join();
		}
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
	}

	public static void main(String args[]) throws IOException, InterruptedException {
		String targets = args[0];

		Clean c = new Clean(targets);
		System.out.println("Checking machines...");
		c.checkAll();
		System.out.println("Finished check.");
		System.out.println("Cleaning machines...");
		c.cleanAll();
		System.out.println("Finished clean.");
	}
}
