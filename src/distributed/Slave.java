package distributed;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import distributed.utils.*;

public class Slave {

	private String hostname;
	private String[] machines;
	private int verbose;

	public Slave(int verbose) throws NumberFormatException, IOException {
		hostname = InetAddress.getLocalHost().getHostName();
		this.verbose = verbose;

		BufferedReader machinesFile = new BufferedReader(new FileReader(Constants.BASEDIR + "/machines.txt"));
		int nbMachines = Integer.parseInt(machinesFile.readLine());
		machines = new String[nbMachines];
		for (int i = 0; i < nbMachines; i++) {
			machines[i] = machinesFile.readLine();
		}
		machinesFile.close();
	}

	public static void map(String filename, int verbose) throws IOException, InterruptedException {
		if (verbose == 1)
			System.out.println("[" + InetAddress.getLocalHost().getHostName() + "]" + "Creating maps directory...");
		Local.createDir(Constants.BASEDIR + "/maps", verbose);

		BufferedReader splitFile = new BufferedReader(new FileReader(filename));
		BufferedWriter mapFile = new BufferedWriter(
				new FileWriter(Constants.BASEDIR + "/maps/UM" + filename.charAt(21) + ".txt"));

		String line;
		String[] words;

		if (verbose == 1)
			System.out.println("[" + InetAddress.getLocalHost().getHostName() + "]" + "Maping words from file "
					+ filename + "...");
		while ((line = splitFile.readLine()) != null) {
			words = line.split(" ");
			for (String w : words) {
				if (!w.isBlank()) {
					w = w.toLowerCase();
					mapFile.write(w + " 1\n");
				}
			}
		}

		splitFile.close();
		mapFile.close();
	}

	public void hash(String filename) throws IOException, InterruptedException {
		if (verbose == 1)
			System.out.println("[" + hostname + "]" + "Creating shuffles directory...");
		Local.createDir(Constants.BASEDIR + "/shuffles", verbose);

		BufferedReader mapFile = new BufferedReader(new FileReader(filename));
		ArrayList<Integer> hashs = new ArrayList<Integer>();

		String line;
		String[] words;

		int nbMachines = machines.length;

		if (verbose == 1)
			System.out.println("[" + hostname + "]" + "Hashing words in " + filename + " ...");
		while ((line = mapFile.readLine()) != null) {
			words = line.split(" ");
			int hash = Local.positiveHash(words[0]);
			if(!hashs.contains(hash)) hashs.add(hash);
			String hashedFileName = Constants.BASEDIR + "/shuffles/" + hash + "-" + hostname + ".txt";
			BufferedWriter hashedFile = new BufferedWriter(new FileWriter(hashedFileName, true));
			hashedFile.write(line + "\n");
			hashedFile.close();
		}
		mapFile.close();

		if (verbose == 1)
			System.out.println("[" + hostname + "]" + "Creating zips directory...");
		Local.createDir(Constants.BASEDIR + "/shuffles/zips", verbose);
		for (int hash : hashs) {
			int machineIndex = hash % nbMachines;
			String machine = machines[machineIndex];
			String hashedFile = Constants.BASEDIR + "/shuffles/" + hash + "-" + hostname + ".txt";
			String zipFilename = Constants.BASEDIR + "/shuffles/zips/" + machine + "_" + hostname;

			if (verbose == 1)
				System.out.println("[" + hostname + "]" + "Zipping file " + hashedFile + "...");
			Local.zipFile(hashedFile, zipFilename, verbose);
		}
	}

	public void shuffle(String filename) throws NumberFormatException, IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		hash(filename);

		File zipDir = new File(Constants.BASEDIR + "/shuffles/zips");

		for (File file : zipDir.listFiles()) {
			String sourceDir = Constants.BASEDIR + "/shuffles/zips/";
			String zipFilename = file.getName();
			String machine = zipFilename.split("_")[0];
			String destdir = Constants.BASEDIR + "/shufflesreceived";

			if (verbose == 1)
				System.out.println("[" + hostname + "]" + "Sending zip to " + machine + "...");
			CopyAndUnzip cp = new CopyAndUnzip(sourceDir, zipFilename, machine, destdir, verbose);
			threads.add(cp);
			cp.start();
		}

		for (Thread t : threads) {
			t.join();
		}
	}

	public static void reduce(int verbose) throws IOException, InterruptedException {
		HashMap<String, Integer> reduces = new HashMap<String, Integer>();
		Integer count;

		if (verbose == 1)
			System.out.println("[" + InetAddress.getLocalHost().getHostName() + "]" + "Creating reduces directory...");
		Local.createDir(Constants.BASEDIR + "/reduces", verbose);

		File shufflesreceivedDir = new File(Constants.BASEDIR + "/shufflesreceived/tmp/jpestre/shuffles");
		for (File file : shufflesreceivedDir.listFiles()) {
			BufferedReader hashedFile = new BufferedReader(new FileReader(file));
			String line;
			if (verbose == 1)
				System.out.println("[" + InetAddress.getLocalHost().getHostName() + "]" + "Counting entries in "
						+ file.getName() + "...");
			while ((line = hashedFile.readLine()) != null) {
				String word = line.split(" ")[0];
				count = reduces.putIfAbsent(word, 1);
				if (count != null) {
					reduces.put(word, count + 1);
				}
			}
			hashedFile.close();
		}

		for (Map.Entry<String, Integer> entry : reduces.entrySet()) {
			String word = entry.getKey();
			int hash = Local.positiveHash(word);
			count = entry.getValue();
			BufferedWriter reduceFile = new BufferedWriter(
					new FileWriter(Constants.BASEDIR + "/reduces/" + hash + ".txt"));
			reduceFile.write(word + " " + count + "\n");
			reduceFile.close();
		}
	}

	public static void main(String args[]) throws InterruptedException, IOException {

		int mode = Integer.parseInt(args[0]);
		int verbose;
		String filename;
		Slave slave;

		switch (mode) {
			case 0:
				filename = args[1];
				verbose = Integer.parseInt(args[2]);
				map(filename, verbose);
				break;
			case 1:
				filename = args[1];
				verbose = Integer.parseInt(args[2]);
				slave = new Slave(verbose);
				slave.shuffle(filename);
				break;
			case 2:
				verbose = Integer.parseInt(args[1]);
				reduce(verbose);
				break;
			default:
				break;
		}
	}

}
