package distributed;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import distributed.utils.*;

public class Master {

	private BufferedReader machinesOn;
	private ArrayList<String> machinesUsed;
	private int nbMachines;

	public Master(String filename, int _nbMachines) throws NumberFormatException, IOException {
		machinesOn = new BufferedReader(new FileReader(filename));
		int maxNbMachines = Integer.parseInt(machinesOn.readLine());
		this.nbMachines = _nbMachines > maxNbMachines ? maxNbMachines : _nbMachines;
		machinesUsed = new ArrayList<String>();
	}

	public void split(String input) throws IOException, InterruptedException {
		System.out.println("Spliting file into " + nbMachines + " splits...");
		Local.createDir(Constants.BASEDIR + "/splits");

		ArrayList<Thread> threads = new ArrayList<Thread>();
		RandomAccessFile inputFile = new RandomAccessFile(input, "r");

		BufferedWriter machines = new BufferedWriter(new FileWriter("machines.txt"));
		machines.write("" + nbMachines + "\n");

		long fileSize = inputFile.length();
		long splitSize = fileSize / nbMachines;
		long splitPos = 1;
		long splitEnd = splitSize;
		for (int i = 0; i < nbMachines; i++) {
			String splitName = Constants.BASEDIR + "/splits/S" + i + ".txt";
			BufferedWriter split = new BufferedWriter(new FileWriter(splitName));
			char c = ' ';
			while (splitPos < splitEnd || !Character.isWhitespace(c)) {
				try {
					c = (char) inputFile.readByte();
					split.write(c);
					splitPos++;
				} catch (EOFException e) {
					break;
				}
			}
			splitEnd = splitPos + splitSize - 1;
			split.close();

			String machine = machinesOn.readLine();
			machinesUsed.add(machine);
			machines.write(machine + "\n");

			CopyFile cp = new CopyFile(splitName, machine, Constants.BASEDIR + "/splits");
			threads.add(cp);
			cp.start();
		}
		inputFile.close();
		machines.close();

		for (Thread t : threads) {
			t.join();
		}
		System.out.println("SPLIT FINISHED");
	}

	public void map() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		for (int i = 0; i < nbMachines; i++) {
			String machine = machinesUsed.get(i);
			String file = Constants.BASEDIR + "/splits/S" + i + ".txt";

			RunSlave rs = new RunSlave(machine, 0, file);
			threads.add(rs);
			System.out.println("Calling map on machine " + machine + " and file " + file);
			rs.start();
		}

		for (Thread t : threads) {
			t.join();
		}
		System.out.println("MAP FINISHED");
	}

	public void prepareShuffle() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		for (String machine : machinesUsed) {
			CopyFile cp = new CopyFile("machines.txt", machine, Constants.BASEDIR);
			threads.add(cp);
			cp.start();
		}

		for (Thread t : threads) {
			t.join();
		}
	}

	public void suffle() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		prepareShuffle();
		for (int i = 0; i < machinesUsed.size(); i++) {
			String machine = machinesUsed.get(i);
			String file = Constants.BASEDIR + "/maps/UM" + i + ".txt";

			RunSlave rs = new RunSlave(machine, 1, file);
			threads.add(rs);
			System.out.println("Calling shuffle on machine " + machine + " and file " + file);
			rs.start();
		}

		for (Thread t : threads) {
			t.join();
		}
		System.out.println("SHUFFLE FINISHED");
	}

	public void reduce() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		for (String machine : machinesUsed) {
			RunSlave rs = new RunSlave(machine, 2);
			threads.add(rs);
			System.out.println("Calling reduce on machine " + machine);
			rs.start();
		}

		for (Thread t : threads) {
			t.join();
		}
		System.out.println("REDUCE FINNISHED");
	}

	public void getReducedFiles() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		Local.createDir(Constants.BASEDIR + "/results");
		for (String machine : machinesUsed) {
			GetFilesInDir get = new GetFilesInDir(machine, Constants.BASEDIR + "/reduces",
					Constants.BASEDIR + "/results");
			threads.add(get);
			get.start();
		}

		for (Thread t : threads) {
			t.join();
		}
	}

	public static void printEntry(Map.Entry<String, Integer> entry) {
		System.out.println(entry.getKey() + " " + entry.getValue());
	}

	public void getResults() throws IOException, InterruptedException {
		System.out.println("Getting results...");
		HashMap<String, Integer> results = new HashMap<String, Integer>();
		getReducedFiles();

		File resultsDir = new File(Constants.BASEDIR + "/results");
		for (File file : resultsDir.listFiles()) {
			BufferedReader reducedFile = new BufferedReader(new FileReader(file));
			String[] data = reducedFile.readLine().split(" ");
			results.put(data[0], Integer.parseInt(data[1]));
			reducedFile.close();
		}
		System.out.println("RESULTS FINISHED");

		results.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByKey())
				.sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder()))
				.forEach(Master::printEntry);
	}

	public static void main(String args[]) throws IOException, InterruptedException {
		String machinesOn = args[0];
		String input = args[1];
		int nbMachines = Integer.parseInt(args[2]);

		long startTime, endTime, timeToSplit, timeToMap, timeToShuffle, timeToReduce;

		Master master = new Master(machinesOn, nbMachines);

		startTime = System.currentTimeMillis();
		master.split(input);
		endTime = System.currentTimeMillis();
		timeToSplit = endTime - startTime;

		startTime = System.currentTimeMillis();
		master.map();
		endTime = System.currentTimeMillis();
		timeToMap = endTime - startTime;

		startTime = System.currentTimeMillis();
		master.suffle();
		endTime = System.currentTimeMillis();
		timeToShuffle = endTime - startTime;

		startTime = System.currentTimeMillis();
		master.reduce();
		endTime = System.currentTimeMillis();
		timeToReduce = endTime - startTime;

		master.getResults();

		System.out.println("Time to split: " + timeToSplit + "ms");
		System.out.println("Time to map: " + timeToMap + "ms");
		System.out.println("Time to shuffle: " + timeToShuffle + "ms");
		System.out.println("Time to reduce: " + timeToReduce + "ms");
	}

}
