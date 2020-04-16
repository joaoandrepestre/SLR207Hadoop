package distributed;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import distributed.utils.*;

/* 
* The class Master is the main class of the MapReduce system, 
* calls the slave in the other machines in the system in order 
* to count the words of an input text file.
 */
public class Master {

	private BufferedReader machinesOn; /* file with the names of active machines */
	private ArrayList<String> machinesUsed; /* list of machines used by the system */
	private int nbMachines; /* number of machines used */
	private PrintStream out = null; /* output file printer */
	private int verbose; /* if 1, the execution will log more details */

	/* 
	* Contructor. Initializes the variables. Limits the number of machines between 1 and the number of active machines
	 */
	public Master(String filename, int _nbMachines, PrintStream out, int verbose)
			throws NumberFormatException, IOException {
		machinesOn = new BufferedReader(new FileReader(filename));
		int maxNbMachines = Integer.parseInt(machinesOn.readLine());
		this.nbMachines = _nbMachines > maxNbMachines ? maxNbMachines : _nbMachines;
		this.nbMachines = this.nbMachines == 0 ? 1 : nbMachines;
		machinesUsed = new ArrayList<String>();
		this.out = out;
		this.verbose = verbose;
	}

	/* 
	* Splits the input file into nbMachines pieces and sends it to the other machines used.
	* @param input Name of the input file
	 */
	public void split(String input) throws IOException, InterruptedException {
		if (verbose == 1)
			System.out.println("Creating splits directory...");
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
			RandomAccessFile split = new RandomAccessFile(splitName, "rw");
			char c = ' ';
			byte b;
			while (splitPos < splitEnd || !Character.isWhitespace(c)) {
				try {
					b = inputFile.readByte();
					c = (char) b;
					split.write(b);
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

			if (verbose == 1)
				System.out.println("Copying split " + splitName + " to machine " + machine);
			CopyFile cp = new CopyFile(splitName, machine, Constants.BASEDIR + "/splits", verbose);
			threads.add(cp);
			cp.start();
		}
		inputFile.close();
		machines.close();

		for (Thread t : threads) {
			t.join();
		}
	}

	/* 
	* Calls the slave in map mode in all machines used.
	 */
	public void map() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		for (int i = 0; i < nbMachines; i++) {
			String machine = machinesUsed.get(i);
			String file = Constants.BASEDIR + "/splits/S" + i + ".txt";

			RunSlave rs = new RunSlave(machine, 0, file, verbose);
			threads.add(rs);
			if (verbose == 1)
				System.out.println("Calling map on machine " + machine + " and file " + file);
			rs.start();
		}

		for (Thread t : threads) {
			t.join();
		}
	}

	/* 
	* Sends the file with the machines used to all machines used.
	 */
	public void prepareShuffle() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		for (String machine : machinesUsed) {
			if (verbose == 1)
				System.out.println("Copying file machines.txt to " + machine);
			CopyFile cp = new CopyFile("machines.txt", machine, Constants.BASEDIR, verbose);
			threads.add(cp);
			cp.start();
		}

		for (Thread t : threads) {
			t.join();
		}
	}

	/* 
	* Calls the slave in shuffle mode in all machines used.
	 */
	public void suffle() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		prepareShuffle();
		for (int i = 0; i < machinesUsed.size(); i++) {
			String machine = machinesUsed.get(i);
			String file = Constants.BASEDIR + "/maps/UM" + i + ".txt";

			RunSlave rs = new RunSlave(machine, 1, file, verbose);
			threads.add(rs);
			if (verbose == 1)
				System.out.println("Calling shuffle on machine " + machine + " and file " + file);
			rs.start();
		}

		for (Thread t : threads) {
			t.join();
		}
	}

	/* 
	* Calls the slave in reduce mode in all machines used.
	 */
	public void reduce() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		for (String machine : machinesUsed) {
			RunSlave rs = new RunSlave(machine, 2, verbose);
			threads.add(rs);
			if (verbose == 1)
				System.out.println("Calling reduce on machine " + machine);
			rs.start();
		}

		for (Thread t : threads) {
			t.join();
		}
	}

	/* 
	* Gets all reduced files from the machines used.
	 */
	public void getReducedFiles() throws IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		if (verbose == 1)
			System.out.println("Creating results directory...");
		Local.createDir(Constants.BASEDIR + "/results");
		for (String machine : machinesUsed) {
			GetFilesInDir get = new GetFilesInDir(machine, Constants.BASEDIR + "/reduces",
					Constants.BASEDIR + "/results", verbose);
			threads.add(get);
			if (verbose == 1)
				System.out.println("Getting reduced files from machine" + machine + "...");
			get.start();
		}

		for (Thread t : threads) {
			t.join();
		}
	}

	/* 
	* Gets all reduced files and adds up the results. Sorts and prints this result.
	 */
	public void getResults() throws IOException, InterruptedException {
		HashMap<String, Integer> results = new HashMap<String, Integer>();
		getReducedFiles();

		File resultsDir = new File(Constants.BASEDIR + "/results");
		for (File file : resultsDir.listFiles()) {
			if(verbose == 1) System.out.println("Reading results from file " + file.getName() + "...");
			BufferedReader reducedFile = new BufferedReader(new FileReader(file));
			String[] data = reducedFile.readLine().split(" ");
			results.put(data[0], Integer.parseInt(data[1]));
			reducedFile.close();
		}

		if(verbose == 1) System.out.println("Sorting and printing to output file...");
	
		results.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByKey())
				.sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder()))
				.forEach(entry -> {
					out.println(entry.getKey() + " " + entry.getValue());
				});
	}

	/* 
	* Splits the input file, maps, shuffles and reduces. Times each step.
	 */
	public static void main(String args[]) throws IOException, InterruptedException {
		String machinesOn = args[0];
		String input = args[1];
		int nbMachines = Integer.parseInt(args[2]);
		String outFile = args[3];
		int verbose = Integer.parseInt(args[4]);

		PrintStream out = new PrintStream(outFile);

		long startTime, endTime, timeToSplit, timeToMap, timeToShuffle, timeToReduce;

		Master master = new Master(machinesOn, nbMachines, out, verbose);

		System.out.println("Spliting file into " + nbMachines + " splits...");
		startTime = System.currentTimeMillis();
		master.split(input);
		endTime = System.currentTimeMillis();
		System.out.println("SPLIT FINISHED");
		timeToSplit = endTime - startTime;

		System.out.println("Starting map...");
		startTime = System.currentTimeMillis();
		master.map();
		endTime = System.currentTimeMillis();
		System.out.println("MAP FINISHED");
		timeToMap = endTime - startTime;

		System.out.println("Starting shuffle...");
		startTime = System.currentTimeMillis();
		master.suffle();
		endTime = System.currentTimeMillis();
		System.out.println("SHUFFLE FINISHED");
		timeToShuffle = endTime - startTime;

		System.out.println("Starting reduce...");
		startTime = System.currentTimeMillis();
		master.reduce();
		endTime = System.currentTimeMillis();
		System.out.println("REDUCE FINNISHED");
		timeToReduce = endTime - startTime;

		System.out.println("Getting results...");
		master.getResults();
		System.out.println("RESULTS FINISHED");

		out.println("Time to split: " + timeToSplit + "ms");
		System.out.println("Time to split: " + timeToSplit + "ms");
		out.println("Time to map: " + timeToMap + "ms");
		System.out.println("Time to map: " + timeToMap + "ms");
		out.println("Time to shuffle: " + timeToShuffle + "ms");
		System.out.println("Time to shuffle: " + timeToShuffle + "ms");
		out.println("Time to reduce: " + timeToReduce + "ms");
		System.out.println("Time to reduce: " + timeToReduce + "ms");
	}

}
