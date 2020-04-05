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

	public Slave() throws NumberFormatException, IOException {
		hostname = InetAddress.getLocalHost().getHostName();

		BufferedReader machinesFile = new BufferedReader(new FileReader(Constants.BASEDIR + "/machines.txt"));
		int nbMachines = Integer.parseInt(machinesFile.readLine());
		machines = new String[nbMachines];
		for (int i = 0; i < nbMachines; i++) {
			machines[i] = machinesFile.readLine();
		}
		machinesFile.close();
	}

	public static void map(String filename) throws IOException, InterruptedException {
		Local.createDir(Constants.BASEDIR + "/maps");

		BufferedReader splitFile = new BufferedReader(new FileReader(filename));
		BufferedWriter mapFile = new BufferedWriter(
				new FileWriter(Constants.BASEDIR + "/maps/UM" + filename.charAt(21) + ".txt"));

		String line;
		String[] words;

		while ((line = splitFile.readLine()) != null) {
			words = line.split(" ");
			for (String w : words) {
				if(!w.isBlank()){
					w.toLowerCase();
					mapFile.write(w + " 1\n");
				}
			}
		}

		splitFile.close();
		mapFile.close();
	}

	public void hash(String filename) throws IOException, InterruptedException {
		Local.createDir(Constants.BASEDIR + "/shuffles");

		BufferedReader mapFile = new BufferedReader(new FileReader(filename));
		ArrayList<Integer> hashs = new ArrayList<Integer>();

		String line;
		String[] words;

		int nbMachines = machines.length;

		while ((line = mapFile.readLine()) != null) {
			words = line.split(" ");
			int hash = Local.positiveHash(words[0]);
			hashs.add(hash);
			String hashedFileName = Constants.BASEDIR + "/shuffles/" + hash + "-" + hostname + ".txt";
			BufferedWriter hashedFile = new BufferedWriter(
					new FileWriter(hashedFileName, true));
			hashedFile.write(line + "\n");
			hashedFile.close();
		}
		mapFile.close();

		Local.createDir(Constants.BASEDIR + "/shuffles/zips");
		for(int hash: hashs){
			int machineIndex = hash % nbMachines;
			String machine = machines[machineIndex];
			String hashedFile = Constants.BASEDIR + "/shuffles/" + hash + "-" + hostname + ".txt";
			String zipFilename = Constants.BASEDIR + "/shuffles/zips/" + machine + "_" + hostname;

			Local.zipFile(hashedFile, zipFilename);
		}
	}

	public void shuffle(String filename) throws NumberFormatException, IOException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<Thread>();

		hash(filename);

		File zipDir = new File(Constants.BASEDIR + "/shuffles/zips");

		for(File file: zipDir.listFiles()){
			String sourceDir = Constants.BASEDIR + "/shuffles/zips/";
			String zipFilename = file.getName();
			String machine = zipFilename.split("_")[0]; 
			String destdir = Constants.BASEDIR + "/shufflesreceived";
			
			CopyAndUnzip cp = new CopyAndUnzip(sourceDir, zipFilename, machine, destdir);
			threads.add(cp);
			cp.start();
		}

		for (Thread t : threads) {
			t.join();
		}
	}

	public static void reduce() throws IOException, InterruptedException {
		HashMap<String, Integer> reduces = new HashMap<String, Integer>();
		Integer count;

		Local.createDir(Constants.BASEDIR + "/reduces");

		File shufflesreceivedDir = new File(Constants.BASEDIR + "/shufflesreceived/tmp/jpestre/shuffles");
		for (File file : shufflesreceivedDir.listFiles()) {
			BufferedReader hashedFile = new BufferedReader(new FileReader(file));
			String line;
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
		String filename;
		Slave slave;

		switch (mode) {
			case 0:
				filename = args[1];
				map(filename);
				break;
			case 1:
				slave = new Slave();
				filename = args[1];
				slave.shuffle(filename);
				break;
			case 2:
				reduce();
				break;
			default:
				break;
		}
	}

}
