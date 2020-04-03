package distributed.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class Check extends Thread {

	private ConcurrentLinkedQueue<String> machinesOn;
	private String machine;

	public Check(String machine, ConcurrentLinkedQueue<String> machinesOn) {
		this.machine = machine;
		this.machinesOn = machinesOn;
	}

	private boolean check() throws IOException, InterruptedException {
		ProcessBuilder pb = new ProcessBuilder("ssh", machine, "hostname");
		pb.redirectErrorStream(true);
		Process p = pb.start();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

		boolean b = p.waitFor(Constants.TIMEOUT, TimeUnit.SECONDS);

		String output;
		if ((output = reader.readLine()) != null) {
			b = output.equals(machine);
		}

		return b;

	}

	public void run() {
		try {
			if (check()) {
				machinesOn.add(machine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
