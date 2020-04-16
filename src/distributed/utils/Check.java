package distributed.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/* 
* The class Check is used to check if a machine is active to be used in the system.
 */
public class Check extends Thread {

	private ConcurrentLinkedQueue<String> machinesOn; /* data structure to store the name of the machine, if it's active */
	private String machine;  /* name of the machine to check */

	/* 
	* Constructor. Initializes the variables
	 */
	public Check(String machine, ConcurrentLinkedQueue<String> machinesOn) {
		this.machine = machine;
		this.machinesOn = machinesOn;
	}

	/* 
	* Connects to the machine through ssh and gets its hostname. Compares the response with the name of the machine.
	* if they are the same, the check worked.
	* @return true if the machine is active, false otherwise
	 */
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

	/* 
	* Main thread method. If the machine is active, adds it to the data structure.
	 */
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
