package sequential;

import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

/* 
* The class WordCounter provides a sequential implementation to count the words on a text file
*/
public class WordCounter {
	
	BufferedReader in = null;
	PrintStream out = null;
	HashMap<String, Integer> wordsCounted;
	
	/* 
	* Constructor. Initializes the input file reader, the output file printer and the HashMap of words counted.
	* @param filename Name of the input file
	* @param out PrintStream to print the results
	*/
	public WordCounter(String filename, PrintStream out) throws IOException{
		in = new BufferedReader(new FileReader(filename));
		this.out = out;
		wordsCounted = new HashMap<String, Integer>();
	}
	
	/* 
	* Reads the file line by line and counts the words. Saves the results in the HashMap.
	*/
	public void count() throws IOException {
		String line;
		String[] words;
		Integer count;
		while((line = in.readLine()) != null) {
				words = line.split(" ");
				for(String w : words) {
					if(!w.isBlank()) {
						w = w.toLowerCase();
						count = wordsCounted.putIfAbsent(w,1);
						if(count != null) {
							wordsCounted.put(w, count+1);
						}
					}
				}
		}
	}
	
	/* 
	* Print the sorted results to the out PrintStream.
	* @param sorted Sorted Stream of map entries 
	*/
	public void print(Stream<Entry<String, Integer>> sorted){
		sorted.forEach(entry->{
			out.println(entry.getKey() + " " + entry.getValue());
		});
	}
	
	/* 
	* Sorts the results using Stream
	* @resutl The sorted Stream
	*/
	public Stream<Entry<String, Integer>> sort() {
		return wordsCounted.entrySet()
		  .stream()
		  .sorted(Map.Entry.<String, Integer>comparingByKey())
		  .sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder()));
	}
	
	@Override
	protected void finalize() throws IOException {
		in.close();
	}
	
	/* 
	* Main function. Counts the words and sorts the results while timing it.
	 */
	public static void main(String args[]) throws IOException {
		String filename = args[0];
		String outFile = args[1];
		PrintStream out = new PrintStream(outFile);

		WordCounter wc = new WordCounter(filename, out);
		long startTime = System.currentTimeMillis();
		wc.count();
		long endTime = System.currentTimeMillis();
		long timeToCount = endTime - startTime;
		startTime = System.currentTimeMillis();
		Stream<Entry<String, Integer>> sorted = wc.sort();
		endTime = System.currentTimeMillis();
		long timeToSort = endTime - startTime;
		wc.print(sorted);
		
		out.println("Time to count: " + timeToCount);
		out.println("Time to sort: " + timeToSort);

	}

}
