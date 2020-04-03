package sequential;

import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

public class WordCounter {
	
	BufferedReader in = null;
	HashMap<String, Integer> wordsCounted;
	
	public WordCounter(String filename) throws IOException{
		in = new BufferedReader(new FileReader(filename));
		wordsCounted = new HashMap<String, Integer>();
	}
	
	public void count() throws IOException {
		String line;
		String[] words;
		Integer count;
		while((line = in.readLine()) != null) {
				//line = line.replaceAll("\\p{P}", "");
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
	
	public static void printEntry(Map.Entry<String, Integer> entry) {
		System.out.println(entry.getKey() + " " + entry.getValue());
	}
	
	
	
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
	
	public static void main(String args[]) throws IOException {
		String filename = args[0];
		
		WordCounter wc = new WordCounter(filename);
		long startTime = System.currentTimeMillis();
		wc.count();
		long endTime = System.currentTimeMillis();
		long timeToCount = endTime - startTime;
		startTime = System.currentTimeMillis();
		Stream<Entry<String, Integer>> sorted = wc.sort();
		endTime = System.currentTimeMillis();
		long timeToSort = endTime - startTime;
		sorted.forEach(WordCounter::printEntry);
		
		System.out.println("Time to count: " + timeToCount);
		System.out.println("Time to sort: " + timeToSort);

	}

}
