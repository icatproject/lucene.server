package icat.lucene;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TT {

	public static void main(String[] args) {
		System.out.println("Hello");
		System.out.println(directories.computeIfAbsent("A", v -> fn(v)));
		System.out.println(directories.computeIfAbsent("A", v -> fn(v)));
		System.out.println(directories.computeIfAbsent("Aardvark", v -> fn(v)));
		System.out.println(directories.computeIfAbsent("A", v -> fn(v)));
		System.out.println(directories.computeIfAbsent("Aardvark", v -> fn(v)));
		System.out.println(directories);
	}

	private static Integer fn(String v) {
		System.out.println("Compute for " + v);
		return v.length();
	}

	private static Map<String, Integer> directories = new ConcurrentHashMap<>();

}
