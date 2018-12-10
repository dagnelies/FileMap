package com.github.dagnelies.filemap;


import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;


public class CachedFileMapTest {

	public static void main(String[] args) throws IOException, InterruptedException {
		long t = System.currentTimeMillis();
		Map<String, String> map = new CachedFileMap<>(new File("this-is-a-test.jkv"));
		//Map<String, String> map = new HashMap<>();
		System.out.println("Created");
		System.out.println(map.get("something"));
		System.out.println("Size: " + map.size());
		System.out.println("Reads: " + map.size() / (System.currentTimeMillis() - t + 1) + "k t/s");
		
		map.put("something", "here");
		t = System.currentTimeMillis() + 1000;
		int size = map.size();
		while(size < 1000 * 1000) {
			if( System.currentTimeMillis() > t ) {
				t += 1000;
				int sizeNow = map.size();
				System.out.println("Entries: " + sizeNow / 1000 + "k");
				System.out.println("Writes: " + (sizeNow - size) / 1000 + "k t/s");
				size = sizeNow;
				//System.gc();
				
				System.out.println("Free memory (mb): " + (Runtime.getRuntime().freeMemory() / 1000 / 1000));
			}
			map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		}
		map.put("something", "changed");
	}
}
