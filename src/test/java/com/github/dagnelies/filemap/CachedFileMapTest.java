package com.github.dagnelies.filemap;


import java.io.IOException;
import java.util.Map;
import java.util.UUID;


public class CachedFileMapTest {

	public static void main(String[] args) throws IOException, InterruptedException {
		Map<String, String> map = new CachedFileMap<>("this-is-a-test.jkv", String.class, String.class);
		//Map<String, String> map = new HashMap<>();
		System.out.println("Created");
		System.out.println(map.get("something"));
		System.out.println("Size: " + map.size());
		
		map.put("something", "here");
		long next = System.currentTimeMillis() + 1000;
		int size = map.size();
		while(size < 1000 * 1000 * 100) {
			if( System.currentTimeMillis() > next ) {
				next += 1000;
				int sizeNow = map.size();
				System.out.println("Entries: " + sizeNow / 1000 + "k");
				System.out.println("TPS: " + (sizeNow - size) / 1000 + "k/s");
				size = sizeNow;
				//System.gc();
				
				System.out.println("Free memory (mb): " + (Runtime.getRuntime().freeMemory() / 1000 / 1000));
			}
			map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
		}
		map.put("something", "changed");
	}
}
