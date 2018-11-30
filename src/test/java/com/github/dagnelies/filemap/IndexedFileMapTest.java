package com.github.dagnelies.filemap;


import java.io.IOException;
import java.util.Map;
import java.util.UUID;


public class IndexedFileMapTest<K,V> {

	public static void main(String[] args) throws IOException, InterruptedException {
		Map<String, String> map = new IndexedFileMap<>("this-is-a-test.jkv");
		//Map<String, String> map = new HashMap<>();
		System.out.println("Created");
		System.out.println("Size: " + map.size());
		System.out.println("lucky number: " + map.get("lucky number"));
		
		long next = System.currentTimeMillis() + 1000;
		int size = map.size();
		while(size < 1000 * 1000) {
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
			if(Math.random() < 0.001) {
				map.put("lucky number", "Entry number " + map.size());
			}
		}
	}
}
