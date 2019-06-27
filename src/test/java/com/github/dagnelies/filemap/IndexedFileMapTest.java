package com.github.dagnelies.filemap;


import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.fasterxml.jackson.core.type.TypeReference;


public class IndexedFileMapTest {

	public static class MyGuid {
		public String guid;
		
		MyGuid() {
			this.guid = UUID.randomUUID().toString();
		}
	}
	
	public static<T> TypeReference<T> build2() {
		return new TypeReference<T>() {};
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		// create blank DB, even if it already exists
		File file = new File("this-is-a-test.jkv");
		//if( file.exists() )
		//	file.delete();
		
		AbstractFileMap<String, MyGuid> map = new IndexedFileMap<String, MyGuid>(file, String.class, MyGuid.class);
		
		// insert and get single element
		MyGuid myGuid = new MyGuid();
		System.out.println(myGuid.guid);
		map.put("test", new MyGuid());
		myGuid = map.get("test");
		System.out.println(myGuid.guid);
		map.close();
		
		// reload it
		map = new IndexedFileMap<>(new File("this-is-a-test.jkv"), String.class, MyGuid.class);
		myGuid = map.get("test");
		System.out.println(myGuid.guid);
		
		
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
			
			map.put(UUID.randomUUID().toString(), new MyGuid());
			/*if(Math.random() < 0.001) {
				map.put("lucky number", "Entry number " + map.size());
			}*/
		}
	}
}
