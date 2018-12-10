package com.github.dagnelies.filemap;


import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class BufferedRandomAccessFileTest {
	
	public static void main(String[] args) throws IOException, InterruptedException {
		BufferedRandomAccessFile bf = new BufferedRandomAccessFile(new File("this-is-a-test.txt"), "rw");
		
		// clear the file
		bf.truncate(0);
		
		int N = 1000*1;
		long t = System.currentTimeMillis();
		int count;
		
		// write N lines
		for (int i = 0; i < N; i++) {
			bf.write(UUID.randomUUID() + "\n");
		}
		
		System.out.println("Inserted " + N + " lines in " + (System.currentTimeMillis() - t) + " ms");
		t = System.currentTimeMillis();
		
		
		// skip N lines
		bf.seek(0);
		count = 0;
		while(!bf.isEOF()) {
			bf.skipUntil((byte) '\n');
			count++;
		}
		assert N == count;
		System.out.println("Skip " + count + " lines in " + (System.currentTimeMillis() - t) + " ms");
		
		
		// read N lines
		bf.seek(0);
		count = 0;
		while(!bf.isEOF()) {
			bf.readLine();
			count++;
		}
		assert N == count;
		System.out.println("Read " + count + " lines in " + (System.currentTimeMillis() - t) + " ms");
		
		
	}

}
