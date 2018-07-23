package dagnelies;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class FakeFileInputStream extends InputStream {
	
	InputStream is;
	
	FakeFileInputStream(InputStream is) {
		this.is = is;
	}

	@Override
	public int read() throws IOException {
		return is.read();
	}
	
	@Override
	public int read(byte b[], int off, int len) throws IOException {
		return is.read(b, off, len);
	}
	
	public static void main(String[] args) throws IOException {
		InputStream is = new FileInputStream("this-is-a-test.db");
		//BufferedReader bf = new BufferedReader(new InputStreamReader(is));
		BufferedReader bf = new BufferedReader(new InputStreamReader(new FakeFileInputStream(is)));
		//BufferedReader bf = new BufferedReader(new InputStreamReader(new FakeFileInputStream( new FileInputStream("this-is-a-test.db")));
				/*
				System.out.println(bf.readLine());
				System.out.println(bf.readLine());
				System.out.println(bf.readLine());
				System.out.println(bf.readLine());
				System.out.println(bf.readLine());
				System.out.println(bf.readLine());
				System.out.println(bf.readLine());
				System.out.println(bf.readLine());
				*/
				
				//Thread.sleep(1000*5);
				int count = 0;
				long start = System.currentTimeMillis();
				long next = start + 1000;
				while(true) {
					String line = bf.readLine();
					if( line == null )
						break;
					count++;
					if( System.currentTimeMillis() > next ) {
						next += 1000;
						System.out.println("line read: " + (count / 1000) + "k");
					}
				}
				System.out.println("count: " + count / 1000);
				System.out.println("ms: " + (System.currentTimeMillis() - start));
	}

}
