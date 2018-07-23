package dagnelies;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;

import com.fasterxml.jackson.core.util.ByteArrayBuilder;


@Deprecated
public class BufferedRandomAccessFile2 extends InputStream {

	static final int DEFAULT_BUFFER_SIZE = 8192;
	byte[] buffer;
	
	//long file_pos = 0; // we keep track of this separately since RandomAccessFile.getFilePointer() is an expensive operation
	//long length = 0; // we keep track of this separately since RandomAccessFile.length() is an expensive operation
	
	int buffer_pos = 0;
	RandomAccessFile raf;
	//FileInputStream raf;
	public BufferedRandomAccessFile2(String filename, String mode) throws IOException {
		raf = new RandomAccessFile(filename, mode);
		//raf = new FileInputStream(filename);
	}
	
	void clearBuffer() {
		buffer = null;
		buffer_pos = 0;
	}
	/*
	public void write(byte[] data) throws IOException {
		clearBuffer();
		raf.write(data);
	}
	
	public void close() throws IOException {
		clearBuffer();
		raf.close();
	}
	
	public void truncate(long len) throws IOException {
		clearBuffer();
		raf.setLength(len);
	}
	
	public void seek(long pos) throws IOException {
		clearBuffer();
		raf.seek(pos);
	}
	*/
	public long pos() throws IOException {
		if( buffer_pos > 0 )
			return raf.getFilePointer() - buffer.length + buffer_pos;
		else
			return raf.getFilePointer();
	}
	
	public long length() throws IOException {
		return raf.length();
	}
	
	boolean eof = false;
	
	public boolean isEOF() throws IOException {
		return eof; //pos() >= length(); // <- the underlying "getFilePointer()" and "length()" are reaaaaaaaaalllyyy time consuming!
	}
	/*
	 * Reading a line in UTF-8
	 */
	public String readLine() throws IOException {
		if( isEOF() )
			return null;
		
		if( buffer == null )
			expandBuffer(); // fill it
		
		// reduce its size if unnecessary big (due to a previous large read)
		if( buffer.length > DEFAULT_BUFFER_SIZE ) {
			buffer = Arrays.copyOfRange(buffer, buffer.length-DEFAULT_BUFFER_SIZE, buffer.length);
			buffer_pos = buffer_pos % DEFAULT_BUFFER_SIZE;
		}
		
		// Note: this might make the buffer grow.
		
		// Using a constant size buffer and constructing the string line chunk after chunk might sound easier.
		// However, this would be problematic due to multibyte characters spanning consecutive buffers.
		int start = buffer_pos;
		while(true) {
			if( buffer_pos >= buffer.length ) {
				if( isEOF() )
					break;
				else
					expandBuffer();
			}
			if(buffer[buffer_pos] == '\n') {
				buffer_pos++; // read it anyway
				break;
			}
			buffer_pos++;
		}
		int len = buffer_pos-start-1; // -1 to ignore the line ending itself
		String line = new String(buffer, start, len, StandardCharsets.UTF_8);
		return line;
	}
	
	
	private void expandBuffer() throws IOException {
		assert !isEOF();
		assert buffer == null || buffer_pos <= buffer.length;
		if( buffer == null ) {
			// create it
			buffer_pos = 0;
			buffer = new byte[DEFAULT_BUFFER_SIZE];
		}
		if( buffer_pos == buffer.length ) {
			// expand it
			buffer = Arrays.copyOf(buffer, buffer.length + DEFAULT_BUFFER_SIZE);
		}
		// read last chunk
		int len = read(buffer, buffer_pos, DEFAULT_BUFFER_SIZE);
		
		// if necessary, trim the end
		if(len < DEFAULT_BUFFER_SIZE) {
			buffer = Arrays.copyOf(buffer, buffer.length - DEFAULT_BUFFER_SIZE + len);
			eof = true;
		}
		
		//System.out.println("New buffer size: " + buffer.length);
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		BufferedRandomAccessFile2 bf = new BufferedRandomAccessFile2("this-is-a-test.db", "rw");
		//BufferedReader bf = new BufferedReader(new InputStreamReader(new BufferedRandomAccessFile("this-is-a-test.db", "r")));
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
		System.out.println("\ncount: " + count / 1000);
		System.out.println("ms: " + (System.currentTimeMillis() - start));
	}

	@Override
	public int read() throws IOException {
		return raf.read();
	}
	
	@Override
	public int read(byte b[], int off, int len) throws IOException {
		return raf.read(b, off, len);
	}
}
