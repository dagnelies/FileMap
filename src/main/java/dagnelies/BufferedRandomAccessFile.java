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

public class BufferedRandomAccessFile extends InputStream {

	static final int DEFAULT_BUFFER_SIZE = 8192;
	
	// keep track of this separately since RandomAccessFile.getFilePointer() is an expensive operation
	long file_pos = 0; 
	
	// keep track of this separately since RandomAccessFile.length() is an expensive operation
	long length = 0; 
	
	// the buffer used when reading
	byte[] buffer;
	
	// the position inside the buffer
	int buffer_pos = 0;
	
	// the underlying file
	RandomAccessFile raf;
	
	
	public BufferedRandomAccessFile(String filename, String mode) throws IOException {
		raf = new RandomAccessFile(filename, mode);
		length = raf.length();
	}
	
	
	void clearBuffer() {
		buffer = null;
		buffer_pos = 0;
	}
	
	public void write(byte[] data) throws IOException {
		clearBuffer();
		raf.write(data);
		file_pos += data.length;
		if( length < file_pos )
			length = file_pos;
	}
	
	public void write(byte b) throws IOException {
		clearBuffer();
		raf.write(b);
		file_pos += 1;
		if( length < file_pos )
			length = file_pos;
	}
	
	public void close() throws IOException {
		clearBuffer();
		raf.close();
	}
	
	public void truncate(long len) throws IOException {
		clearBuffer();
		raf.setLength(len);
		length = len;
		if( file_pos > len )
			file_pos = len;
	}
	
	public void seek(long pos) throws IOException {
		clearBuffer();
		if( pos != file_pos ) {
			raf.seek(pos);
			file_pos = pos;
		}
	}
	
	public long pos() throws IOException {
		if( buffer_pos > 0 )
			return file_pos - buffer.length + buffer_pos;
		else
			return file_pos;
	}
	
	public long length() throws IOException {
		return length;
	}
	
	
	public boolean isEOF() throws IOException {
		return pos() >= length();
	}
	
	public byte[] readUntil(byte delimiter) throws IOException {
		if( isEOF() )
			return null;
		
		if( buffer == null || buffer_pos == buffer.length ) {
			fillBuffer(); // fill it
		}
		else if( buffer.length > DEFAULT_BUFFER_SIZE ) {
			// reduce its size if unnecessary big (due to a previous large read)
			buffer = Arrays.copyOfRange(buffer, buffer_pos, buffer.length); // removes everything before buffer_pos
			buffer_pos = 0;
		}
		
		assert buffer.length > 0;
		assert buffer_pos < buffer.length;
		
		// Note: this might make the buffer grow.
		// Using a constant size buffer and constructing the string line chunk after chunk might sound easier.
		// However, this would be problematic due to multibyte characters spanning consecutive buffers.
		int start = buffer_pos;
		while(buffer[buffer_pos] != delimiter) {
			buffer_pos++;
			if( buffer_pos == buffer.length ) {
				if( isEOF() )
					break;
				else
					expandBuffer();
			}
		}
		assert isEOF() || buffer[buffer_pos] == delimiter;
		
		byte[] result = Arrays.copyOfRange(buffer, start, buffer_pos);
		
		if( !isEOF() )
			buffer_pos++; // "consume" the new line character
		
		return result;
	}
	
	/**
	 * Skip all bytes until delimiter(inclusive) is encountered.
	 * 
	 * @param delimiter
	 * @throws IOException
	 */
	public void skipUntil(byte delimiter) throws IOException {
		if( isEOF() )
			return;
		
		if( buffer == null || buffer_pos == buffer.length )
			fillBuffer(); // fill it
		
		while(buffer[buffer_pos] != delimiter) {
			buffer_pos++;
			if( buffer_pos == buffer.length ) {
				if( isEOF() )
					return;
				else
					fillBuffer();
			}
		}
		assert buffer[buffer_pos] == delimiter;
		buffer_pos++; // read the character
	}
	
	/**
	 * Reads the next line, interpreted as UTF-8, excluding the newline character.
	 */
	public String readLine() throws IOException {
		byte[] bytes = readUntil((byte) '\n');
		if( isEOF() )
			return null;
		String line = new String(bytes, StandardCharsets.UTF_8);
		return line;
	}
	
	// due to huge reads, the buffer might be quite big
	// this trims the part of the buffer before buffer_pos
	private void reduceBuffer() {
		buffer = Arrays.copyOfRange(buffer, buffer_pos, buffer.length);
		buffer_pos = 0;
	}
	
	/**
	 * Reads the next chunk of file and append it to the buffer.
	 */
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
		}
		
		//System.out.println("New buffer size: " + buffer.length);
	}
	
	/**
	 * Reads the next chunk of file into the buffer.
	 */
	private void fillBuffer() throws IOException {
		assert !isEOF();
		assert buffer == null || buffer_pos == buffer.length;
		
		// reset buffer 
		buffer_pos = 0;
		if( buffer == null || buffer.length != DEFAULT_BUFFER_SIZE)
			buffer = new byte[DEFAULT_BUFFER_SIZE];
		
		// read last chunk
		int len = read(buffer, 0, DEFAULT_BUFFER_SIZE);
		
		// if necessary, trim the end
		if(len < DEFAULT_BUFFER_SIZE) {
			buffer = Arrays.copyOf(buffer, len);
		}
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		BufferedRandomAccessFile bf = new BufferedRandomAccessFile("this-is-a-test.db", "rw");
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
		/*
		 * while(!bf.isEOF()) {
			bf.skipUntil((byte) '\n');
		 */
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
		System.out.println("\ncount: " + count / 1000 + "k");
		System.out.println("ms: " + (System.currentTimeMillis() - start));
	}

	@Override
	public int read() throws IOException {
		file_pos++;
		return raf.read();
	}
	
	@Override
	public int read(byte b[], int off, int len) throws IOException {
		file_pos += len;
		return raf.read(b, off, len);
	}
}
