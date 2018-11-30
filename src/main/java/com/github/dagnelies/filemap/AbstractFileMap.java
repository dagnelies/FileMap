package com.github.dagnelies.filemap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This abstract class is a utility to easely manipulate lines in a random access file and converting
 * text lines into key/value format and vice-versa.
 * 
 * @author dagnelies
 *
 * @param <K>
 * @param <V>
 */
public abstract class AbstractFileMap<K,V>  implements FileMap<K,V> {

	private BufferedRandomAccessFile file;
	private static final String MODE = "rw";
	private long entriesWritten = 0;
	
	protected TypeReference<K> keyType;
	protected TypeReference<V> valueType;
	
	static ObjectMapper mapper = new ObjectMapper();
	
	public AbstractFileMap(String path) throws IOException {
		file = new BufferedRandomAccessFile(path, MODE);
		this.keyType = new TypeReference<K>(){};
		this.valueType = new TypeReference<V>(){};
		
		init();
		
		while(!file.isEOF()) {
			long offset = file.pos();			
			
			String line = file.readLine();
			if( line == null ||  line.isEmpty() || line.startsWith("#") )
				continue;
			
			firstLoad(offset, line);
			entriesWritten++;
		}
	}
	
	protected long getEntriesWritten() {
		return entriesWritten;
	}
	
	protected abstract void init() throws IOException;
	
	protected abstract void firstLoad(long offset, String line) throws IOException;
	
	
	protected Entry<K, V> parseLine(String line) throws IOException {
		int i = line.indexOf('\t');
		if( i <= 0 ) {
			throw new IOException("Failed to parse line: " + line);
		}
		String keyJson = line.substring(0, i);
		String valueJson = line.substring(i+1);
		K key = mapper.readValue(keyJson, keyType);
		V value = mapper.readValue(valueJson, valueType);
		
		return new AbstractMap.SimpleEntry<K,V>(key, value);
	}
	
	protected K parseKey(String line) throws IOException {
		int i = line.indexOf('\t');
		if( i <= 0 ) {
			throw new IOException("Failed to parse line: " + line);
		}
		String keyJson = line.substring(0, i);
		K key = mapper.readValue(keyJson, keyType);
		return key;
	}
	
	protected V parseValue(String line) throws IOException {
		int i = line.indexOf('\t');
		if( i <= 0 ) {
			throw new IOException("Failed to parse line: " + line);
		}
		String valueJson = line.substring(i+1);
		V value = mapper.readValue(valueJson, valueType);
		
		return value;
	}

	protected String readLine(long offset) throws IOException {
		file.seek(offset);
		return file.readLine();
	}
	
	protected long writeLine(K key, V value) {
		try {
			entriesWritten++;
			
			String keyJson = mapper.writeValueAsString(key);
			String valueJson = mapper.writeValueAsString(value);
			String line = keyJson + "\t" + valueJson + "\n";
			
			long offset = file.length;
			file.seek(offset);
			file.write( line.getBytes(StandardCharsets.UTF_8) );
			return offset;
		} catch (IOException e) {
			throw new RuntimeException("Failed to save entry for " + key, e);
		}
	}
	
	
	@Override
	public synchronized void putAll(Map<? extends K, ? extends V> m) {
		for( Entry<? extends K, ? extends V> entry : m.entrySet() )
			put(entry.getKey(), entry.getValue());
	}
	
	protected synchronized void clearLines() {
		try {
			file.seek(0);
			file.truncate(0);
			entriesWritten = 0;
		} catch (IOException e) {
			throw new RuntimeException("Failed to clear persistent map", e);
		}
	}

	/**
	 * Returns an estimate of the file's content fragmentation. It is the ratio of obsolete data in the file.
	 * When entries are frequently updated and removed, the old entries are still stored in the file.
	 * For example, a fragmentation of 2/3 would mean that roughly 2/3 of the file is filled with obsolete content.
	 *  
	 * @return
	 */
	public double getFragmentation() {
		return 1.0 - ((double) this.size() / entriesWritten);
	}

	
	public long diskSize() {
		return file.length();
	}

	
	public void close() throws IOException {
		file.close();
	}
	
}
