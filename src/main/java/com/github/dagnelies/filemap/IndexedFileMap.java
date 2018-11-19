package com.github.dagnelies.filemap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This thread safe hash map stores its key/values on disk.
 * Only the keys are are in memory, along with the value's position in the file.
 * This allows to store much more data than would normally fit in memory.
 * Each insertion/update/removal is saved to the file in a readable JSON format.
 * Note that the file works like a log and old entries are not "removed".
 * They will only be overridden. This means that the file only grows.
 *  
 * 
 * @author dagnelies
 *
 * @param <K>
 * @param <V>
 */
public class IndexedFileMap<K,V>  implements FileMap<K,V> {

	Map<K,Long> offsets = new HashMap<>();
	
	BufferedRandomAccessFile file;
	String MODE = "rw";
	long operationsCount = 0;
	
	Class<? extends K> keyClass;
	Class<? extends V> valueClass;
	
	static ObjectMapper mapper = new ObjectMapper();
	
	public IndexedFileMap(String filename, Class<? extends K> keyClass, Class<? extends V> valueClass) throws IOException {
		file = new BufferedRandomAccessFile(filename, MODE);
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		
		int count = 0;
		while(!file.isEOF()) {
			long offset = file.pos();			
			
			String line = file.readLine();
			if( line == null ||  line.isEmpty() || line.startsWith("#") )
				continue;
			K key = parseKey(line);
			
			/*
			// not really better
			byte[] keyBuffer = file.readUntil((byte) '\t');
			file.skipUntil((byte) '\n');
			K key = parseKey(keyBuffer);
			*/
			offsets.put(key, offset);
			
			count++;
		}
		
		System.out.println("Size: " + offsets.size());
		System.out.println("Lines: " + count);
	}
	
	private Entry<K, V> parseLine(String line) throws IOException {
		int i = line.indexOf('\t');
		if( i <= 0 ) {
			throw new IOException("Failed to parse line: " + line);
		}
		String keyJson = line.substring(0, i);
		String valueJson = line.substring(i+1);
		K key = mapper.readValue(keyJson, keyClass);
		V value = mapper.readValue(valueJson, valueClass);
		
		return new AbstractMap.SimpleEntry<K,V>(key, value);
	}
	
	
	private K parseKey(byte[] buf) throws IOException {
		K key = mapper.readValue(buf, keyClass);
		return key;
	}
	
	private K parseKey(String line) throws IOException {
		int i = line.indexOf('\t');
		if( i <= 0 ) {
			throw new IOException("Failed to parse line: " + line);
		}
		String keyJson = line.substring(0, i);
		K key = mapper.readValue(keyJson, keyClass);
		return key;
	}
	
	private V parseValue(String line) throws IOException {
		int i = line.indexOf('\t');
		if( i <= 0 ) {
			throw new IOException("Failed to parse line: " + line);
		}
		String valueJson = line.substring(i+1);
		V value = mapper.readValue(valueJson, valueClass);
		
		return value;
	}
	
	@Override
	public synchronized int size() {
		return offsets.size();
	}

	@Override
	public synchronized boolean isEmpty() {
		return offsets.isEmpty();
	}

	@Override
	public synchronized boolean containsKey(Object key) {
		return offsets.containsKey(key);
	}

	@Override
	public synchronized boolean containsValue(Object value) {
		throw new RuntimeException("This operation is not supported for this kind of map.");
	}

	@Override
	public synchronized V get(Object key) {
		if( !offsets.containsKey(key) )
			return null;
		long offset = offsets.get(key);
		try {
			file.seek(offset);
			String line = file.readLine();
			V value = parseValue(line);
			return value;
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized V put(K key, V value) {
		long offset = save(key, value);
		offsets.put(key, offset);
		return null;
		
	}

	@Override
	public synchronized V remove(Object key) {
		save((K) key, null);
		offsets.remove(key);
		return null;
	}
	
	
	private long save(K key, V value) {
		try {
			operationsCount++;
			
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
	
	/*
	// not really better
	private long save(K key, V value) {
		try {
			operationsCount++;
			
			byte[] keyJson = mapper.writeValueAsBytes(key);
			byte[] valueJson = mapper.writeValueAsBytes(value);
			//String line = keyJson + "\t" + valueJson + "\n";
			byte[] buf = new byte[keyJson.length + 1 + valueJson.length + 1];
			System.arraycopy(keyJson, 0, buf, 0, keyJson.length);
			buf[keyJson.length] = (byte) '\t';
			System.arraycopy(valueJson, 0, buf, keyJson.length+1, valueJson.length);
			buf[keyJson.length + 1 + valueJson.length] = (byte) '\n';
			
			long offset = file.length();
			file.seek(offset);
			file.write( buf );
			return offset;
		} catch (IOException e) {
			throw new RuntimeException("Failed to save entry for " + key, e);
		}
	}
	*/
	
	@Override
	public synchronized void putAll(Map<? extends K, ? extends V> m) {
		for( Entry<? extends K, ? extends V> entry : m.entrySet() ) {
			long offset = save(entry.getKey(), entry.getValue());
			offsets.put(entry.getKey(), offset);
		}
		
	}

	@Override
	public synchronized void clear() {
		try {
			file.seek(0);
			file.truncate(0);
		} catch (IOException e) {
			throw new RuntimeException("Failed to clear persistent map", e);
		}
		offsets.clear();
	}

	@Override
	public synchronized Set<K> keySet() {
		return offsets.keySet();
	}


	@Override
	public synchronized Collection<V> values() {
		throw new RuntimeException("This operation is not supported for this kind of map.");
	}

	@Override
	public synchronized Set<Entry<K, V>> entrySet() {
		throw new RuntimeException("This operation is not supported for this kind of map.");
	}
	
	/**
	 * Returns an estimate of the file's content overhead, of the ratio: obsolete data / used data.
	 * When entries are frequently updated and removed, the old entries are still stored in the file.
	 * For example, an overhead of 3 would mean that roughly 3/4 of the file is filled with obsolete content.
	 *  
	 * @return
	 */
	public synchronized double getFileOverhead() {
		long obsoleteOps = operationsCount - this.size();
		return 1.0 * obsoleteOps / this.size();
	}

	@Override
	public long diskSize() {
		return file.length();
	}

	@Override
	public void close() throws IOException {
		file.close();
	}
	
}
