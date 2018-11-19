package com.github.dagnelies.filemap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This thread safe hash map is stored both in memory and on disk.
 * Each insertion/update/removal is saved to the file in a readable JSON format.
 * This offers high throughput, together with crash-safe file persistence. 
 * Note that the file works like a log and old entries are not "removed".
 * They will only be overridden. This means that the file only grows.
 * 
 * @author dagnelies
 *
 * @param <K>
 * @param <V>
 */
public class CachedFileMap<K,V>  implements FileMap<K,V> {

	Map<K,V> internal = new HashMap<>();
	Path path;
	RandomAccessFile file;
	String MODE = "rw";
	long operationsCount = 0;
	
	static ObjectMapper mapper = new ObjectMapper();
	
	public CachedFileMap(String filename, Class<? extends K> kkk, Class<? extends V> vvv) throws IOException {
		path = Paths.get(filename);
		file = new RandomAccessFile(filename, MODE);
		if( file.length() > 0 ) {
			BufferedReader reader = Files.newBufferedReader(path , StandardCharsets.UTF_8);
			while(true) {
				String line = reader.readLine();
				if( line == null )
					break; // end of file
				if( line.isEmpty() )
					continue;
				int i = line.indexOf('\t');
				if( i <= 0 ) {
					System.err.println("Failed to parse line, skipping " + line);
					continue;
				}
				String keyJson = line.substring(0, i);
				String valueJson = line.substring(i+1);
				K key = mapper.readValue(keyJson, kkk);
				V value = mapper.readValue(valueJson, vvv);
				
				internal.put(key, value);
			}
			reader.close();
		}
	}
	
	
	
	@Override
	public synchronized int size() {
		return internal.size();
	}

	@Override
	public synchronized boolean isEmpty() {
		return internal.isEmpty();
	}

	@Override
	public synchronized boolean containsKey(Object key) {
		return internal.containsKey(key);
	}

	@Override
	public synchronized boolean containsValue(Object value) {
		return internal.containsValue(value);
	}

	@Override
	public synchronized V get(Object key) {
		return internal.get(key);
	}

	@Override
	public synchronized V put(K key, V value) {
		save(key, value, true);
		return internal.put(key, value);
	}

	@Override
	public synchronized V remove(Object key) {
		save((K) key, null, true);
		return internal.remove(key);
	}

	private void save(K key, V value, boolean flush) {
		try {
			operationsCount++;
			String keyJson = mapper.writeValueAsString(key);
			String valueJson = mapper.writeValueAsString(value);
			String line = keyJson + "\t" + valueJson + "\n";
			//writer.write(line);
			file.write( line.getBytes(StandardCharsets.UTF_8) );
			if( flush ) {
			//	writer.flush();
			//	writer.getFD().sync();
			}
			//writer.write( ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
		} catch (IOException e) {
			throw new RuntimeException("Failed to save entry for " + key, e);
		}
	}
	
	
	@Override
	public synchronized void putAll(Map<? extends K, ? extends V> m) {
		for( Entry<? extends K, ? extends V> entry : m.entrySet() )
			save(entry.getKey(), entry.getValue(), false);
		/*
		try {
			writer.flush();
		} catch (IOException e) {
			throw new RuntimeException("Failed to save batch entries", e);
		}
		*/
		internal.putAll(m);
	}

	@Override
	public synchronized void clear() {
		try {
			file.seek(0);
			file.setLength(0);
		} catch (IOException e) {
			throw new RuntimeException("Failed to clear persistent map", e);
		}
		internal.clear();
	}

	@Override
	public synchronized Set<K> keySet() {
		return internal.keySet();
	}


	@Override
	public synchronized Collection<V> values() {
		return internal.values();
	}

	@Override
	public synchronized Set<Entry<K, V>> entrySet() {
		return internal.entrySet();
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
	public long diskSize() throws IOException {
		return file.length();
	}



	@Override
	public void close() throws IOException {
		file.close();
	}
	
}
