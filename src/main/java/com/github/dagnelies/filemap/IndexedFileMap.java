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
public class IndexedFileMap<K,V>  extends AbstractFileMap<K,V> {

	private Map<K,Long> offsets;
	
	public IndexedFileMap(String path) throws IOException {
		super(path);
	}
	
	@Override
	protected void init() {
		offsets = new HashMap<>();
	}
	
	@Override
	protected void firstLoad(long offset, String line) throws IOException {
		K key = parseKey(line);
		offsets.put(key, offset);
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
			String line = super.readLine(offset);
			V value = parseValue(line);
			return value;
		}
		catch(IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized V put(K key, V value) {
		long offset = writeLine(key, value);
		offsets.put(key, offset);
		return value;
	}

	@Override
	public synchronized V remove(Object key) {
		writeLine((K) key, null);
		offsets.remove(key);
		return null;
	}
	
	
	@Override
	public synchronized void clear() {
		super.clearLines();
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


	
}
