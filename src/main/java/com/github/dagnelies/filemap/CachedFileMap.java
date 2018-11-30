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
public class CachedFileMap<K,V> extends AbstractFileMap<K,V> {

	private Map<K,V> internal;
	
	public CachedFileMap(String path) throws IOException {
		super(path);
	}
	
	@Override
	protected void init() throws IOException {
		internal = new HashMap<>();
	}
	
	@Override
	protected void firstLoad(long offset, String line) throws IOException {
		Entry<K, V> entry = parseLine(line);
		internal.put(entry.getKey(), entry.getValue());
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
		writeLine(key, value);
		return internal.put(key, value);
	}

	@Override
	public synchronized V remove(Object key) {
		writeLine((K) key, null);
		return internal.remove(key);
	}

	@Override
	public synchronized void clear() {
		super.clearLines();
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

}
