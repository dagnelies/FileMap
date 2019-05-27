package com.github.dagnelies.filemap;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
/*
# tagging old entries is mostly useful if table is scanned
for write intensive workloads, it is less interesting
metadata can enhance:
	parsing speed (String length)
	timestamp (for concurrency)
	versionning (with pointers to prior)

 ...it is possible to store {"t":timestamp,"p":offset,"v":value} to implement the feature
at the cost of slightly more bytes per entry
*/
public class IndexedFileMap<K,V>  extends AbstractFileMap<K,V> {

	private Map<K,Long> offsets;
	
	public IndexedFileMap(File file, Class<K> keyType, Class<V> valueType) throws IOException {
		super(file, keyType, valueType);
	}
	
	@Override
	protected void init() throws IOException {
		offsets = new HashMap<>();
	}
	
	@Override
	protected void loadEntry(long offset, String line) throws IOException {
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

	public Iterable<LineEntry> entries() {
		return new Iterable<LineEntry>() {

			@Override
			public Iterator<LineEntry> iterator() {
				try {
					fileio.seek(0);
				} catch (IOException e1) {
					throw new RuntimeException(e1);
				}
				return new Iterator<LineEntry>() {

					@Override
					public boolean hasNext() {
						return !fileio.isEOF();
					}

					@Override
					public LineEntry next() {
						try {
							while(!fileio.isEOF()) {
								long offset = fileio.pos();			
								
								String line = fileio.readLine();
								
								if( line == null ||  line.isEmpty() || line.startsWith("#") )
									continue;
								
								K key = parseKey(line);
								if(offsets.get(key) != offset)
									continue; // obsolete entry
								
								return new LineEntry(line); // could be slightly improved since key is parsed twice
							}
							return null; // EOF
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
				};
			}
			
		};
	}
}
