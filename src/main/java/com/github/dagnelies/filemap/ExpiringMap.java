package com.github.dagnelies.filemap;


import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ExpiringMap<K,V> implements Map<K,V>, Runnable {

	Map<K,V> internal = new HashMap<>();
	Map<K,Long> timestamps = new HashMap<>();

	long timeToLive;
	long cleanupDelay;
	
	public ExpiringMap() {
		this(1000*60);
	}
	
	public ExpiringMap(long timeToLive) {
		this(timeToLive, timeToLive / 10);
	}

	public ExpiringMap(long timeToLive, long cleanupDelay) {
		if( timeToLive < 10 )
			throw new IllegalArgumentException("Time-to-live should be >= 10");
		if( cleanupDelay <= 0 || cleanupDelay >= timeToLive)
			throw new IllegalArgumentException("Cleanup delay should be > 0, and smaller than the time-to-live.");
		
		this.timeToLive = timeToLive;
		this.cleanupDelay = cleanupDelay;
		
		Thread cleanupThread = new Thread(this);
		cleanupThread.start();
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
		if( internal.containsKey(key) )
			timestamps.put((K) key, System.currentTimeMillis());
		return internal.get(key);
	}

	@Override
	public synchronized V put(K key, V value) {
		timestamps.put(key, System.currentTimeMillis());
		return internal.put(key, value);
	}

	@Override
	public synchronized V remove(Object key) {
		timestamps.remove(key);
		return internal.remove(key);
	}

	@Override
	public synchronized void putAll(Map<? extends K, ? extends V> m) {
		for( K key : m.keySet() )
			timestamps.put(key, System.currentTimeMillis());
		internal.putAll(m);
	}

	@Override
	public synchronized void clear() {
		timestamps.clear();
		internal.clear();
	}

	@Override
	public synchronized Set<K> keySet() {
		// this does not update timestamps
		return internal.keySet();
	}

	private void updateAllTimestamps() {
		long now = System.currentTimeMillis();
		for( K key : timestamps.keySet() )
			timestamps.put(key, now);
	}
	
	@Override
	public synchronized Collection<V> values() {
		updateAllTimestamps();
		return internal.values();
	}

	@Override
	public synchronized Set<Entry<K, V>> entrySet() {
		updateAllTimestamps();
		return internal.entrySet();
	}

	
	@Override
	public void run() {
		while(true) {
			cleanup();	
			try {
				Thread.sleep(cleanupDelay);
			} catch (InterruptedException e) {
				// nothing to do
			}
		}
	}
	
	
	public synchronized void cleanup() {
		if( isEmpty() )
			return; //early check
		
		long now = System.currentTimeMillis();
		Iterator<Entry<K, Long>> iter = timestamps.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<K,Long> entry = iter.next();
			if(entry.getValue() + timeToLive < now) {
				internal.remove(entry.getKey());
				iter.remove();
			}
		}
	}
}
