package com.github.dagnelies.filemap;

public interface ExpiredEntryListener<K,V> {
	
	public void onExpired(K key, V value);

}
