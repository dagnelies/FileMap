package com.github.dagnelies.filemap;

import java.io.IOException;
import java.util.Map;

public interface FileMap<K, V> extends Map<K, V> {

	public long diskSize() throws IOException;
	
	public void close() throws IOException;
}
