package com.github.dagnelies.filemap;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public interface FileMap<K, V> extends Map<K, V> {

	public File getFile();
	
	public long diskSize() throws IOException;
	
	public void close() throws IOException;
}
