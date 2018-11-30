package com.github.dagnelies.filemap;

public interface Formatter<T> {
	T parse(String line);
	String encode(String line);
}
