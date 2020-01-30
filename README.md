FileMap
=======

FileMap provides Java Maps, backed by a file on disk.

It offers high performance, is crash safe and stores its data in a readable JSON related format.

Two kind of maps are available:

- CachedFileMap: this map caches everything in memory and on disk, allowing optimal access performance.

- IndexedFileMap: this map stores only the keys in memory while the values are stored on disk only. This allows to handle very large map that would usually not fit in memory, at the cost of disk IO.

Usage
-----

	Map<String, String> myMap;
	myMap = new CachedFileMap("mydir/somefile.db", String.class, String.class); // or IndexedFileMap
	// ...
	myMap.put("hello", "persistence!");
	String value = myMap.get("hello");
	// ...
	myMap.close();
	

Benchmarks
----------

Typically, you can read/write many thousands of items per second on commodity hardware.


IOExceptions
------------
This library also faces a dilemma. Obviously, since the maps are backed by a file, IOExceptions can occur for all Map operations. On the other hand, throwing IOExceptions would make it incompatible with the Map interface. As a compromise, this library decided to wrap these IOExceptions inside RuntimeExceptions to keep interface compatibility.


How is it persisted?
--------------------
Every time an entry is added/updated/removed from the map, a line is added to the file, like:

	"my-key-encoded-as-json"	{"some":"serialized object","as":"json","isOneLiner":true}

A line consists of the key and value formatted as JSON, and separated by a tab character.
Since both tabs and new lines are escaped by JSON, these characters can be used as separators.
