FileMap
=======

This implements a basic key-value store which is highly efficient, crash safe, and storing its data in a file in a JSON related format.

Every time an entry is added/updated/removed from the map, a line is added to the file.
A line consists of the key and value formatted as JSON, and separated by a tab character.
Since both tabs and new lines are escaped by JSON, these characters can be used as separators.
