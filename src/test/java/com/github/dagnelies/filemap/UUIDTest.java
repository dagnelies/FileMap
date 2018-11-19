package com.github.dagnelies.filemap;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class UUIDTest {

	public static void main(String[] args) {
		Map<String, Boolean> uuids = new HashMap<>();
		int count = 0;
		while(true) {
			String uuid = UUID.randomUUID().toString();
			if( uuids.containsKey(uuid) ) {
				count++;
				System.out.println("UUID collision found. " + count + " in " + uuids.size());
			}
			uuids.put(uuid, true);
			if( uuids.size() % 100000 == 0 ) {
				System.out.println(uuids.size());
			}
		}
	}
}
