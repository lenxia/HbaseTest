package org.guohd.demo02;

import java.io.IOException;


public class ImportThread extends Thread {
	

	@Override
	public void run() {
		try {
			HbaseUtils.insert(HbaseProps.TABLE_NAME,100000);

		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
