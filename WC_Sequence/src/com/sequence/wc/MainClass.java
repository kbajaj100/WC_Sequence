package com.sequence.wc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public class MainClass {

	public static void main(String[] args) throws FileNotFoundException, IOException, SQLException {
		// TODO Auto-generated method stub

		/*
		 * Get the Claim IDs in a table with Table ID identity
		 * For each Claim ID, get the distinct list of DXs
		 * Identify if there are any L or Ms
		 * Arrange the rest alphabetically
		 */
		
		int max;
		
		DBExecute myDB = new DBExecute();

		max = myDB.getmax();
	
		//myDB.createclaimlist();
		
		//myDB.createDXlist(max);
		
		System.out.println(max);
		
		myDB.markedClaims();
		
		//myDB.destroyclaimlist();
	}

}
