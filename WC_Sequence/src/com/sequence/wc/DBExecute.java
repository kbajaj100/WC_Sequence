package com.sequence.wc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public class DBExecute {
	
	private String SQL = "";
	private int max;
	private DBConn myconn = new DBConn();
	
	public int getmax() throws FileNotFoundException, IOException, SQLException {
		// TODO Auto-generated method stub
		
		initiateDBConn();
		
		SQL = "select count(distinct Claim_ID) count from rcmods.claims_physician";
		
		max = myconn.execSQL_returnint(SQL);
		
		return max;
	}

	private void initiateDBConn() throws FileNotFoundException, IOException, SQLException {
		// TODO Auto-generated method stub

		myconn.setDBConn("C:/Props/WC_Sequence/DBprops.properties");
		
	}
	
	

}
