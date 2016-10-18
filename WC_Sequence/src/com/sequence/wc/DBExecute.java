package com.sequence.wc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

import com.sun.rowset.CachedRowSetImpl;

public class DBExecute {
	
	private String SQL = "";
	private int max ;
	private DBConn myconn = new DBConn();
	private String Sequence;
	private String Sequence_nonWC;
	private String Table_Write = "";
	private String Table_Read = "";
	private int Marked = 0;
	
	public int getmax() throws FileNotFoundException, IOException, SQLException {
		// TODO Auto-generated method stub
		
		initiateDBConn();
		
		Table_Read = "rcmods.claims_physician";
		
		SQL = "select count(distinct Claim_ID) count from " + Table_Read;
		
		max = myconn.execSQL_returnint(SQL);
		
		return max;
	}

	private void initiateDBConn() throws FileNotFoundException, IOException, SQLException {
		// TODO Auto-generated method stub

		myconn.setDBConn("C:/Props/WC_Sequence/DBprops.properties");
		
	}


	public void createclaimlist() {
		// TODO Auto-generated method stub
		
		Table_Write = "rcmods.claims_physician_sequence";
		
		SQL = "insert into  " + Table_Write + " " + 
			  "(Claim_ID, Marked) " + 
			  "select distinct Claim_ID, 0 " + 
			  "from " + Table_Read;
		
		myconn.execSQL(SQL);
		
	}

	public void createDXlist(int max2) {
		// TODO Auto-generated method stub
		
		max = max2;
		
		int claim_id = 0;
		
		for (int i = 1; i <=max; ++i){
			
			System.out.println("TableID: " + i);
			
			SQL = "select Claim_ID count " +
				  "from  " + Table_Write + " " + 
				  "where Table_ID = " + i;
			
			claim_id = myconn.execSQL_returnint(SQL);
			
			System.out.println("Claim: " + claim_id);
			
			getDXSequence(claim_id);
			
			insertSequence(claim_id);
		}
		
	}

	private void insertSequence(int claim_id) {
			
		SQL = "update " + Table_Write + " " + 
			  "set Sequence = '" + Sequence + "' " + 
			  "where Claim_ID = " + claim_id;
		
		System.out.println(SQL);
		
		myconn.execSQL(SQL);
		
		SQL = "update " + Table_Write + " " + 
				  "set Marked = " + Marked + " " + 
				  "where Claim_ID = " + claim_id;
			
		myconn.execSQL(SQL);
		
	}

	private void getDXSequence(int claim_id) {
		// TODO Auto-generated method stub
		
		/*
		 * Get the union of the list of DX from Dx1-4 in alphabetical order for L and M codes
		 * Get the union of the list of DX from Dx1-4 in alphabetical order for non-L and M codes
		 * Create the sequence 
		*/

		getDXSequence_WC(claim_id);
		getDXSequence_NonWC(claim_id);
		
		
		if ((Sequence_nonWC.length() > 0) && (Sequence.length() > 0))
			Sequence = Sequence + "->" + Sequence_nonWC;
		else if ((Sequence_nonWC.length() > 0) && (Sequence.length() == 0))
			Sequence = Sequence_nonWC;
		
	}

	private void getDXSequence_WC(int claim_id) {
		// TODO Auto-generated method stub

		Sequence = "";
		Marked = 0;
		
		SQL = "select a11.DX " + 
			  "from " + 
			  "( " + 
			  "select DX1 DX " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " +  claim_id + 
			  "and i1.DX1 not like 'NULL' and i1.DX1 not like ' ' " + 
			  "and " +
			  "(i1.DX1 like 'L%' or i1.DX1 like 'M%') " + 
			  "union " + 
			  "select DX2 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id +  
			  "and i1.DX2 not like 'NULL' and i1.DX2 not like ' ' " +
			  "and " +  
			  "(i1.DX2 like 'L%' or i1.DX2 like 'M%') " + 
			  "union " + 
			  "select DX3 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id +
			  "and i1.DX3 not like 'NULL' and i1.DX3 not like ' ' " +
			  "and " +  
			  "(i1.DX3 like 'L%' or i1.DX3 like 'M%') " +
			  "union " + 
			  "select DX4 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id +
			  "and i1.DX4 not like 'NULL' and i1.DX4 not like ' ' " + 
			  "and " +
			  "(i1.DX4 like 'L%' or i1.DX4 like 'M%') " + 
			  ") a11 " + 
			  "order by a11.DX";
		
		if(!myconn.execSQL_crs(SQL))
			System.exit(1);
		
	    try {

	    	CachedRowSetImpl crs = new CachedRowSetImpl();
	    	crs = myconn.getRowSet();

	    	while (crs.next()) {
	    		if (Sequence.length() == 0)
	    				Sequence = crs.getString(1);
	    		else 
	    		{
	    			Sequence = Sequence + "->" + crs.getString(1);
	    			Marked = 1;
	    		}
	    		
	    		System.out.println("Sequence: " + Sequence);
    	
	    	}
	    } catch (SQLException se){
	    	se.printStackTrace();
	    }
		
	}

	private void getDXSequence_NonWC(int claim_id) {
		// TODO Auto-generated method stub
		
		Sequence_nonWC = "";
		
		SQL = "select a11.DX " + 
			  "from " + 
			  "( " + 
			  "select DX1 DX " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " +  claim_id + 
			  "and i1.DX1 not like 'NULL' and i1.DX1 not like ' ' " +
			  "and " +
			  "(i1.DX1 not like 'L%' and i1.DX1 not like 'M%') " + 
			  "union " + 
			  "select DX2 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id +  
			  "and i1.DX2 not like 'NULL' and i1.DX2 not like ' ' " + 
			  "and " +  
			  "(i1.DX2 not like 'L%' and i1.DX2 not like 'M%') " + 
			  "union " + 
			  "select DX3 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id +
			  "and i1.DX3 not like 'NULL' and i1.DX3 not like ' ' " +
			  "and " +  
			  "(i1.DX3 not like 'L%' and i1.DX3 not like 'M%') " +
			  "union " + 
			  "select DX4 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id +
			  "and i1.DX4 not like 'NULL' and i1.DX4 not like ' ' " +
			  "and " +
			  "(i1.DX4 not like 'L%' and i1.DX4 not like 'M%') " + 
			  ") a11 " + 
			  "order by a11.DX";
		
		if(!myconn.execSQL_crs(SQL))
			System.exit(1);
		
	    try {

	    	CachedRowSetImpl crs = new CachedRowSetImpl();
	    	crs = myconn.getRowSet();

	    	while (crs.next()) {
	    		if (Sequence_nonWC.length() == 0)
	    				Sequence_nonWC = crs.getString(1);
	    		else 
	    			Sequence_nonWC = Sequence_nonWC + "->" + crs.getString(1);
	    		
	    		System.out.println("Sequence_nonWC: " + Sequence_nonWC);
    	
	    	}
	    } catch (SQLException se){
	    	se.printStackTrace();
	    }
		
	}

	public void destroyclaimlist() {
		// TODO Auto-generated method stub
		
		SQL = "truncate table  " + Table_Write + " " ; 
		
		myconn.execSQL(SQL);
	}

	public void markedClaims() {
		// TODO Auto-generated method stub
		
		
		
	}
	
}
