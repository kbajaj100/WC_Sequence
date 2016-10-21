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
	private int Num_WC_Codes;
	private int Num_NonWC_Codes;
	
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
			  ", Marked = " + Marked + " " +
			  ", Num_Codes = " + Num_NonWC_Codes + " " +
			  ", Num_WC_Codes = " + Num_WC_Codes + " " +
			  "where Claim_ID = " + claim_id;
		
		System.out.println(SQL);
		
		myconn.execSQL(SQL);
		
		/*SQL = "update " + Table_Write + " " + 
			  "set Sequence = '" + Sequence + "' " + 
			  "where Claim_ID = " + claim_id;
		
		
		SQL = "update " + Table_Write + " " + 
				  "set Marked = " + Marked + " " + 
				  "where Claim_ID = " + claim_id;
			
		myconn.execSQL(SQL);
		*/
		
	}

	private void getDXSequence(int claim_id) {
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
		
		Sequence = "";
		Marked = 0;
		Num_WC_Codes = 0;
		
		SQL = "select a11.DX " + 
			  "from " + 
			  "( " + 
			  "select DX1 DX " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " +  claim_id + " " +
			  "and i1.DX1 not like 'NULL' and i1.DX1 not like ' ' " + 
			  "and " +
			  "(i1.DX1 like 'L%' or i1.DX1 like 'M%') " + 
			  "union " + 
			  "select DX2 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +  
			  "and i1.DX2 not like 'NULL' and i1.DX2 not like ' ' " +
			  "and " +  
			  "(i1.DX2 like 'L%' or i1.DX2 like 'M%') " + 
			  "union " + 
			  "select DX3 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +
			  "and i1.DX3 not like 'NULL' and i1.DX3 not like ' ' " +
			  "and " +  
			  "(i1.DX3 like 'L%' or i1.DX3 like 'M%') " +
			  "union " + 
			  "select DX4 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +
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
	    		
	    		++Num_WC_Codes;
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
		Num_NonWC_Codes = 0;
		
		SQL = "select a11.DX " + 
			  "from " + 
			  "( " + 
			  "select DX1 DX " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " +  claim_id + " " + 
			  "and i1.DX1 not like 'NULL' and i1.DX1 not like ' ' " +
			  "and " +
			  "(i1.DX1 not like 'L%' and i1.DX1 not like 'M%') " + 
			  "union " + 
			  "select DX2 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +  
			  "and i1.DX2 not like 'NULL' and i1.DX2 not like ' ' " + 
			  "and " +  
			  "(i1.DX2 not like 'L%' and i1.DX2 not like 'M%') " + 
			  "union " + 
			  "select DX3 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +
			  "and i1.DX3 not like 'NULL' and i1.DX3 not like ' ' " +
			  "and " +  
			  "(i1.DX3 not like 'L%' and i1.DX3 not like 'M%') " +
			  "union " + 
			  "select DX4 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +
			  "and i1.DX4 not like 'NULL' and i1.DX4 not like ' ' " +
			  "and " +
			  "(i1.DX4 not like 'L%' and i1.DX4 not like 'M%') " + 
			  ") a11 " + 
			  "order by a11.DX";
		
		System.out.println(SQL);
		
		if(!myconn.execSQL_crs(SQL))
			System.exit(1);
		
	    try {

	    	CachedRowSetImpl crs = new CachedRowSetImpl();
	    	crs = myconn.getRowSet();

	    	while (crs.next()) {
	    		++Num_NonWC_Codes;
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
		
		int claim_id;
		int count_claims;
		
		Table_Write = "rcmods.claims_physician_sequence_wc ";
		Table_Read =  "rcmods.claims_physician_sequence ";
		
		SQL = "select count(distinct Claim_ID) count " +
				  "from " + Table_Read + 
				  "where Marked = 1 and Num_Codes > 0";

		count_claims = myconn.execSQL_returnint(SQL);
		System.out.println(count_claims);
		
		SQL = "select distinct Claim_ID, Sequence, Num_Codes, Num_WC_Codes " +
			  "from " + Table_Read + 
			  "where Marked = 1 and Num_Codes > 0";
	
		System.out.println(SQL);

		if(!myconn.execSQL_crs(SQL))
			System.exit(1);

	    try {

	    	CachedRowSetImpl crs1 = new CachedRowSetImpl();
	    	crs1 = myconn.getRowSet();

	    	while (crs1.next()) {
	    		
				claim_id = crs1.getInt(1);
	    		Sequence = crs1.getString(2);
				Num_NonWC_Codes = crs1.getInt(3);
				Num_WC_Codes = crs1.getInt(4);
						
	    		SplitSequence(claim_id);
				
	    	}
	    } catch (SQLException se){
	    	se.printStackTrace();
	    }
		
	}

	private void SplitSequence(int claim_id) {
		// TODO Auto-generated method stub
		
		Table_Read = "rcmods.claims_physician ";
		getDXSequence_NonWC(claim_id);
		
		System.out.println(Sequence_nonWC);
			
		String Code;
		
		SQL = "select a11.DX " + 
			  "from " + 
			  "( " + 
			  "select DX1 DX " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " +  claim_id + " " +
			  "and i1.DX1 not like 'NULL' and i1.DX1 not like ' ' " + 
			  "and " +
			  "(i1.DX1 like 'L%' or i1.DX1 like 'M%') " + 
			  "union " + 
			  "select DX2 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " + 
			  "and i1.DX2 not like 'NULL' and i1.DX2 not like ' ' " +
			  "and " +  
			  "(i1.DX2 like 'L%' or i1.DX2 like 'M%') " + 
			  "union " + 
			  "select DX3 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +
			  "and i1.DX3 not like 'NULL' and i1.DX3 not like ' ' " +
			  "and " +  
			  "(i1.DX3 like 'L%' or i1.DX3 like 'M%') " +
			  "union " + 
			  "select DX4 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +
			  "and i1.DX4 not like 'NULL' and i1.DX4 not like ' ' " + 
			  "and " +
			  "(i1.DX4 like 'L%' or i1.DX4 like 'M%') " + 
			  ") a11 " + 
			  "order by a11.DX";
		
		System.out.println(SQL);
		if(!myconn.execSQL_crs(SQL))
			System.exit(1);
			
		    try {

		    	CachedRowSetImpl crs2 = new CachedRowSetImpl();
		    	crs2 = myconn.getRowSet();

		    	while (crs2.next()) {
		    		
		    		Code = crs2.getString(1);
		    		
		    		Sequence = Code + "->" + Sequence_nonWC;
		    		
		    		SQL = "insert into " + Table_Write + " " + 
		    			  "(Claim_ID, Sequence) " + 
		    			  "values(" + claim_id + ", '" + Sequence + "')";
		    		
		    		System.out.println(SQL);
		    		
		    		myconn.execSQL(SQL);
		    	}
		    } catch (SQLException se){
		    	se.printStackTrace();
		    }	
			
	}

	public void createDXGroup() {
		
		setDXGroup2();
		setDXGroup3();
		setDXGroup4();
		setDXGroup7();

		
		
	}

	
	private void getDXSequence_Grp(int claim_id) {
		
		Sequence = "";
		Marked = 0;
		Num_WC_Codes = 0;
		
		SQL = "select a11.DX " + 
			  "from " + 
			  "( " + 
			  "select DX1 DX " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " +  claim_id + " " +
			  "and i1.DX1 not like 'NULL' and i1.DX1 not like ' ' " + 
			  "and " +
			  "(i1.DX1 like 'L%' or i1.DX1 like 'M%') " + 
			  "union " + 
			  "select DX2 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +  
			  "and i1.DX2 not like 'NULL' and i1.DX2 not like ' ' " +
			  "and " +  
			  "(i1.DX2 like 'L%' or i1.DX2 like 'M%') " + 
			  "union " + 
			  "select DX3 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +
			  "and i1.DX3 not like 'NULL' and i1.DX3 not like ' ' " +
			  "and " +  
			  "(i1.DX3 like 'L%' or i1.DX3 like 'M%') " +
			  "union " + 
			  "select DX4 " + 
			  "from  " + Table_Read + " i1 " + 
			  "where i1.Claim_ID = " + claim_id + " " +
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
	    		
	    		++Num_WC_Codes;
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
	
	private void setDXGroup7() {

		SQL = "update rcmods.claims_physician set DX1_Grp  = DX1 where DX1 like 'L%' or  DX1 like 'M%' or DX1 like 'Q%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX2_Grp  = DX2 where DX2 like 'L%' or  DX2 like 'M%' or DX2 like 'Q%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX3_Grp  = DX3 where DX3 like 'L%' or  DX3 like 'M%' or DX3 like 'Q%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX4_Grp  = DX4 where DX4 like 'L%' or  DX4 like 'M%' or DX4 like 'Q%'";
		myconn.execSQL(SQL);
		
	}

	private void setDXGroup4() {

		SQL = "update rcmods.claims_physician set DX1_Grp  = SUBSTRING(DX1,1,4) where DX1 like 'G%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX2_Grp  = SUBSTRING(DX2,1,4) where DX2 like 'G%'";
		myconn.execSQL(SQL);		
		
		SQL = "update rcmods.claims_physician set DX3_Grp  = SUBSTRING(DX3,1,4) where DX3 like 'G%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX4_Grp  = SUBSTRING(DX4,1,4) where DX4 like 'G%'";
		myconn.execSQL(SQL);		
		
	}

	private void setDXGroup3() {
		// TODO Auto-generated method stub

		SQL = "update rcmods.claims_physician set DX1_Grp  = SUBSTRING(DX1,1,3) where DX1 like 'D5%' or DX1 like 'D6%' or DX1 like 'D7%' or DX1 like 'D8%' or DX1 like 'E%' or DX1 like 'H6%' or DX1 like 'H7%' or DX1 like 'H8%' or DX1 like 'H90%' or DX1 like 'H91%' or DX1 like 'H92%' or DX1 like 'H93%' or DX1 like 'H94%' or DX1 like 'H95%' or DX1 like 'I%' or DX1 like 'J%' or DX1 like 'K%' or DX1 like 'N%' or DX1 like 'O%' or DX1 like 'P%' or DX1 like 'Z%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX2_Grp  = SUBSTRING(DX2,1,3) where DX2 like 'D5%' or DX2 like 'D6%' or DX2 like 'D7%' or DX2 like 'D8%' or DX2 like 'E%' or DX2 like 'H6%' or DX2 like 'H7%' or DX2 like 'H8%' or DX2 like 'H90%' or DX2 like 'H91%' or DX2 like 'H92%' or DX2 like 'H93%' or DX2 like 'H94%' or DX2 like 'H95%' or DX2 like 'I%' or DX2 like 'J%' or DX2 like 'K%' or DX2 like 'N%' or DX2 like 'O%' or DX2 like 'P%' or DX2 like 'Z%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX3_Grp  = SUBSTRING(DX3,1,3) where DX3 like 'D5%' or DX3 like 'D6%' or DX3 like 'D7%' or DX3 like 'D8%' or DX3 like 'E%' or DX3 like 'H6%' or DX3 like 'H7%' or DX3 like 'H8%' or DX3 like 'H90%' or DX3 like 'H91%' or DX3 like 'H92%' or DX3 like 'H93%' or DX3 like 'H94%' or DX3 like 'H95%' or DX3 like 'I%' or DX3 like 'J%' or DX3 like 'K%' or DX3 like 'N%' or DX3 like 'O%' or DX3 like 'P%' or DX3 like 'Z%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX4_Grp  = SUBSTRING(DX4,1,3) where DX4 like 'D5%' or DX4 like 'D6%' or DX4 like 'D7%' or DX4 like 'D8%' or DX4 like 'E%' or DX4 like 'H6%' or DX4 like 'H7%' or DX4 like 'H8%' or DX4 like 'H90%' or DX4 like 'H91%' or DX4 like 'H92%' or DX4 like 'H93%' or DX4 like 'H94%' or DX4 like 'H95%' or DX4 like 'I%' or DX4 like 'J%' or DX4 like 'K%' or DX4 like 'N%' or DX4 like 'O%' or DX4 like 'P%' or DX4 like 'Z%'";
		myconn.execSQL(SQL);

	}

	private void setDXGroup2() {

		// Group of 2s
		
		SQL = "update rcmods.claims_physician set DX1_Grp  = SUBSTRING(DX1,1,2) where DX1 like 'A%' or DX1 like 'B%' or DX1 like 'C%' or DX1 like 'D4%' or DX1 like 'H0%' or DX1 like 'H1%' or DX1 like 'H2%' or DX1 like 'H3%' or DX1 like 'H4%' or DX1 like 'H5%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX2_Grp  = SUBSTRING(DX2,1,2) where DX2 like 'A%' or DX2 like 'B%' or DX2 like 'C%' or DX2 like 'D4%' or DX2 like 'H0%' or DX2 like 'H1%' or DX2 like 'H2%' or DX2 like 'H3%' or DX2 like 'H4%' or DX2 like 'H5%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX3_Grp  = SUBSTRING(DX3,1,2) where DX3 like 'A%' or DX3 like 'B%' or DX3 like 'C%' or DX3 like 'D4%' or DX3 like 'H0%' or DX3 like 'H1%' or DX3 like 'H2%' or DX3 like 'H3%' or DX3 like 'H4%' or DX3 like 'H5%'";
		myconn.execSQL(SQL);
		
		SQL = "update rcmods.claims_physician set DX4_Grp  = SUBSTRING(DX4,1,2) where DX4 like 'A%' or DX4 like 'B%' or DX4 like 'C%' or DX4 like 'D4%' or DX4 like 'H0%' or DX4 like 'H1%' or DX4 like 'H2%' or DX4 like 'H3%' or DX4 like 'H4%' or DX4 like 'H5%'";
		myconn.execSQL(SQL);
		
	}

	
	
}
	

