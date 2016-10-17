package com.sequence.wc;


import java.util.Properties;
import javax.sql.rowset.CachedRowSet;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import javax.sql.RowSet;
import com.sun.rowset.CachedRowSetImpl;

public class DBConn {

	private String dbName = "";
	private String user = "";
	private String pass = "";
	private String dbUrl = "";

	//private String SQL = ""; 
	
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs= null;
	private CachedRowSetImpl crs;
	
	
	public void setDBConn(String PropFileLocation) throws FileNotFoundException, IOException, SQLException{
					
			//1. Load the properties file
			Properties props = new Properties();
			props.load(new FileInputStream(PropFileLocation)); 
			
			//2. Read the props
			user = props.getProperty("user");
			pass = props.getProperty("password");
			dbName = props.getProperty("dbName");
			dbUrl = props.getProperty("dbUrl") +  ";databaseName="+ dbName + ";user=" + user + ";password=" + pass;
			//System.out.println(dbUrl);
			
	}

	public String getdbName(){
		return dbName;
	}
	
	public String getuser(){
		return user;
	}
	
	public String getdbUrl(){
		//System.out.println(dbUrl);
		return dbUrl;
	}
	
	public int execSQL_returnint(String SQL){
		
		int count = 0;
		
		try {
			//Step 1. Connection to the db
			conn = DriverManager.getConnection(dbUrl);
		
			// Create statement object
			stmt = conn.createStatement();
		
			// 3. Execute SQL query
			rs = stmt.executeQuery(SQL);
			
			//4. Process result set
			while (rs.next()){
				
				count = rs.getInt("count");
				//System.out.println(count);
				return count;
			}
		}
		
		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		}
		
		finally {
			//close(myConn, myStmt, myRS);
			if (rs   != null) try { rs.close();   } catch(Exception e) {}
			if (stmt != null) try { stmt.close(); } catch(Exception e) {}
			if (conn != null) try { conn.close(); } catch(Exception e) {}
		}
		
		return count;
	}

	public String execSQL_returnString(String SQL)
	{
		String code = "";
		
		try {
			//Step 1. Connection to the db
			conn = DriverManager.getConnection(dbUrl);
		
			// Create statement object
			stmt = conn.createStatement();
		
			// 3. Execute SQL query
			rs = stmt.executeQuery(SQL);
			
			//4. Process result set
			while (rs.next()){
				
				code = rs.getString("code");
				//System.out.println(count);
				return code;
			}
		}
		
		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		}
		
		finally {
			//close(myConn, myStmt, myRS);
			if (rs   != null) try { rs.close();   } catch(Exception e) {}
			if (stmt != null) try { stmt.close(); } catch(Exception e) {}
			if (conn != null) try { conn.close(); } catch(Exception e) {}
		}
		
		return code;
		
	}

	public int execSQL(String SQL){
		
		try {
			//Step 1. Connection to the db
			conn = DriverManager.getConnection(dbUrl);
		
			// Create statement object
			stmt = conn.createStatement();
		
			// 3. Execute SQL query
			stmt.executeUpdate(SQL);
			
		}
		
		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}

	public boolean execSQL_crs(String SQL)
	{
		
		try {
			//Step 1. Connection to the db
			conn = DriverManager.getConnection(dbUrl);
		
			// Create statement object
			stmt = conn.createStatement();
		
			// 3. Execute SQL query
			rs = stmt.executeQuery(SQL);
			
			// create CachedRowSet and populate
	        crs = new CachedRowSetImpl();
	        crs.populate(rs);
		}
		
		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		}
		
		finally {
			//close(myConn, myStmt, myRS);
			if (rs   != null) try { rs.close();   } catch(Exception e) {}
			if (stmt != null) try { stmt.close(); } catch(Exception e) {}
			if (conn != null) try { conn.close(); } catch(Exception e) {}
		}
		
		return true;
		
	}
	
	public CachedRowSetImpl getRowSet() {
	      return crs;
	}
	
}

