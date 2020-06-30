package jdbc2mssql;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcMsSql {

		public static Connection connObj;
		public static void main(String[] args) {
			try (InputStream input = new FileInputStream("config.properties")){
				Properties prop = new Properties();
				prop.load(input);
				//System.out.println(prop.getProperty("server"));
				
		        String connectionUrl =
		                "jdbc:sqlserver://"
		        				+ prop.getProperty("server") + ":" + prop.getProperty("port") + ";"
		                        + "database=" + prop.getProperty("db") + ";"
		                        + "integratedSecurity=" + prop.getProperty("integrated_security") + ";"
		                        + "user=" + prop.getProperty("user") + ";"
		                        + "password=" + prop.getProperty("pass") + ";"
		                        + "encrypt=" + prop.getProperty("use_encryption") + ";"
		                        + "trustServerCertificate=" + prop.getProperty("trust_server_cert") + ";"
		                        + "loginTimeout=" + prop.getProperty("timeout") + ";";
		        
		        System.out.print("Connection string: ");
		        System.out.println(connectionUrl);
		        try {
		            // Load SQL Server JDBC driver and establish connection.
		            System.out.print("Connecting to SQL Server ... ");
		            try (Connection connection = DriverManager.getConnection(connectionUrl)) {
		                System.out.println("Done.");
		                DatabaseMetaData metaObj = (DatabaseMetaData) connection.getMetaData();
		                System.out.println("Driver Name?= " + metaObj.getDriverName() + ", Driver Version?= " + metaObj.getDriverVersion() + ", Product Name?= " + metaObj.getDatabaseProductName() + ", Product Version?= " + metaObj.getDatabaseProductVersion());
		                
		                // Try to query the database
		                ResultSet resultSet = null;
	                	String selectSql = prop.getProperty("query");
	                    
		                try (Statement statement = connection.createStatement()){
		                	System.out.print("Querrying the database ... ");
		                	resultSet = statement.executeQuery(selectSql);
		                	System.out.println("query complete ... here comes the data...");
		                	ResultSetMetaData resultSetMeta = resultSet.getMetaData();
		                	int columnsNumber = resultSetMeta.getColumnCount();
	
		                    // Print results from select statement
		                    while (resultSet.next()) {
		                    	for (int i = 1; i <= columnsNumber; i++) {
		                            if (i > 1) System.out.print(",  ");
		                            String columnValue = resultSet.getString(i);
		                            System.out.print(columnValue + " " + resultSetMeta.getColumnName(i));
		                        }
		                        System.out.println("");
		                    }
		                }
		                catch (SQLException e ) {
		                	System.out.println("query failed");
		    	            e.printStackTrace();
		                }
		            }
		        } catch (Exception e) {
		            System.out.println("failed");
		            e.printStackTrace();
		        }
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
}
