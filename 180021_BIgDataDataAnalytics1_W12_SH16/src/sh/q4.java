package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
//Write a HiveQL using Java API to create a bucket table by country and load data into partition table from Olympic Table 
public class q4 {

	private static String driverClass = "org.apache.hive.jdbc.HiveDriver";
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException exception) {
			System.out.println(exception.toString());
			System.exit(1);
		}
		Connection connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");

		Statement statement = connection.createStatement();
	
				String Bucket = "create table countryByBucket ("
						+ "athlete_name string, age int, country string, year int, closing_date string, sport string, gold int, silver int, bronze int, total int"
						+ ") clustered by (country) into 10 buckets"
						+ " row format delimited" + " fields terminated by ','";
				try {
					statement.execute(Bucket);
					System.out.println("Created bucket table.");
				} catch (SQLException ex) {
					System.out.println(ex.toString());
				}
				
				try {
					statement.execute("insert overwrite table countryByBucket select * from olympic");
					System.out.println("Data loaded successfully");
				} catch (SQLException ex) {
					System.out.println(ex.toString());
				}
				statement.close();
				connection.close();
			}
		}