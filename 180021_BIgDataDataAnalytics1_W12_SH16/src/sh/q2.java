package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
//Write a HiveQL using Java API for loading data into Olympic Table from the given the data set file. 
public class q2 {
	
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

		String loadData = "load data local inpath '/home/cloudera/Downloads/olympic_data.csv' overwrite into table olympic";
		try {
			statement.execute(loadData);
			System.out.println("Data added into olympic table successfully.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}
		statement.close();
		connection.close();
	}
}
