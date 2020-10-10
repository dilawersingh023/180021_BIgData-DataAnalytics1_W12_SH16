package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
//Write a HiveQL using Java API to find out total number of medals won by each country.
public class q5 {

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

		String query = "select country, sum(total) from olympic"
				+ " group by country";

		ResultSet result = null;
		try {
			result = statement.executeQuery(query);
			ResultSetMetaData meta = result.getMetaData();

			int numCols = meta.getColumnCount();

			System.out.println("Total medals won by each country");
			System.out.println("Country, Num of Medals");
			while (result.next()) {
				for (int i = 1; i <= numCols; i++) {
					if (i != numCols)
						System.out.print(result.getString(i) + ", ");
					else
						System.out.print(result.getString(i) + "\n");
				}
			}
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		statement.close();
		connection.close();
	}
}