/**
* This Java program exemplifies the basic usage of JDBC.
* Requirements:
* (1) JDK 8.0+
* (2) SQLite3.
* (3) SQLite3 JDBC jar (https://bitbucket.org/xerial/sqlitejdbc/downloads/sqlite-jdbc-3.8.7.jar).
*/
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
public class PA2 {
 public static void main(String[] args) {
 Connection conn = null; // Database connection.
 try {
 // Load the JDBC class.
 Class.forName("org.sqlite.JDBC");
 // Get the connection to the database.
 // - "jdbc" : JDBC connection name prefix.
 // - "sqlite" : The concrete database implementation
 // (e.g., sqlserver, postgresql).
 // - "pa2.db" : The name of the database. In this project,
 // we use a local database named "pa2.db". This can also
 // be a remote database name.
 conn = DriverManager.getConnection("jdbc:sqlite:test_filtered.db");
 System.out.println("Opened database successfully.");
 // Use case #1: Create and populate a table.
 // Get a Statement object.
 Statement stmt = conn.createStatement();
 stmt.executeUpdate("DROP TABLE IF EXISTS Connected;");
 // Student table is being created just as an example. You
 // do not need Student table in PA2
 stmt.executeUpdate(
 "CREATE TABLE Connected(Airline char(32), Origin char(32), Destination char(32), Stops int);");
 // Edges in graph are 0 stops away
 stmt.executeUpdate(
 "INSERT INTO Connected SELECT Airline, Origin, Destination, 0 FROM Flights;");
 // This will store new connections found at n + 1 step
 stmt.executeUpdate(
 "CREATE TABLE newConnections(Airline char(32), Origin char(32), Destination char(32));");
 stmt.executeUpdate(
 "INSERT INTO newConnections SELECT * FROM Flights;");
 // This will store connections from nth step
 stmt.executeUpdate(
 "CREATE TABLE oldConnections(Airline char(32), Origin char(32), Destination char(32));");
 stmt.executeUpdate(
 "INSERT INTO oldConnections SELECT * FROM Flights;");
 // This will store the difference between nth step and n + 1 step
 stmt.executeUpdate(
 "CREATE TABLE delta(Airline char(32), Origin char(32), Destination char(32));");
 stmt.executeUpdate(
 "INSERT INTO delta SELECT * FROM Flights;");
 int counter = 1;
 ResultSet deltaSize = stmt.executeQuery("SELECT 1 AS numEntries FROM Flights;");
 System.out.println("Starting loop.");
while(deltaSize.getInt("numEntries") > 0) {
    System.out.println(String.format("Checking for entries where Stops = %d", counter));
    // Set oldConnections to current newConnections
    //stmt.executeUpdate(
    //"DELETE FROM oldConnections;");
    System.out.println("Inserting into oldConnections from newConnections");
    stmt.executeUpdate(
    "INSERT INTO oldConnections SELECT * FROM newConnections;");
    //stmt.executeUpdate(
    //"DELETE FROM newConnections;");
    System.out.println("Updating newConnections");
    stmt.executeUpdate(
    "INSERT INTO newConnections SELECT * FROM " +
         "(SELECT * FROM oldConnections " +
         "UNION " +
         "SELECT x.Airline, y.Origin, x.Destination " +
         "FROM Flights x, delta y " +
         "WHERE x.Airline = y.Airline AND x.Origin = y.Destination);");
    stmt.executeUpdate(
    "DELETE FROM delta;");
    System.out.println("Updating delta");
    stmt.executeUpdate(
    "INSERT INTO delta SELECT * FROM newConnections a WHERE a.Origin <> a.Destination AND NOT EXISTS " +
    "(SELECT * FROM oldConnections WHERE a.Airline = Airline AND a.Origin = Origin AND a.Destination = Destination);");
    System.out.println("Inserting into Connected");
    stmt.executeUpdate(
    String.format("INSERT INTO Connected SELECT Airline, Origin, Destination, %d FROM delta;", counter));
    deltaSize = stmt.executeQuery("SELECT COUNT(*) AS numEntries FROM delta;");
    counter = counter + 1;
}
  stmt.executeUpdate(
 "DROP TABLE oldConnections;");
stmt.executeUpdate(
 "DROP TABLE newConnections;");
stmt.executeUpdate(
 "DROP TABLE delta;");


 

 // Use case #2: Query the Student table with Statement.
 // Returned query results are stored in a ResultSet
 // object.
// ResultSet rset = stmt.executeQuery("SELECT * from Student;");
 // Print the FirstName and LastName columns.
// System.out.println ("\nStatement result:");
 // This shows how to traverse the ResultSet object.
// while (rset.next()) {
 // Get the attribute value.
// System.out.print(rset.getString("FirstName"));
// System.out.print("---");
// System.out.println(rset.getString("LastName"));
// }
 // Use case #3: Query the Student table with
 // PreparedStatement (having wildcards).
// PreparedStatement pstmt = conn.prepareStatement(
//"SELECT * FROM Student WHERE FirstName = ?;");
 // Assign actual value to the wildcard.
// pstmt.setString (1, "F1");
// rset = pstmt.executeQuery ();
// System.out.println ("\nPrepared statement result:");
// while (rset.next()) {
// System.out.print(rset.getString("FirstName"));
// System.out.print("---");
// System.out.println(rset.getString("LastName"));
// }
 // Close the ResultSet and Statement objects.
 deltaSize.close();
 stmt.close();
 } catch (Exception e) {
 throw new RuntimeException("There was a runtime problem!", e);
 } finally {
 try {
 if (conn != null) conn.close();
 } catch (SQLException e) {
 throw new RuntimeException(
"Cannot close the connection!", e);
 }
 }
 }
}
