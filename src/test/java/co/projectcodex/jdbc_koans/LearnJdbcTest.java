package co.projectcodex.jdbc_koans;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class LearnJdbcTest {

    final String KOANS_DATABASE_URL = "jdbc:h2:./target/jdbc_koans_db";

    public Connection getConnection() throws Exception {
        // TODO - add a username of "sa" and a blank password ""
        // TODO - if the db is created via default flyway config the username will be "sa" with a blank password
        // you can change this by removing the user element containing sa in the pom.xml file
        // if not be sure to delete the *.db files in your target folder before running mvn flyway:migrate the first time
        // and be sure the set the username to "sa" password blank ""
        // if your remove the user element from the pom.xml file you are use a username of "" and a password of ""
        Connection conn = DriverManager.getConnection(KOANS_DATABASE_URL, "sa", "");
        return conn;
    }

    @BeforeEach
    public void cleanUpTables() {
        // don't touch any code in here!!!
        try {
            try(Connection conn = getConnection()) {
                // I repeat don't touch any code in here!!!
                Statement statement = conn.createStatement();
                statement.addBatch("delete from fruit where name in ('Guava', 'Orange')");
                statement.addBatch("update fruit set price = 4.75  where name = 'red apple'");
                statement.executeBatch();
                // I repeat once again don't touch any code in here!!!
            }
        } catch(Exception ex) {
            System.out.println("These test will fail until the fruit table is created: " + ex);
        }
    }

    @Test
    public void loadJdbcDriver() {

        try {
            /*
                To fix this error add a dependency for the H2 database driver
                As per this link: http://www.h2database.com/html/build.html#maven2
            */
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            fail(e);
        }
    }

    @Test
    public void connectToDatabase() {

        try {
            Class.forName("org.h2.Driver");
            // to fix this set the KOANS_DATABASE_URL to a valid value of `jdbc:h2:./target/jdbc_koans_db` - it will create an
            // embedded database in the target folder
            Connection conn = DriverManager.getConnection(KOANS_DATABASE_URL, "sa", "");
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void executeSQLStatement() {

        try {

            //TODO - ensure you fixed the username & password in executeSQLStatement
            Connection conn = getConnection();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery ("select * from fruit");

            // add the flyway plugin to you maven configuration in pom.xml
            // use the correct database name - jdbc_koans_db in the flyway maven config

            // https://flywaydb.org/getstarted/firststeps/maven

            // add a V1__create_fruit.sql file in the src/main/resources/db/migration folder
            // add a create table script in there to create a fruit table

            /*
                create table fruit (
                    id INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                    name varchar(40),
                    price double
                );
             */

            // run the migration using the in your project root folder using:
            // mvn flyway:migrate

        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void addDataToFruitTableViaMigration() {

        try {

            Connection conn = getConnection();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery ("select count(*) as fruit_count from fruit");

            if (rs.next()) {
                // mmm... how many rows was actually added in the V2__add_fruit.sql migration file
                assertEquals(4, rs.getInt("fruit_count"));
            }

            // todo - add a V2__add_fruit.sql file in the src/main/db/migration folder
            // todo - add a create table script in there to create a fruit table

            /*
                insert into fruit (name, price) values ('red apple', 4.75);
                insert into fruit (name, price) values ('green apple', 2.64);
                insert into fruit (name, price) values ('banana', 3.15);
                insert into fruit (name, price) values ('lemon', 5.75);
             */

            // todo - run the migration using the in your project root folder using:
            // mvn flyway:migrate

        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void addDataFruitUsingPreparedStatement() {

        try {

            Connection conn = getConnection();
            final String INSERT_FRUIT_SQL = "insert into fruit (name, price) values (?, ?)";
            final String FIND_FRUIT_SQL = "select name, price from fruit where name = ?";

            // PreparedStatement are SQL statements that can be called
            // over and over with different parameters
            PreparedStatement addFruitPreparedStatement = conn.prepareStatement(INSERT_FRUIT_SQL);

            // use it to add 2 new fruits an Orange costing 2.37 and a Guava costing 4.13

            // todo - add Orange
            addFruitPreparedStatement.setString(1, "Orange");
            addFruitPreparedStatement.setDouble(2, 2.37);
            addFruitPreparedStatement.execute();

            // todo - add a Guava below costing 4.13
            // todo - add the appropriate prepared statement below
            addFruitPreparedStatement.setString(1, "Guava");
            addFruitPreparedStatement.setDouble(2, 4.13);
            addFruitPreparedStatement.execute();

            ResultSet rs = conn.createStatement().executeQuery("select * from fruit where name in ('Guava', 'Orange')");

            int counter = 0;
            while(rs.next()) {
                counter++;
                if (counter == 1) {
                    assertEquals(2.37, rs.getDouble("price"));
                }
                else if ( counter == 2) {
                    // what is the correct price for a Guava
                    assertEquals(4.13, rs.getDouble("price"));
                }
            }
            assertEquals(2, counter);

        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void findFruitOver4() {

        try {

            Connection conn = getConnection();
            final String FIND_FRUIT_SQL = "select name, price from fruit where price > ? order by id asc";

            // PreparedStatement are SQL statements that can be called
            // over and over with different parameters
            PreparedStatement findFruitPreparedStatement = conn.prepareStatement(FIND_FRUIT_SQL);

            // todo - why is this failing?
            // todo - tip what parameter needs to set on the PreparedStatement be added here?
            findFruitPreparedStatement.setDouble(1, 4.00);
            ResultSet rs = findFruitPreparedStatement.executeQuery();
            int counter = 0;
            while(rs.next()) {
                counter++;
                if (counter == 1) {
                    assertEquals("red apple", rs.getString("name"));
                    assertEquals(4.75, rs.getDouble("price"));
                }
                else if ( counter == 2) {
                    // what is the correct price for a Guava
                    assertEquals("lemon", rs.getString("name"));
                    assertEquals(5.75, rs.getDouble("price"));
                }
            }
            assertEquals(2, counter);

        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void updateRedApplePrice() {

        try {

            Connection conn = getConnection();
            final String FIND_FRUIT_BY_NAME_SQL = "select price from fruit where name = ? order by id asc";
            final String UPDATE_FRUIT_BY_NAME_SQL = "update fruit set price = ? where name = ?";

            PreparedStatement updateFruitPreparedStatement = conn.prepareStatement(UPDATE_FRUIT_BY_NAME_SQL);
            // don't change anything above this line

            // todo - use the updateFruitPreparedStatement to update the apple price to 5.99 ...
            // todo - use the updateFruitPreparedStatement here
            updateFruitPreparedStatement.setDouble(1,5.99);
            updateFruitPreparedStatement.setString(2,"red apple");
            updateFruitPreparedStatement.execute();
            // don't change any code below this line
            PreparedStatement findFruitPreparedStatement = conn.prepareStatement(FIND_FRUIT_BY_NAME_SQL);
            findFruitPreparedStatement.setString(1, "red apple");
            ResultSet rs = findFruitPreparedStatement.executeQuery();

            if (rs.next()) {
                assertEquals(5.99, rs.getDouble("price"));
            } else {
                fail("Should find the red apple in the database");
            }

        } catch (Exception e) {
            fail(e);
        }
    }

}
