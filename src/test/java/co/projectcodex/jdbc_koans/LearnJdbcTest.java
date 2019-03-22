package co.projectcodex.jdbc_koans;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class LearnJdbcTest {

    final String KOANS_DATABASE_URL = "jdbc:h2:./target/jdbc_koans_db";

    @BeforeEach
    public void cleanUpTables() {

        try {
            try(Connection conn = DriverManager.getConnection(KOANS_DATABASE_URL)) {
                // delete fruits that the tests are adding

                Statement statement = conn.createStatement();
                statement.addBatch("delete from fruit where name in ('Guava', 'Orange')");
                statement.addBatch("update fruit set price = 4.75  where name = 'red apple'");
                statement.executeBatch();

                //
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
            // to fix this set the JDBC_URL to a valid value of `jdbc:h2:./target/jdbc_koans_db` - it will create an
            // embedded database in the target folder
            final String JDBC_URL = "";
            Connection conn = DriverManager.getConnection(JDBC_URL);


        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void executeSQLStatement() {

        try {

            Connection conn = DriverManager.getConnection(KOANS_DATABASE_URL, "sa", "");
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

            Connection conn = DriverManager.getConnection(KOANS_DATABASE_URL, "sa", "");
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery ("select count(*) as fruit_count from fruit");

            if (rs.next()) {
                // mmm... how many rows was actually added in the V2__add_fruit.sql migration file
                assertEquals(3, rs.getInt("fruit_count"));
            }

            // add a V2__add_fruit.sql file in the src/main/db/migration folder
            // add a create table script in there to create a fruit table

            /*
                insert into fruit (name, price) values ('red apple', 4.75);
                insert into fruit (name, price) values ('green apple', 2.64);
                insert into fruit (name, price) values ('banana', 3.15);
                insert into fruit (name, price) values ('lemon', 5.75);
             */

            // run the migration using the in your project root folder using:
            // mvn flyway:migrate

        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    public void addDataFruitUsingPreparedStatement() {

        try {

            Connection conn = DriverManager.getConnection(KOANS_DATABASE_URL, "sa", "");
            final String INSERT_FRUIT_SQL = "insert into fruit (name, price) values (?, ?)";
            final String FIND_FRUIT_SQL = "select name, price from fruit where name = ?";

            // PreparedStatement are SQL statements that can be called
            // over and over with different parameters
            PreparedStatement addFruitPreparedStatement = conn.prepareStatement(INSERT_FRUIT_SQL);

            // use it to add 2 new fruits an Orange costing 2.37 and a Guava costing 4.13

            // todo - add Orange
            addFruitPreparedStatement.setString(1, "__");
            addFruitPreparedStatement.setDouble(2, 0.00);
            addFruitPreparedStatement.execute();

            // todo - add a Guava below costing 4.13
            // todo - add the appropriate prepared statement below

            ResultSet rs = conn.createStatement().executeQuery("select * as the_count from fruit where name in ('Guava', 'Orange')");

            int counter = 0;
            while(rs.next()) {
                counter++;
                if (counter == 1) {
                    assertEquals(2.37, rs.getDouble("price"));
                }
                else if ( counter == 2) {
                    // what is the correct price for a Guava
                    assertEquals(0.00, rs.getDouble("price"));
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

            Connection conn = DriverManager.getConnection(KOANS_DATABASE_URL, "sa", "");
            final String FIND_FRUT_SQL_4 = "select name, price from fruit where price > ? order by id asc";

            // PreparedStatement are SQL statements that can be called
            // over and over with different parameters
            PreparedStatement findFruitPreparedStatement = conn.prepareStatement(FIND_FRUT_SQL_4);

            // use it to add 2 new fruits an Orange costing 2.37 and a Guava costing 4.13

            // why is this failing ?
            ResultSet rs = findFruitPreparedStatement.executeQuery();


            int counter = 0;
            while(rs.next()) {
                counter++;
                if (counter == 1) {
                    assertEquals("rad apple", rs.getString("name"));
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

            Connection conn = DriverManager.getConnection(KOANS_DATABASE_URL, "sa", "");
            final String FIND_FRUIT_BY_NAME_SQL = "select price from fruit where name = ? order by id asc";
            final String UPDATE_FRUIT_BY_NAME_SQL = "update fruit set price = ? where name = ?";

            PreparedStatement updateFruitPreparedStatement = conn.prepareStatement(UPDATE_FRUIT_BY_NAME_SQL);

            // todo - set the params on the findFruitPreparedStatement and run the query;
            // update the price to 5.99 ...
            // use it to add 2 new fruits an Orange costing 2.37 and a Guava costing 4.13

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
