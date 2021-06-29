import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class IntroductionToDBApps {

    public static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/";
    public static final String DB_NAME = "minions_db";
    public static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    public static Connection connection;

    public static void main(String[] args) throws SQLException, IOException {

        connection = getConnection();

        System.out.println("Enter Exercises Number:");
        int exNumber = Integer.parseInt(reader.readLine());

        switch (exNumber) {
            case 2 -> exercise2();
            case 3 -> exercise3();
            case 4 -> exercise4();
            case 5 -> exercise5();
            case 6 -> exercise6();
            case 7 -> exercise7();
            case 8 -> exercise8();
            case 9 -> exercise9();
        }
    }

    private static void exercise9() throws IOException, SQLException {
        System.out.println("Please, enter minion id: ");
        int minionId = Integer.parseInt(reader.readLine());

        String query = "CALL usp_get_older(?)";

        CallableStatement callableStatement = connection.prepareCall(query);
        callableStatement.setInt(1, minionId);
        callableStatement.execute();
    }

    private static void exercise8() throws IOException, SQLException {
        System.out.println("Please, enter minion IDs: ");
        String[] minionIDs = reader.readLine().split("\\s+");

        for (String minionID : minionIDs) {
            String query = "UPDATE minions SET age = age + 1, name = LOWER(name) WHERE id = ?";
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setInt(1, Integer.parseInt(minionID));

            statement.execute();
        }

        String querySelect = "SELECT name, age FROM minions";
        PreparedStatement statementSelect = connection.prepareStatement(querySelect);

        ResultSet resultSet = statementSelect.executeQuery();
        while (resultSet.next()) {
            System.out.printf("%s %d%n", resultSet.getString("name"), resultSet.getInt("age"));
        }
    }

    private static void exercise7() throws SQLException {
        String query = "SELECT name FROM minions LIMIT 25";
        String reversedQuery = "SELECT name FROM minions ORDER BY id DESC LIMIT 25";

        PreparedStatement statement = connection.prepareStatement(query);
        PreparedStatement reversedStatement = connection.prepareStatement(reversedQuery);

        ResultSet resultSet = statement.executeQuery();
        ResultSet reversedResultSet = reversedStatement.executeQuery();

        while (resultSet.next() && reversedResultSet.next()) {
            System.out.println(resultSet.getString("name"));
            System.out.println(reversedResultSet.getString("name"));
        }
    }

    private static void exercise6() throws IOException, SQLException {
        System.out.println("Please, enter villain id: ");
        int villainId = Integer.parseInt(reader.readLine());

        String villainName = getEntityNameById(villainId, "villains");
        if (villainName == null) {
            System.out.println("No such villain was found");
            return;
        }

        String deleteMappingQuery = "DELETE FROM minions_villains WHERE villain_id = ?";
        PreparedStatement deleteStatement = connection.prepareStatement(deleteMappingQuery);
        deleteStatement.setInt(1, villainId);
        int releasedMinions = deleteStatement.executeUpdate();

        String deleteVillainQuery = "DELETE FROM villains WHERE id = ?";
        PreparedStatement deleteVillainStatement = connection.prepareStatement(deleteVillainQuery);
        deleteVillainStatement.setInt(1, villainId);
        deleteVillainStatement.executeUpdate();

        System.out.printf("%s was deleted%n%d minions released%n", villainName, releasedMinions);
    }

    private static void exercise5() throws IOException, SQLException {
        System.out.println("Enter country name:");
        String countryName = reader.readLine();

        PreparedStatement preparedStatement = connection
                .prepareStatement("UPDATE towns SET name = UPPER(name) WHERE country = ?;");

        preparedStatement.setString(1, countryName);

        int countAffectedTowns = preparedStatement.executeUpdate();
        if (countAffectedTowns == 0) {
            System.out.println("No town names were affected.");
            return;
        }

        System.out.printf("%d town names were affected.%n", countAffectedTowns);

        String selectTownsQuery = "SELECT name FROM towns WHERE country = ?";
        PreparedStatement selectTownsStatement = connection.prepareStatement(selectTownsQuery);
        selectTownsStatement.setString(1, countryName);

        ResultSet townsAffected = selectTownsStatement.executeQuery();

        List<String> townsCombined = new ArrayList<>();
        while (townsAffected.next()) {
            townsCombined.add(townsAffected.getString("name"));
        }
        System.out.println(townsCombined);
    }

    private static void exercise4() throws IOException, SQLException {
        System.out.println("Enter input:");
        String[] minionData = reader.readLine().split(": ")[1].split("\\s+");
        String villainName = reader.readLine().split(": ")[1];
        String minionName = minionData[0];
        int age = Integer.parseInt(minionData[1]);
        String townName = minionData[2];

        int townId = getEntityIdByName(townName, "towns");

        if (townId < 0) {

            String query = "INSERT INTO towns(name) value(?)";
            PreparedStatement statement = connection.prepareStatement(query);

            statement.setString(1, townName);
            statement.execute();

            townId = getEntityIdByName(townName, "towns");
            System.out.printf("Town %s was added to the database.%n", townName);
        }

        int villainId = getEntityIdByName(villainName, "villains");
        if (villainId < 0) {
            String query = "INSERT INTO villains(name, evilness_factor) values(?, 'evil')";
            PreparedStatement statement = connection.prepareStatement(query);

            statement.setString(1, villainName);
            statement.execute();

            villainId = getEntityIdByName(villainName, "villains");
            System.out.printf("Villain %s was added to the database.%n", villainName);
        }

        int minionId = getEntityIdByName(minionName, "minions");
        if (minionId < 0) {
            String query = "INSERT INTO minions(name, age, town_id) values(?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(query);

            statement.setString(1, minionName);
            statement.setInt(2, age);
            statement.setInt(3, townId);
            statement.execute();

            minionId = getEntityIdByName(minionName, "minions");

        }
        String query = "INSERT INTO minions_villains VALUES(?, ?)";
        PreparedStatement statement = connection.prepareStatement(query);

        statement.setInt(1, minionId);
        statement.setInt(2, villainId);
        statement.execute();

        System.out.printf("Successfully added %s to be minion of %s.%n", minionName, villainName);

    }

    private static void exercise3() throws IOException, SQLException {
        System.out.println("Enter villain id:");
        int villainId = Integer.parseInt(reader.readLine());

        String villainName = getEntityNameById(villainId, "villains");

        if (villainName == null) {
            System.out.printf("No villain with ID %d exists in the database.", villainId);
            return;
        }
        System.out.printf("Villain: %s%n", villainName);

        String query = "SELECT m.name, m.age FROM minions m " +
                "JOIN minions_villains mv on m.id = mv.minion_id " +
                "WHERE mv.villain_id = ?";

        PreparedStatement statement = connection.prepareStatement(query);
        statement.setInt(1, villainId);

        ResultSet resultSet = statement.executeQuery();

        int counter = 0;
        while (resultSet.next()) {
            System.out.printf("%d. %s %d%n",
                    ++counter, resultSet.getString("name"), resultSet.getInt("age"));
        }
    }

    private static int getEntityIdByName(String entityName, String tableName) throws SQLException {
        String query = String.format("SELECT id FROM %s WHERE name = ?", tableName);

        PreparedStatement statement = connection.prepareStatement(query);

        statement.setString(1, entityName);
        ResultSet resultSet = statement.executeQuery();

        return resultSet.next() ? resultSet.getInt(1) : -1;

    }

    private static String getEntityNameById(int entityId, String tableName) throws SQLException {
        String query = String.format("SELECT name FROM %s WHERE id = ?", tableName);

        PreparedStatement statement = connection.prepareStatement(query);

        statement.setInt(1, entityId);

        ResultSet resultSet = statement.executeQuery();

        return resultSet.next() ? resultSet.getString("name") : null;
    }

    private static void exercise2() throws SQLException {
        PreparedStatement preparedStatement = connection
                .prepareStatement("SELECT v.name, COUNT(DISTINCT mv.minion_id) AS m_count " +
                        "from villains v " +
                        "JOIN minions_villains mv on v.id = mv.villain_id " +
                        "GROUP BY v.name " +
                        "HAVING m_count > ?;");

        preparedStatement.setInt(1, 15);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            System.out.printf("%s %d %n", resultSet.getString(1),
                    resultSet.getInt(2));
        }
    }

    private static Connection getConnection() throws SQLException, IOException {
        System.out.println("Enter user:");
        String user = reader.readLine();
        System.out.println("Enter password:");
        String password = reader.readLine();

        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);

        return DriverManager
                .getConnection(CONNECTION_STRING + DB_NAME, properties);
    }
}
