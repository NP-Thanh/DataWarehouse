package vn.edu.hcmuaf.fit.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataMartDBConfig {
    private static final String DB_URL = System.getenv().getOrDefault("DATA_MART_DB_URL", "jdbc:mysql://152.42.163.144:3306/data_mart");
    private static final String DB_USER = System.getenv().getOrDefault("DATA_MART_DB_USER", "dev01");
    private static final String DB_PASS = System.getenv().getOrDefault("DATA_MART_DB_PASS", "Abcd1234!");

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }
}