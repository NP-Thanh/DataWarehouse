package vn.edu.hcmuaf.fit.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class WarehouseDBConfig {
    private static final String DB_URL = "jdbc:mysql://152.42.163.144:3306/warehouse_db";
    private static final String DB_USER = "dev01";
    private static final String DB_PASS = "Abcd1234!";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }
}
