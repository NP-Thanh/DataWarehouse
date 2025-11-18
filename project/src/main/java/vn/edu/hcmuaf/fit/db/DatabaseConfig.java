package vn.edu.hcmuaf.fit.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseConfig {
    public static final String DB_URL = "jdbc:mysql://152.42.163.144:3306/staging_db";
    public static final String DB_USER = "dev01";
    public static final String DB_PASS = "Abcd1234!";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }

    //1.1 Tạo bảng "stg_products_raw"
    public static void createTableIfNotExists() {
        String sql = """
            CREATE TABLE IF NOT EXISTS stg_products_raw (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_name TEXT,
                brand TEXT,
                price TEXT,
                original_price TEXT,
                url TEXT,
                image_url TEXT,
                crawl_date DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """;
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            System.err.println("Lỗi khi tạo bảng: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        DatabaseConfig.createTableIfNotExists();
    }
}


