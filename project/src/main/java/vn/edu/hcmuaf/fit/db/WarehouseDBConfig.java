package vn.edu.hcmuaf.fit.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class WarehouseDBConfig {
    private static final String DB_URL = System.getenv().getOrDefault("WAREHOUSE_DB_URL", "jdbc:mysql://152.42.163.144:3306/warehouse_db");
    private static final String DB_USER = System.getenv().getOrDefault("WAREHOUSE_DB_USER", "dev01");
    private static final String DB_PASS = System.getenv().getOrDefault("WAREHOUSE_DB_PASS", "Abcd1234!");

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }

    public static void createAggregateTablesIfNotExists() {
        String createAggDailySalesSql = """
            CREATE TABLE IF NOT EXISTS agg_daily_sales (
                date_key INT NOT NULL,
                product_key VARCHAR(50) NOT NULL,
                total_revenue DECIMAL(18, 2) DEFAULT 0.00,
                units_sold INT DEFAULT 0,
                PRIMARY KEY (date_key, product_key),
                INDEX idx_date (date_key),
                INDEX idx_product (product_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """;

        String createAggDailySummarySql = """
            CREATE TABLE IF NOT EXISTS agg_daily_summary (
                full_date DATE NOT NULL,
                total_products INT DEFAULT 0,
                total_revenue DECIMAL(18, 2) DEFAULT 0.00,
                total_units_sold INT DEFAULT 0,
                avg_price DECIMAL(12, 2) DEFAULT 0.00,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (full_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """;

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(createAggDailySalesSql);
            stmt.execute(createAggDailySummarySql);
            System.out.println("Warehouse aggregate tables created or already exists.");
        } catch (SQLException e) {
            System.err.println("Lỗi khi tạo bảng aggregate trong warehouse_db: " + e.getMessage());
        }
    }
}