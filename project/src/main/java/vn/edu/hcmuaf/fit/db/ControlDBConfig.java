package vn.edu.hcmuaf.fit.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ControlDBConfig {
    public static final String DB_URL = "jdbc:mysql://152.42.163.144:3306/control_db";
    public static final String DB_USER = "dev01";
    public static final String DB_PASS = "Abcd1234!";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }

    public static void createTablesIfNotExists() {
        String createSourceSql = """
            CREATE TABLE IF NOT EXISTS source (
                source_id INT AUTO_INCREMENT PRIMARY KEY,
                source_name VARCHAR(50) NOT NULL,
                source_url VARCHAR(255) NOT NULL,
                crawl_frequency VARCHAR(20),
                active_flag BOOLEAN DEFAULT TRUE,
                last_run_time DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """;

        String createLogSql = """
            CREATE TABLE IF NOT EXISTS log (
                run_id VARCHAR(36) PRIMARY KEY,
                source_id INT NOT NULL,
                start_time DATETIME NOT NULL,
                end_time DATETIME,
                status ENUM('SUCCESS', 'RUNNING', 'FAILED') NOT NULL,
                record_count INT,
                error_message TEXT,
                rollback_to_run_id VARCHAR(36),
                operator VARCHAR(50),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (source_id) REFERENCES source(source_id)
            )
        """;

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(createSourceSql);
            stmt.execute(createLogSql);
            System.out.println("Control DB Tables created or already exists.");
        } catch (SQLException e) {
            System.err.println("Lỗi khi tạo bảng Control DB: " + e.getMessage());
        }
    }
}