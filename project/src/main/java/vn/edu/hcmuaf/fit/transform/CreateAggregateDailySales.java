package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class CreateAggregateDailySales {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 5.1a] Tạo bảng aggregate agg_daily_sales (GIÁ THẬT) ===");

        String dropTable = "DROP TABLE IF EXISTS warehouse_db.agg_daily_sales";

        String createTable = """
            CREATE TABLE warehouse_db.agg_daily_sales (
                agg_daily_sales_key INT AUTO_INCREMENT PRIMARY KEY,
                date_key INT NOT NULL,
                product_id INT NOT NULL,
                avg_price DECIMAL(15, 2) DEFAULT 0,
                min_price DECIMAL(15, 2) DEFAULT 0,
                max_price DECIMAL(15, 2) DEFAULT 0,
                UNIQUE KEY (date_key, product_id),
                FOREIGN KEY (date_key) REFERENCES warehouse_db.dim_date(date_key),
                FOREIGN KEY (product_id) REFERENCES warehouse_db.dim_product(product_id)
            )
        """;

        String createAndLoadAgg = """
            INSERT INTO warehouse_db.agg_daily_sales (date_key, product_id, avg_price, min_price, max_price)
            SELECT 
                f.date_key, 
                f.product_id, 
                AVG(f.price) as avg_price,
                MIN(f.price) as min_price,
                MAX(f.price) as max_price
            FROM warehouse_db.fact_product_price_daily f
            GROUP BY f.date_key, f.product_id
        """;

        int count = 0;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            LoggerUtil.log("⚡ Dùng PURE SQL - DROP → CREATE → INSERT (NO JAVA)...");

            try (Statement stmt = warehouseConn.createStatement()) {
                stmt.executeUpdate(dropTable);
            }

            try (Statement stmt = warehouseConn.createStatement()) {
                stmt.executeUpdate(createTable);
            }

            try (Statement stmt = warehouseConn.createStatement()) {
                count = stmt.executeUpdate(createAndLoadAgg);
            }

            LoggerUtil.log("✅ Tạo và load agg_daily_sales hoàn tất: " + count + " rows (avg/min/max price)");
        } catch (Exception e) {
            LoggerUtil.log("❌ Lỗi Script 5.1: " + e.getMessage());
            throw e;
        }

        return count;
    }
}