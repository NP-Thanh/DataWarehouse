package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class CreateAggregateDailySales {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 5.1a] Tạo bảng aggregate agg_daily_sales (GIÁ THẬT) ===");

        String createTable = """
            CREATE TABLE IF NOT EXISTS agg_daily_sales (
                agg_daily_sales_key INT AUTO_INCREMENT PRIMARY KEY,
                date_key INT NOT NULL,
                product_key VARCHAR(50) NOT NULL,
                avg_price DECIMAL(15, 2) DEFAULT 0,
                min_price DECIMAL(15, 2) DEFAULT 0,
                max_price DECIMAL(15, 2) DEFAULT 0,
                UNIQUE KEY (date_key, product_key),
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
            )
        """;

        String createAndLoadAgg = """
            INSERT INTO agg_daily_sales (date_key, product_key, avg_price, min_price, max_price)
            SELECT 
                f.date_key, 
                f.product_key, 
                AVG(f.price) as avg_price,
                MIN(f.price) as min_price,
                MAX(f.price) as max_price
            FROM fact_product_price_daily f
            GROUP BY f.date_key, f.product_key
            ON DUPLICATE KEY UPDATE
                avg_price = VALUES(avg_price),
                min_price = VALUES(min_price),
                max_price = VALUES(max_price)
        """;

        int count = 0;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            warehouseConn.setAutoCommit(false);

            PreparedStatement psCreateTable = warehouseConn.prepareStatement(createTable);
            psCreateTable.executeUpdate();
            psCreateTable.close();

            PreparedStatement psInsert = warehouseConn.prepareStatement(createAndLoadAgg);
            int result = psInsert.executeUpdate();
            warehouseConn.commit();

            count = result;
            LoggerUtil.log("✅ Tạo và load agg_daily_sales hoàn tất: " + count + " rows (avg/min/max price)");
        }

        return count;
    }
}