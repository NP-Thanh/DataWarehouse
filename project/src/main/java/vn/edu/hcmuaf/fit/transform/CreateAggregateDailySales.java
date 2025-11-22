package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class CreateAggregateDailySales {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 5.1a] Tạo bảng aggregate agg_daily_sales trong warehouse_db ===");

        String createTable = """
            CREATE TABLE IF NOT EXISTS agg_daily_sales (
                agg_daily_sales_key INT AUTO_INCREMENT PRIMARY KEY,
                date_key INT NOT NULL,
                product_key VARCHAR(50) NOT NULL,
                units_sold INT DEFAULT 0,
                total_revenue DECIMAL(15, 2) DEFAULT 0,
                UNIQUE KEY (date_key, product_key),
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
            )
        """;

        String createAndLoadAgg = """
            INSERT INTO agg_daily_sales (date_key, product_key, units_sold, total_revenue)
            SELECT f.date_key, f.product_key, SUM(f.units_sold) as units_sold, SUM(f.total_revenue) as total_revenue
            FROM fact_product_price_daily f
            GROUP BY f.date_key, f.product_key
            ON DUPLICATE KEY UPDATE
                units_sold = VALUES(units_sold),
                total_revenue = VALUES(total_revenue)
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
            LoggerUtil.log("✅ Tạo và load agg_daily_sales hoàn tất: " + count + " rows");
        }

        return count;
    }
}