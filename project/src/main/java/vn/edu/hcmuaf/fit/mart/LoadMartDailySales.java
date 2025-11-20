package vn.edu.hcmuaf.fit.mart;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.db.DataMartDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadMartDailySales {

    public static void load() throws Exception {
        LoggerUtil.log("=== [Script 5.1] Bắt đầu load mart_daily_sales ===");


        String selectFromWarehouse = """
            SELECT 
                f.date_key,
                f.product_key,                                  -- ← ĐÚNG: varchar(50)
                SUM(f.total_revenue) as total_revenue,
                SUM(f.units_sold) as units_sold
            FROM fact_product_price_daily f
            GROUP BY f.date_key, f.product_key
            ORDER BY f.date_key, f.product_key
            """;

        // product_key giờ là String → setString
        String insertToDataMart = """
            INSERT INTO mart_daily_sales (
                date_key, product_key, total_revenue, units_sold
            )
            VALUES (?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE 
                total_revenue = VALUES(total_revenue),
                units_sold = VALUES(units_sold)
        """;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection();
             Connection dataMartConn = DataMartDBConfig.getConnection()) {

            dataMartConn.setAutoCommit(false);

            PreparedStatement psSelect = warehouseConn.prepareStatement(selectFromWarehouse);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psInsert = dataMartConn.prepareStatement(insertToDataMart);

            int count = 0;

            while (rs.next()) {
                psInsert.setInt(1, rs.getInt("date_key"));
                psInsert.setString(2, rs.getString("product_key"));   // ← String, không phải Int
                psInsert.setBigDecimal(3, rs.getBigDecimal("total_revenue"));
                psInsert.setInt(4, rs.getInt("units_sold"));

                psInsert.addBatch();

                if (++count % 200 == 0) {
                    psInsert.executeBatch();
                    dataMartConn.commit();
                    LoggerUtil.log("Đã tổng hợp " + count + " daily sales records...");
                }
            }

            psInsert.executeBatch();
            dataMartConn.commit();

            LoggerUtil.log("Load mart_daily_sales hoàn tất. Tổng: " + count + " records");
        }
    }

    public static void main(String[] args) {
        try {
            load();
            System.out.println("[DONE] LoadMartDailySales completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR] LoadMartDailySales failed: " + e.getMessage());
        }
    }
}