package vn.edu.hcmuaf.fit.mart;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.db.DataMartDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadMartDailySummary {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 5.2b] Load mart_daily_summary từ agg_daily_summary sang data_mart ===");

        String dropTable = """
            DROP TABLE IF EXISTS mart_daily_summary
        """;

        String createTable = """
            CREATE TABLE mart_daily_summary (
                mart_daily_summary_key INT AUTO_INCREMENT PRIMARY KEY,
                summary_date DATE NOT NULL UNIQUE,
                total_products INT DEFAULT 0,
                total_revenue DECIMAL(15, 2) DEFAULT 0,
                total_units_sold INT DEFAULT 0,
                avg_price DECIMAL(10, 2) DEFAULT 0
            )
        """;

        String selectFromWarehouse = """
            SELECT 
                full_date,
                total_products,
                total_revenue,
                total_units_sold,
                avg_price
            FROM agg_daily_summary
        """;

        String insertIntoDataMart = """
            INSERT INTO mart_daily_summary (
                summary_date, total_products, total_revenue, total_units_sold, avg_price
            )
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                total_products = VALUES(total_products),
                total_revenue = VALUES(total_revenue),
                total_units_sold = VALUES(total_units_sold),
                avg_price = VALUES(avg_price)
        """;

        int count = 0;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection();
             Connection dataMartConn = DataMartDBConfig.getConnection()) {

            dataMartConn.setAutoCommit(false);

            PreparedStatement psDropTable = dataMartConn.prepareStatement(dropTable);
            psDropTable.executeUpdate();
            psDropTable.close();

            PreparedStatement psCreateTable = dataMartConn.prepareStatement(createTable);
            psCreateTable.executeUpdate();
            psCreateTable.close();

            PreparedStatement psSelect = warehouseConn.prepareStatement(selectFromWarehouse);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psInsert = dataMartConn.prepareStatement(insertIntoDataMart);

            while (rs.next()) {
                Date summaryDate = rs.getDate("full_date");
                int totalProducts = rs.getInt("total_products");
                java.math.BigDecimal totalRevenue = rs.getBigDecimal("total_revenue");
                int totalUnitsSold = rs.getInt("total_units_sold");
                java.math.BigDecimal avgPrice = rs.getBigDecimal("avg_price");

                psInsert.setDate(1, summaryDate);
                psInsert.setInt(2, totalProducts);
                psInsert.setBigDecimal(3, totalRevenue);
                psInsert.setInt(4, totalUnitsSold);
                psInsert.setBigDecimal(5, avgPrice);

                psInsert.addBatch();
                count++;

                if (count % 100 == 0) {
                    psInsert.executeBatch();
                    dataMartConn.commit();
                    LoggerUtil.log("Đã insert " + count + " records vào mart_daily_summary...");
                }
            }

            psInsert.executeBatch();
            dataMartConn.commit();

            LoggerUtil.log("✅ Load mart_daily_summary hoàn tất: " + count + " records");
        }

        return count;
    }
}