package vn.edu.hcmuaf.fit.mart;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.db.DataMartDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadMartDailySales {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 5.1b] Load mart_daily_sales từ agg_daily_sales sang data_mart ===");

        String dropTable = """
            DROP TABLE IF EXISTS mart_daily_sales
        """;

        String createTable = """
            CREATE TABLE mart_daily_sales (
                mart_daily_sales_key INT AUTO_INCREMENT PRIMARY KEY,
                sale_date DATE NOT NULL,
                product_name VARCHAR(255) NOT NULL,
                brand VARCHAR(100),
                units_sold INT DEFAULT 0,
                total_revenue DECIMAL(15, 2) DEFAULT 0,
                UNIQUE KEY (sale_date, product_name)
            )
        """;

        String selectFromWarehouse = """
            SELECT 
                d.full_date,
                p.product_name,
                p.brand,
                a.units_sold,
                a.total_revenue
            FROM agg_daily_sales a
            INNER JOIN dim_date d ON a.date_key = d.date_key
            INNER JOIN dim_product p ON a.product_key = p.product_key
        """;

        String insertIntoDataMart = """
            INSERT INTO mart_daily_sales (
                sale_date, product_name, brand, units_sold, total_revenue
            )
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                units_sold = VALUES(units_sold),
                total_revenue = VALUES(total_revenue)
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
                Date saleDate = rs.getDate("full_date");
                String productName = rs.getString("product_name");
                String brand = rs.getString("brand");
                int unitsSold = rs.getInt("units_sold");
                java.math.BigDecimal totalRevenue = rs.getBigDecimal("total_revenue");

                psInsert.setDate(1, saleDate);
                psInsert.setString(2, productName);
                psInsert.setString(3, brand);
                psInsert.setInt(4, unitsSold);
                psInsert.setBigDecimal(5, totalRevenue);

                psInsert.addBatch();
                count++;

                if (count % 200 == 0) {
                    psInsert.executeBatch();
                    dataMartConn.commit();
                    LoggerUtil.log("Đã insert " + count + " records vào mart_daily_sales...");
                }
            }

            psInsert.executeBatch();
            dataMartConn.commit();

            LoggerUtil.log("✅ Load mart_daily_sales hoàn tất: " + count + " records");
        }

        return count;
    }
}