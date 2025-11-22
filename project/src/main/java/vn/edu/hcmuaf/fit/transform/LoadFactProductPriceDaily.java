package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.StagingDBConfig;
import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Random;

public class LoadFactProductPriceDaily {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 4.3] Bắt đầu load fact_product_price_daily ===");

        String selectFromStaging = """
            SELECT product_name, brand, price, url, crawl_date
            FROM stg_products_clean
        """;

        String getProductKey = """
            SELECT product_key
            FROM dim_product
            WHERE url = ? 
              AND ? BETWEEN start_date AND COALESCE(end_date, '9999-12-31')
        """;

        String getDateKey = """
            SELECT date_key
            FROM dim_date
            WHERE full_date = ?
        """;

        String insertFact = """
            INSERT INTO fact_product_price_daily (
                date_key, product_key, units_sold, total_revenue, crawl_date
            )
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE 
                units_sold = VALUES(units_sold),
                total_revenue = VALUES(total_revenue),
                crawl_date = VALUES(crawl_date)
        """;

        Random random = new Random();
        int count = 0;

        try (Connection stagingConn = StagingDBConfig.getConnection();
             Connection warehouseConn = WarehouseDBConfig.getConnection()) {

            warehouseConn.setAutoCommit(false);

            PreparedStatement psSelect = stagingConn.prepareStatement(selectFromStaging);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psProductKey = warehouseConn.prepareStatement(getProductKey);
            PreparedStatement psDateKey = warehouseConn.prepareStatement(getDateKey);
            PreparedStatement psInsert = warehouseConn.prepareStatement(insertFact);

            int skipped = 0;

            while (rs.next()) {
                String url = rs.getString("url");
                BigDecimal price = rs.getBigDecimal("price");
                Timestamp crawlDate = rs.getTimestamp("crawl_date");

                psProductKey.setString(1, url);
                psProductKey.setDate(2, new java.sql.Date(crawlDate.getTime()));
                ResultSet rsProduct = psProductKey.executeQuery();
                if (!rsProduct.next()) {
                    skipped++;
                    continue;
                }
                String productKey = rsProduct.getString("product_key");

                psDateKey.setDate(1, new java.sql.Date(crawlDate.getTime()));
                ResultSet rsDate = psDateKey.executeQuery();
                if (!rsDate.next()) {
                    skipped++;
                    continue;
                }
                int dateKey = rsDate.getInt("date_key");

                int unitsSold = random.nextInt(50) + 1;
                BigDecimal totalRevenue = price.multiply(new BigDecimal(unitsSold));

                psInsert.setInt(1, dateKey);
                psInsert.setString(2, productKey);
                psInsert.setInt(3, unitsSold);
                psInsert.setBigDecimal(4, totalRevenue);
                psInsert.setTimestamp(5, new Timestamp(crawlDate.getTime()));

                psInsert.addBatch();
                count++;

                if (count % 200 == 0) {
                    psInsert.executeBatch();
                    warehouseConn.commit();
                    LoggerUtil.log("Đã insert " + count + " fact records...");
                }
            }

            psInsert.executeBatch();
            warehouseConn.commit();

            LoggerUtil.log("✅ Load fact_product_price_daily hoàn tất:");
            LoggerUtil.log("   - Đã insert: " + count + " records");
            LoggerUtil.log("   - Bỏ qua (không tìm thấy key): " + skipped + " records");
        }

        return count;
    }
}