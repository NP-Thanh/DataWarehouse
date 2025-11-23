package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.StagingDBConfig;
import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.math.BigDecimal;
import java.sql.*;

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
            INSERT IGNORE INTO fact_product_price_daily (
                date_key, product_key, price, crawl_date
            )
            VALUES (?, ?, ?, ?)
        """;

        int count = 0;

        try (Connection stagingConn = StagingDBConfig.getConnection();
             Connection warehouseConn = WarehouseDBConfig.getConnection()) {

            warehouseConn.setAutoCommit(false);

            try (Statement stmtSelect = stagingConn.createStatement();
                 ResultSet rs = stmtSelect.executeQuery(selectFromStaging)) {

                try (PreparedStatement psProductKey = warehouseConn.prepareStatement(getProductKey);
                     PreparedStatement psDateKey = warehouseConn.prepareStatement(getDateKey);
                     PreparedStatement psInsert = warehouseConn.prepareStatement(insertFact)) {

                    int skipped = 0;
                    int batchCount = 0;

                    while (rs.next()) {
                        String url = rs.getString("url");
                        BigDecimal price = rs.getBigDecimal("price");
                        Timestamp crawlDate = rs.getTimestamp("crawl_date");

                        psProductKey.setString(1, url);
                        psProductKey.setDate(2, new java.sql.Date(crawlDate.getTime()));

                        String productKey = null;
                        try (ResultSet rsProduct = psProductKey.executeQuery()) {
                            if (rsProduct.next()) {
                                productKey = rsProduct.getString("product_key");
                            }
                        }

                        if (productKey == null) {
                            skipped++;
                            continue;
                        }

                        psDateKey.setDate(1, new java.sql.Date(crawlDate.getTime()));

                        int dateKey = -1;
                        try (ResultSet rsDate = psDateKey.executeQuery()) {
                            if (rsDate.next()) {
                                dateKey = rsDate.getInt("date_key");
                            }
                        }

                        if (dateKey == -1) {
                            skipped++;
                            continue;
                        }

                        psInsert.setInt(1, dateKey);
                        psInsert.setString(2, productKey);
                        psInsert.setBigDecimal(3, price);
                        psInsert.setTimestamp(4, new Timestamp(crawlDate.getTime()));

                        psInsert.addBatch();
                        count++;
                        batchCount++;

                        if (batchCount % 50 == 0) {
                            psInsert.executeBatch();
                            warehouseConn.commit();
                            LoggerUtil.log("Đã insert " + count + " fact records...");
                            psInsert.clearBatch();
                        }
                    }

                    if (batchCount % 50 != 0) {
                        psInsert.executeBatch();
                    }
                    warehouseConn.commit();

                    LoggerUtil.log("✅ Load fact_product_price_daily hoàn tất:");
                    LoggerUtil.log("   - Đã insert: " + count + " records (CHỈ GIÁ THẬT từ Cellphones.vn)");
                    LoggerUtil.log("   - Bỏ qua (không tìm thấy key): " + skipped + " records");
                }
            }

        } catch (Exception e) {
            LoggerUtil.log("❌ Lỗi Script 4.3: " + e.getMessage());
            throw e;
        }

        return count;
    }
}