package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.StagingDBConfig;
import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoadDimProductToWarehouse {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 4.2] Bắt đầu load dim_product với SCD Type 2 ===");

        int totalRecords = 0;

        try (Connection stagingConn = StagingDBConfig.getConnection();
             Connection warehouseConn = WarehouseDBConfig.getConnection()) {

            warehouseConn.setAutoCommit(false);

            // Get max product_id
            String getMaxProductId = "SELECT COALESCE(MAX(product_id), 0) as max_id FROM dim_product";
            int currentMaxId = 0;
            try (Statement stmt = warehouseConn.createStatement();
                 ResultSet rs = stmt.executeQuery(getMaxProductId)) {
                if (rs.next()) {
                    currentMaxId = rs.getInt("max_id");
                }
            }

            // Get all products from staging
            String selectFromStaging = """
                SELECT DISTINCT product_name, brand, price, original_price, url, image_url, crawl_date
                FROM stg_products_clean
                ORDER BY url
                """;

            try (Statement stmt = stagingConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                 ResultSet rs = stmt.executeQuery(selectFromStaging)) {

                int newRecords = 0;
                int updatedRecords = 0;
                int unchangedRecords = 0;
                int processCount = 0;

                while (rs.next()) {
                    String productName = rs.getString("product_name");
                    String brand = rs.getString("brand");
                    BigDecimal currentPrice = rs.getBigDecimal("price");
                    BigDecimal originalPrice = rs.getBigDecimal("original_price");
                    String url = rs.getString("url");
                    String imageUrl = rs.getString("image_url");
                    Date crawlDate = rs.getDate("crawl_date");

                    if (currentPrice == null) currentPrice = BigDecimal.ZERO;
                    if (originalPrice == null) originalPrice = BigDecimal.ZERO;

                    try {
                        // Check if product exists
                        String checkSql = "SELECT product_key, product_id, current_price, original_price FROM dim_product WHERE url = ? AND is_current = 1";
                        String existingKey = null;
                        int productId = 0;
                        BigDecimal existingPrice = null;
                        BigDecimal existingOriginal = null;
                        boolean exists = false;

                        try (PreparedStatement psCheck = warehouseConn.prepareStatement(checkSql)) {
                            psCheck.setString(1, url);
                            try (ResultSet rsCheck = psCheck.executeQuery()) {
                                if (rsCheck.next()) {
                                    exists = true;
                                    existingKey = rsCheck.getString("product_key");
                                    productId = rsCheck.getInt("product_id");
                                    existingPrice = rsCheck.getBigDecimal("current_price");
                                    existingOriginal = rsCheck.getBigDecimal("original_price");

                                    if (existingPrice == null) existingPrice = BigDecimal.ZERO;
                                    if (existingOriginal == null) existingOriginal = BigDecimal.ZERO;
                                }
                            }
                        }

                        if (exists) {
                            boolean priceChanged = currentPrice.compareTo(existingPrice) != 0
                                    || originalPrice.compareTo(existingOriginal) != 0;

                            if (priceChanged) {
                                // Close old version
                                String closeSql = "UPDATE dim_product SET end_date = ?, is_current = 0 WHERE product_key = ?";
                                try (PreparedStatement psClose = warehouseConn.prepareStatement(closeSql)) {
                                    psClose.setDate(1, new java.sql.Date(crawlDate.getTime()));
                                    psClose.setString(2, existingKey);
                                    psClose.executeUpdate();
                                }

                                // Insert new version
                                String newKey = "P" + productId + "_" + new SimpleDateFormat("yyyyMMdd").format(crawlDate);
                                String insertSql = """
                                    INSERT IGNORE INTO dim_product (
                                        product_key, product_id, product_name, brand, 
                                        current_price, original_price, url, image_url,
                                        start_date, end_date, is_current
                                    )
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
                                    """;
                                try (PreparedStatement psInsert = warehouseConn.prepareStatement(insertSql)) {
                                    psInsert.setString(1, newKey);
                                    psInsert.setInt(2, productId);
                                    psInsert.setString(3, productName);
                                    psInsert.setString(4, brand);
                                    psInsert.setBigDecimal(5, currentPrice);
                                    psInsert.setBigDecimal(6, originalPrice);
                                    psInsert.setString(7, url);
                                    psInsert.setString(8, imageUrl);
                                    psInsert.setDate(9, new java.sql.Date(crawlDate.getTime()));
                                    psInsert.executeUpdate();
                                }

                                updatedRecords++;
                            } else {
                                unchangedRecords++;
                            }
                        } else {
                            // Insert new product
                            currentMaxId++;
                            String newKey = "P" + currentMaxId + "_" + new SimpleDateFormat("yyyyMMdd").format(crawlDate);
                            String insertSql = """
                                INSERT IGNORE INTO dim_product (
                                    product_key, product_id, product_name, brand, 
                                    current_price, original_price, url, image_url,
                                    start_date, end_date, is_current
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
                                """;
                            try (PreparedStatement psInsert = warehouseConn.prepareStatement(insertSql)) {
                                psInsert.setString(1, newKey);
                                psInsert.setInt(2, currentMaxId);
                                psInsert.setString(3, productName);
                                psInsert.setString(4, brand);
                                psInsert.setBigDecimal(5, currentPrice);
                                psInsert.setBigDecimal(6, originalPrice);
                                psInsert.setString(7, url);
                                psInsert.setString(8, imageUrl);
                                psInsert.setDate(9, new java.sql.Date(crawlDate.getTime()));
                                psInsert.executeUpdate();
                            }

                            newRecords++;
                        }

                        processCount++;
                        if (processCount % 10 == 0) {
                            warehouseConn.commit();
                            LoggerUtil.log("Đã xử lý " + (newRecords + updatedRecords + unchangedRecords) + " sản phẩm...");
                        }

                    } catch (SQLException e) {
                        warehouseConn.rollback();
                        LoggerUtil.log("❌ Lỗi xử lý product: " + productName + " - " + e.getMessage());
                        throw e;
                    }
                }

                warehouseConn.commit();
                totalRecords = newRecords + updatedRecords;

                LoggerUtil.log("✅ Load dim_product hoàn tất:");
                LoggerUtil.log("   - Sản phẩm mới: " + newRecords);
                LoggerUtil.log("   - Sản phẩm cập nhật (SCD Type 2): " + updatedRecords);
                LoggerUtil.log("   - Không thay đổi: " + unchangedRecords);
            }

        } catch (Exception e) {
            LoggerUtil.log("❌ Lỗi Script 4.2: " + e.getMessage());
            throw e;
        }

        return totalRecords;
    }
}