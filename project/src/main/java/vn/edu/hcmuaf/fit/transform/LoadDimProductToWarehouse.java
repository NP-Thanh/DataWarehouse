package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.StagingDBConfig;
import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoadDimProductToWarehouse {

    public static void load() throws Exception {
        LoggerUtil.log("=== [Script 4.2] Bắt đầu load dim_product với SCD Type 2 ===");

        String selectFromStaging = """
            SELECT DISTINCT product_name, brand, price, original_price, url, image_url, crawl_date
            FROM stg_products_clean
            ORDER BY url
        """;

        String checkCurrentProduct = """
            SELECT product_key, product_id, current_price, original_price
            FROM dim_product
            WHERE url = ? AND is_current = 1
        """;

        String closePreviousVersion = """
            UPDATE dim_product
            SET end_date = ?, is_current = 0
            WHERE product_key = ?
        """;

        String insertNewVersion = """
            INSERT INTO dim_product (
                product_key, product_id, product_name, brand, 
                current_price, original_price, url, image_url,
                start_date, end_date, is_current
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
        """;

        String getMaxProductId = """
            SELECT COALESCE(MAX(product_id), 0) as max_id FROM dim_product
        """;

        try (Connection stagingConn = StagingDBConfig.getConnection();
             Connection warehouseConn = WarehouseDBConfig.getConnection()) {

            warehouseConn.setAutoCommit(false);

            PreparedStatement psMaxId = warehouseConn.prepareStatement(getMaxProductId);
            ResultSet rsMaxId = psMaxId.executeQuery();
            rsMaxId.next();
            int currentMaxId = rsMaxId.getInt("max_id");

            PreparedStatement psSelect = stagingConn.prepareStatement(selectFromStaging);
            ResultSet rs = psSelect.executeQuery();

            PreparedStatement psCheck = warehouseConn.prepareStatement(checkCurrentProduct);
            PreparedStatement psClose = warehouseConn.prepareStatement(closePreviousVersion);
            PreparedStatement psInsert = warehouseConn.prepareStatement(insertNewVersion);

            int newRecords = 0;
            int updatedRecords = 0;
            int unchangedRecords = 0;

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

                psCheck.setString(1, url);
                ResultSet rsCheck = psCheck.executeQuery();

                if (rsCheck.next()) {
                    String existingKey = rsCheck.getString("product_key");
                    int productId = rsCheck.getInt("product_id");
                    BigDecimal existingPrice = rsCheck.getBigDecimal("current_price");
                    BigDecimal existingOriginal = rsCheck.getBigDecimal("original_price");

                    if (existingPrice == null) existingPrice = BigDecimal.ZERO;
                    if (existingOriginal == null) existingOriginal = BigDecimal.ZERO;

                    boolean priceChanged = currentPrice.compareTo(existingPrice) != 0
                            || originalPrice.compareTo(existingOriginal) != 0;

                    if (priceChanged) {
                        psClose.setDate(1, new java.sql.Date(crawlDate.getTime()));
                        psClose.setString(2, existingKey);
                        psClose.executeUpdate();

                        String newKey = "P" + productId + "_" + new SimpleDateFormat("yyyyMMdd").format(crawlDate);
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

                        updatedRecords++;
                        LoggerUtil.log("🔄 Cập nhật SCD Type 2 cho product_id=" + productId + " (giá thay đổi)");
                    } else {
                        unchangedRecords++;
                    }
                } else {
                    currentMaxId++;
                    String newKey = "P" + currentMaxId + "_" + new SimpleDateFormat("yyyyMMdd").format(crawlDate);

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

                    newRecords++;
                }

                if ((newRecords + updatedRecords) % 100 == 0) {
                    warehouseConn.commit();
                    LoggerUtil.log("Đã xử lý " + (newRecords + updatedRecords + unchangedRecords) + " sản phẩm...");
                }
            }

            warehouseConn.commit();
            LoggerUtil.log("✅ Load dim_product hoàn tất:");
            LoggerUtil.log("   - Sản phẩm mới: " + newRecords);
            LoggerUtil.log("   - Sản phẩm cập nhật (SCD Type 2): " + updatedRecords);
            LoggerUtil.log("   - Không thay đổi: " + unchangedRecords);
        }
    }

    public static void main(String[] args) {
        try {
            load();
            System.out.println("[DONE] LoadDimProductToWarehouse completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR] LoadDimProductToWarehouse failed: " + e.getMessage());
        }
    }
}
