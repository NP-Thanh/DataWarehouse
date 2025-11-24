package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadDimProductToWarehouse {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 4.2] Bắt đầu load dim_product (SIMPLIFIED - NO SCD Type 2) ===");
        LoggerUtil.log("📌 Cột: product_id, product_name, brand, url, image_url, created_at");

        String insertProducts = """
            INSERT IGNORE INTO warehouse_db.dim_product 
                (product_name, brand, url, image_url, created_at)
            SELECT DISTINCT 
                product_name, 
                brand, 
                url, 
                image_url, 
                NOW() as created_at
            FROM staging_db.stg_products_clean
        """;

        int count = 0;
        int totalCount = 0;
        long startTime = System.currentTimeMillis();
        String errorMsg = null;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            LoggerUtil.log("⚡ Dùng BULK INSERT IGNORE - 1 lệnh SQL, tự động skip duplicate URLs...");

            try (Statement stmt = warehouseConn.createStatement()) {
                count = stmt.executeUpdate(insertProducts);
            }

            String countSql = "SELECT COUNT(*) FROM warehouse_db.dim_product";
            try (Statement stmt = warehouseConn.createStatement();
                 ResultSet rs = stmt.executeQuery(countSql)) {
                if (rs.next()) {
                    totalCount = rs.getInt(1);
                }
            }

            long duration = System.currentTimeMillis() - startTime;

            LoggerUtil.log("✅ Load dim_product hoàn tất:");
            LoggerUtil.log("   - Sản phẩm mới thêm: " + count);
            LoggerUtil.log("   - Tổng sản phẩm trong warehouse: " + totalCount);
            LoggerUtil.log("   - Thời gian: " + duration + "ms");

            LoggerUtil.logStep("4.2", "LoadDimProduct", count, duration, "SUCCESS", null);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            errorMsg = e.getMessage();
            LoggerUtil.log("❌ Lỗi Script 4.2: " + errorMsg);
            LoggerUtil.logStep("4.2", "LoadDimProduct", count, duration, "FAILED", errorMsg);
            e.printStackTrace();
            throw e;
        }

        return count;
    }
}