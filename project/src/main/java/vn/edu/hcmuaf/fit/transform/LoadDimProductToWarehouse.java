package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.StagingDBConfig;
import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadDimProductToWarehouse {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 4.2] Bắt đầu load dim_product (SIMPLIFIED - NO SCD Type 2) ===");

        // ⚡ BULK INSERT IGNORE - Chỉ 1 lệnh SQL duy nhất!
        // IGNORE sẽ skip duplicate URLs (UNIQUE constraint)
        String insertProducts = """
            INSERT IGNORE INTO warehouse_db.dim_product (product_name, brand, url, image_url)
            SELECT DISTINCT product_name, brand, url, image_url
            FROM staging_db.stg_products_clean
        """;

        int count = 0;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            LoggerUtil.log("⚡ Dùng BULK INSERT IGNORE - 1 lệnh SQL, tự động skip duplicate URLs...");

            try (Statement stmt = warehouseConn.createStatement()) {
                count = stmt.executeUpdate(insertProducts);
            }

            // Get total count in table
            String countSql = "SELECT COUNT(*) FROM warehouse_db.dim_product";
            int totalCount = 0;
            try (Statement stmt = warehouseConn.createStatement();
                 ResultSet rs = stmt.executeQuery(countSql)) {
                if (rs.next()) {
                    totalCount = rs.getInt(1);
                }
            }

            LoggerUtil.log("✅ Load dim_product hoàn tất:");
            LoggerUtil.log("   - Sản phẩm mới thêm: " + count);
            LoggerUtil.log("   - Tổng sản phẩm trong warehouse: " + totalCount);
        } catch (Exception e) {
            LoggerUtil.log("❌ Lỗi Script 4.2: " + e.getMessage());
            throw e;
        }

        return count;
    }
}