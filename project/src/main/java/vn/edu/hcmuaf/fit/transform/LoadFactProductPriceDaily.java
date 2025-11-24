package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadFactProductPriceDaily {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 4.3] Bắt đầu load fact_product_price_daily ===");

        // ⚡⚡⚡ PURE SQL - KHÔNG LOOP JAVA
        // FIX: Thêm COLLATE utf8mb4_unicode_ci để unify encoding
        String insertFact = """
            INSERT IGNORE INTO warehouse_db.fact_product_price_daily (
                date_key, product_id, price, crawl_date
            )
            SELECT 
                d.date_key,
                p.product_id,
                s.price,
                s.crawl_date
            FROM staging_db.stg_products_clean s
            INNER JOIN warehouse_db.dim_product p 
                ON s.url COLLATE utf8mb4_unicode_ci = p.url COLLATE utf8mb4_unicode_ci
            INNER JOIN warehouse_db.dim_date d 
                ON DATE(s.crawl_date) = d.full_date
        """;

        int count = 0;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            LoggerUtil.log("⚡⚡⚡ Dùng PURE SQL - 1 lệnh INSERT SELECT JOIN (NO JAVA LOOP)...");

            try (Statement stmt = warehouseConn.createStatement()) {
                count = stmt.executeUpdate(insertFact);
            }

            LoggerUtil.log("✅ Load fact_product_price_daily hoàn tất:");
            LoggerUtil.log("   - Đã insert: " + count + " records (CHỈ GIÁ THẬT từ Cellphones.vn)");
        } catch (Exception e) {
            LoggerUtil.log("❌ Lỗi Script 4.3: " + e.getMessage());
            throw e;
        }

        return count;
    }
}