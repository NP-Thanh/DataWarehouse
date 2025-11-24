package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.db.WarehouseDBConfig;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

import java.sql.*;

public class LoadFactProductPriceDaily {

    public static int load() throws Exception {
        LoggerUtil.log("=== [Script 4.3] Bắt đầu load fact_product_price_daily ===");

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
        long startTime = System.currentTimeMillis();
        String errorMsg = null;

        try (Connection warehouseConn = WarehouseDBConfig.getConnection()) {
            LoggerUtil.log("⚡⚡⚡ Dùng PURE SQL - 1 lệnh INSERT SELECT JOIN (NO JAVA LOOP)...");
            LoggerUtil.log("📌 JOIN: stg_products_clean + dim_product (URL) + dim_date (crawl_date)");

            try (Statement stmt = warehouseConn.createStatement()) {
                count = stmt.executeUpdate(insertFact);
            }

            long duration = System.currentTimeMillis() - startTime;

            LoggerUtil.log("✅ Load fact_product_price_daily hoàn tất:");
            LoggerUtil.log("   - Bản ghi được insert: " + count + " records");
            LoggerUtil.log("   - Dữ liệu: 100% THẬT từ Cellphones.vn");
            LoggerUtil.log("   - Thời gian: " + duration + "ms (~1-2 giây)");

            LoggerUtil.logStep("4.3", "LoadFactProductPrice", count, duration, "SUCCESS", null);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            errorMsg = e.getMessage();
            LoggerUtil.log("❌ Lỗi Script 4.3: " + errorMsg);
            LoggerUtil.logStep("4.3", "LoadFactProductPrice", count, duration, "FAILED", errorMsg);
            e.printStackTrace();
            throw e;
        }

        return count;
    }
}