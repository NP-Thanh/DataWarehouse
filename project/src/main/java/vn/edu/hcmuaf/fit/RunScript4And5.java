package vn.edu.hcmuaf.fit;

import vn.edu.hcmuaf.fit.util.LoggerUtil;
import vn.edu.hcmuaf.fit.transform.LoadDimDateToWarehouse;
import vn.edu.hcmuaf.fit.transform.LoadDimProductToWarehouse;
import vn.edu.hcmuaf.fit.transform.LoadFactProductPriceDaily;
import vn.edu.hcmuaf.fit.transform.CreateAggregateDailySales;
import vn.edu.hcmuaf.fit.transform.CreateAggregateDailySummary;

/**
 * SCRIPT 4 & 5 - CHẠY ĐỘC LẬP
 *
 * Chức năng: Load dữ liệu từ staging_db vào warehouse_db
 * - Script 4: Tạo dim_date, dim_product, fact_product_price_daily
 * - Script 5: Tạo aggregate tables (agg_daily_sales, agg_daily_summary)
 *
 * Chạy lệnh:
 *   mvn clean compile exec:java -Dexec.mainClass="vn.edu.hcmuaf.fit.RunScript4And5" -DskipTests
 */
public class RunScript4And5 {
    public static void main(String[] args) {
        try {

            LoggerUtil.log("SCRIPT 4 & 5 - LOAD VÀO WAREHOUSE");


            // ==================== SCRIPT 4: WAREHOUSE ====================
            LoggerUtil.log("\n=== SCRIPT 4: LOAD DỮ LIỆU VÀO WAREHOUSE ===\n");

            // 4.1 Load dim_date
            LoggerUtil.log("▶ 4.1 Bắt đầu load dim_date...");
            LoadDimDateToWarehouse.load();
            LoggerUtil.log("✅ 4.1 dim_date → warehouse_db HOÀN TẤT\n");

            // 4.2 Load dim_product (Simplified - NO SCD Type 2)
            LoggerUtil.log("▶ 4.2 Bắt đầu load dim_product (SIMPLIFIED)...");
            LoadDimProductToWarehouse.load();
            LoggerUtil.log("✅ 4.2 dim_product (product_id duy nhất) → warehouse_db HOÀN TẤT\n");

            // 4.3 Load fact_product_price_daily
            LoggerUtil.log("▶ 4.3 Bắt đầu load fact_product_price_daily...");
            LoadFactProductPriceDaily.load();
            LoggerUtil.log("✅ 4.3 fact_product_price_daily (GIÁ THẬT) → warehouse_db HOÀN TẤT\n");

            // ==================== SCRIPT 5: AGGREGATE ====================
            LoggerUtil.log("=== SCRIPT 5: TẠO AGGREGATE TABLES (GIÁ) ===\n");

            // 5.1 Create agg_daily_sales
            LoggerUtil.log("▶ 5.1a Bắt đầu tạo agg_daily_sales...");
            CreateAggregateDailySales.load();
            LoggerUtil.log("✅ 5.1a agg_daily_sales (avg/min/max price) → warehouse_db HOÀN TẤT\n");

            // 5.2 Create agg_daily_summary
            LoggerUtil.log("▶ 5.2a Bắt đầu tạo agg_daily_summary...");
            CreateAggregateDailySummary.load();
            LoggerUtil.log("✅ 5.2a agg_daily_summary (avg/min/max price) → warehouse_db HOÀN TẤT\n");

            // ==================== THÀNH CÔNG ====================
            LoggerUtil.log("║✅ SCRIPT 4 & 5 HOÀN THÀNH THÀNH CÔNG!                    ");

        } catch (Exception e) {
            LoggerUtil.log("❌ LỖI SCRIPT 4 & 5: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
