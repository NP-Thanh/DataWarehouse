package vn.edu.hcmuaf.fit;

import vn.edu.hcmuaf.fit.util.LoggerUtil;
import vn.edu.hcmuaf.fit.transform.LoadDimDateToWarehouse;
import vn.edu.hcmuaf.fit.transform.LoadDimProductToWarehouse;
import vn.edu.hcmuaf.fit.transform.LoadFactProductPriceDaily;
import vn.edu.hcmuaf.fit.transform.CreateAggregateDailySales;
import vn.edu.hcmuaf.fit.transform.CreateAggregateDailySummary;

public class RunScript4And5 {
    public static void main(String[] args) {
        String runId = null;
        int totalRecords = 0;

        try {
            // Tạo bảng script_log nếu chưa tồn tại
            LoggerUtil.createScriptLogTableIfNotExists();

            // ==================== GHI LOG BẮT ĐẦU ====================
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, "Script 4 & 5 - Warehouse Load");


            LoggerUtil.log("SCRIPT 4 & 5 - LOAD VÀO WAREHOUSE (ĐỘC LẬP)        ");

            // ==================== SCRIPT 4: WAREHOUSE ====================
            LoggerUtil.log("\n=== SCRIPT 4: LOAD DỮ LIỆU VÀO WAREHOUSE ===\n");

            // 4.1 Load dim_date
            LoggerUtil.log("▶ 4.1 Bắt đầu load dim_date...");
            int count41 = LoadDimDateToWarehouse.load();
            totalRecords += count41;
            LoggerUtil.log("✅ 4.1 dim_date → warehouse_db HOÀN TẤT\n");

            // 4.2 Load dim_product
            LoggerUtil.log("▶ 4.2 Bắt đầu load dim_product (SIMPLIFIED)...");
            int count42 = LoadDimProductToWarehouse.load();
            totalRecords += count42;
            LoggerUtil.log("✅ 4.2 dim_product (product_id duy nhất + created_at) → warehouse_db HOÀN TẤT\n");

            // 4.3 Load fact_product_price_daily
            LoggerUtil.log("▶ 4.3 Bắt đầu load fact_product_price_daily...");
            int count43 = LoadFactProductPriceDaily.load();
            totalRecords += count43;
            LoggerUtil.log("✅ 4.3 fact_product_price_daily (GIÁ THẬT) → warehouse_db HOÀN TẤT\n");

            // ==================== SCRIPT 5: AGGREGATE ====================
            LoggerUtil.log("=== SCRIPT 5: TẠO AGGREGATE TABLES (GIÁ) ===\n");

            // 5.1 Create agg_daily_sales
            LoggerUtil.log("▶ 5.1a Bắt đầu tạo agg_daily_sales...");
            int count51 = CreateAggregateDailySales.load();
            totalRecords += count51;
            LoggerUtil.log("✅ 5.1a agg_daily_sales (avg/min/max price) → warehouse_db HOÀN TẤT\n");

            // 5.2 Create agg_daily_summary
            LoggerUtil.log("▶ 5.2a Bắt đầu tạo agg_daily_summary...");
            int count52 = CreateAggregateDailySummary.load();
            totalRecords += count52;
            LoggerUtil.log("✅ 5.2a agg_daily_summary (avg/min/max price) → warehouse_db HOÀN TẤT\n");

            // ==================== THÀNH CÔNG ====================
            LoggerUtil.log("✅ SCRIPT 4 & 5 HOÀN THÀNH THÀNH CÔNG");


            // ==================== GHI LOG KẾT THÚC ====================
            LoggerUtil.endProcess(totalRecords, "SUCCESS", null);

        } catch (Exception e) {
            LoggerUtil.log("❌ LỖI SCRIPT 4 & 5: " + e.getMessage());
            if (runId != null) {
                LoggerUtil.endProcess(totalRecords, "FAILED", e.getMessage());
            }
            e.printStackTrace();
            System.exit(1);
        }
    }
}