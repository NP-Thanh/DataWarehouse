package vn.edu.hcmuaf.fit;

import vn.edu.hcmuaf.fit.crawl.Extract;
import vn.edu.hcmuaf.fit.db.DatabaseConfig;
import vn.edu.hcmuaf.fit.load_data.LoadToDB;
import vn.edu.hcmuaf.fit.mart.LoadToMart;
import vn.edu.hcmuaf.fit.transform.*;
import vn.edu.hcmuaf.fit.util.LoggerUtil;

public class Main {
    public static void main(String[] args) {
        try {
            LoggerUtil.log("=== BẮT ĐẦU TOÀN BỘ ETL PIPELINE CELLPHONES ===");

            // ── Script 1: Extract ─────────────────────────────────────
            LoggerUtil.log("=== SCRIPT 1: EXTRACT DỮ LIỆU TỪ CELLPHONES ===");
            String csvFile = Extract.crawlToCSV();
            LoggerUtil.log("Script 1 HOÀN TẤT - File: " + csvFile);

            // ── Script 2: Load to Staging ─────────────────────────────
            LoggerUtil.log("=== SCRIPT 2: LOAD TO STAGING ===");
            DatabaseConfig.createTableIfNotExists();
            LoadToDB.load(csvFile);
            LoggerUtil.log("Script 2 HOÀN TẤT");

            // ── Script 4: Load to Warehouse (SCD Type 2) ──────────────
            LoggerUtil.log("=== SCRIPT 4: LOAD TO WAREHOUSE (DIM + FACT) ===");
            LoadDimDateToWarehouse.load();
            LoadDimProductToWarehouse.load();
            LoadFactProductPriceDaily.load();
            LoggerUtil.log("Script 4 HOÀN TẤT");

            // ── Script 5: Create Aggregate Tables ─────────────────────
            LoggerUtil.log("=== SCRIPT 5: TẠO AGGREGATE TABLES ===");
            CreateAggregateDailySales.load();
            CreateAggregateDailySummary.load();
            LoggerUtil.log("Script 5 HOÀN TẤT");

            // ── Script 6: Load to Data Mart
            LoggerUtil.log("=== SCRIPT 6: LOAD TO DATA MART (PRESENTATION LAYER) ===");
            LoadToMart.load();
            LoggerUtil.log("Script 6 HOÀN TẤT - DATA MART SẴN SÀNG");

            LoggerUtil.log("=== TẤT CẢ 6 SCRIPT HOÀN THÀNH 100% ===");

        } catch (Exception e) {
            LoggerUtil.log("LỖI PIPELINE: " + e.getMessage());
            e.printStackTrace();
        }
    }
}