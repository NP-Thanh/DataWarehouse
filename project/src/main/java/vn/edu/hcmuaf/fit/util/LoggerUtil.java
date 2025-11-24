package vn.edu.hcmuaf.fit.util;

import vn.edu.hcmuaf.fit.db.ControlDBConfig;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class LoggerUtil {

    public static final int SOURCE_CELLPHONES_ID = 1;
    public static final String OPERATOR_ETL_JOB = "ETL_Scheduler_01";
    private static final StringBuilder logBuilder = new StringBuilder();

    private static final ThreadLocal<String> currentRunId = new ThreadLocal<>();
    private static final String DATE_FORMAT = "dd_MM_yy";

    public static void log(String message) {
        String time = new SimpleDateFormat("HH:mm:ss").format(new Date());
        String runId = currentRunId.get() != null ? currentRunId.get().substring(0, 8) : "N/A";
        String full = "[" + time + "] [RUN_ID:" + runId + "] " + message;
        System.out.println(full);
        logBuilder.append(full).append("\n");
    }

    public static String startProcess(int sourceId, String operator) {
        String runId = UUID.randomUUID().toString();
        currentRunId.set(runId);

        String sql = """
            INSERT INTO log (run_id, source_id, start_time, status, operator)
            VALUES (?, ?, NOW(), 'RUNNING', ?)
        """;

        try (Connection conn = ControlDBConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, runId);
            ps.setInt(2, sourceId);
            ps.setString(3, operator);
            ps.executeUpdate();

            log("Tiến trình BẮT ĐẦU với Run ID: " + runId);
        } catch (SQLException e) {
            log("Lỗi khi khởi tạo tiến trình vào Control DB: " + e.getMessage());
            currentRunId.remove();
            return null;
        }
        return runId;
    }

    public static void endProcess(int recordCount, String status, String errorMessage) {
        String runId = currentRunId.get();
        if (runId == null) {
            log("Không tìm thấy Run ID hiện tại. Kết thúc không được ghi nhận.");
            return;
        }

        String sql = """
            UPDATE log
            SET end_time = NOW(), status = ?, record_count = ?, error_message = ?
            WHERE run_id = ?
        """;

        try (Connection conn = ControlDBConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, status);
            ps.setInt(2, recordCount);
            ps.setString(3, errorMessage != null ? errorMessage : "");
            ps.setString(4, runId);
            ps.executeUpdate();

            if ("SUCCESS".equals(status)) {
                log("Tiến trình KẾT THÚC THÀNH CÔNG. Bản ghi: " + recordCount);
                updateSourceLastRunTime(runId, ControlDBConfig.getConnection());
            } else {
                log("Tiến trình KẾT THÚC THẤT BẠI. Lỗi: " + errorMessage);
            }
        } catch (SQLException e) {
            log("Lỗi khi cập nhật kết quả tiến trình vào Control DB: " + e.getMessage());
        } finally {
            currentRunId.remove();
        }
    }

    /**
     * Tạo bảng script_log nếu chưa tồn tại
     */
    public static void createScriptLogTableIfNotExists() {
        String sql = """
            CREATE TABLE IF NOT EXISTS script_log (
                script_log_id INT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(36) NOT NULL,
                script_step VARCHAR(10) NOT NULL,
                script_name VARCHAR(100) NOT NULL,
                record_count INT DEFAULT 0,
                duration_ms BIGINT DEFAULT 0,
                status VARCHAR(20) NOT NULL,
                error_message TEXT,
                log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_run_id (run_id),
                INDEX idx_script_step (script_step),
                INDEX idx_log_time (log_time),
                FOREIGN KEY (run_id) REFERENCES log(run_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """;

        try (Connection conn = ControlDBConfig.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            log("✅ Bảng script_log đã được kiểm tra/tạo");
        } catch (SQLException e) {
            log("⚠️ Không thể tạo bảng script_log: " + e.getMessage());
        }
    }

    /**
     * Ghi log chi tiết cho từng bước con của script (4.1, 4.2, 4.3, 5.1, 5.2)
     */
    public static void logStep(String scriptStep, String scriptName, int recordCount, long duration, String status, String errorMessage) {
        String runId = currentRunId.get();
        if (runId == null) {
            log("⚠️ Không có Run ID - bước " + scriptStep + " không được ghi log vào database");
            return;
        }

        String sql = """
            INSERT INTO script_log (run_id, script_step, script_name, record_count, duration_ms, status, error_message, log_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
        """;

        try (Connection conn = ControlDBConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, runId);
            ps.setString(2, scriptStep);
            ps.setString(3, scriptName);
            ps.setInt(4, recordCount);
            ps.setLong(5, duration);
            ps.setString(6, status);
            ps.setString(7, errorMessage != null ? errorMessage : "");
            ps.executeUpdate();

            log("📝 Script " + scriptStep + " ghi log: " + status + " (" + recordCount + " records, " + duration + "ms)");
        } catch (SQLException e) {
            log("⚠️ Lỗi ghi log bước " + scriptStep + ": " + e.getMessage());
        }
    }

    private static void updateSourceLastRunTime(String runId, Connection connection) {
        String updateSql = """
            UPDATE source s
            JOIN log l ON s.source_id = l.source_id
            SET s.last_run_time = l.end_time
            WHERE l.run_id = ? AND l.status = 'SUCCESS'
        """;
        try (PreparedStatement ps = connection.prepareStatement(updateSql)) {
            ps.setString(1, runId);
            ps.executeUpdate();
        } catch (SQLException e) {
            log("Lỗi cập nhật last_run_time: " + e.getMessage());
        }
    }

    public static String exportConfigFile(String dateStr) {
        String configFileName = dateStr + "_config.txt";
        String fullPath = configFileName;

        String lastRunTime = getLastRunTime(SOURCE_CELLPHONES_ID);

        try (FileWriter fw = new FileWriter(fullPath)) {
            fw.write("# --- ETL Configuration File for Cellphones --- \n");
            fw.write("# Generated on: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
            fw.write("# Lưu trữ các tham số quan trọng cho Scheduler.\n\n");

            fw.write("LAST_SUCCESSFUL_RUN_TIME=" + (lastRunTime != null ? lastRunTime : "1900-01-01 00:00:00") + "\n");
            fw.write("CURRENT_RUN_ID=" + currentRunId.get() + "\n");
            fw.write("STAGING_CSV_FILE=" + dateStr + "_products.csv\n");

            log("Đã xuất file cấu hình: " + fullPath);
            return fullPath;
        } catch (IOException e) {
            log("Lỗi khi xuất file cấu hình: " + e.getMessage());
            return null;
        }
    }

    private static String getLastRunTime(int sourceId) {
        String sql = "SELECT last_run_time FROM source WHERE source_id = ?";
        try (Connection conn = ControlDBConfig.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, sourceId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getTimestamp("last_run_time") != null) {
                    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rs.getTimestamp("last_run_time"));
                }
            }
        } catch (SQLException e) {
            log("Lỗi lấy last_run_time: " + e.getMessage());
        }
        return null;
    }
    // Hàm này để xuất toàn bộ log ra file (giống hệt mấy bạn 10 điểm)
    public static String getFullLog() {
        return logBuilder.toString();
    }
}