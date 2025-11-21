package vn.edu.hcmuaf.fit.util;

import vn.edu.hcmuaf.fit.db.ControlDBConfig;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class LoggerUtil {

    public static final int SOURCE_CELLPHONES_ID = 1; // ID cố định cho nguồn Cellphones
    public static final String OPERATOR_ETL_JOB = "ETL_Scheduler_01";
    public static final String PATH_CONFIG = "T:/DataWarehouse_github/DataWarehouse/project/"; // Folder lưu file config theo ngày

    private static final ThreadLocal<String> currentRunId = new ThreadLocal<>();

    private static final String DATE_FORMAT = "dd_MM_yy";


    public static void log(String message) {
        String time = new SimpleDateFormat("HH:mm:ss").format(new Date());
        String runId = currentRunId.get() != null ? currentRunId.get().substring(0, 8) : "N/A";
        String full = "[" + time + "] [RUN_ID:" + runId + "] " + message;
        System.out.println(full);
    }

    public static String startProcess(int sourceId, String operator) {
        String runId = UUID.randomUUID().toString();
        currentRunId.set(runId); // Lưu run_id vào ThreadLocal

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
            // Trả về null để báo hiệu lỗi
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
                // Cập nhật last_run_time cho bảng source nếu là SUCCESS
                updateSourceLastRunTime(runId, ControlDBConfig.getConnection());
            } else {
                log("Tiến trình KẾT THÚC THẤT BẠI. Lỗi: " + errorMessage);
            }
        } catch (SQLException e) {
            log("Lỗi khi cập nhật kết quả tiến trình vào Control DB: " + e.getMessage());
        } finally {
            currentRunId.remove(); // Xóa run_id khỏi ThreadLocal khi job kết thúc
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

        // 1. Lấy thông tin cần thiết (ví dụ: last_run_time) từ Control DB
        String lastRunTime = getLastRunTime(SOURCE_CELLPHONES_ID);

        // 2. Ghi nội dung vào file
        try (FileWriter fw = new FileWriter(fullPath)) {
            fw.write("# --- ETL Configuration File for Cellphones --- \n");
            fw.write("# Generated on: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
            fw.write("# Lưu trữ các tham số quan trọng cho Scheduler.\n\n");

            // Tham số quan trọng 1: Thời điểm chạy cuối cùng thành công
            fw.write("LAST_SUCCESSFUL_RUN_TIME=" + (lastRunTime != null ? lastRunTime : "1900-01-01 00:00:00") + "\n");

            // Tham số quan trọng 2: ID của lần chạy này (dùng để truy vết)
            fw.write("CURRENT_RUN_ID=" + currentRunId.get() + "\n");

            // Tham số quan trọng 3: Đường dẫn file CSV staging
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
}