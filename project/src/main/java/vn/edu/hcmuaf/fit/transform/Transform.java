package vn.edu.hcmuaf.fit.transform;

import vn.edu.hcmuaf.fit.util.LoggerUtil;

import static vn.edu.hcmuaf.fit.load_data.LoadCleanToDB.clean;
import static vn.edu.hcmuaf.fit.transform.LoadDimProduct.loadDim;
import static vn.edu.hcmuaf.fit.transform.LoadFactProductSnapshot.loadFactSnapshot;

public class Transform {
    public static void main(String[] args) {
        String runId = null;
        int dim = 0;
        int fact = 0;
        int clean = 0;

        try {
            // Clean data
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, "Clean data");
            if (runId == null) {
                throw new Exception("Không thể khởi tạo Run ID hoặc kết nối Control DB bị lỗi.");
            }
            LoggerUtil.log("Bắt đầu thực thi CLEAN DATA.");

            clean = clean();

            LoggerUtil.endProcess(clean, "SUCCESS", null);
            LoggerUtil.log("✅ Clean & Load hoàn tất, tổng bản ghi sạch: " + clean);

            // Load dim
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, "Load dim_product");
            if (runId == null) {
                throw new Exception("Không thể khởi tạo Run ID hoặc kết nối Control DB bị lỗi.");
            }
            LoggerUtil.log("Bắt đầu thực thi Script 3");

            dim = loadDim();

            LoggerUtil.endProcess(dim, "SUCCESS", null);
            LoggerUtil.log("Load Dim Product hoàn tất. Tổng bản ghi được xử lý: " + dim);

            // Load fact
            runId = LoggerUtil.startProcess(LoggerUtil.SOURCE_CELLPHONES_ID, "Load fact_product_snapshot");
            if (runId == null) {
                throw new Exception("Không thể khởi tạo Run ID hoặc kết nối Control DB bị lỗi.");
            }
            LoggerUtil.log("Bắt đầu thực thi Script 3");

            fact = loadFactSnapshot();

            LoggerUtil.endProcess(fact, "SUCCESS", null);
            LoggerUtil.log("Load Dim Product hoàn tất. Tổng bản ghi được xử lý: " + fact);

        } catch (Exception e) {
            if (runId != null) {
                LoggerUtil.endProcess(dim+fact+clean, "FAILED", "Lỗi Load Dim Product: " + e.getMessage());
            }
            System.err.println("[ERROR] Load dim_product failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
