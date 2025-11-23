package vn.edu.hcmuaf.fit.util;

import java.math.BigDecimal;

public class CleanUtil {
    //Làm sạch và chuẩn hóa tên product
    public static String cleanProductName(String raw) {
        if (raw == null) return null;

        String cleaned = raw;

        //Danh sách các chuỗi rác cố định
        String[] garbage = {
                "Chỉ có tại CellphoneS",
                "Chỉ bán online",
                "Giá tốt",
                "Hot sale",
                "Giảm giá sốc",
                "Hotsale",
                "Giá rẻ mỗi ngày",
                "Ưu đãi",
                "New",
                "HOT",
                "- Chính hãng",
                "Chính hãng",
                "VN/A",
                "ĐKH Online",
                "NFC",
                "bản đặc biệt"
        };

        for (String g : garbage) {
            cleaned = cleaned.replace(g, "");
        }

        //Xoá các ký tự phân tách dư thừa như | - /
        cleaned = cleaned.replaceAll("[|\\-/]", " ");

        //Xoá mọi nội dung trong ngoặc ()
        cleaned = cleaned.replaceAll("\\(.*?\\)", "");

        //Giữ dung lượng GB/TB đầu tiên, xoá các dung lượng sau
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\b\\d+\\s*(GB|TB)\\b", java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = pattern.matcher(cleaned);

        String firstSize = null;
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            if (firstSize == null) {
                firstSize = matcher.group(); // giữ dung lượng đầu tiên
            } else {
                matcher.appendReplacement(sb, ""); // xoá các dung lượng sau
            }
        }
        matcher.appendTail(sb);
        cleaned = sb.toString();

        //Loại bỏ nhiều khoảng trắng thành 1 khoảng trắng
        cleaned = cleaned.replaceAll("\\s+", " ").trim();

        return cleaned;
    }

    //Làm sạch và chuẩn hóa giá
    public static BigDecimal cleanPrice(String price) {
        if (price == null || price.isEmpty()) return BigDecimal.ZERO;
        //Loại bỏ các kí tự không phải số
        String cleaned = price.replaceAll("[^0-9]", "");
        if (cleaned.isEmpty()) return BigDecimal.ZERO;
        return new BigDecimal(cleaned);
    }

    //Làm sạch và chuẩn hóa ngày
    public static java.sql.Date cleanDate(String rawDate) {
        if (rawDate == null || rawDate.isEmpty()) return null;
        // chuẩn hóa ngày thành dạng yy-mm-dd
        String dateStr = rawDate.substring(0, 10).replace("/", "-");
        return java.sql.Date.valueOf(dateStr);
    }
}
