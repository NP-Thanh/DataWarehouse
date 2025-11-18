package vn.edu.hcmuaf.fit.util;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoggerUtil {
    static String dateStr = new SimpleDateFormat("dd_MM_yy").format(new Date());
    private static final String LOG_FILE = dateStr +  "_log.txt";

    public static void log(String message) {
        String time = new SimpleDateFormat("HH:mm:ss").format(new Date());
        String full = "[" + time + "] " + message;
        System.out.println(full);
        try (FileWriter fw = new FileWriter(LOG_FILE, true)) {
            fw.write(full + "\n");
        } catch (IOException ignored) {}
    }
}

