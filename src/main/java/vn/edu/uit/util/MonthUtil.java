package vn.edu.uit.util;

import java.util.Optional;

public final class MonthUtil {

    private MonthUtil() {
        super();
    }

    public static Optional<String> getMonth(String string) {
        final String[] parts = string.split("-");
        if (parts.length > 0) {
            return Optional.of(parts[0]);
        }
        return Optional.empty();
    }
}
