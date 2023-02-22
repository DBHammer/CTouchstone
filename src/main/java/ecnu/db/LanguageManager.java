package ecnu.db;

import java.util.Locale;
import java.util.ResourceBundle;

public class LanguageManager {
    private static final LanguageManager INSTANCE = new LanguageManager();
    //private final Locale lc = Locale.getDefault();
    private final Locale lc = new Locale("zh", "CN");
    private final ResourceBundle rb = ResourceBundle.getBundle("messageResource", lc);

    public static LanguageManager getInstance() {
        return INSTANCE;
    }

    public ResourceBundle getRb() {
        return rb;
    }
}
