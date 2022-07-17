package algorithm;

import java.util.regex.*;

public class PatternMatch {
    public static void main(String[] args) {
        String p = "\\d";
        char i = '5';

        System.out.println(Pattern.matches(p, i + ""));
    }
}
