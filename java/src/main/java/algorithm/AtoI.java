package algorithm;
import java.util.*;
import java.util.regex.Pattern;

public class AtoI {
    public static void main(String[] args) {
        AtoI executor = new AtoI();
        System.out.println(executor.myAtoi(" -41"));
        String str = "0123456789 ";
        byte[] bytes = str.getBytes();

        System.out.println(bytes);
    }


    public int myAtoi(String s) {
        char[] input = s.toCharArray();
        boolean readDigit = false;
        int startIndex = 0;
        int endIndex = input.length - 1;
        boolean isPositive = false;
        String p = "\\d";

        for (int i = 0; i < input.length; i++) {
            if (!readDigit) {
                // start to remove prefix
                if (!Pattern.matches(p, input[i] + "")) {
                    startIndex++;
                } else {
                    // start to read digit
                    readDigit = true;
                }
            } else {
                // read digit util encoutering
                if (!Pattern.matches(p, input[i] + "")) {
                    endIndex = i;
                    break;
                }
            }
        }

        if (startIndex == 0) {
            isPositive = true;
        } else if (startIndex > 0
                && (input[startIndex - 1] == '+' || input[startIndex - 1] != '-')) {
            isPositive = true;
        }

        String outputStr = (isPositive ? "" : "-") + new String(Arrays.copyOfRange(input, startIndex, endIndex + 1));
        try {
            int ret = Integer.parseInt(outputStr);
            return ret;
        } catch(Exception e) {
            if (isPositive) return Integer.MAX_VALUE;
            else return Integer.MIN_VALUE;
        }

    }

}
