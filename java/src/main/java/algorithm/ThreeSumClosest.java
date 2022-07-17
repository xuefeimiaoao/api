package algorithm;

import java.util.Arrays;
import java.util.HashMap;

public class ThreeSumClosest {
    public static void main(String[] args) {
        Solution solution = new Solution();
        System.out.println("12".substring(0));

    }
}

class Solution {
    public int threeSumClosest(int[] nums, int target) {

        // -4 -1 1 2 4
        int[] sorted = nums;
        Arrays.sort(sorted);
        int ret = 0;

        for (int i = 0; i < sorted.length - 2; i++) {
            int first = sorted[i];
            int thirdIndex = sorted.length - 1;
            for (int j = i + 1; j < sorted.length - 1; j++) {
                if (j >= thirdIndex) {
                    break;
                }
                int sum = first + sorted[j] + sorted[thirdIndex];
                if (sum == target) {
                    return sum;
                } else if (sum > target) {
                    while ((sum = first + sorted[j] + sorted[thirdIndex]) > target) {
                        if (Math.abs(sum - target) < Math.abs(ret - target)) {
                            ret = sum;
                        }
                        thirdIndex--;
                    }
                } else {
                    while ((sum = first + sorted[j] + sorted[thirdIndex]) < target) {
                        if (Math.abs(sum - target) < Math.abs(ret - target)) {
                            ret = sum;
                        }
                        j++;
                    }
                }
            }
        }
        return ret;
    }
}
