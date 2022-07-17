package algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FourSum {
    public static void main(String[] args) {
        FourSum executor = new FourSum();
        //System.out.println(executor.fourSum(new int[]{-3, -1, 0, 2, 4, 5}, 2));
        System.out.println(executor.fourSum(new int[]{1000000000,1000000000,1000000000,1000000000}, -294967296));
    }


    public List<List<Integer>> fourSum(int[] nums, int target) {
        if (nums.length < 4) {
            return new ArrayList();
        }
        Arrays.sort(nums);
        List<List<Integer>> ret = new ArrayList();
        int first = nums[0];
        for (int i = 0; i < nums.length; i++) {
            if (i > 0 && first == nums[i]) {
                continue;
            } else {
                first = nums[i];
            }
            int second = nums[0];
            for (int j = i + 1; j < nums.length; j++) {
                if (j > i + 1 && second == nums[j]) {
                    continue;
                } else {
                    second = nums[j];
                }
                int right = nums.length - 1;

                // 注意，最好写成while循环
                int left = j + 1;
                int leftVal = nums[0];
                while (left < right) {
                    if (left > j + 1 && nums[left] == leftVal) {
                        left++;
                        continue;
                    } else {
                        leftVal = nums[left];
                    }

                    long sum = Long.valueOf(nums[i]) + Long.valueOf(nums[j]) + Long.valueOf(nums[left]) + Long.valueOf(nums[right]);
                    if (sum == target) {
                        ret.add(Arrays.asList(nums[i], nums[j], nums[left], nums[right]));
                        if (nums[left] == nums[right]) {
                            break;
                        }
                        left++;
                    } else if (sum > target) {
                        right--;
                    } else {
                        left++;
                    }

                }
            }
        }
        return ret;
    }

}