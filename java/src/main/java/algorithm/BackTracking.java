package algorithm;

import java.util.*;

class BackTracking {
    public static void main(String[] args) {
        BackTracking executor = new BackTracking();
        System.out.println(executor.permuteUnique(new int[]{1, 1, 2, 2}));
    }
    public List<List<Integer>> permuteUnique(int[] nums) {
        List<List<Integer>> ret = new ArrayList();
        List<Integer> candidates = new ArrayList();
        HashMap<Integer, Integer> chosenMap = new HashMap();
        HashMap<Integer, Integer> limitMap = new HashMap();
        for (int i = 0; i < nums.length; i++) {
            int c = limitMap.getOrDefault(nums[i], 0);
            limitMap.put(nums[i], c + 1);
        }
        backTracking(nums, ret, candidates, chosenMap, limitMap);
        return ret;
    }

    public void backTracking(int[] nums, List<List<Integer>> ret, List<Integer> candidates, HashMap<Integer, Integer> chosenMap, HashMap<Integer, Integer> limitMap) {
        if (candidates.size() == nums.length) {
            ret.add(new ArrayList(candidates));
            return;
        }

        List<Integer> prune = new ArrayList();


        for (int i = 0; i < nums.length; i++) {
            if (prune.contains(nums[i])) {
                continue;
            }
            // notice that we should not put here, in case that the value is chosen before in chose[]
            prune.add(nums[i]);
            if (chosenMap.get(nums[i])==limitMap.get(nums[i])) {
                continue;
            } else {
                //prune.add(nums[i]);
                candidates.add(nums[i]);
                int va = chosenMap.getOrDefault(nums[i], 0);
                chosenMap.put(nums[i], va + 1);
                backTracking(nums, ret, candidates, chosenMap, limitMap);
                candidates.remove(candidates.lastIndexOf(nums[i]));
                chosenMap.compute(nums[i], (k, v) -> --v);
            }
        }
    }
}
