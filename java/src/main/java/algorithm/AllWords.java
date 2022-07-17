package algorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class AllWords {
    public static void main(String[] args) {
        String str1 = "barfoothefoobarman";
        String[] strs1 = {"foo", "bar"};
        String str = "wordgoodgoodgoodbestword";
        String[] strs = {"word","good","best","good"};
        String str3 = "lingmindraboofooowingdingbarrwingmonkeypoundcake";
        String[] strs3 = new String[]{"fooo","barr","wing","ding","wing"};

        String str4 = "ababababab";
        String[] strs4 = new String[]{"ababa","babab"};

        String str5 = "aaaaaaaaaaaaaa";
        String[] strs5 = new String[]{"aa","aa"};

        AllWords executor = new AllWords();
//        System.out.println(executor.findSubstring(str1, strs1) + "; expect: 0,9");
//        System.out.println(executor.findSubstring(str, strs) + "; expect: 8");
//        System.out.println(executor.findSubstring(str3, strs3) + "; expect: 13");
//        System.out.println(executor.findSubstring(str4, strs4) + "; expect: 0");
        System.out.println(executor.findSubstring(str5, strs5) + "; expect: [0,1,2,3,4,5,6,7,8,9,10]");
    }
    public List<Integer> findSubstring(String s, String[] words) {
        HashMap<String, Integer> map = new HashMap();
        int wordsLen = words.length;

        int wordLen = words[0].length();
        if (s.length() < wordLen * wordsLen) {
            return new ArrayList();
        }
        List<Integer> ret = new ArrayList();
        // 注意，这边不能直接写-1，因为初始的时候也可能存在重复元素
        for (int i = 0; i < wordLen; i++) {
            // 注意，需要clear。
            map.clear();
            for (int j = 0; j < wordsLen; j++) {
                if (map.containsKey(words[j])) {
                    map.compute(words[j], (k, v) -> --v);
                } else {
                    map.putIfAbsent(words[j], -1);
                }
            }
            // 数组越界，如果i
            // 前N个
            int index = i;
            // first不能错
            String first = s.substring(index, index + wordLen);
            String last;
            // todo 如果j的第一个值就不满足会走循环吗
            for (int j = i; j <= i + wordLen * (wordsLen - 1) && j + wordLen <= s.length(); j += wordLen) {
                String str = s.substring(j, j + wordLen);
                map.computeIfPresent(str, (k, v) -> ++v);
            }
            if (map.values().stream().noneMatch(v -> v != 0)) {
                // 全是0
                ret.add(i);
            }

            index = i;
            while (true) {
                index += wordLen;
                if (index + wordLen * wordsLen > s.length()) {
                    break;
                }
                map.computeIfPresent(first, (k, v) -> --v);
                first = s.substring(index, index + wordLen);
                last = s.substring(index + wordLen * (wordsLen - 1), index + wordLen * wordsLen);
                map.computeIfPresent(last, (k, v) -> ++v);
                if (map.values().stream().noneMatch(v -> v != 0)) {
                    // 全是0
                    ret.add(index);
                }
            }
        }
        return ret;
    }
}
