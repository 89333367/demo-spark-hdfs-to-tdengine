package sunyu.demo.util;

public class MyHash {
    public static int hash(String key) {
        int hash = 0;
        int length = key.length();
        for (int i = 0; i < length; i++) {
            hash += key.charAt(i);
            hash += (hash << 10);
            hash ^= (hash >>> 6);
        }
        hash += (hash << 3);
        hash ^= (hash >>> 11);
        hash += (hash << 15);
        return hash;
    }
}
