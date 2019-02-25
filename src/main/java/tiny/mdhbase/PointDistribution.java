package tiny.mdhbase;

import java.util.Arrays;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/2/24
 */
public class PointDistribution {

    private long childSizeA;

    private long childSizeB;

    private byte[] key;

    private int prefixLength;

    public long getChildSizeA() {
        return childSizeA;
    }

    public void setChildSizeA(long childSizeA) {
        this.childSizeA = childSizeA;
    }

    public long getChildSizeB() {
        return childSizeB;
    }

    public void setChildSizeB(long childSizeB) {
        this.childSizeB = childSizeB;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public int getPrefixLength() {
        return prefixLength;
    }

    public void setPrefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
    }

    @Override
    public String toString() {
        return "PointDistribution{" +
                "childSizeA=" + childSizeA +
                ", childSizeB=" + childSizeB +
                ", key=" + Arrays.toString(key) +
                ", prefixLength=" + prefixLength +
                '}';
    }
}
