package tiny.mdhbase;

/**
 * @Author : guoyang
 * @Description :
 * @Date : Created on 2019/2/24
 */
public class PointDistribution {

    private int totalSize;

    private int childSizeA;

    private int childSizeB;

    private byte[] key;

    private int prefixLength;

    public int getChildSizeA() {
        return childSizeA;
    }

    public void setChildSizeA(int childSizeA) {
        this.childSizeA = childSizeA;
    }

    public int getChildSizeB() {
        return childSizeB;
    }

    public void setChildSizeB(int childSizeB) {
        this.childSizeB = childSizeB;
    }

    public int getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(int totalSize) {
        this.totalSize = totalSize;
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
}
