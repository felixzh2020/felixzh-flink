import java.io.Serializable;

public class HBaseData implements Serializable {
    private static final long serialVersionUID = 1L;
    private byte[] rowKey;
    private byte[] columnFamily;
    private byte[] cfQualifier;
    private byte[] data;

    public HBaseData() {
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(byte[] columnFamily) {
        this.columnFamily = columnFamily;
    }

    public byte[] getCfQualifier() {
        return cfQualifier;
    }

    public void setCfQualifier(byte[] cfQualifier) {
        this.cfQualifier = cfQualifier;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
