package pmj.bdp.dynamo.lab10;

import java.math.BigDecimal;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

/** One TPC-H LINEITEM row, stored as a map inside the order item's "lineitems" list. */
@DynamoDbBean
public class LineItem {

    private Integer linenumber;
    private Long partkey;
    private Long suppkey;
    private BigDecimal quantity;
    private BigDecimal extendedprice;
    private BigDecimal discount;
    private BigDecimal tax;
    private String returnflag;
    private String linestatus;
    private String shipdate;
    private String commitdate;
    private String receiptdate;
    private String shipinstruct;
    private String shipmode;
    private String comment;

    public LineItem() {
    }

    public Integer getLinenumber() {
        return linenumber;
    }

    public void setLinenumber(Integer linenumber) {
        this.linenumber = linenumber;
    }

    public Long getPartkey() {
        return partkey;
    }

    public void setPartkey(Long partkey) {
        this.partkey = partkey;
    }

    public Long getSuppkey() {
        return suppkey;
    }

    public void setSuppkey(Long suppkey) {
        this.suppkey = suppkey;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public void setQuantity(BigDecimal quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getExtendedprice() {
        return extendedprice;
    }

    public void setExtendedprice(BigDecimal extendedprice) {
        this.extendedprice = extendedprice;
    }

    public BigDecimal getDiscount() {
        return discount;
    }

    public void setDiscount(BigDecimal discount) {
        this.discount = discount;
    }

    public BigDecimal getTax() {
        return tax;
    }

    public void setTax(BigDecimal tax) {
        this.tax = tax;
    }

    public String getReturnflag() {
        return returnflag;
    }

    public void setReturnflag(String returnflag) {
        this.returnflag = returnflag;
    }

    public String getLinestatus() {
        return linestatus;
    }

    public void setLinestatus(String linestatus) {
        this.linestatus = linestatus;
    }

    public String getShipdate() {
        return shipdate;
    }

    public void setShipdate(String shipdate) {
        this.shipdate = shipdate;
    }

    public String getCommitdate() {
        return commitdate;
    }

    public void setCommitdate(String commitdate) {
        this.commitdate = commitdate;
    }

    public String getReceiptdate() {
        return receiptdate;
    }

    public void setReceiptdate(String receiptdate) {
        this.receiptdate = receiptdate;
    }

    public String getShipinstruct() {
        return shipinstruct;
    }

    public void setShipinstruct(String shipinstruct) {
        this.shipinstruct = shipinstruct;
    }

    public String getShipmode() {
        return shipmode;
    }

    public void setShipmode(String shipmode) {
        this.shipmode = shipmode;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    /** extendedprice * (1 - discount) * (1 + tax): what the customer pays for this line. */
    BigDecimal netValue() {
        return extendedprice.multiply(BigDecimal.ONE.subtract(discount))
                .multiply(BigDecimal.ONE.add(tax));
    }
}
