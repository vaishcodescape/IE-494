package pmj.bdp.dynamo.lab10;

import java.math.BigDecimal;
import java.util.List;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/**
 * Order item (TPC-H ORDERS row) with all of its LINEITEM rows embedded.
 * pk = CUST#custkey, sk = ORDER#orderdate#orderkey. orderkey is the partition key
 * of the global secondary index OrderIndex, so an order can also be found by O_ORDERKEY alone.
 */
@DynamoDbBean
public class Order {

    private String pk;
    private String sk;
    private String type;
    private Long orderkey;
    private Long custkey;
    private String orderstatus;
    private BigDecimal totalprice;
    private String orderdate;
    private Integer year;
    private String orderpriority;
    private String clerk;
    private Integer shippriority;
    private String comment;
    private List<LineItem> lineitems;

    public Order() {
    }

    @DynamoDbPartitionKey
    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    @DynamoDbSortKey
    public String getSk() {
        return sk;
    }

    public void setSk(String sk) {
        this.sk = sk;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = Keys.ORDER_INDEX)
    public Long getOrderkey() {
        return orderkey;
    }

    public void setOrderkey(Long orderkey) {
        this.orderkey = orderkey;
    }

    public Long getCustkey() {
        return custkey;
    }

    public void setCustkey(Long custkey) {
        this.custkey = custkey;
    }

    public String getOrderstatus() {
        return orderstatus;
    }

    public void setOrderstatus(String orderstatus) {
        this.orderstatus = orderstatus;
    }

    public BigDecimal getTotalprice() {
        return totalprice;
    }

    public void setTotalprice(BigDecimal totalprice) {
        this.totalprice = totalprice;
    }

    public String getOrderdate() {
        return orderdate;
    }

    public void setOrderdate(String orderdate) {
        this.orderdate = orderdate;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getOrderpriority() {
        return orderpriority;
    }

    public void setOrderpriority(String orderpriority) {
        this.orderpriority = orderpriority;
    }

    public String getClerk() {
        return clerk;
    }

    public void setClerk(String clerk) {
        this.clerk = clerk;
    }

    public Integer getShippriority() {
        return shippriority;
    }

    public void setShippriority(Integer shippriority) {
        this.shippriority = shippriority;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public List<LineItem> getLineitems() {
        return lineitems;
    }

    public void setLineitems(List<LineItem> lineitems) {
        this.lineitems = lineitems;
    }
}
