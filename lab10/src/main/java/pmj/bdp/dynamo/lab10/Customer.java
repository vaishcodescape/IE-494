package pmj.bdp.dynamo.lab10;

import java.math.BigDecimal;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/** Customer profile item (TPC-H CUSTOMER row). pk = CUST#custkey, sk = PROFILE. */
@DynamoDbBean
public class Customer {

    private String pk;
    private String sk;
    private String type;
    private Long custkey;
    private String name;
    private String address;
    private Integer nationkey;
    private String phone;
    private BigDecimal acctbal;
    private String mktsegment;
    private String comment;

    public Customer() {
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

    public Long getCustkey() {
        return custkey;
    }

    public void setCustkey(Long custkey) {
        this.custkey = custkey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Integer getNationkey() {
        return nationkey;
    }

    public void setNationkey(Integer nationkey) {
        this.nationkey = nationkey;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public BigDecimal getAcctbal() {
        return acctbal;
    }

    public void setAcctbal(BigDecimal acctbal) {
        this.acctbal = acctbal;
    }

    public String getMktsegment() {
        return mktsegment;
    }

    public void setMktsegment(String mktsegment) {
        this.mktsegment = mktsegment;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
