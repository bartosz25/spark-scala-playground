package com.waitingforcode.sql.avro;

import java.util.List;
import java.util.Map;

public class Order {
    private Long id;
    private String creationDate;
    private Integer promotionalCode;
    private List<Product> products;
    private Map<String, String> extraInfo;
    private Customer customer;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public Integer getPromotionalCode() {
        return promotionalCode;
    }

    public void setPromotionalCode(Integer promotionalCode) {
        this.promotionalCode = promotionalCode;
    }

    public List<Product> getProducts() {
        return products;
    }

    public void setProducts(List<Product> products) {
        this.products = products;
    }

    public Map<String, String> getExtraInfo() {
        return extraInfo;
    }

    public void setExtraInfo(Map<String, String> extraInfo) {
        this.extraInfo = extraInfo;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public static Order valueOf(Long id, String creationDate, Integer promotionalCode, List<Product> products,
                                Map<String, String> extraInfo, Customer customer) {
        Order order = new Order();
        order.setId(id);
        order.setCreationDate(creationDate);
        order.setPromotionalCode(promotionalCode);
        order.setProducts(products);
        order.setExtraInfo(extraInfo);
        order.setCustomer(customer);
        return order;
    }

}
