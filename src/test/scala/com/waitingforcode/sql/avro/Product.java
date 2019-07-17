package com.waitingforcode.sql.avro;

public class Product {
    private Long id;
    private String name;
    private double unitaryPrice;
    private int productsInBasket;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getUnitaryPrice() {
        return unitaryPrice;
    }

    public void setUnitaryPrice(double unitaryPrice) {
        this.unitaryPrice = unitaryPrice;
    }

    public int getProductsInBasket() {
        return productsInBasket;
    }

    public void setProductsInBasket(int productsInBasket) {
        this.productsInBasket = productsInBasket;
    }

    public static Product valueOf(Long id, String name, double price, int productsInBasket) {
        Product product = new Product();
        product.setId(id);
        product.setName(name);
        product.setUnitaryPrice(price);
        product.setProductsInBasket(productsInBasket);
        return product;
    }
}
