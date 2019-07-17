package com.waitingforcode.sql.avro;

public class Customer {

    private CustomerTypes customerType;
    private Address address;

    public Customer() {}

    public Customer(CustomerTypes customerType, Address address) {
        this.customerType = customerType;
        this.address = address;
    }

    public CustomerTypes getCustomerType() {
        return customerType;
    }

    public Address getAddress() {
        return address;
    }

    public void setCustomerType(CustomerTypes customerType) {
        this.customerType = customerType;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public static class Address {
        private int postalCode;
        private String city;

        public Address() {}

        public Address(int postalCode, String city) {
            this.postalCode = postalCode;
            this.city = city;
        }

        public int getPostalCode() {
            return postalCode;
        }

        public String getCity() {
            return city;
        }

        public void setPostalCode(int postalCode) {
            this.postalCode = postalCode;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }
}
