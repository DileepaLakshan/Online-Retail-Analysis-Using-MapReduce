package com.retail;

import org.apache.hadoop.util.ProgramDriver;

public class OnlineRetailDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("salespercountry", SalesPerCountry.class, "Calculate total sales per country");
            pgd.addClass("mostsoldproducts", MostSoldProducts.class, "Identify most sold products");
            pgd.addClass("mostactivecustomers", MostActiveCustomers.class, "Identify most active customers");
            exitCode = pgd.run(argv);
        }
        catch(Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }
}
