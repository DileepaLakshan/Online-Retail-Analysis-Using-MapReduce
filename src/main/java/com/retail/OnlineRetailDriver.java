package com.retail;

import org.apache.hadoop.util.ProgramDriver;

public class OnlineRetailDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {

            exitCode = pgd.run(argv);
        }
        catch(Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }
}
