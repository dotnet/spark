/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

// Tests the runner's supervision/redaction only; this is NOT a Spark reproducer.
public final class ProbeProcessFixture {
    public static void main(String[] args) throws InterruptedException {
        if (args[0].equals("noise")) {
            System.out.println("synthetic-sensitive-value-do-not-persist");
            System.err.println("synthetic-sensitive-value-do-not-persist");
            System.out.println("x".repeat(5000));
        } else {
            Thread.sleep(60000);
        }
    }
}
