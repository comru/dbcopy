/*
 * Copyright (c) 2011 Haulmont Technology Ltd. All Rights Reserved.
 * Haulmont Technology proprietary and confidential.
 * Use is subject to license terms.
 */

package com.haulmont.dbcopy;

/**
 * <p>$Id: Log.java 6805 2011-12-21 15:29:12Z krivopustov $</p>
 *
 * @author krivopustov
 */
public class Log {

    private static boolean debug;

    public static void init(boolean debug) {
        Log.debug = debug;
    }

    public static void error(String message) {
        System.out.println("ERROR: " + message);
    }

    public static void info(String message) {
        System.out.println(message);
    }

    public static void debug(String message) {
        if (debug)
            System.out.println("\t" + message);
    }
}
