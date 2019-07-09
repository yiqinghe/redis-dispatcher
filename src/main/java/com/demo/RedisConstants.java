package com.demo;

/**
 * @author caigaonian
 *         Created on 17/11/8.
 */
public class RedisConstants {
    public static final String RESPONSE_SUCCESS = "OK";
    public static final String SET_IF_NOT_EXIST = "NX";
    public static final String SET_IF_EXIST = "XX";
    public static final String SET_WITH_EXPIRE_SECONDS = "EX";
    public static final String SET_WITH_EXPIRE_MILLIS = "PX";

    public static final String RUNNING_TASK_SEPERATOR = "@^_^@";
    public static final long HEART_BEAT_MILLIS = 5 * 1000;

    public static final String TASK_STATUS_RUNNING = "running";
    public static final String TASK_STATUS_DONE = "done";

    public static final String PERIOD_TASK_SET_KEY = "usertag.period.tasks";

    public static final int SLEEP_MILLIS_WHEN_IDLE = 1000;
    public static final int WAIT_MILLIS_WHEN_TASK_DONE = 3000;

    public static final String ELEMENT_SEPEARTOR = ":";

    public static final String HBASE_HAS_DATA_SYN_FLAG = "HBASE_HAS_DATA_SYN_FLAG";
}
