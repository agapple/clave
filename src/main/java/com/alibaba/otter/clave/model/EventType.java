package com.alibaba.otter.clave.model;

public enum EventType {

    /**
     * Insert row.
     */
    INSERT("I"),

    /**
     * Update row.
     */
    UPDATE("U"),

    /**
     * Delete row.
     */
    DELETE("D"),

    /**
     * Create table.
     */
    CREATE("C"),

    /**
     * Alter table.
     */
    ALTER("A"),

    /**
     * Erase table.
     */
    ERASE("E");

    private String value;

    private EventType(String value){
        this.value = value;
    }

    public boolean isInsert() {
        return this.equals(EventType.INSERT);
    }

    public boolean isUpdate() {
        return this.equals(EventType.UPDATE);
    }

    public boolean isDelete() {
        return this.equals(EventType.DELETE);
    }

    public boolean isCreate() {
        return this.equals(EventType.CREATE);
    }

    public boolean isAlter() {
        return this.equals(EventType.ALTER);
    }

    public boolean isErase() {
        return this.equals(EventType.ERASE);
    }

    public static EventType valuesOf(String value) {
        EventType[] eventTypes = values();
        for (EventType eventType : eventTypes) {
            if (eventType.value.equalsIgnoreCase(value)) {
                return eventType;
            }
        }
        return null;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
