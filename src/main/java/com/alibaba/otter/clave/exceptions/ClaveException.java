package com.alibaba.otter.clave.exceptions;

import org.apache.commons.lang.exception.NestableRuntimeException;

public class ClaveException extends NestableRuntimeException {

    private static final long serialVersionUID = -7288830284122672209L;

    private String            errorCode;
    private String            errorDesc;

    public ClaveException(String errorCode) {
        super(errorCode);
    }

    public ClaveException(String errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public ClaveException(String errorCode, String errorDesc) {
        super(errorCode + ":" + errorDesc);
    }

    public ClaveException(String errorCode, String errorDesc, Throwable cause) {
        super(errorCode + ":" + errorDesc, cause);
    }

    public ClaveException(Throwable cause) {
        super(cause);
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorDesc() {
        return errorDesc;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
