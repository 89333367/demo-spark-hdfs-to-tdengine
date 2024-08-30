package sunyu.demo.domain;

public enum Params3Enum {
    ERROR("ERROR", Byte.valueOf("0")),
    REALTIME("REALTIME", Byte.valueOf("1")),
    HISTORY("HISTORY", Byte.valueOf("2")),
    TERMIN("TERMIN", Byte.valueOf("3")),
    TERMOUT("TERMOUT", Byte.valueOf("4")),
    LINKSTATUS("LINKSTATUS", Byte.valueOf("5"));

    private String name;
    private Byte code;

    Params3Enum(String name, Byte code) {
        this.name = name;
        this.code = code;
    }

    public Byte getCode() {
        return code;
    }
}
