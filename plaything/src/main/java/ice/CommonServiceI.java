package ice;

import ice.meta._CommonServiceDisp;

public class CommonServiceI extends _CommonServiceDisp {
    public static final long serialVersionUID = -1562167471L;

    public void request(String s, Ice.Current current) {
        System.out.println(s);
    }
}
