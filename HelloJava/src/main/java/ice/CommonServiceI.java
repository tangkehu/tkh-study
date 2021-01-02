package ice;

import ice.meta._CommonServiceDisp;

public class CommonServiceI extends _CommonServiceDisp {
    public void request(String s, Ice.Current current) {
        System.out.println(s);
    }
}
