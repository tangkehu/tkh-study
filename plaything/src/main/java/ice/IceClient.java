package ice;

import ice.meta.CommonServicePrx;
import ice.meta.CommonServicePrxHelper;

public class IceClient {
    public static void main(String[] args) {
        int status = 0;
        Ice.Communicator ic = null;
        try {
            ic = Ice.Util.initialize(new String[]{
                    "--Ice.Plugin.IceSSL=IceSSL.PluginFactory",
                    "--IceSSL.DefaultDir=./ice/certs",
                    "--IceSSL.Keystore=client.jks",
                    "--IceSSL.Truststore=cacert.jks",
                    "--IceSSL.Password=password"
            });
            String TCP="SimpleService:tcp -p 10001";
            String SSL="SimpleService:ssl -p 10000";
            Ice.ObjectPrx base = ic.stringToProxy(TCP);
            Ice.ObjectPrx baseSSL = ic.stringToProxy(SSL);
            CommonServicePrx proxy = CommonServicePrxHelper.checkedCast(base);
            CommonServicePrx proxySSL = CommonServicePrxHelper.checkedCast(baseSSL);
            if (proxy == null || proxySSL == null)
                throw new Error("Invalid proxy");

            int MAX=100;
            long timeTcp = 0;
            long timeSSL = 0;
            for (int i=0; i<MAX; i++) {
                Long start = System.nanoTime();
                proxy.request("Hello World!");
                timeTcp+=(System.nanoTime()-start);
                start = System.nanoTime();
                proxySSL.request("Hello World!");
                timeSSL+=(System.nanoTime()-start);
            }
            System.out.println("tcp用时总计 " + timeTcp + " 平均每次请求用时 " + timeTcp/MAX);
            System.out.println("ssl用时总计 " + timeSSL + " 平均每次请求用时 " + timeSSL/MAX);
            System.out.println("ssl比tcp性能下降" + (timeSSL-timeTcp)*100.0/timeTcp+"%");
        } catch (Ice.LocalException e) {
            e.printStackTrace();
            status = 1;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            status = 1;
        }
        if (ic != null) {
            // Clean up
            //
            try {
                ic.destroy();
            } catch (Exception e) {
                System.err.println(e.getMessage());
                status = 1;
            }
        }
        System.exit(status);
    }
}
