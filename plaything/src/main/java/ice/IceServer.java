package ice;

public class IceServer {
    public static void main(String[] args){
        int status = 0;
        Ice.Communicator ic = null;
        try {
            ic = Ice.Util.initialize(new String[]{
                    "--Ice.Plugin.IceSSL=IceSSL.PluginFactory",
                    "--IceSSL.DefaultDir=./ice/certs",
                    "--IceSSL.Keystore=server.jks",
                    "--IceSSL.Truststore=cacert.jks",
                    "--IceSSL.Password=password"
            });
            Ice.ObjectAdapter adapter = ic.createObjectAdapterWithEndpoints("SimpleServiceAdapter", "ssl -p 10000:tcp -p 10001");
            Ice.Object object = new CommonServiceI();
            adapter.add(object, ic.stringToIdentity("SimpleService"));
            adapter.activate();
            ic.waitForShutdown();
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
