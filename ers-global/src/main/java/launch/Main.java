package launch;
import java.io.File;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.Context;

public class Main {

    public static void main(String[] args) throws Exception {

        String webappDirLocation = "src/main/webapp/";
        Tomcat tomcat = new Tomcat();

        //The port that we should run on can be set into an environment variable
        //Look for that variable and default to 8080 if it isn't there.
        String webPort = System.getenv("PORT");
        if(webPort == null || webPort.isEmpty()) {
            webPort = "8888";
        }

        tomcat.setPort(Integer.valueOf(webPort));

        Context context = tomcat.addWebapp("/", new File(webappDirLocation).getAbsolutePath());
        System.out.println("configuring app with basedir: " + new File("./" + webappDirLocation).getAbsolutePath());

        for (String s : args) {
            int pos = s.indexOf('=');
            if (pos == -1) continue;

            String param_name = s.substring(0, pos);
            String param_value = s.substring(pos + 1);

            System.out.println("web.xml parameter override: " + s);

            context.addParameter(param_name + "-OVERRIDE", param_value);
        }

        tomcat.start();
        tomcat.getServer().await();  
    }

}
