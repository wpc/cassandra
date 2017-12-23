package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.lang.management.*;
import javax.management.*;

import io.airlift.command.Command;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import com.facebook.cassandra.fb303.HealthcheckStatus;

@Command(name = "getfb303status", description = "Shows the FB303 status for this node")
public class GetFb303Status extends NodeToolCmd
{
    public static final String OVERRIDE_METRIC_NAME = "HealthcheckOverride";

    @Override
    public void execute(NodeProbe probe)
    {
        String clusterName = DatabaseDescriptor.getClusterName();
        String objName = "com.facebook.cassandra.fb303:type=HealthCheck,scope=" + clusterName;
        MBeanServerConnection mbs = probe.getMBeanServerConnection();

        try {
            ObjectName overrideMetricName = new ObjectName(objName + ",name=" + OVERRIDE_METRIC_NAME);
            HealthcheckStatus s = (HealthcheckStatus) mbs.getAttribute(overrideMetricName, "Status");
            System.out.println("FB303 Status: " + s);
        } catch (MalformedObjectNameException
            | MBeanException
            | AttributeNotFoundException
            | InstanceNotFoundException
            | IOException
            | ReflectionException e) {
              System.out.println("Error fetching fb303 status: " + e);
        }
    }
}
