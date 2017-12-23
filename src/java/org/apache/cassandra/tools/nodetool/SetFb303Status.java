package org.apache.cassandra.tools.nodetool;

import org.apache.commons.lang3.StringUtils;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import java.io.IOException;
import java.lang.management.*;
import javax.management.*;

import io.airlift.command.Arguments;
import io.airlift.command.Command;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import com.facebook.cassandra.fb303.HealthcheckStatus;

@Command(name = "setfb303status", description = "Manually override the FB303 status for this node")
public class SetFb303Status extends NodeToolCmd
{
    @Arguments(title = "status", usage = "<status>", description = "Set the fb303 status of this node", required = true)
    private String status = EMPTY;

    @Override
    public void execute(NodeProbe probe)
    {
        String clusterName = DatabaseDescriptor.getClusterName();
        String objName = "com.facebook.cassandra.fb303:type=HealthCheck,scope=" + clusterName;
        MBeanServerConnection mbs = probe.getMBeanServerConnection();

        HealthcheckStatus targetStatus;
        try {
          targetStatus = HealthcheckStatus.valueOf(status.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value '" + status + "' for fb303 status, valid choices are: " + StringUtils.join(HealthcheckStatus.values(), ", "));
        }

        try {
            ObjectName overrideMetricName = new ObjectName(objName + ",name=" + GetFb303Status.OVERRIDE_METRIC_NAME);
            Attribute attr = new Attribute("Status", targetStatus);
            mbs.setAttribute(overrideMetricName, attr);
            System.out.println("FB303 Status: " + targetStatus);
        } catch (MalformedObjectNameException
            | MBeanException
            | AttributeNotFoundException
            | InstanceNotFoundException
            | IOException
            | InvalidAttributeValueException
            | ReflectionException e) {
              System.out.println("Error setting fb303 status: " + e);
        }
    }
}
