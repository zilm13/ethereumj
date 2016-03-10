package org.ethereum.net.eureka;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.MyDataCenterInstanceConfig;
import com.netflix.discovery.*;
import com.netflix.discovery.shared.Application;
import org.ethereum.config.SystemProperties;
import org.ethereum.net.rlpx.Node;
import org.ethereum.net.rlpx.discover.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * Created by Anton Nashatyrev on 10.03.2016.
 */
@Component
public class EurekaDiscover {
    private static final Logger logger = LoggerFactory.getLogger("discover");

    private class EthereumjConfig extends MyDataCenterInstanceConfig {
        @Override
        public String getInstanceId() {
            return Hex.toHexString(config.nodeId());
        }
        @Override
        public int getNonSecurePort() {
            return config.listenPort();
        }
        @Override
        public boolean isNonSecurePortEnabled() {
            return true;
        }
    }

    @Autowired
    private SystemProperties config = SystemProperties.CONFIG;

    @Autowired
    private NodeManager nodeManager;

    private EthereumjConfig eurekaInstanceConfig;
    private boolean published = false;
    private Set<Node> activeNodes = new HashSet<>();

    public EurekaDiscover() {
        init();
    }

    @PostConstruct
    private void init() {
        eurekaInstanceConfig = new EthereumjConfig();
        if (config.getConfig().hasPath("peer.discovery.eureka.enabled") &&
                config.getConfig().getBoolean("peer.discovery.eureka.enabled")) {
            register();
        }
    }

    public void register() {
        logger.info("Registering and activating node in Eureka...");
        DiscoveryManager.getInstance().initComponent(
                eurekaInstanceConfig, new DefaultEurekaClientConfig());
        ApplicationInfoManager.getInstance().setInstanceStatus(InstanceInfo.InstanceStatus.UP);

        EurekaClient eurekaClient = DiscoveryManager.getInstance().getEurekaClient();
        eurekaClient.registerEventListener(new EurekaEventListener() {
            @Override
            public void onEvent(EurekaEvent event) {
                if (event instanceof CacheRefreshedEvent) {
                    refreshed();
                }
            }
        });
    }

    private void refreshed() {
        List<Node> availableNodes = getAvailableNodes();
        logger.debug("Currently available Eureka nodes: " + availableNodes.size());
        for (Node node : availableNodes) {
            if (!activeNodes.contains(node)) {
                activeNodes.add(node);
                nodeAppeared(node);
            }
        }
    }

    private void nodeAppeared(Node node) {
        if (!published) {
            if (Arrays.equals(node.getId(), config.nodeId())) {
                published = true;
                logger.info("Home node published in Eureka!");
            }
        } else {
            logger.debug("New Eureka node appeared: " + node);
            if (nodeManager != null) {
                nodeManager.addTrustedNode(node);
            }
        }
    }

    public List<Node> getAvailableNodes() {
        EurekaClient eurekaClient = DiscoveryManager.getInstance().getEurekaClient();
        Application application = eurekaClient.getApplication(eurekaInstanceConfig.getAppname());
        List<Node> ret = new ArrayList<>();
        if (application != null) {
            for (InstanceInfo info : application.getInstances()) {
                if (info.getStatus() == InstanceInfo.InstanceStatus.UP) {
                    try {
                        Node node = new Node(Hex.decode(info.getId()), info.getIPAddr(), info.getPort());
                        ret.add(node);
                    } catch (Exception e) {
                        logger.warn("Can't parse NodeID: " + info.getId() + ", " + info.getIPAddr() + ":" + info.getPort());
                    }
                }
            }
        }
        return ret;
    }
}
