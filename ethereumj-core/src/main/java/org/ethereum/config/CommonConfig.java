package org.ethereum.config;

import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.MyDataCenterInstanceConfig;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryManager;
import com.typesafe.config.ConfigException;
import org.ethereum.core.PendingTransaction;
import org.ethereum.core.Repository;
import org.ethereum.core.Transaction;
import org.ethereum.datasource.*;
import org.ethereum.datasource.mapdb.MapDBFactory;
import org.ethereum.datasource.redis.RedisConnection;
import org.ethereum.db.RepositoryImpl;
import org.ethereum.validator.*;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBuilder;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.*;

import static java.util.Arrays.asList;

@Configuration
@EnableTransactionManagement
@ComponentScan(
        basePackages = "org.ethereum",
        excludeFilters = @ComponentScan.Filter(NoAutoscan.class))
public class CommonConfig {

    private static final Logger logger = LoggerFactory.getLogger("general");

    @Autowired
    private RedisConnection redisConnection;
    @Autowired
    private MapDBFactory mapDBFactory;
    @Autowired
    SystemProperties config = SystemProperties.CONFIG;


    @Bean
    Repository repository() {
        return new RepositoryImpl(keyValueDataSource(), keyValueDataSource());
    }

    KeyValueDataSource oneDB;

    @Bean
    EurekaInstanceConfig getEurekaInstanceConfig() {
        MyDataCenterInstanceConfig config = new MyDataCenterInstanceConfig() {
            @Override
            public String getInstanceId() {
                return Hex.toHexString(CommonConfig.this.config.nodeId());
            }

            @Override
            public int getNonSecurePort() {
                return CommonConfig.this.config.listenPort();
            }

            @Override
            public boolean isNonSecurePortEnabled() {
                return true;
            }
        };
        DiscoveryManager.getInstance().initComponent(config, new DefaultEurekaClientConfig());
        return config;
    }

    private RemoteDataSource connectRemoteDb() {
        String vip = config.getConfig().getString("remote.datasource.eureka.vip");
        List<InstanceInfo> dbInst = null;
        while (dbInst == null || dbInst.size() == 0) {
            logger.info("Quering Eureka for DB service at " + vip);
            getEurekaInstanceConfig();
            dbInst = DiscoveryManager.getInstance().getEurekaClient().getInstancesByVipAddress(vip, false);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        logger.info("DB service located (" + dbInst.size() + "). Connecting RemoteDB...");

        RemoteDataSource client = new RemoteDataSource();
        client.setName("remote");
        client.init();
        client.startClient(dbInst.get(0).getIPAddr(), dbInst.get(0).getPort());

        return client;
    }

    @Bean
    @Scope("prototype")
    public KeyValueDataSource keyValueDataSource() {
        if (oneDB == null) {
//            oneDB = new LevelDbDataSource();
//            oneDB.setName("one");
//            oneDB.init();
//            RemoteDataSource client = new RemoteDataSource();
//            client.setName("remote");
//            client.init();
//            client.startClient("localhost", 666);
            oneDB = connectRemoteDb();
        }
        return new XorDataSource(oneDB);
//        String dataSource = config.getKeyValueDataSource();
//        try {
//            if ("redis".equals(dataSource) && redisConnection.isAvailable()) {
//                // Name will be defined before initialization
//                return redisConnection.createDataSource("");
//            } else if ("mapdb".equals(dataSource)) {
//                return mapDBFactory.createDataSource();
//            } else if ("gemfire".equals(dataSource)) {
//                return new GemFireDataSource();
//            }
//
//            dataSource = "leveldb";
//            return new LevelDbDataSource();
//        } finally {
//            logger.info(dataSource + " key-value data source created.");
//        }
    }

    @Bean
    public Set<PendingTransaction> wireTransactions() {
        String storage = "Redis";
        try {
            if (redisConnection.isAvailable()) {
                return redisConnection.createPendingTransactionSet("wireTransactions");
            }

            storage = "In memory";
            return Collections.synchronizedSet(new HashSet<PendingTransaction>());
        } finally {
            logger.info(storage + " 'wireTransactions' storage created.");
        }
    }

    @Bean
    public List<Transaction> pendingStateTransactions() {
        return Collections.synchronizedList(new ArrayList<Transaction>());
    }

    @Bean
    @Lazy
    public SessionFactory sessionFactory() {
        LocalSessionFactoryBuilder builder =
                new LocalSessionFactoryBuilder(dataSource());
        builder.scanPackages("org.ethereum.db")
                .addProperties(getHibernateProperties());

        return builder.buildSessionFactory();
    }

    private Properties getHibernateProperties() {

        Properties prop = new Properties();

        if (config.databaseReset())
            prop.put("hibernate.hbm2ddl.auto", "create-drop");
        else
            prop.put("hibernate.hbm2ddl.auto", "update");

        prop.put("hibernate.format_sql", "true");
        prop.put("hibernate.connection.autocommit", "false");
        prop.put("hibernate.connection.release_mode", "after_transaction");
        prop.put("hibernate.jdbc.batch_size", "1000");
        prop.put("hibernate.order_inserts", "true");
        prop.put("hibernate.order_updates", "true");

// todo: useful but annoying consider define by system.properties
//        prop.put("hibernate.show_sql", "true");
        prop.put("hibernate.dialect",
                "org.hibernate.dialect.H2Dialect");
        return prop;
    }

    @Bean
    @Lazy
    public HibernateTransactionManager txManager() {
        return new HibernateTransactionManager(sessionFactory());
    }


    @Bean(name = "dataSource")
    public DriverManagerDataSource dataSource() {

        logger.info("Connecting to the block store");

        System.setProperty("hsqldb.reconfig_logging", "false");

        String url =
                String.format("jdbc:h2:./%s/blockchain/blockchain.db;CACHE_SIZE=200000",
                        config.databaseDir());

        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl(url);
        ds.setUsername("sa");

        return ds;

    }

    @Bean
    public BlockHeaderValidator headerValidator() {

        List<BlockHeaderRule> rules = new ArrayList<>(asList(
                new GasValueRule(),
                new ExtraDataRule(),
                new ProofOfWorkRule(),
                new GasLimitRule()
        ));

        return new BlockHeaderValidator(rules);
    }

    @Bean
    public ParentBlockHeaderValidator parentHeaderValidator() {

        List<DependentBlockHeaderRule> rules = new ArrayList<>(asList(
                new ParentNumberRule(),
                new DifficultyRule(),
                new ParentGasLimitRule()
        ));

        return new ParentBlockHeaderValidator(rules);
    }

    @Bean
    public SystemProperties systemProperties() {
        return SystemProperties.CONFIG;
    }
}
