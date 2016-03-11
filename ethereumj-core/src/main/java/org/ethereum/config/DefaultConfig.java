package org.ethereum.config;

import org.ethereum.datasource.*;
import org.ethereum.db.BlockStore;
import org.ethereum.db.IndexedBlockStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;

import javax.annotation.PostConstruct;

import static org.ethereum.db.IndexedBlockStore.BLOCK_INFO_SERIALIZER;

/**
 *
 * @author Roman Mandeleil
 * Created on: 27/01/2015 01:05
 */
@Configuration
@Import(CommonConfig.class)
public class DefaultConfig {
    private static Logger logger = LoggerFactory.getLogger("general");

    @Autowired
    ApplicationContext appCtx;

    @Autowired
    CommonConfig commonConfig;

    @Autowired
    SystemProperties config;

    @PostConstruct
    public void init() {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught exception", e);
            }
        });
    }

    @Bean
    public BlockStore blockStore(){

        KeyValueDataSource blocksDB = commonConfig.keyValueDataSource();
        blocksDB.setName("blocks");
        blocksDB.init();
        KeyValueDataSource indexDS = commonConfig.keyValueDataSource();
        indexDS.setName("index");
        indexDS.init();

        IndexedBlockStore indexedBlockStore = new IndexedBlockStore();
        CachingDataSource cds = new CachingDataSource(indexDS);
        indexedBlockStore.init(new ObjectArray<>(new DatasourceArray(cds), BLOCK_INFO_SERIALIZER),
                blocksDB, null, null);


        return indexedBlockStore;
    }

    @Bean
    LevelDbDataSource levelDbDataSource() {
        return new LevelDbDataSource();
    }
    @Bean
    LevelDbDataSource levelDbDataSource(String name) {
        return new LevelDbDataSource(name);
    }

    @Lazy @Bean @Scope("prototype")
    GemFireDataSource gemFireDataSource(String name) {
        return new GemFireDataSource(name);
    }

    @Lazy @Bean @Scope("prototype")
    GemFireDataSource gemFireDataSource() {
        return new GemFireDataSource();
    }
}
