/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.storage.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.confluent.connect.storage.errors.HiveMetaStoreException;

public class HiveMetaStore {

  private static final Logger log = LoggerFactory.getLogger(HiveMetaStore.class);
  protected final IMetaStoreClient client;

  public HiveMetaStore(AbstractConfig connectorConfig) {
    this(new Configuration(), connectorConfig);
  }

  public HiveMetaStore(Configuration conf, AbstractConfig connectorConfig)
      throws HiveMetaStoreException {
    HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
    String hiveConfDir = connectorConfig.getString(HiveConfig.HIVE_CONF_DIR_CONFIG);
    String hiveMetaStoreUris = connectorConfig.getString(HiveConfig.HIVE_METASTORE_URIS_CONFIG);
    if (hiveMetaStoreUris.isEmpty()) {
      log.warn(
          "hive.metastore.uris empty, an embedded Hive metastore will be created in the directory"
          + " the connector is started. You need to start Hive in that specific directory to "
          + "query the data."
      );
    }
    if (!hiveConfDir.equals("")) {
      String hiveSitePath = hiveConfDir + "/hive-site.xml";
      File hiveSite = new File(hiveSitePath);
      if (!hiveSite.exists()) {
        log.warn(
            "hive-site.xml does not exist in provided Hive configuration directory {}.",
            hiveConf
        );
      }
      hiveConf.addResource(new Path(hiveSitePath));
    }
    hiveConf.set("hive.metastore.uris", hiveMetaStoreUris);
    try {
      client = HCatUtil.getHiveMetastoreClient(hiveConf);
    } catch (IOException | MetaException e) {
      throw new HiveMetaStoreException(e);
    }
  }

  private interface ClientAction<R> {

    R call() throws TException;
  }

  private <R> R doAction(ClientAction<R> action) throws HiveMetaStoreException {
    // No need to implement retries here. We use RetryingMetaStoreClient which creates a proxy
    // for a IMetaStoreClient implementation and retries calls to it on failure. The retrying
    // client is conscious of the socket timeout and does not call reconnect on an open
    // connection. Since HiveMetaStoreClient's reconnect method does not check the status of the
    // connection, blind retries may cause a huge spike in the number of connections to the Hive
    // MetaStore.
    try {
      return action.call();
    } catch (MetaException e) {
      throw new HiveMetaStoreException("Hive MetaStore exception", e);
    } catch (TException e) {
      throw new HiveMetaStoreException("Exception communicating with the Hive MetaStore", e);
    }
  }

  public void addPartition(final String database, final String tableName, final String path)
      throws HiveMetaStoreException {
    ClientAction<Void> addPartition = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        try {
          // purposely don't check if the partition already exists because getPartition(db,
          // table, path) will throw an exception to indicate the partition doesn't exist also.
          // this way, it's only one call.
          client.appendPartition(database, tableName, path);
        } catch (AlreadyExistsException e) {
          // this is okay
        } catch (InvalidObjectException e) {
          throw new HiveMetaStoreException(
              "Invalid partition for " + database + "." + tableName + ": " + path,
              e
          );
        }
        return null;
      }
    };

    doAction(addPartition);
  }

  public void dropPartition(final String database, final String tableName, final String path)
      throws HiveMetaStoreException {
    ClientAction<Void> dropPartition = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        try {
          client.dropPartition(database, tableName, path, false);
        } catch (NoSuchObjectException e) {
          // this is okay
        } catch (InvalidObjectException e) {
          throw new HiveMetaStoreException(
              "Invalid partition for " + database + "." + tableName + ": " + path,
              e
          );
        }
        return null;
      }
    };

    doAction(dropPartition);
  }


  public void createDatabase(final String database) throws HiveMetaStoreException {
    ClientAction<Void> create = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        try {
          client.createDatabase(
              new Database(database, "Database created by Kafka Connect", null, null)
          );
        } catch (AlreadyExistsException e) {
          log.warn("Hive database already exists: {}", database);
        } catch (InvalidObjectException e) {
          throw new HiveMetaStoreException("Invalid database: " + database, e);
        }
        return null;
      }
    };

    doAction(create);
  }

  public void dropDatabase(
      final String name,
      final boolean deleteData
  ) throws HiveMetaStoreException {
    ClientAction<Void> drop = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        try {
          client.dropDatabase(name, deleteData, true);
        } catch (NoSuchObjectException e) {
          // this is ok
        }
        return null;
      }
    };

    doAction(drop);
  }

  public void createTable(final Table table) throws HiveMetaStoreException {
    ClientAction<Void> create = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        try {
          client.createTable(table.getTTable());
        } catch (NoSuchObjectException e) {
          throw new HiveMetaStoreException(
              "Hive table not found: " + table.getDbName() + "." + table.getTableName()
          );
        } catch (AlreadyExistsException e) {
          // this is ok
          log.warn("Hive table already exists: {}.{}", table.getDbName(), table.getTableName());
        } catch (InvalidObjectException e) {
          throw new HiveMetaStoreException("Invalid table", e);
        }
        return null;
      }
    };

    createDatabase(table.getDbName());
    doAction(create);
  }

  public void alterTable(final Table table) throws HiveMetaStoreException {
    ClientAction<Void> alter = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        try {
          client.alter_table(table.getDbName(), table.getTableName(), table.getTTable());
        } catch (NoSuchObjectException e) {
          throw new HiveMetaStoreException(
              "Hive table not found: " + table.getDbName() + "." + table.getTableName()
          );
        } catch (InvalidObjectException e) {
          throw new HiveMetaStoreException("Invalid table", e);
        } catch (InvalidOperationException e) {
          throw new HiveMetaStoreException("Invalid table change", e);
        }
        return null;
      }
    };

    doAction(alter);
  }

  public void dropTable(final String database, final String tableName) {
    ClientAction<Void> drop = new ClientAction<Void>() {
      @Override
      public Void call() throws TException {
        try {
          client.dropTable(database, tableName, false, true);
        } catch (NoSuchObjectException e) {
          // this is okay
        }
        return null;
      }
    };

    doAction(drop);
  }

  public boolean tableExists(
      final String database,
      final String tableName
  ) throws HiveMetaStoreException {
    ClientAction<Boolean> exists = new ClientAction<Boolean>() {
      @Override
      public Boolean call() throws TException {
        try {
          return client.tableExists(database, tableName);
        } catch (UnknownDBException e) {
          return false;
        }
      }
    };

    return doAction(exists);
  }

  public Table getTable(
      final String database,
      final String tableName
  ) throws HiveMetaStoreException {
    ClientAction<Table> getTable = new ClientAction<Table>() {
      @Override
      public Table call() throws TException {
        try {
          return new Table(client.getTable(database, tableName));
        } catch (NoSuchObjectException e) {
          throw new HiveMetaStoreException("Hive table not found: " + database + "." + tableName);
        }
      }
    };

    Table table = doAction(getTable);
    if (table == null) {
      throw new HiveMetaStoreException("Could not find info for table: " + tableName);
    }
    return table;
  }

  public List<String> listPartitions(final String database, final String tableName, final short max)
      throws HiveMetaStoreException {
    ClientAction<List<String>> listPartitions = new ClientAction<List<String>>() {
      @Override
      public List<String> call() throws TException {
        try {
          List<Partition> partitions = client.listPartitions(database, tableName, max);
          List<String> paths = new ArrayList<>();
          for (Partition partition : partitions) {
            paths.add(partition.getSd().getLocation());
          }
          return paths;
        } catch (NoSuchObjectException e) {
          return new ArrayList<>();
        }
      }
    };

    return doAction(listPartitions);
  }

  public List<String> getAllTables(final String database) throws HiveMetaStoreException {
    ClientAction<List<String>> getAllTables = new ClientAction<List<String>>() {
      @Override
      public List<String> call() throws TException {
        try {
          return client.getAllTables(database);
        } catch (NoSuchObjectException e) {
          return new ArrayList<>();
        }
      }
    };

    return doAction(getAllTables);
  }

  public List<String> getAllDatabases() throws HiveMetaStoreException {
    ClientAction<List<String>> create = new ClientAction<List<String>>() {
      @Override
      public List<String> call() throws TException {
        try {
          return client.getAllDatabases();
        } catch (NoSuchObjectException e) {
          return new ArrayList<>();
        }
      }
    };

    return doAction(create);
  }
}
