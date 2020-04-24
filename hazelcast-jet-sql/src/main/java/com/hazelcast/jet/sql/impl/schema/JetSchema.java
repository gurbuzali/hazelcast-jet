/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.sql.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.imap.IMapSqlConnector;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.jet.impl.util.ReflectionUtils.newInstance;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Schema operating on registered sources/sinks
 */
public class JetSchema extends AbstractSchema {

    /**
     * The name under which the IMap connector is registered. Use as an
     * argument to {@link #createServer} when connecting to remote cluster.
     */
    public static final String IMAP_CONNECTOR = "imap";

    /**
     * The server name under which the local hazelcast IMap connector registered.
     */
    public static final String IMAP_LOCAL_SERVER = "local_imap";

    public static final String OPTION_CLASS_NAME = "className";
    private static final String OPTION_CONNECTOR_NAME = JetSchema.class + ".connectorName";
    private static final String OPTION_SERVER_NAME = JetSchema.class + ".serverName";

    private final JetInstance instance;

    private final Map<String, SqlConnector> connectorsByName;
    private final Map<String, JetServer> serversByName;
    // TODO: add serverName to JetTable ?
    private final Map<String, String> serverNamesByTableName;
    private final Map<String, JetTable> tablesByName;
    private final Map<String, Table> unmodifiableTableMap;

    public JetSchema(JetInstance instance) {
        this.instance = instance;

        this.connectorsByName = new HashMap<>();
        this.serversByName = new HashMap<>();
        this.serverNamesByTableName = new HashMap<>();
        this.tablesByName = new ConcurrentHashMap<>();
        this.unmodifiableTableMap = Collections.unmodifiableMap(tablesByName);

        // insert the IMap connector and local cluster server by default
        createConnector(IMAP_CONNECTOR, new IMapSqlConnector(), false);
        createServer(IMAP_LOCAL_SERVER, IMAP_CONNECTOR, emptyMap(), false);
    }

    @Override
    public Map<String, Table> getTableMap() {
        return unmodifiableTableMap;
    }

    public synchronized void createConnector(
            String connectorName,
            Map<String, String> connectorOptions,
            boolean replace
    ) {
        String className = requireNonNull(connectorOptions.get(OPTION_CLASS_NAME),
                "missing " + OPTION_CLASS_NAME + " option");
        SqlConnector connector = newInstance(className);
        createConnector(connectorName, connector, replace);
    }

    private synchronized void createConnector(
            String connectorName,
            SqlConnector connector,
            boolean replace
    ) {
        if (replace) {
            connectorsByName.put(connectorName, connector);
        } else if (connectorsByName.putIfAbsent(connectorName, connector) != null) {
            throw new JetException("'" + connectorName + "' connector already exists");
        }
    }

    public synchronized void removeConnector(String connectorName) {
        if (connectorsByName.remove(connectorName) == null) {
            throw new IllegalArgumentException("'" + connectorName + "' does not exist");
        }
        for (Entry<String, JetServer> server : serversByName.entrySet()) {
            if (connectorName.equals(server.getValue().connectorName())) {
                removeServer(server.getKey());
            }
        }
    }

    public synchronized void createServer(
            String serverName,
            String connectorName,
            Map<String, String> serverOptions,
            boolean replace
    ) {
        if (!connectorsByName.containsKey(connectorName)) {
            throw new IllegalArgumentException("Unknown connector: " + connectorName);
        }
        serverOptions = new HashMap<>(serverOptions); // convert to a HashMap so that we can mutate it
        if (serverOptions.put(OPTION_CONNECTOR_NAME, connectorName) != null) {
            throw new IllegalArgumentException("Private option used");
        }

        JetServer server = new JetServer(serverName, connectorName, serverOptions);
        if (replace) {
            serversByName.put(serverName, server);
        } else if (serversByName.putIfAbsent(serverName, server) != null) {
            throw new JetException("'" + serverName + "' server already exists");
        }
    }

    public synchronized void removeServer(String serverName) {
        if (serversByName.remove(serverName) == null) {
            throw new IllegalArgumentException("'" + serverName + "' does not exist");
        }
        for (Entry<String, String> tableToServer : serverNamesByTableName.entrySet()) {
            if (serverName.equals(tableToServer.getValue())) {
                removeTable(tableToServer.getKey());
            }
        }
    }

    public synchronized void createTable(
            String tableName,
            String serverName,
            Map<String, String> tableOptions,
            List<Entry<String, QueryDataType>> fields,
            boolean replace
    ) {
        JetServer server = serversByName.get(serverName);
        if (server == null) {
            throw new IllegalArgumentException("Unknown server: " + serverName);
        }
        SqlConnector connector = connectorsByName.get(server.connectorName());
        if (connector == null) {
            throw new IllegalArgumentException("Server references unknown connector: " + server.connectorName());
        }
        tableOptions = new HashMap<>(tableOptions); // convert to a HashMap so that we can mutate it
        if (tableOptions.put(OPTION_SERVER_NAME, serverName) != null) {
            throw new IllegalArgumentException("Private option used");
        }

        JetTable table;
        if (fields == null) {
            table = connector.createTable(instance, tableName, server.options(), tableOptions);
        } else {
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("zero fields");
            }
            if (fields.stream().anyMatch(e -> e.getValue() == QueryDataType.NULL)) {
                throw new IllegalArgumentException("NULL type not supported");
            }
            table = connector.createTable(instance, tableName, server.options(), tableOptions, fields);
        }

        if (replace) {
            tablesByName.put(tableName, table);
        } else if (tablesByName.putIfAbsent(tableName, table) != null) {
            throw new JetException("'" + tableName + "' table already exists");
        }
        serverNamesByTableName.put(tableName, server.name());
    }

    public synchronized void removeTable(String tableName) {
        if (tablesByName.remove(tableName) == null) {
            throw new IllegalArgumentException("'" + tableName + "' does not exist");
        }
        serverNamesByTableName.remove(tableName);
    }
}
