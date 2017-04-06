/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.command;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.AccessControl;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.utils.ServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Blobstore {
    private static final Logger LOG = LoggerFactory.getLogger(Blobstore.class);

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("You should provide command.");
        }

        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);

        switch (command) {
            case "cat":
                readCli(newArgs);
                break;

            case "create":
                createCli(newArgs);
                break;

            case "update":
                updateCli(newArgs);
                break;

            case "delete":
                deleteCli(newArgs);
                break;

            case "list":
                listCli(newArgs);
                break;

            case "set-acl":
                setAclCli(newArgs);
                break;

            case "replication":
                replicationCli(newArgs);
                break;

            default:
                throw new RuntimeException("" + command + " is not a supported blobstore command");
        }
    }

    private static void readCli(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("f", "file", null, CLI.AS_STRING)
                .arg("key", CLI.FIRST_WINS)
                .parse(args);
        final String key = (String) cl.get("key");
        final String file = (String) cl.get("f");

        if (StringUtils.isNotEmpty(file)) {
            try (BufferedOutputStream f = new BufferedOutputStream(new FileOutputStream(file))) {
                BlobStoreSupport.readBlob(key, f);
            }
        } else {
            BlobStoreSupport.readBlob(key, System.out);
        }
    }

    private static void createCli(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("f", "file", null, CLI.AS_STRING)
                .opt("a", "acl", Collections.emptyList(), new AsAclParser())
                .opt("r", "replication-factor", -1, CLI.AS_INT)
                .arg("key", CLI.FIRST_WINS)
                .parse(args);

        final String key = (String) cl.get("key");
        final String file = (String) cl.get("f");
        final List<AccessControl> acl = (List<AccessControl>) cl.get("a");
        final Integer replicationFactor = (Integer) cl.get("r");

        SettableBlobMeta meta = new SettableBlobMeta(acl);
        meta.set_replication_factor(replicationFactor);

        ServerUtils.validateKeyName(key);

        LOG.info("Creating {} with ACL {}", key, generateAccessControlsInfo(acl));

        if (StringUtils.isNotEmpty(file)) {
            try (BufferedInputStream f = new BufferedInputStream(new FileInputStream(file))) {
                BlobStoreSupport.createBlobFromStream(key, f, meta);
            }
        } else {
            BlobStoreSupport.createBlobFromStream(key, System.in, meta);
        }

        LOG.info("Successfully created {}", key);
    }

    private static void updateCli(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("f", "file", null, CLI.AS_STRING)
                .arg("key", CLI.FIRST_WINS)
                .parse(args);

        final String key = (String) cl.get("key");
        final String file = (String) cl.get("f");

        if (StringUtils.isNotEmpty(file)) {
            try (BufferedInputStream f = new BufferedInputStream(new FileInputStream(file))) {
                BlobStoreSupport.updateBlobFromStream(key, f);
            }
        } else {
            BlobStoreSupport.updateBlobFromStream(key, System.in);
        }

        LOG.info("Successfully updated {}", key);
    }

    private static void deleteCli(final String[] args) throws Exception {
        ClientBlobStore.withConfiguredClient(new ClientBlobStore.WithBlobstore() {
            @Override
            public void run(ClientBlobStore blobStore) throws Exception {
                for (String key : args) {
                    blobStore.deleteBlob(key);

                    LOG.info("deleted {}", key);
                }
            }
        });
    }

    private static void listCli(final String[] args) throws Exception {
        ClientBlobStore.withConfiguredClient(new ClientBlobStore.WithBlobstore() {
            @Override
            public void run(ClientBlobStore blobStore) throws Exception {
                Iterator<String> keys;
                boolean isArgsEmpty = (args == null || args.length == 0);
                if (isArgsEmpty) {
                    keys = blobStore.listKeys();
                } else {
                    keys = Arrays.asList(args).iterator();
                }

                while (keys.hasNext()) {
                    String key = keys.next();

                    try {
                        ReadableBlobMeta meta = blobStore.getBlobMeta(key);
                        long version = meta.get_version();
                        List<AccessControl> acl = meta.get_settable().get_acl();

                        LOG.info("{} {} {}", key, version, generateAccessControlsInfo(acl));
                    } catch (AuthorizationException ae) {
                        if (!isArgsEmpty) {
                            LOG.error("ACCESS DENIED to key: {}", key);
                        }
                    } catch (KeyNotFoundException knf) {
                        if (!isArgsEmpty) {
                            LOG.error("{} NOT FOUND", key);
                        }
                    }
                }
            }
        });
    }

    private static void setAclCli(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("s", "set", Collections.emptyList(), new AsAclParser())
                .arg("key", CLI.FIRST_WINS)
                .parse(args);

        final String key = (String) cl.get("key");
        final List<AccessControl> setAcl = (List<AccessControl>) cl.get("s");

        ClientBlobStore.withConfiguredClient(new ClientBlobStore.WithBlobstore() {
            @Override
            public void run(ClientBlobStore blobStore) throws Exception {
                ReadableBlobMeta meta = blobStore.getBlobMeta(key);
                List<AccessControl> acl = meta.get_settable().get_acl();
                List<AccessControl> newAcl;
                if (setAcl != null && !setAcl.isEmpty()) {
                    newAcl = setAcl;
                } else {
                    newAcl = acl;
                }

                SettableBlobMeta newMeta = new SettableBlobMeta(newAcl);
                LOG.info("Setting ACL for {} to {}", key, generateAccessControlsInfo(newAcl));
                blobStore.setBlobMeta(key, newMeta);
            }
        });
    }

    private static void replicationCli(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("replication command needs at least subcommand as parameter.");
        }
        final String subCommand = args[0];
        final String[] newArgs = Arrays.copyOfRange(args, 1, args.length);

        ClientBlobStore.withConfiguredClient(new ClientBlobStore.WithBlobstore() {
            @Override
            public void run(ClientBlobStore blobStore) throws Exception {
                switch (subCommand) {
                    case "--read":
                        if (newArgs.length == 0) {
                            throw new IllegalArgumentException("replication --read needs key as parameter.");
                        }

                        String key = newArgs[0];
                        int blobReplication = blobStore.getBlobReplication(key);
                        LOG.info("Current replication factor {}", blobReplication);
                        break;

                    case "--update":
                        updateReplicationFactor(blobStore, newArgs);
                        break;

                    default:
                        throw new RuntimeException("" + subCommand + " is not a supported blobstore command");
                }
            }

            private void updateReplicationFactor(ClientBlobStore blobStore, String[] args) throws Exception {
                Map<String, Object> cl = CLI.opt("r", "replication-factor", null, CLI.AS_INT)
                        .arg("key", CLI.FIRST_WINS)
                        .parse(args);

                final String key = (String) cl.get("key");
                final Integer replicationFactor = (Integer) cl.get("r");

                if (replicationFactor == null) {
                    throw new RuntimeException("Please set the replication factor");
                }

                int blobReplication = blobStore.updateBlobReplication(key, replicationFactor);
                LOG.info("Replication factor is set to {}", blobReplication);
            }
        });
    }

    private static final class BlobStoreSupport {
        static void readBlob(final String key, final OutputStream os) throws Exception {
            ClientBlobStore.withConfiguredClient(new ClientBlobStore.WithBlobstore() {
                @Override
                public void run(ClientBlobStore blobStore) throws Exception {
                    try (InputStreamWithMeta is = blobStore.getBlob(key)) {
                        IOUtils.copy(is, os);
                    }
                }
            });
        }

        static void createBlobFromStream(final String key, final InputStream is, final SettableBlobMeta meta) throws Exception {
            ClientBlobStore.withConfiguredClient(new ClientBlobStore.WithBlobstore() {
                @Override
                public void run(ClientBlobStore blobStore) throws Exception {
                    AtomicOutputStream os = blobStore.createBlob(key, meta);
                    copyInputStreamToBlobOutputStream(is, os);
                }
            });
        }

        static void updateBlobFromStream(final String key, final InputStream is) throws Exception {
            ClientBlobStore.withConfiguredClient(new ClientBlobStore.WithBlobstore() {
                @Override
                public void run(ClientBlobStore blobStore) throws Exception {
                    AtomicOutputStream os = blobStore.updateBlob(key);
                    copyInputStreamToBlobOutputStream(is, os);
                }
            });
        }

        static void copyInputStreamToBlobOutputStream(InputStream is, AtomicOutputStream os) throws IOException {
            try {
                IOUtils.copy(is, os);
                os.close();
            } catch (Exception e) {
                os.cancel();
                throw e;
            }
        }
    }

    private static List<String> generateAccessControlsInfo(List<AccessControl> acl) {
        List<String> accessControlStrings = new ArrayList<>();
        for (AccessControl ac : acl) {
            accessControlStrings.add(BlobStoreAclHandler.accessControlToString(ac));
        }
        return accessControlStrings;
    }

    private static final class AsAclParser implements CLI.Parse {
        @Override
        public Object parse(String value) {
            List<AccessControl> accessControls = new ArrayList<>();
            for (String part : value.split(",")) {
                accessControls.add(asAccessControl(part));
            }

            return accessControls;
        }

        private AccessControl asAccessControl(String param) {
            return BlobStoreAclHandler.parseAccessControl(param);
        }
    }
}
