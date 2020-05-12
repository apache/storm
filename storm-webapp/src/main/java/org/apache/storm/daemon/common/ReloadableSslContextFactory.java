package org.apache.storm.daemon.common;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadableSslContextFactory extends SslContextFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ReloadableSslContextFactory.class);

    private boolean enableSslReload;
    private FileWatcher keyStoreWatcher;
    private FileWatcher trustStoreWatcher;

    public ReloadableSslContextFactory() {
        this(false);
    }

    public ReloadableSslContextFactory(boolean enableSslReload) {
        this.enableSslReload = enableSslReload;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        if (enableSslReload) {
            LOG.info("Enabling reloading of SSL credentials without server restart");

            String keyStorePathStr = getKeyStorePath();
            if (keyStorePathStr != null) {
                Path keyStorePath = Paths.get(URI.create(keyStorePathStr).getPath());
                FileWatcher.Callback keyStoreWatcherCallback = () ->
                    ReloadableSslContextFactory.this.reload((scf) -> LOG.info("Reloading SslContextFactory due to keystore change"));
                keyStoreWatcher = new FileWatcher(keyStorePath, keyStoreWatcherCallback);
                keyStoreWatcher.start();
            } else {
                LOG.warn("KeyStore is null; it won't be watched/reloaded");
            }

            String trustStorePathStr = getTrustStorePath();
            if (trustStorePathStr != null) {
                Path trustStorePath = Paths.get(URI.create(trustStorePathStr).getPath());
                FileWatcher.Callback trustStoreWatcherCallback = () ->
                    ReloadableSslContextFactory.this.reload((scf) -> LOG.info("Reloading SslContextFactory due to truststore change"));
                trustStoreWatcher = new FileWatcher(trustStorePath, trustStoreWatcherCallback);
                trustStoreWatcher.start();
            } else {
                LOG.warn("TrustStore is null; it won't be watched/reloaded");
            }
        }
    }

    @Override
    protected void doStop() throws Exception {
        if (keyStoreWatcher != null) {
            keyStoreWatcher.stop();
        }
        if (trustStoreWatcher != null) {
            trustStoreWatcher.stop();
        }
        super.doStop();
    }
}