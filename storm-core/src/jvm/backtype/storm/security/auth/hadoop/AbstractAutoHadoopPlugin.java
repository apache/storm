package backtype.storm.security.auth.hadoop;

import backtype.storm.security.INimbusCredentialPlugin;
import backtype.storm.security.auth.IAutoCredentials;
import backtype.storm.security.auth.ICredentialsRenewer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.ObjectInputStream;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

public abstract class AbstractAutoHadoopPlugin implements IAutoCredentials, ICredentialsRenewer, INimbusCredentialPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractAutoHadoopPlugin.class);

    protected abstract byte[] getHadoopCredentials(Map conf);

    protected abstract String getCredentialKey();

    public void prepare(Map conf) {
        //no op.
    }

    @Override
    public void shutdown() {
        //no op.
    }

    @Override
    public void populateCredentials(Map<String, String> credentials, Map conf) {
        try {
            credentials.put(getCredentialKey(), DatatypeConverter.printBase64Binary(getHadoopCredentials(conf)));
        } catch (Exception e) {
            LOG.warn("Could not populate HBase credentials.", e);
        }
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        //no op.
    }

    /*
     *
     * @param credentials map with creds.
     * @return instance of org.apache.hadoop.security.Credentials.
     * this class's populateCredentials must have been called before.
     */
    @SuppressWarnings("unchecked")
    protected Object getCredentials(Map<String, String> credentials) {
        Object credential = null;
        if (credentials != null && credentials.containsKey(getCredentialKey())) {
            try {
                byte[] credBytes = DatatypeConverter.parseBase64Binary(credentials.get(getCredentialKey()));
                ByteArrayInputStream bai = new ByteArrayInputStream(credBytes);
                ObjectInputStream in = new ObjectInputStream(bai);

                Class credentialClass = Class.forName("org.apache.hadoop.security.Credentials");
                credential = credentialClass.newInstance();
                Method readMethod  = credentialClass.getMethod("readFields", DataInput.class);
                readMethod.invoke(credential, in);
            } catch (Exception e) {
                LOG.warn("Could not obtain credentials from credentials map.", e);
            }
        }
        return credential;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
    }

    @SuppressWarnings("unchecked")
    private void addCredentialToSubject(Subject subject, Map<String, String> credentials) {
        try {
            Object credential = getCredentials(credentials);
            if (credential != null) {
                subject.getPrivateCredentials().add(credential);
            } else {
                LOG.info("No credential found in credentials");
            }
        } catch (Exception e) {
            LOG.warn("Failed to initialize and get UserGroupInformation.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public void renew(Map<String, String> credentials, Map topologyConf) {
        Object credential = getCredentials(credentials);
        //maximum allowed expiration time until which tokens will keep renewing,
        //currently set to 1 day.
        final long MAX_ALLOWED_EXPIRATION_MILLIS = 24 * 60 * 60 * 1000;

        /**
         * We are trying to do the following :
         * List<Token> tokens = credential.getAllTokens();
         * for(Token token: tokens) {
         *      long expiration = token.renew(configuration);
         * }
         */
        try {
            if (credential != null) {
                Class configurationClass = Class.forName("org.apache.hadoop.conf.Configuration");
                Object configuration = configurationClass.newInstance();

                Class credentialClass = Class.forName("org.apache.hadoop.security.Credentials");
                Class tokenClass = Class.forName("org.apache.hadoop.security.token.Token");

                Method renewMethod = tokenClass.getMethod("renew", configurationClass);
                Method getAllTokensMethod = credentialClass.getMethod("getAllTokens");

                Collection<?> tokens = (Collection<?>) getAllTokensMethod.invoke(credential);

                for (Object token : tokens) {
                    long expiration = (Long) renewMethod.invoke(token, configuration);
                    if(expiration < MAX_ALLOWED_EXPIRATION_MILLIS) {
                        LOG.debug("expiration {} is less then MAX_ALLOWED_EXPIRATION_MILLIS {}, getting new tokens",
                                expiration, MAX_ALLOWED_EXPIRATION_MILLIS);
                        populateCredentials(credentials, topologyConf);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("could not renew the credentials, one of the possible reason is tokens are beyond " +
                    "renewal period so attempting to get new tokens.", e);
            populateCredentials(credentials);
        }
    }

}
