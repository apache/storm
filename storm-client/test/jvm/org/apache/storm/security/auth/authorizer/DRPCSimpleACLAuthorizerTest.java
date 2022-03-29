/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.auth.authorizer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.security.auth.IAuthorizer;
import org.apache.storm.security.auth.KerberosPrincipalToLocal;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.SingleUserPrincipal;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DRPCSimpleACLAuthorizerTest {

    private static final String function = "jump";
    private static final String partialFunction = "partial";
    private static final String wrongFunction = "wrongFunction";
    private static final String aclFile = "drpc-simple-acl-test-scenario.yaml";
    private static final ReqContext aliceContext = makeMockContext("alice");
    private static final ReqContext aliceKerbContext = makeMockContext("alice@SOME.RELM");
    private static final ReqContext bobContext = makeMockContext("bob");
    private static final ReqContext charlieContext = makeMockContext("charlie");
    private static IAuthorizer strictHandler;
    private static IAuthorizer permissiveHandler;

    @BeforeAll
    public static void setup() {
        strictHandler = new DRPCSimpleACLAuthorizer();
        strictHandler.prepare(ImmutableMap
                                  .of(Config.DRPC_AUTHORIZER_ACL_STRICT, true, Config.DRPC_AUTHORIZER_ACL_FILENAME, aclFile,
                                      Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, KerberosPrincipalToLocal.class.getName()));

        permissiveHandler = new DRPCSimpleACLAuthorizer();
        permissiveHandler.prepare(ImmutableMap
                                      .of(Config.DRPC_AUTHORIZER_ACL_STRICT, false, Config.DRPC_AUTHORIZER_ACL_FILENAME, aclFile,
                                          Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, KerberosPrincipalToLocal.class.getName()));
    }

    private static ReqContext makeMockContext(String user) {
        ReqContext mockContext = Mockito.mock(ReqContext.class);
        Mockito.when(mockContext.principal()).thenReturn(new SingleUserPrincipal(user));
        return mockContext;
    }

    @Test
    public void test_partial_authorization() {

        assertFalse(isPermitted(strictHandler, ReqContext.context(), "execute", partialFunction),
            "Deny execute to unauthorized user");

        assertTrue(isPermitted(strictHandler, aliceKerbContext, "execute", partialFunction),
            "Allow execute to authorized kerb user for correct function");

        assertFalse(isPermitted(strictHandler, aliceKerbContext, "fetchRequest", partialFunction),
            "Deny fetchRequest to unauthorized user for correct function");
    }

    @Test
    public void test_client_authorization_strict() {

        assertFalse(isPermitted(strictHandler, ReqContext.context(), "execute", function),
            "Deny execute to unauthorized user");

        assertFalse(isPermitted(strictHandler, aliceContext, "execute", wrongFunction),
            "Deny execute to valid user for incorrect function");

        assertTrue(isPermitted(strictHandler, aliceKerbContext, "execute", function),
            "Allow execute to authorized kerb user for correct function");

        assertTrue(isPermitted(strictHandler, aliceContext, "execute", function),
            "Allow execute to authorized user for correct function");
    }

    @Test
    public void test_client_authorization_permissive() {

        assertFalse(isPermitted(permissiveHandler, ReqContext.context(), "execute", function),
            "deny execute to unauthorized user for correct function");

        assertTrue(isPermitted(permissiveHandler, aliceContext, "execute", wrongFunction),
            "allow execute for user for incorrect function when permissive");

        assertTrue(isPermitted(permissiveHandler, aliceKerbContext, "execute", wrongFunction),
            "allow execute for user for incorrect function when permissive");

        assertTrue(isPermitted(permissiveHandler, bobContext, "execute", function),
            "allow execute to authorized user for correct function");
    }

    @Test
    public void test_invocation_authorization_strict() {
        for (String operation : new String[]{ "fetchRequest", "failRequest", "result" }) {
            assertFalse(isPermitted(strictHandler, aliceContext, operation, function),
                "Deny " + operation + " to unauthorized user for correct function");

            assertFalse(isPermitted(strictHandler, charlieContext, operation, wrongFunction),
                "Deny " + operation + " to user for incorrect function when strict");

            assertTrue(isPermitted(strictHandler, charlieContext, operation, function),
                "allow " + operation + " to authorized user for correct function");
        }
    }

    @Test
    public void test_invocation_authorization_permissive() {
        for (String operation : new String[]{ "fetchRequest", "failRequest", "result" }) {
            assertFalse(isPermitted(permissiveHandler, bobContext, operation, function),
                "Deny " + operation + " to unauthorized user for correct function");

            assertTrue(isPermitted(permissiveHandler, charlieContext, operation, wrongFunction),
                "Allow " + operation + " to user for incorrect function when permissive");

            assertTrue(isPermitted(permissiveHandler, charlieContext, operation, function),
                "allow " + operation + " to authorized user");
        }
    }

    @Test
    public void test_deny_when_no_function_given() {
        assertFalse(strictHandler.permit(aliceContext, "execute", new HashMap<>()));

        assertFalse(isPermitted(strictHandler, aliceContext, "execute", null));

        assertFalse(permissiveHandler.permit(bobContext, "execute", new HashMap<>()));

        assertFalse(isPermitted(permissiveHandler, bobContext, "execute", null));
    }

    @Test
    public void test_deny_when_invalid_user_given() {
        assertFalse(isPermitted(strictHandler, Mockito.mock(ReqContext.class), "execute", function));

        assertFalse(isPermitted(strictHandler, null, "execute", function));

        assertFalse(isPermitted(permissiveHandler, Mockito.mock(ReqContext.class), "execute", function));

        assertFalse(isPermitted(permissiveHandler, null, "execute", function));

    }

    private boolean isPermitted(IAuthorizer authorizer, ReqContext context, String operation, String function) {
        Map<String, Object> config = new HashMap<>();
        config.put(DRPCSimpleACLAuthorizer.FUNCTION_KEY, function);
        return authorizer.permit(context, operation, config);
    }

    /**
     * {@link DRPCSimpleACLAuthorizer} should still work even if {@link Config#DRPC_AUTHORIZER_ACL} has no values.
     * @throws IOException if there is any issue with creating or writing the temp file.
     */
    @Test
    public void test_read_acl_no_values() throws IOException {
        DRPCSimpleACLAuthorizer authorizer = new DRPCSimpleACLAuthorizer();

        File tempFile = File.createTempFile("drpcacl", ".yaml");
        tempFile.deleteOnExit();
        BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
        writer.write("drpc.authorizer.acl:");
        writer.close();

        authorizer.prepare(ImmutableMap
                .of(Config.DRPC_AUTHORIZER_ACL_STRICT, true, Config.DRPC_AUTHORIZER_ACL_FILENAME, tempFile.toString(),
                        Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, KerberosPrincipalToLocal.class.getName()));

        Map<String, DRPCSimpleACLAuthorizer.AclFunctionEntry> acl = authorizer.readAclFromConfig();
        assertEquals(0, acl.size());
    }

    /**
     * The file of {@link Config#DRPC_AUTHORIZER_ACL_FILENAME} can not be empty.
     * @throws IOException if there is any issue with creating the temp file.
     */
    @Test
    public void test_read_acl_empty_file() throws IOException {
        DRPCSimpleACLAuthorizer authorizer = new DRPCSimpleACLAuthorizer();

        File tempFile = File.createTempFile("drpcacl", ".yaml");
        tempFile.deleteOnExit();

        authorizer.prepare(ImmutableMap
                .of(Config.DRPC_AUTHORIZER_ACL_STRICT, true, Config.DRPC_AUTHORIZER_ACL_FILENAME, tempFile.toString(),
                        Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, KerberosPrincipalToLocal.class.getName()));

        Exception exception = assertThrows(RuntimeException.class, authorizer::readAclFromConfig);
        assertTrue(exception.getMessage().contains("doesn't have any valid storm configs"));
    }
}
