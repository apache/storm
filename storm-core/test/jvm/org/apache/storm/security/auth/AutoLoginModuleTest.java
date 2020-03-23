/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.security.auth;

import org.apache.storm.security.auth.kerberos.AutoTGTKrb5LoginModule;
import org.apache.storm.security.auth.kerberos.AutoTGTKrb5LoginModuleTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginException;
import java.net.InetAddress;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AutoLoginModuleTest {

    @Test
    public void loginModuleNoSubjNoTgtTest() throws Exception {
        // Behavior is correct when there is no Subject or TGT
        AutoTGTKrb5LoginModule loginModule = new AutoTGTKrb5LoginModule();
        Assertions.assertThrows(LoginException.class, loginModule::login);
        assertThat(loginModule.commit(), is(false));
        assertThat(loginModule.abort(), is(false));
        assertThat(loginModule.logout(), is(true));
    }

    @Test
    public void loginModuleReadonlySubjNoTgtTest() throws Exception {
        // Behavior is correct when there is a read-only Subject and no TGT
        Subject readonlySubject = new Subject(true, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        AutoTGTKrb5LoginModule loginModule = new AutoTGTKrb5LoginModule();
        loginModule.initialize(readonlySubject, null, null, null);
        assertThat(loginModule.commit(), is(false));
        assertThat(loginModule.logout(), is(true));
    }

    @Test
    public void loginModuleWithSubjNoTgtTest() throws Exception {
        // Behavior is correct when there is a Subject and no TGT
        AutoTGTKrb5LoginModule loginModule = new AutoTGTKrb5LoginModule();
        loginModule.initialize(new Subject(), null, null, null);
        Assertions.assertThrows(LoginException.class, loginModule::login);
        assertThat(loginModule.commit(), is(false));
        assertThat(loginModule.abort(), is(false));
        assertThat(loginModule.logout(), is(true));
    }

    @Test
    public void loginModuleNoSubjWithTgtTest() throws Exception {
        // Behavior is correct when there is no Subject and a TGT
        AutoTGTKrb5LoginModuleTest loginModule = new AutoTGTKrb5LoginModuleTest();
        loginModule.setKerbTicket(Mockito.mock(KerberosTicket.class));
        assertThat(loginModule.login(), is(true));
        Assertions.assertThrows(LoginException.class, loginModule::commit);
        loginModule.setKerbTicket(Mockito.mock(KerberosTicket.class));
        assertThat(loginModule.abort(), is(true));
        assertThat(loginModule.logout(), is(true));
    }

    @Test
    public void loginModuleReadonlySubjWithTgtTest() throws Exception {
        // Behavior is correct when there is a read-only Subject and a TGT
        Subject readonlySubject = new Subject(true, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        AutoTGTKrb5LoginModuleTest loginModule = new AutoTGTKrb5LoginModuleTest();
        loginModule.initialize(readonlySubject, null, null, null);
        loginModule.setKerbTicket(Mockito.mock(KerberosTicket.class));
        assertThat(loginModule.login(), is(true));
        Assertions.assertThrows(LoginException.class, loginModule::commit);
        loginModule.setKerbTicket(Mockito.mock(KerberosTicket.class));
        assertThat(loginModule.abort(), is(true));
        assertThat(loginModule.logout(), is(true));
    }

    @Test
    public void loginModuleWithSubjAndTgt() throws Exception {
        // Behavior is correct when there is a Subject and a TGT
        AutoTGTKrb5LoginModuleTest loginModule = new AutoTGTKrb5LoginModuleTest();
        loginModule.client = Mockito.mock(Principal.class);
        Date endTime = new SimpleDateFormat("ddMMyyyy").parse("31122030");
        byte[] asn1Enc = new byte[10];
        Arrays.fill(asn1Enc, (byte)122);
        byte[] sessionKey = new byte[10];
        Arrays.fill(sessionKey, (byte)123);
        KerberosTicket ticket = new KerberosTicket(
                asn1Enc,
                new KerberosPrincipal("client/localhost@local.com"),
                new KerberosPrincipal("server/localhost@local.com"),
                sessionKey,
                234,
                new boolean[]{false, true, false, true, false, true, false},
                new Date(),
                new Date(),
                endTime,
                endTime,
                new InetAddress[]{InetAddress.getByName("localhost")}
        );
        loginModule.initialize(new Subject(), null, null, null);
        loginModule.setKerbTicket(ticket);
        assertThat(loginModule.login(), is(true));
        assertThat(loginModule.commit(), is(true));
        assertThat(loginModule.abort(), is(true));
        assertThat(loginModule.logout(), is(true));
    }
}
