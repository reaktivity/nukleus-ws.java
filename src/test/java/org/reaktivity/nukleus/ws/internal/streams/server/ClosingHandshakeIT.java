/**
 * Copyright 2016-2021 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.ws.internal.streams.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configuration;

/**
 * RFC-6455, section 5.2 "Base Framing Protocol"
 */
public class ClosingHandshakeIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("net", "org/reaktivity/specification/nukleus/ws/streams/network/closing")
        .addScriptRoot("app", "org/reaktivity/specification/nukleus/ws/streams/application/closing");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .configurationRoot("org/reaktivity/specification/nukleus/ws/config")
        .external("app#0")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);


    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/client.send.empty.close.frame/handshake.request.and.frame",
        "${app}/client.send.empty.close.frame/handshake.response.and.frame" })
    public void shouldCompleteCloseHandshakeWhenClientSendEmptyCloseFrame()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/client.send.close.frame.with.code.1005/handshake.request.and.frame",
        "${app}/client.send.close.frame.with.code.1005/handshake.response.and.frame" })
    public void shouldCompleteCloseHandshakeWhenClientSendCloseFrameWithCode1005()
            throws Exception
    {
        k3po.finish();
    }
}
