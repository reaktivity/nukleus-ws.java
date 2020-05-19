/**
 * Copyright 2016-2020 The Reaktivity Project
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
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControlIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/ws/control/route")
            .addScriptRoot("client", "org/reaktivity/specification/ws/control")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/ws/streams/control");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .nukleus("ws"::equals)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.send.pong.payload.length.0/handshake.request.and.frame",
        "${server}/client.send.pong.payload.length.0/handshake.response" })
    public void shouldReceiveClientPongFrameWithEmptyPayload() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.send.pong.payload.length.125/handshake.request.and.frame",
        "${server}/client.send.pong.payload.length.125/handshake.response" })
    public void shouldReceiveClientPongFrameWithPayload() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO: WebSocket close handshake with status code")
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.send.pong.payload.length.126/handshake.request.and.frame",
        "${server}/client.send.pong.payload.length.126/handshake.response" })
    public void shouldRejectClientPongFrameWithPayloadTooLong() throws Exception
    {
        k3po.finish();
    }
}
