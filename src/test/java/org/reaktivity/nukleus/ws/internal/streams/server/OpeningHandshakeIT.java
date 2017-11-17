/**
 * Copyright 2016-2017 The Reaktivity Project
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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

/**
 * RFC-6455, section 4.1 "Client-Side Requirements" RFC-6455, section 4.2
 * "Server-Side Requirements"
 */
public class OpeningHandshakeIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/ws/control/route")
            .addScriptRoot("client", "org/reaktivity/specification/ws/opening")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/ws/streams/opening");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .nukleus("ws"::equals)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    //TODO: check "response" tests for correct functionality, the "request" tests should be fine now

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection.established/handshake.request",
        "${server}/connection.established/handshake.response" })
    public void shouldEstablishConnection() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Not working yet")
    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.cookie/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldEstablishConnectionWithCookieRequestHeader()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.headers.random.case/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldEstablishConnectionWithRandomCaseRequestHeaders()
            throws Exception
    {
        k3po.finish();
    }


    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.headers.random.case/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldEstablishConnectionWithRandomCaseResponseHeaders()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.origin/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldEstablishConnectionWithRequestHeaderOrigin()
            throws Exception
    {
        k3po.finish();
    }


    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/request.header.sec.websocket.protocol/handshake.request",
        "${server}/connection.established/handshake.response" })
    public void shouldEstablishConnectionWithRequestHeaderSecWebSocketProtocol() throws Exception
    {
        k3po.finish();
    }

    /*
        Test from this point down may not work
     */

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.sec.websocket.protocol.negotiated/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldEstablishConnectionWithRequestHeaderSecWebSocketProtocolNegotiated()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.sec.websocket.extensions/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldEstablishConnectionWithRequestHeaderSecWebSocketExtensions()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.header.sec.websocket.extensions.partial.agreement/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldEstablishConnectionWithSomeExtensionsNegotiated()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.header.sec.websocket.extensions.reordered/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldEstablishConnectionWhenOrderOfExtensionsNegotiatedChanged()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.method.not.get/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailHandshakeWhenMethodNotGet() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.version.not.http.1.1/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailHandshakeWhenVersionNotHttp11() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.upgrade.missing/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailHandshakeWhenRequestHeaderUpgradeMissing()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.upgrade.not.websocket/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailHandshakeWhenRequestHeaderUpgradeNotWebSocket()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.connection.missing/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailHandshakeWhenRequestHeaderConnectionMissing()
            throws Exception
    {
        k3po.finish();
    }


    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.connection.not.upgrade/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailHandshakeWhenRequestHeaderConnectionNotUpgrade()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.sec.websocket.key.missing/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailHandshakeWhenRequestHeaderSecWebSocketKeyMissing()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.sec.websocket.key.not.16bytes.base64/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailHandshakeWhenRequestHeaderSecWebSocketKeyNot16BytesBase64()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/request.header.sec.websocket.version.not.13/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailHandshakeWhenRequestHeaderSecWebSocketVersionNot13()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.header.connection.missing/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailConnectionWhenResponseHeaderConnectionMissing()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.header.connection.not.upgrade/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailConnectionWhenResponseHeaderConnectionNotUpgrade()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.header.upgrade.missing/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailConnectionWhenResponseHeaderUpgradeMissing()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.header.upgrade.not.websocket/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailConnectionWhenResponseHeaderUpgradeNotWebSocket()
            throws Exception
    {
        k3po.finish();
    }


    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.header.sec.websocket.accept.missing/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailConnectionWhenResponseHeaderSecWebSocketAcceptMissing()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.header.sec.websocket.accept.not.hashed/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailConnectionWhenResponseHeaderSecWebSocketAcceptNotHashed()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.header.sec.websocket.extensions.not.negotiated/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailConnectionWhenResponseHeaderSecWebSocketExtensionsNotNegotiated()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/response.header.sec.websocket.protocol.not.negotiated/handshake.request",
            "${server}/connection.established/handshake.response" })
    public void shouldFailConnectionWhenResponseHeaderSecWebSocketProtocolNotNegotiated()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${client}/multiple.connections.established/handshake.requests",
            "${server}/connection.established/handshake.response" })
    public void shouldEstablishMultipleConnections() throws Exception
    {
        k3po.finish();
    }

}
