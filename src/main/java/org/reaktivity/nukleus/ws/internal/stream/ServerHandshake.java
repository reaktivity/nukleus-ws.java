/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.nukleus.ws.internal.stream;

public final class ServerHandshake
{
    private final long acceptRouteId;
    private final long acceptId;
    private final String acceptName;
    private final long correlationId;
    private final String handshakeHash;
    private final String protocol;

    public ServerHandshake(
        long acceptRouteId,
        long acceptId,
        String acceptName,
        long correlationId,
        String handshakeHash,
        String protocol)
    {
        this.acceptRouteId = acceptRouteId;
        this.acceptId = acceptId;
        this.acceptName = acceptName;
        this.correlationId = correlationId;
        this.handshakeHash = handshakeHash;
        this.protocol = protocol;
    }

    public long acceptRouteId()
    {
        return acceptRouteId;
    }

    public long acceptId()
    {
        return acceptId;
    }

    public String acceptName()
    {
        return acceptName;
    }

    public long correlationId()
    {
        return correlationId;
    }

    public String handshakeHash()
    {
        return handshakeHash;
    }

    public String protocol()
    {
        return protocol;
    }
}
