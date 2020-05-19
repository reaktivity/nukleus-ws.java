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
package org.reaktivity.nukleus.ws.internal;

import static org.reaktivity.nukleus.route.RouteKind.CLIENT;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;

import java.util.EnumMap;
import java.util.Map;

import org.reaktivity.nukleus.Elektron;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.nukleus.ws.internal.stream.WsClientFactoryBuilder;
import org.reaktivity.nukleus.ws.internal.stream.WsServerFactoryBuilder;

final class WsElektron implements Elektron
{
    private final Map<RouteKind, StreamFactoryBuilder> buildersByKind;

    WsElektron(
        WsConfiguration config)
    {
        final EnumMap<RouteKind, StreamFactoryBuilder> buildersByKind = new EnumMap<>(RouteKind.class);
        buildersByKind.put(SERVER, new WsServerFactoryBuilder(config));
        buildersByKind.put(CLIENT, new WsClientFactoryBuilder(config));
        this.buildersByKind = buildersByKind;
    }

    @Override
    public StreamFactoryBuilder streamFactoryBuilder(
        RouteKind kind)
    {
        return buildersByKind.get(kind);
    }

    @Override
    public String toString()
    {
        return String.format("%s %s", getClass().getSimpleName(), buildersByKind);
    }
}
