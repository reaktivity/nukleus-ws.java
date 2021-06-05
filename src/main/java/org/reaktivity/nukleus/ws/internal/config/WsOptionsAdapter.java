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
package org.reaktivity.nukleus.ws.internal.config;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.bind.adapter.JsonbAdapter;

import org.reaktivity.nukleus.ws.internal.WsNukleus;
import org.reaktivity.reaktor.config.Options;
import org.reaktivity.reaktor.config.OptionsAdapterSpi;

public final class WsOptionsAdapter implements OptionsAdapterSpi, JsonbAdapter<Options, JsonObject>
{
    private static final String DEFAULTS_NAME = "defaults";
    private static final String PROTOCOL_NAME = "protocol";
    private static final String SCHEME_NAME = "scheme";
    private static final String AUTHORITY_NAME = "authority";
    private static final String PATH_NAME = "path";

    @Override
    public String type()
    {
        return WsNukleus.NAME;
    }

    @Override
    public JsonObject adaptToJson(
        Options options)
    {
        WsOptions wsOptions = (WsOptions) options;

        JsonObjectBuilder object = Json.createObjectBuilder();

        if (wsOptions.protocol != null ||
            wsOptions.scheme != null ||
            wsOptions.authority != null ||
            wsOptions.path != null)
        {
            JsonObjectBuilder entries = Json.createObjectBuilder();

            if (wsOptions.protocol != null)
            {
                entries.add(PROTOCOL_NAME, wsOptions.protocol);
            }

            if (wsOptions.scheme != null)
            {
                entries.add(SCHEME_NAME, wsOptions.scheme);
            }

            if (wsOptions.authority != null)
            {
                entries.add(AUTHORITY_NAME, wsOptions.authority);
            }

            if (wsOptions.path != null)
            {
                entries.add(PATH_NAME, wsOptions.path);
            }

            object.add(DEFAULTS_NAME, entries);
        }

        return object.build();
    }

    @Override
    public Options adaptFromJson(
        JsonObject object)
    {
        JsonObject defaults = object.containsKey(DEFAULTS_NAME)
                ? object.getJsonObject(DEFAULTS_NAME)
                : null;

        String protocol = null;
        String scheme = null;
        String authority = null;
        String path = null;

        if (defaults != null)
        {
            if (defaults.containsKey(PROTOCOL_NAME))
            {
                protocol = defaults.getString(PROTOCOL_NAME);
            }

            if (defaults.containsKey(SCHEME_NAME))
            {
                scheme = defaults.getString(SCHEME_NAME);
            }

            if (defaults.containsKey(AUTHORITY_NAME))
            {
                authority = defaults.getString(AUTHORITY_NAME);
            }

            if (defaults.containsKey(PATH_NAME))
            {
                path = defaults.getString(PATH_NAME);
            }
        }

        return new WsOptions(protocol, scheme, authority, path);
    }
}
