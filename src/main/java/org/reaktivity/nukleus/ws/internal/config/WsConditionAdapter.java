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
import org.reaktivity.reaktor.config.Condition;
import org.reaktivity.reaktor.config.ConditionAdapterSpi;

public final class WsConditionAdapter implements ConditionAdapterSpi, JsonbAdapter<Condition, JsonObject>
{
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
        Condition condition)
    {
        WsCondition wsCondition = (WsCondition) condition;

        JsonObjectBuilder object = Json.createObjectBuilder();

        if (wsCondition.protocol != null)
        {
            object.add(PROTOCOL_NAME, wsCondition.protocol);
        }

        if (wsCondition.scheme != null)
        {
            object.add(SCHEME_NAME, wsCondition.scheme);
        }

        if (wsCondition.authority != null)
        {
            object.add(AUTHORITY_NAME, wsCondition.authority);
        }

        if (wsCondition.path != null)
        {
            object.add(PATH_NAME, wsCondition.path);
        }

        return object.build();
    }

    @Override
    public Condition adaptFromJson(
        JsonObject object)
    {
        String protocol = object.containsKey(PROTOCOL_NAME)
                ? object.getString(PROTOCOL_NAME)
                : null;

        String scheme = object.containsKey(SCHEME_NAME)
                ? object.getString(SCHEME_NAME)
                : null;

        String authority = object.containsKey(AUTHORITY_NAME)
                ? object.getString(AUTHORITY_NAME)
                : null;

        String path = object.containsKey(PATH_NAME)
                ? object.getString(PATH_NAME)
                : null;

        return new WsCondition(protocol, scheme, authority, path);
    }
}
