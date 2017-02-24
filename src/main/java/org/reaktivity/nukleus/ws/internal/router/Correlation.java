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
package org.reaktivity.nukleus.ws.internal.router;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public class Correlation
{
    private final long id;
    private final String source;
    private final RouteKind established;
    private final String hash;

    public Correlation(
        long id,
        String source,
        RouteKind established,
        String hash)
    {
        this.id = id;
        this.source = requireNonNull(source, "source");
        this.established = requireNonNull(established, "established");
        this.hash = hash;
    }

    public long id()
    {
        return id;
    }

    public String source()
    {
        return source;
    }

    public RouteKind established()
    {
        return established;
    }

    public String hash()
    {
        return hash;
    }

    @Override
    public int hashCode()
    {
        int result = Long.hashCode(id);
        result = 31 * result + source.hashCode();
        result = 31 * result + established.hashCode();
        result = 31 * result + hash.hashCode();

        return result;
    }

    @Override
    public boolean equals(
        Object obj)
    {
        if (!(obj instanceof Correlation))
        {
            return false;
        }

        Correlation that = (Correlation) obj;
        return this.id == that.id &&
                this.established == that.established &&
                Objects.equals(this.source, that.source) &&
                Objects.equals(this.hash, that.hash);
    }

    @Override
    public String toString()
    {
        return String.format("[id=%d, source=\"%s\", established=%s, hash=\"%s\"]", id, source, established, hash);
    }
}
