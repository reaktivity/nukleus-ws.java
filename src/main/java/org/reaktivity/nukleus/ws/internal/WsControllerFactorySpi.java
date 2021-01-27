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
package org.reaktivity.nukleus.ws.internal;

import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.ControllerBuilder;
import org.reaktivity.nukleus.ControllerFactorySpi;

public final class WsControllerFactorySpi implements ControllerFactorySpi<WsController>
{
    @Override
    public String name()
    {
        return WsNukleus.NAME;
    }

    @Override
    public Class<WsController> kind()
    {
        return WsController.class;
    }

    @Override
    public WsController create(
        Configuration config,
        ControllerBuilder<WsController> builder)
    {
        return builder.setFactory(WsController::new)
                      .build();
    }
}
