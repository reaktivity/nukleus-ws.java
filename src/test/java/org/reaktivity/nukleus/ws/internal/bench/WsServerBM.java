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
package org.reaktivity.nukleus.ws.internal.bench;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_DIRECTORY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_STREAMS_BUFFER_CAPACITY;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Control;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.ws.internal.WsController;
import org.reaktivity.nukleus.ws.internal.types.Array32FW;
import org.reaktivity.nukleus.ws.internal.types.Flyweight;
import org.reaktivity.nukleus.ws.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.ws.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.ws.internal.types.stream.DataFW;
import org.reaktivity.nukleus.ws.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.ws.internal.types.stream.WindowFW;
import org.reaktivity.reaktor.Reaktor;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Fork(3)
@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
@OutputTimeUnit(SECONDS)
public class WsServerBM
{
    private final Configuration configuration;
    private final Reaktor reaktor;

    {
        Properties properties = new Properties();
        properties.setProperty(REAKTOR_DIRECTORY.name(), "target/nukleus-benchmarks");
        properties.setProperty(REAKTOR_STREAMS_BUFFER_CAPACITY.name(), Long.toString(1024L * 1024L * 16L));

        configuration = new Configuration(properties);
        reaktor = Reaktor.builder()
                         .config(configuration)
                         .nukleus("ws"::equals)
                         .controller("ws"::equals)
                         .errorHandler(ex -> ex.printStackTrace(System.err))
                         .build()
                         .start();
    }

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();

    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private Source source;
    private Target target;

    private long routeId;

    @Setup(Level.Trial)
    public void reinit() throws Exception
    {
        final WsController controller = reaktor.controller(WsController.class);

        final Random random = new Random();

        this.routeId = controller.route(SERVER, "ws#0", "target#0").get();

        final long sourceRouteId = random.nextLong();
        final long sourceId = random.nextLong();

        source.reinit(sourceRouteId, routeId, sourceId);
        target.reinit();

        source.doBegin();
    }

    @TearDown(Level.Trial)
    public void reset() throws Exception
    {
        WsController controller = reaktor.controller(WsController.class);

        controller.unroute(routeId).get();

        this.source = null;
        this.target = null;
    }

    @Benchmark
    @Group("throughput")
    @GroupThreads(1)
    public void writer(Control control) throws Exception
    {
        while (!control.stopMeasurement &&
               source.process() == 0)
        {
            Thread.yield();
        }
    }

    @Benchmark
    @Group("throughput")
    @GroupThreads(1)
    public void reader(Control control) throws Exception
    {
        while (!control.stopMeasurement &&
               target.read() == 0)
        {
            Thread.yield();
        }
    }

    private final class Source
    {
        private final MessagePredicate streams;
        private final ToIntFunction<MessageConsumer> throttle;

        private BeginFW begin;
        private DataFW data;

        private Source(
            MessagePredicate streams,
            ToIntFunction<MessageConsumer> throttle)
        {
            this.streams = streams;
            this.throttle = throttle;
        }

        private void reinit(
            long sourceRouteId,
            long sourceRef,
            long sourceId)
        {
            final MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[256]);

            final Consumer<Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers = hs ->
            {
                hs.item(h -> h.name(":scheme").value("http"));
                hs.item(h -> h.name(":method").value("GET"));
                hs.item(h -> h.name(":path").value("/"));
                hs.item(h -> h.name("host").value("localhost:8080"));
                hs.item(h -> h.name("upgrade").value("websocket"));
                hs.item(h -> h.name("sec-websocket-key").value("dGhlIHNhbXBsZSBub25jZQ=="));
                hs.item(h -> h.name("sec-websocket-version").value("13"));

                //hs.item(h -> h.name("sec-websocket-protocol").value(protocol));
            };

            // TODO: move to doBegin to avoid writeBuffer overwrite with DataFW
            this.begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(sourceRouteId)
                    .streamId(sourceId)
                    .extension(e -> e.set(visitHttpBeginEx(headers)))
                    .build();

            byte[] charBytes = "Hello, world".getBytes(StandardCharsets.UTF_8);

            byte[] sendArray = new byte[18];
            sendArray[0] = (byte) 0x82; // fin, binary
            sendArray[1] = (byte) 0x8c; // masked, length 12
            sendArray[2] = (byte) 0x01; // masking key (4 bytes)
            sendArray[3] = (byte) 0x02;
            sendArray[4] = (byte) 0x03;
            sendArray[5] = (byte) 0x04;
            sendArray[6] = (byte) (charBytes[0] ^ sendArray[2]);
            sendArray[7] = (byte) (charBytes[1] ^ sendArray[3]);
            sendArray[8] = (byte) (charBytes[2] ^ sendArray[4]);
            sendArray[9] = (byte) (charBytes[3] ^ sendArray[5]);
            sendArray[10] = (byte) (charBytes[4] ^ sendArray[2]);
            sendArray[11] = (byte) (charBytes[5] ^ sendArray[3]);
            sendArray[12] = (byte) (charBytes[6] ^ sendArray[4]);
            sendArray[13] = (byte) (charBytes[7] ^ sendArray[5]);
            sendArray[14] = (byte) (charBytes[8] ^ sendArray[2]);
            sendArray[15] = (byte) (charBytes[9] ^ sendArray[3]);
            sendArray[16] = (byte) (charBytes[10] ^ sendArray[4]);
            sendArray[17] = (byte) (charBytes[11] ^ sendArray[5]);

            this.data = dataRW.wrap(writeBuffer, this.begin.limit(), writeBuffer.capacity())
                              .routeId(sourceRouteId)
                              .streamId(sourceId)
                              .payload(p -> p.set(sendArray))
                              .build();
        }

        private boolean doBegin()
        {
            return streams.test(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
        }

        private int process()
        {
            int work = 0;

            if (streams.test(data.typeId(), data.buffer(), data.offset(), data.sizeof()))
            {
                work++;
            }

            work += throttle.applyAsInt((t, b, i, l) -> {});

            return work;
        }

        private Flyweight.Builder.Visitor visitHttpBeginEx(
            Consumer<Array32FW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
        {
            return (buffer, offset, limit) ->
                httpBeginExRW.wrap(buffer, offset, limit)
                             .headers(headers)
                             .build()
                             .sizeof();
        }
    }

    private final class Target
    {
        private final ToIntFunction<MessageConsumer> streams;
        private final MessagePredicate throttle;

        private MutableDirectBuffer writeBuffer;
        private MessageConsumer readHandler;

        private Target(
            ToIntFunction<MessageConsumer> streams,
            MessagePredicate throttle)
        {
            this.streams = streams;
            this.throttle = throttle;
        }

        private void reinit()
        {
            this.writeBuffer = new UnsafeBuffer(new byte[256]);
            this.readHandler = this::beforeBegin;
        }

        private int read()
        {
            return streams.applyAsInt(readHandler);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            final BeginFW begin = beginRO.wrap(buffer, index, index + length);
            final long streamId = begin.streamId();
            final long routeId = begin.routeId();
            doWindow(streamId, routeId, 0L, 0L);

            this.readHandler = this::afterBegin;
        }

        private void afterBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            final DataFW data = dataRO.wrap(buffer, index, index + length);
            final long routeId = data.routeId();
            final long streamId = data.streamId();
            final long sequence = data.sequence();
            final int reserved = data.reserved();

            doWindow(streamId, routeId, sequence + reserved, sequence + reserved);
        }

        private boolean doWindow(
            final long routeId,
            final long streamId,
            final long sequence,
            final long acknowledge)
        {
            final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(routeId)
                    .streamId(streamId)
                    .sequence(sequence)
                    .acknowledge(acknowledge)
                    .maximum(8192)
                    .padding(0)
                    .build();

            return throttle.test(window.typeId(), window.buffer(), window.offset(), window.sizeof());
        }
    }

    public static void main(String[] args) throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(WsServerBM.class.getSimpleName())
                .forks(0)
                .build();

        new Runner(opt).run();
    }
}
