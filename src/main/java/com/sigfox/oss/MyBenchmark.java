/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.sigfox.oss;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuples;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class MyBenchmark {

    private static final int NB_COLL = 200;
    private static final int NB_UPDATE = 100_000;
    private static final int NB_THREAD = 12;

    @Benchmark
    public void reactiveTemplate(Dependencies dependencies, Fixtures fixtures) {
        Flux
                .fromStream(fixtures.updates)
                .flatMap(t -> reactiveUpdate(t, dependencies, fixtures))
                .subscribeOn(dependencies.scheduler)
                .blockLast();
    }

    @Benchmark
    public void reactiveRaw(Blackhole blackhole, Dependencies dependencies, Fixtures fixtures) {
        blackhole.consume(Flux.fromStream(fixtures.updates)
                .flatMap(update -> dependencies.mongoReactiveClient
                        .getDatabase("test-mongo")
                        .getCollection(update.getT1())
                        .updateOne(new BasicDBObject("_id", update.getT2()), new BasicDBObject("$set", new BasicDBObject()
                                .append("fieldA", fixtures.groupId)
                                .append("fieldB", fixtures.groupId)
                                .append("fieldC", update.getT4())
                                .append("fieldD.0", update.getT3())
                                .append("fieldE.0", update.getT4()))))
                .subscribeOn(dependencies.scheduler)
                .blockLast());
    }

    @Benchmark
    public void mongoTemplate(Blackhole blackhole, Dependencies dependencies, Fixtures fixtures) throws ExecutionException, InterruptedException {
        final Boolean result = fixtures.updates
                .map(t -> CompletableFuture.supplyAsync(() -> syncTplUpdate(t, dependencies, fixtures).wasAcknowledged(), dependencies.executor))
                .reduce((a, b) -> a.thenCombine(b, Boolean::logicalAnd))
                .orElseGet(() -> CompletableFuture.completedFuture(false))
                .get();

        blackhole.consume(result);
    }

    @Benchmark
    public void syncRaw(Blackhole blackhole, Dependencies dependencies, Fixtures fixtures) throws InterruptedException, ExecutionException {
        final Boolean result = fixtures.updates
                .map(t -> CompletableFuture.supplyAsync(() -> syncUpdate(t, dependencies, fixtures).wasAcknowledged(), dependencies.executor))
                .reduce((a, b) -> a.thenCombine(b, Boolean::logicalAnd))
                .orElseGet(() -> CompletableFuture.completedFuture(false))
                .get();

        blackhole.consume(result);
    }

    private Mono<UpdateResult> reactiveUpdate(final Tuple4<String, String, Double, Integer> t, final Dependencies dependencies, final Fixtures fixtures) {
        final Update update = Update
                .update("fieldA", fixtures.groupId)
                .set("fieldB", fixtures.groupId)
                .set("fieldC", t.getT4())
                .set("fieldD.0", t.getT3())
                .set("fieldE.0", t.getT4());

        return dependencies.mongoReactiveTpl.updateFirst(query(where("_id").is(t.getT2())), update, t.getT1());
    }

    private UpdateResult syncTplUpdate(final Tuple4<String, String, Double, Integer> t, final Dependencies dependencies, final Fixtures fixtures) {
        final Update update = Update
                .update("fieldA", fixtures.groupId)
                .set("fieldB", fixtures.groupId)
                .set("fieldC", t.getT4())
                .set("fieldD.0", t.getT3())
                .set("fieldE.0", t.getT4());

        return dependencies.mongoTpl.updateFirst(query(where("_id").is(t.getT2())), update, t.getT1());
    }

    private UpdateResult syncUpdate(final Tuple4<String, String, Double, Integer> update, final Dependencies dependencies, final Fixtures fixtures) {
        return dependencies.mongoClient
                .getDatabase("test-mongo")
                .getCollection(update.getT1())
                .updateOne(new BasicDBObject("_id", update.getT2()), new BasicDBObject("$set", new BasicDBObject()
                        .append("fieldA", fixtures.groupId)
                        .append("fieldB", fixtures.groupId)
                        .append("fieldC", update.getT4())
                        .append("fieldD.0", update.getT3())
                        .append("fieldE.0", update.getT4())));
    }

    @State(Scope.Benchmark)
    public static class Dependencies {

        private Scheduler scheduler;
        private ExecutorService executor;
        private com.mongodb.client.MongoClient mongoClient;
        private MongoClient mongoReactiveClient;
        private ReactiveMongoTemplate mongoReactiveTpl;
        private MongoTemplate mongoTpl;
        private SimpleReactiveMongoDatabaseFactory reactiveMongoFactory;

        @Setup
        public void doSetup() {
            final MongoClientSettings mongoSettings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString("mongodb://localhost"))
                    .applyToConnectionPoolSettings(b -> {
                        b.maxSize(50);
                        b.maxWaitTime(120000L, TimeUnit.MILLISECONDS);
                    }).build();
            mongoClient = com.mongodb.client.MongoClients.create(mongoSettings);
            mongoReactiveClient = MongoClients.create(mongoSettings);
            reactiveMongoFactory = new SimpleReactiveMongoDatabaseFactory(mongoReactiveClient, "test-mongo");
            mongoReactiveTpl = new ReactiveMongoTemplate(reactiveMongoFactory);
            mongoTpl = new MongoTemplate(mongoClient, "test-mongo");
            executor = Executors.newFixedThreadPool(NB_THREAD);
            scheduler = Schedulers.newBoundedElastic(NB_THREAD, Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE, "test");
        }

        @TearDown
        public void cleanUp() throws Exception {
            executor.shutdownNow();
            try {
                executor.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            mongoClient.close();
            mongoReactiveClient.close();
        }
    }

    @State(Scope.Thread)
    public static class Fixtures {

        private final String groupId = new ObjectId().toString();

        private final List<Tuple4<String, String, Double, Integer>> data = Flux.range(0, NB_UPDATE)
                .map(i -> Tuples.of("Test_" + (i % NB_COLL), "doc-" + (i % NB_UPDATE), Math.random(), (int) (System.currentTimeMillis() / 1000)))
                .collectList()
                .block();

        private Stream<Tuple4<String, String, Double, Integer>> updates;

        @Setup(Level.Invocation)
        public void doSetup() {
            updates = data.stream();
        }
    }

}
