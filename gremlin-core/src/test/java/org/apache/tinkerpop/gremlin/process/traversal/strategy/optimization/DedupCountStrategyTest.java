/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DedupCountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.dedup;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

@RunWith(Parameterized.class)
public class DedupCountStrategyTest {

    @Parameterized.Parameter(value = 0)
    public Traversal original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Test
    public void doTest() {
        this.original.asAdmin().setParent(new TraversalVertexProgramStep(EmptyTraversal.instance(), EmptyTraversal.instance())); // trick it
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(DedupCountStrategy.instance());
        this.original.asAdmin().setStrategies(strategies);
        this.original.asAdmin().applyStrategies();

        assertEquals(this.optimized, this.original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        return Arrays.asList(new Traversal[][]{
                {out().count().dedup(), out().count().dedup()},
                {dedup().count(), addDedupCount(__.start())},
                {out().dedup().count(), addDedupCount(out())},
                {out().dedup().count().as("a"), addDedupCount(out()).as("a")},
                {out().dedup().as("a").count().as("b", "c"), addDedupCount(out()).as("b", "c")},
                {out().dedup("a", "b").count(), addDedupCount(out(), Arrays.asList("a", "b"))},
                {out().dedup("a", "b").by("name").count(), addDedupCount(out(), Arrays.asList("a", "b"), new ElementValueTraversal("name"))},
                {out().dedup("a", "b").by(out("knows").count()).count(), addDedupCount(out(), Arrays.asList("a", "b"), __.out("knows").count())},
        });
    }

    private static GraphTraversal.Admin<?, ?> addDedupCount(final GraphTraversal<?, ?> traversal, final Collection<String> dedupLabels, final Traversal dedupTraversal) {
        final DedupCountGlobalStep<?> step = new DedupCountGlobalStep<>((Traversal.Admin) traversal, new DedupGlobalStep<>(traversal.asAdmin(), dedupLabels.toArray(new String[dedupLabels.size()])));
        if (!(dedupTraversal instanceof EmptyTraversal))
            step.setDedupTraversal(dedupTraversal.asAdmin());
        return traversal.asAdmin().addStep(step);
    }

    private static GraphTraversal.Admin<?, ?> addDedupCount(final GraphTraversal<?, ?> traversal) {
        return addDedupCount(traversal, Collections.emptyList(), EmptyTraversal.instance());
    }

    private static GraphTraversal.Admin<?, ?> addDedupCount(final GraphTraversal<?, ?> traversal, final Collection<String> dedupLabels) {
        return addDedupCount(traversal, dedupLabels, EmptyTraversal.instance());
    }


}
