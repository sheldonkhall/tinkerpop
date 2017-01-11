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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashSetSupplier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DedupCountGlobalStep<S> extends ReducingBarrierStep<S, Object> implements TraversalParent, Scoping {

    private Traversal.Admin<S, Object> dedupTraversal;
    private final Set<String> dedupLabels;

    public DedupCountGlobalStep(final Traversal.Admin traversal, final DedupGlobalStep<S> dedupGlobalStep) {
        super(traversal);
        this.dedupLabels = dedupGlobalStep.getScopeKeys().isEmpty() ? null : dedupGlobalStep.getScopeKeys();
        this.dedupTraversal = dedupGlobalStep.getLocalChildren().isEmpty() ? null : (Traversal.Admin<S, Object>) dedupGlobalStep.getLocalChildren().get(0);
        this.labels.addAll(dedupGlobalStep.getNextStep().getLabels());
        this.setSeedSupplier((Supplier) HashSetSupplier.instance());
        this.setReducingBiOperator(Operator.addAll);
    }

    @Override
    public Traverser.Admin<Object> processNextStart() {
        final Traverser.Admin<Object> traverser = super.processNextStart();
        traverser.set((long) ((Set) traverser.get()).size());
        return traverser;
    }

    @Override
    public List<Traversal<S, Object>> getLocalChildren() {
        return null == this.dedupTraversal ? Collections.emptyList() : Collections.singletonList(this.dedupTraversal);
    }

    public void setDedupTraversal(final Traversal.Admin<S, Object> dedupTraversal) {
        this.dedupTraversal = dedupTraversal;
    }

    @Override
    public DedupCountGlobalStep<S> clone() {
        final DedupCountGlobalStep<S> clone = (DedupCountGlobalStep<S>) super.clone();
        if (null != this.dedupTraversal)
            clone.dedupTraversal = this.dedupTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.dedupTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (this.dedupTraversal != null)
            result ^= this.dedupTraversal.hashCode();
        if (this.dedupLabels != null)
            result ^= this.dedupLabels.hashCode();
        return result;
    }

    @Override
    public Object projectTraverser(final Traverser.Admin<S> traverser) {
        final Object object;
        if (null != this.dedupLabels) {
            object = new ArrayList<>(this.dedupLabels.size());
            for (final String label : this.dedupLabels) {
                ((List) object).add(TraversalUtil.applyNullable((S) this.getScopeValue(Pop.last, label, traverser), this.dedupTraversal));
            }
        } else
            object = TraversalUtil.applyNullable(traverser, this.dedupTraversal);
        ///
        final Set<Object> set = new HashSet<>();
        set.add(object);
        return set;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.dedupLabels, this.dedupTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.dedupLabels == null ?
                this.getSelfAndChildRequirements(TraverserRequirement.BULK) :
                this.getSelfAndChildRequirements(TraverserRequirement.LABELED_PATH, TraverserRequirement.BULK);
    }

    @Override
    public Set<String> getScopeKeys() {
        return null == this.dedupLabels ? Collections.emptySet() : this.dedupLabels;
    }

}
