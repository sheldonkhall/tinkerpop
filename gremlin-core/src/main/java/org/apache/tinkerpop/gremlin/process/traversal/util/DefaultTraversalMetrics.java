/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation for {@link TraversalMetrics} that aggregates {@link ImmutableMetrics} instances from a
 * {@link Traversal}.
 *
 * @author Bob Briody (http://bobbriody.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class DefaultTraversalMetrics implements TraversalMetrics, Serializable {
    /**
     * toString() specific headers
     */
    private static final String[] HEADERS = {"Step", "Count", "Traversers", "Time (ms)", "% Dur"};

    /**
     * {@link ImmutableMetrics} indexed by their step identifier.
     */
    private final Map<String, ImmutableMetrics> stepIndexedMetrics = new HashMap<>();

    /**
     * A computed value representing the total time spent on all steps.
     */
    private long totalStepDuration;

    /**
     * {@link ImmutableMetrics} indexed by their step position.
     */
    private Map<Integer, ImmutableMetrics> positionIndexedMetrics = new LinkedHashMap<>();

    /**
     * Determines if final metrics have been computed
     */
    private volatile boolean finalized = false;

    public DefaultTraversalMetrics() {
    }

    /**
     * This is only a convenient constructor needed for GraphSON deserialization.
     */
    public DefaultTraversalMetrics(final long totalStepDurationNs, final List<MutableMetrics> orderedMetrics) {
        totalStepDuration = totalStepDurationNs;
        for (int ix = 0; ix < orderedMetrics.size(); ix++) {
            stepIndexedMetrics.put(orderedMetrics.get(ix).getId(), orderedMetrics.get(ix).getImmutableClone());
            positionIndexedMetrics.put(ix, orderedMetrics.get(ix).getImmutableClone());
        }
    }

    @Override
    public long getDuration(final TimeUnit unit) {
        return unit.convert(this.totalStepDuration, MutableMetrics.SOURCE_UNIT);
    }

    @Override
    public Metrics getMetrics(final int index) {
        return this.positionIndexedMetrics.get(index);
    }

    @Override
    public Metrics getMetrics(final String id) {
        return this.stepIndexedMetrics.get(id);
    }

    @Override
    public Collection<ImmutableMetrics> getMetrics() {
        return this.positionIndexedMetrics.values();
    }

    /**
     * The metrics have been computed and can no longer be modified.
     */
    public boolean isFinalized() {
        return finalized;
    }

    @Override
    public String toString() {
        // Build a pretty table of metrics data.

        // Append headers
        final StringBuilder sb = new StringBuilder("Traversal Metrics\n")
                .append(String.format("%-50s %21s %11s %15s %8s", HEADERS));

        sb.append("\n=============================================================================================================");

        appendMetrics(this.positionIndexedMetrics.values(), sb, 0);

        // Append total duration
        sb.append(String.format("%n%50s %21s %11s %15.3f %8s",
                ">TOTAL", "-", "-", getDuration(TimeUnit.MICROSECONDS) / 1000.0, "-"));

        return sb.toString();
    }

    /**
     * Extracts metrics from the provided {@code traversal} and computes metrics. Calling this method finalizes the
     * metrics such that their values can no longer be modified.
     */
    public synchronized void setMetrics(final Traversal.Admin traversal, final boolean onGraphComputer) {
        if (finalized) throw new IllegalStateException("Metrics have been finalized and cannot be modified");
        finalized = true;
        handleNestedTraversals(traversal, null, onGraphComputer);
        addTopLevelMetrics(traversal, onGraphComputer);
    }

    private void addTopLevelMetrics(final Traversal.Admin traversal, final boolean onGraphComputer) {
        this.totalStepDuration = 0;

        final List<ProfileStep> profileSteps = TraversalHelper.getStepsOfClass(ProfileStep.class, traversal);
        final List<Pair<Integer, MutableMetrics>> tempMetrics = new ArrayList<>(profileSteps.size());

        for (int ii = 0; ii < profileSteps.size(); ii++) {
            // The index is necessary to ensure that step order is preserved after a merge.
            final ProfileStep step = profileSteps.get(ii);
            final MutableMetrics stepMetrics = onGraphComputer ? traversal.getSideEffects().get(step.getId()) : step.getMetrics();

            this.totalStepDuration += stepMetrics.getDuration(MutableMetrics.SOURCE_UNIT);
            tempMetrics.add(Pair.with(ii, stepMetrics.clone()));
        }

        tempMetrics.forEach(m -> {
            final double dur = m.getValue1().getDuration(TimeUnit.NANOSECONDS) * 100.d / this.totalStepDuration;
            m.getValue1().setAnnotation(PERCENT_DURATION_KEY, dur);
        });

        tempMetrics.forEach(p -> {
            this.stepIndexedMetrics.put(p.getValue1().getId(), p.getValue1().getImmutableClone());
            this.positionIndexedMetrics.put(p.getValue0(), p.getValue1().getImmutableClone());
        });
    }

    private void handleNestedTraversals(final Traversal.Admin traversal, final MutableMetrics parentMetrics, final boolean onGraphComputer) {
        long prevDur = 0;
        for (int i = 0; i < traversal.getSteps().size(); i++) {
            final Step step = (Step) traversal.getSteps().get(i);
            if (!(step instanceof ProfileStep))
                continue;

            final MutableMetrics metrics = onGraphComputer ?
                    traversal.getSideEffects().get(step.getId()) :
                    ((ProfileStep) step).getMetrics();

            if (null != metrics) { // this happens when a particular branch never received a .next() call (the metrics were never initialized)
                if (!onGraphComputer) {
                    // subtract upstream duration.
                    final long durBeforeAdjustment = metrics.getDuration(TimeUnit.NANOSECONDS);
                    // adjust duration
                    metrics.setDuration(metrics.getDuration(TimeUnit.NANOSECONDS) - prevDur, TimeUnit.NANOSECONDS);
                    prevDur = durBeforeAdjustment;
                }

                if (parentMetrics != null) {
                    parentMetrics.addNested(metrics);
                }

                if (step.getPreviousStep() instanceof TraversalParent) {
                    for (Traversal.Admin<?, ?> t : ((TraversalParent) step.getPreviousStep()).getLocalChildren()) {
                        handleNestedTraversals(t, metrics, onGraphComputer);
                    }
                    for (Traversal.Admin<?, ?> t : ((TraversalParent) step.getPreviousStep()).getGlobalChildren()) {
                        handleNestedTraversals(t, metrics, onGraphComputer);
                    }
                }
            }
        }
    }

    private void appendMetrics(final Collection<? extends Metrics> metrics, final StringBuilder sb, final int indent) {
        // Append each StepMetric's row. indexToLabelMap values are ordered by index.
        for (Metrics m : metrics) {
            String rowName = m.getName();

            // Handle indentation
            for (int ii = 0; ii < indent; ii++) {
                rowName = "  " + rowName;
            }
            // Abbreviate if necessary
            rowName = StringUtils.abbreviate(rowName, 50);

            // Grab the values
            final Long itemCount = m.getCount(ELEMENT_COUNT_ID);
            final Long traverserCount = m.getCount(TRAVERSER_COUNT_ID);
            Double percentDur = (Double) m.getAnnotation(PERCENT_DURATION_KEY);

            // Build the row string

            sb.append(String.format("%n%-50s", rowName));

            if (itemCount != null) {
                sb.append(String.format(" %21d", itemCount));
            } else {
                sb.append(String.format(" %21s", ""));
            }

            if (traverserCount != null) {
                sb.append(String.format(" %11d", traverserCount));
            } else {
                sb.append(String.format(" %11s", ""));
            }

            sb.append(String.format(" %15.3f", m.getDuration(TimeUnit.MICROSECONDS) / 1000.0));

            if (percentDur != null) {
                sb.append(String.format(" %8.2f", percentDur));
            }

            appendMetrics(m.getNested(), sb, indent + 1);
        }
    }
}
