/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.mirror;

import org.apache.kafka.common.LastMirror;

import java.util.Objects;
import java.util.Optional;

/**
 * Immutable snapshot of mirror partition metadata.
 */
public record MirrorPartitionMetadata(
    MirrorPartitionState state,
    int stateEpoch,
    LastMirror lastMirror,
    Optional<String> errorMessage,
    short retryAttempt,
    MirrorPartitionState previousState
) {
    public static final short NON_RETRYABLE_ERROR = -1;

    public static final MirrorPartitionMetadata EMPTY = new MirrorPartitionMetadata(
        MirrorPartitionState.UNKNOWN, 0, LastMirror.EMPTY, Optional.empty(), (short) 0, MirrorPartitionState.UNKNOWN);

    public static MirrorPartitionMetadata orEmpty(MirrorPartitionMetadata metadata) {
        return metadata != null ? metadata : EMPTY;
    }

    public MirrorPartitionMetadata {
        Objects.requireNonNull(state);
        if (stateEpoch < 0) {
            throw new IllegalArgumentException("stateEpoch must be non-negative");
        }
        Objects.requireNonNull(lastMirror);
        Objects.requireNonNull(errorMessage);
        if (retryAttempt < NON_RETRYABLE_ERROR) {
            throw new IllegalArgumentException("retryAttempt must be >= " + NON_RETRYABLE_ERROR);
        }
        Objects.requireNonNull(previousState);
    }

    public static class Builder {
        private MirrorPartitionState state;
        private int stateEpoch;
        private LastMirror lastMirror;
        private Optional<String> errorMessage;
        private short retryAttempt;
        private MirrorPartitionState previousState;

        public Builder() {
            this(EMPTY);
        }

        public Builder(MirrorPartitionMetadata metadata) {
            this.state = metadata.state();
            this.stateEpoch = metadata.stateEpoch();
            this.lastMirror = metadata.lastMirror();
            this.errorMessage = metadata.errorMessage();
            this.retryAttempt = metadata.retryAttempt();
            this.previousState = metadata.previousState();
        }

        public Builder withState(MirrorPartitionState state) {
            this.state = Objects.requireNonNull(state);
            return this;
        }

        public Builder withStateEpoch(int stateEpoch) {
            this.stateEpoch = stateEpoch;
            return this;
        }

        public Builder withLastMirror(LastMirror lastMirror) {
            this.lastMirror = Objects.requireNonNull(lastMirror);
            return this;
        }

        public Builder withErrorMessage(Optional<String> errorMessage) {
            this.errorMessage = Objects.requireNonNull(errorMessage);
            return this;
        }

        public Builder withRetryAttempt(short retryAttempt) {
            this.retryAttempt = retryAttempt;
            return this;
        }

        public Builder withPreviousState(MirrorPartitionState previousState) {
            this.previousState = Objects.requireNonNull(previousState);
            return this;
        }

        public Builder withFailedStateReset() {
            this.errorMessage = Optional.empty();
            this.retryAttempt = (short) 0;
            this.previousState = MirrorPartitionState.UNKNOWN;
            return this;
        }

        /**
         * Transitions to the given state and computes error-related fields.
         */
        public Builder withStateTransition(MirrorPartitionState targetState,
                                           int newStateEpoch,
                                           Optional<String> newErrorMessage,
                                           boolean nonRetryable,
                                           short maxRetryAttempts) {
            if (targetState == MirrorPartitionState.FAILED) {
                if (maxRetryAttempts < 0) {
                    throw new IllegalArgumentException("maxRetryAttempts must be non-negative");
                }
                this.errorMessage = newErrorMessage;
                if (nonRetryable || this.retryAttempt == NON_RETRYABLE_ERROR) {
                    this.retryAttempt = NON_RETRYABLE_ERROR;
                } else {
                    this.retryAttempt = (short) Math.min(maxRetryAttempts, this.retryAttempt + 1);
                }
                if (this.state != MirrorPartitionState.FAILED) {
                    this.previousState = this.state;
                }
            } else if ((this.state != MirrorPartitionState.FAILED && this.state != targetState)
                    || targetState == MirrorPartitionState.STOPPED
                    || targetState == MirrorPartitionState.PAUSED) {
                withFailedStateReset();
            } else {
                this.errorMessage = Optional.empty();
            }
            this.state = targetState;
            this.stateEpoch = newStateEpoch;
            return this;
        }

        public MirrorPartitionMetadata build() {
            return new MirrorPartitionMetadata(state, stateEpoch,
                    lastMirror, errorMessage, retryAttempt, previousState);
        }
    }

    @SuppressWarnings("cyclomaticComplexity")
    public static boolean isValidStateTransition(MirrorPartitionState source,
                                                 MirrorPartitionState target) {
        if (source == null || target == null) {
            return false;
        }
        if (source == target) {
            return true;
        }
        if (source == MirrorPartitionState.FAILED) {
            return target != MirrorPartitionState.UNKNOWN;
        }
        return switch (target) {
            case LOG_ALIGNMENT -> source == MirrorPartitionState.UNKNOWN
                    || source == MirrorPartitionState.STOPPED;
            case EPOCH_FENCING, ULE_RECOVERY, PAUSING -> source == MirrorPartitionState.MIRRORING;
            case MIRRORING -> source == MirrorPartitionState.LOG_ALIGNMENT
                    || source == MirrorPartitionState.EPOCH_FENCING
                    || source == MirrorPartitionState.PAUSED
                    || source == MirrorPartitionState.ULE_RECOVERY;
            case PAUSED -> source == MirrorPartitionState.PAUSING;
            case STOPPING -> source == MirrorPartitionState.MIRRORING
                    || source == MirrorPartitionState.PAUSED;
            case STOPPED -> source == MirrorPartitionState.STOPPING;
            case FAILED -> true;
            case UNKNOWN -> false;
        };
    }
}
