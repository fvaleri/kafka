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

/**
 * Immutable snapshot of mirror partition metadata.
 *
 * @param state           the current lifecycle state, or null if unknown
 * @param stateEpoch      monotonically increasing epoch incremented on every state transition
 * @param lastMirrorEpoch the last mirrored epoch and offset, or empty if not yet recorded
 * @param errorMessage    the failure reason when in FAILED state, or null otherwise
 * @param retryAttempt    the retry count in FAILED state, 0 if not failed, -1 if non-retryable error
 * @param prevState       the state before entering FAILED, or null if not applicable
 */
public record MirrorPartitionMetadata(MirrorPartitionState state, int stateEpoch, LastMirror lastMirrorEpoch,
                                      String errorMessage, short retryAttempt, MirrorPartitionState prevState) {
    public static final MirrorPartitionMetadata EMPTY = new MirrorPartitionMetadata(MirrorPartitionState.UNKNOWN, 0, LastMirror.EMPTY, null, (short) 0, null);
    public static final short NON_RETRYABLE_ERROR = -1;

    public static MirrorPartitionMetadata orEmpty(MirrorPartitionMetadata mpm) {
        return mpm != null ? mpm : EMPTY;
    }

    public static class Builder {
        private MirrorPartitionState state;
        private int stateEpoch;
        private LastMirror lastMirrorEpoch;
        private String errorMessage;
        private short retryAttempt;
        private MirrorPartitionState prevState;

        public Builder() {
            this.state = EMPTY.state();
            this.stateEpoch = EMPTY.stateEpoch();
            this.lastMirrorEpoch = EMPTY.lastMirrorEpoch();
            this.errorMessage = EMPTY.errorMessage();
            this.retryAttempt = EMPTY.retryAttempt();
            this.prevState = EMPTY.prevState();
        }

        public Builder(MirrorPartitionMetadata existing) {
            existing = orEmpty(existing);
            this.state = existing.state();
            this.stateEpoch = existing.stateEpoch();
            this.lastMirrorEpoch = existing.lastMirrorEpoch();
            this.errorMessage = existing.errorMessage();
            this.retryAttempt = existing.retryAttempt();
            this.prevState = existing.prevState();
        }

        public Builder withState(MirrorPartitionState state) {
            this.state = state;
            return this;
        }

        public Builder withStateEpoch(int stateEpoch) {
            this.stateEpoch = stateEpoch;
            return this;
        }

        public Builder withLastMirrorEpoch(LastMirror lastMirrorEpoch) {
            this.lastMirrorEpoch = lastMirrorEpoch;
            return this;
        }

        public Builder withErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public Builder withRetryAttempt(short retryAttempt) {
            this.retryAttempt = retryAttempt;
            return this;
        }

        public Builder withPrevState(MirrorPartitionState prevState) {
            this.prevState = prevState;
            return this;
        }

        /**
         * Computes error-related fields for a local state transition.
         */
        public Builder withTransitionErrorFields(MirrorPartitionState newState,
                                                 String errorMessage,
                                                 boolean nonRetryable,
                                                 short maxRetryAttempts) {
            if (newState == MirrorPartitionState.FAILED) {
                this.errorMessage = errorMessage;
                // Non-retryable errors stay non-retryable; otherwise increment up to maxRetryAttempts
                if (nonRetryable || this.retryAttempt == NON_RETRYABLE_ERROR) {
                    this.retryAttempt = NON_RETRYABLE_ERROR;
                } else {
                    this.retryAttempt = (short) (this.retryAttempt != 0
                            ? Math.min(maxRetryAttempts, this.retryAttempt + 1) : 1);
                }
                // Preserve the original pre-failure state across repeated failures
                if (this.state != MirrorPartitionState.FAILED
                        || this.prevState == null || this.prevState == MirrorPartitionState.UNKNOWN) {
                    this.prevState = this.state;
                }
            } else if ((this.state != MirrorPartitionState.FAILED && this.state != newState)
                    || newState == MirrorPartitionState.STOPPED
                    || newState == MirrorPartitionState.PAUSED) {
                this.errorMessage = null;
                this.retryAttempt = (short) 0;
                this.prevState = null;
            } else {
                this.errorMessage = errorMessage;
            }
            return this;
        }

        public MirrorPartitionMetadata build() {
            return new MirrorPartitionMetadata(state, stateEpoch, lastMirrorEpoch,
                    errorMessage, retryAttempt, prevState);
        }
    }

    @SuppressWarnings({"cyclomaticComplexity", "BooleanExpressionComplexity"})
    public static boolean isValidStateTransition(MirrorPartitionState source, MirrorPartitionState target) {
        if (source == target || source == MirrorPartitionState.FAILED) {
            return true;
        }
        switch (target) {
            case LOG_ALIGNMENT:
                return source == null
                        || source == MirrorPartitionState.UNKNOWN
                        || source == MirrorPartitionState.STOPPED;
            case EPOCH_FENCING, ULE_RECOVERY, PAUSING:
                return source == MirrorPartitionState.MIRRORING;
            case MIRRORING:
                return source == MirrorPartitionState.LOG_ALIGNMENT
                        || source == MirrorPartitionState.EPOCH_FENCING
                        || source == MirrorPartitionState.PAUSED
                        || source == MirrorPartitionState.ULE_RECOVERY;
            case PAUSED:
                return source == MirrorPartitionState.PAUSING;
            case STOPPING:
                return source == MirrorPartitionState.LOG_ALIGNMENT
                        || source == MirrorPartitionState.EPOCH_FENCING
                        || source == MirrorPartitionState.MIRRORING
                        || source == MirrorPartitionState.PAUSING
                        || source == MirrorPartitionState.PAUSED
                        || source == MirrorPartitionState.ULE_RECOVERY;
            case STOPPED:
                return source == MirrorPartitionState.STOPPING;
            case FAILED:
                return true;
            default:
                return false;
        }
    }
}
