/*
 * Copyright 2023-2024 Transport, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.applooma.rtc.transport.room.track

import com.applooma.rtc.transport.events.BroadcastEventBus
import com.applooma.rtc.transport.events.TrackEvent
import com.applooma.rtc.transport.util.flowDelegate
import com.applooma.rtc.transport.webrtc.RTCStatsGetter
import com.applooma.rtc.transport.webrtc.getStats
import com.applooma.rtc.transport.webrtc.peerconnection.executeBlockingOnRTCThread
import com.applooma.rtc.transport.proto.Models
import com.applooma.rtc.transport.proto.Rtc
import org.webrtc.MediaStreamTrack
import org.webrtc.RTCStatsCollectorCallback
import org.webrtc.RTCStatsReport
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

abstract class Track(
    name: String,
    kind: Kind,
    open val rtcTrack: MediaStreamTrack,
) {
    protected val eventBus = BroadcastEventBus<TrackEvent>()
    val events = eventBus.readOnly()
    var name = name
        internal set

    var kind = kind
        internal set
    var sid: String? = null
        internal set
    var streamState: StreamState by flowDelegate(StreamState.PAUSED) { newValue, oldValue ->
        if (newValue != oldValue) {
            eventBus.tryPostEvent(TrackEvent.StreamStateChanged(this, newValue))
        }
    }
        internal set

    var enabled: Boolean
        get() = withRTCTrack(defaultValue = false) { rtcTrack.enabled() }
        set(value) = withRTCTrack { rtcTrack.setEnabled(value) }

    var statsGetter: RTCStatsGetter? = null

    internal val isDisposed
        get() = rtcTrack.isDisposed

    /**
     * Return the [RTCStatsReport] for this track, or null if none is available.
     */
    suspend fun getRTCStats(): RTCStatsReport? = statsGetter?.getStats()

    /**
     * Calls the [callback] with the [RTCStatsReport] for this track, or null if none is available.
     */
    fun getRTCStats(callback: RTCStatsCollectorCallback) {
        statsGetter?.invoke(callback) ?: callback.onStatsDelivered(null)
    }

    enum class Kind(val value: String) {
        AUDIO("audio"),
        VIDEO("video"),

        // unknown
        UNRECOGNIZED("unrecognized"),
        ;

        fun toProto(): Models.TrackType {
            return when (this) {
                AUDIO -> Models.TrackType.AUDIO
                VIDEO -> Models.TrackType.VIDEO
                UNRECOGNIZED -> Models.TrackType.UNRECOGNIZED
            }
        }

        override fun toString(): String {
            return value
        }

        companion object {
            fun fromProto(tt: Models.TrackType): Kind {
                return when (tt) {
                    Models.TrackType.AUDIO -> AUDIO
                    Models.TrackType.VIDEO -> VIDEO
                    else -> UNRECOGNIZED
                }
            }
        }
    }

    enum class Source {
        CAMERA,
        MICROPHONE,
        SCREEN_SHARE,
        UNKNOWN,
        ;

        fun toProto(): Models.TrackSource {
            return when (this) {
                CAMERA -> Models.TrackSource.CAMERA
                MICROPHONE -> Models.TrackSource.MICROPHONE
                SCREEN_SHARE -> Models.TrackSource.SCREEN_SHARE
                UNKNOWN -> Models.TrackSource.UNKNOWN
            }
        }

        companion object {
            fun fromProto(source: Models.TrackSource): Source {
                return when (source) {
                    Models.TrackSource.CAMERA -> CAMERA
                    Models.TrackSource.MICROPHONE -> MICROPHONE
                    Models.TrackSource.SCREEN_SHARE -> SCREEN_SHARE
                    else -> UNKNOWN
                }
            }
        }
    }

    enum class StreamState {
        ACTIVE,
        PAUSED,
        UNKNOWN,
        ;

        fun toProto(): Rtc.StreamState {
            return when (this) {
                ACTIVE -> Rtc.StreamState.ACTIVE
                PAUSED -> Rtc.StreamState.PAUSED
                UNKNOWN -> Rtc.StreamState.UNRECOGNIZED
            }
        }

        companion object {
            fun fromProto(state: Rtc.StreamState): StreamState {
                return when (state) {
                    Rtc.StreamState.ACTIVE -> ACTIVE
                    Rtc.StreamState.PAUSED -> PAUSED
                    Rtc.StreamState.UNRECOGNIZED -> UNKNOWN
                }
            }
        }
    }

    data class Dimensions(val width: Int, val height: Int)

    /**
     * Starts the track.
     */
    open fun start() {
        enabled = true
    }

    /**
     * Stops the track.
     */
    open fun stop() {
        enabled = false
    }

    /**
     * Disposes the track. Transport will generally take care of disposing tracks for you.
     */
    open fun dispose() {
        withRTCTrack {
            rtcTrack.dispose()
        }
    }

    @OptIn(ExperimentalContracts::class)
    internal inline fun <T> withRTCTrack(crossinline action: MediaStreamTrack.() -> T) {
        contract { callsInPlace(action, InvocationKind.AT_MOST_ONCE) }
        withRTCTrack(Unit, action)
    }

    @OptIn(ExperimentalContracts::class)
    internal inline fun <T> withRTCTrack(defaultValue: T, crossinline action: MediaStreamTrack.() -> T): T {
        contract { callsInPlace(action, InvocationKind.AT_MOST_ONCE) }
        if (isDisposed) {
            return defaultValue
        }
        return executeBlockingOnRTCThread {
            return@executeBlockingOnRTCThread if (isDisposed) {
                defaultValue
            } else {
                action(rtcTrack)
            }
        }
    }
}

sealed class TrackException(message: String? = null, cause: Throwable? = null) :
    Exception(message, cause) {
    class InvalidTrackTypeException(message: String? = null, cause: Throwable? = null) :
        TrackException(message, cause)

    class DuplicateTrackException(message: String? = null, cause: Throwable? = null) :
        TrackException(message, cause)

    class InvalidTrackStateException(message: String? = null, cause: Throwable? = null) :
        TrackException(message, cause)

    class MediaException(message: String? = null, cause: Throwable? = null) :
        TrackException(message, cause)

    class PublishException(message: String? = null, cause: Throwable? = null) :
        TrackException(message, cause)
}

public const val KIND_AUDIO = "audio"
public const val KIND_VIDEO = "video"
