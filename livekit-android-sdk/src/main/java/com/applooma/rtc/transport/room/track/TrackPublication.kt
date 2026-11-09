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
import com.applooma.rtc.transport.events.RoomEvent
import com.applooma.rtc.transport.events.TrackPublicationEvent
import com.applooma.rtc.transport.room.participant.Participant
import com.applooma.rtc.transport.util.FlowObservable
import com.applooma.rtc.transport.util.flowDelegate
import com.applooma.rtc.transport.proto.Models
import java.lang.ref.WeakReference

open class TrackPublication(
    info: Models.TrackInfo,
    track: Track?,
    participant: Participant
) {

    @FlowObservable
    @get:FlowObservable
    open var track: Track? by flowDelegate(track)
        internal set
    var name: String
        internal set
    var sid: String
        private set
    var kind: Track.Kind
        private set

    open val encryptionType: Models.Encryption.Type
        get() {
            return trackInfo?.encryption ?: Models.Encryption.Type.NONE
        }

    protected val eventBus = BroadcastEventBus<TrackPublicationEvent>()
    val events = eventBus.readOnly()

    @FlowObservable
    @get:FlowObservable
    open var muted: Boolean by flowDelegate(false)
        internal set
    open val subscribed: Boolean
        get() {
            return track != null
        }
    var simulcasted: Boolean? = null
        internal set
    var dimensions: Track.Dimensions? = null
        internal set
    var source: Track.Source = Track.Source.UNKNOWN
        internal set
    var mimeType: String? = null
        internal set

    internal var trackInfo: Models.TrackInfo? = null

    var participant: WeakReference<Participant>

    init {
        sid = info.sid
        name = info.name
        kind = Track.Kind.fromProto(info.type)
        this.participant = WeakReference(participant)
        updateFromInfo(info)
    }

    fun updateFromInfo(info: Models.TrackInfo) {
        sid = info.sid
        name = info.name
        kind = Track.Kind.fromProto(info.type)
        muted = info.muted
        source = Track.Source.fromProto(info.source)
        if (kind == Track.Kind.VIDEO) {
            simulcasted = info.simulcast
            dimensions = Track.Dimensions(info.width, info.height)
        }
        mimeType = info.mimeType

        trackInfo = info
    }

    internal fun onTranscriptionReceived(transcription: RoomEvent.TranscriptionReceived) {
        if (transcription.publication != this) {
            return
        }
        eventBus.tryPostEvent(
            TrackPublicationEvent.TranscriptionReceived(
                publication = this,
                transcriptions = transcription.transcriptionSegments,
            ),
        )
    }
}
