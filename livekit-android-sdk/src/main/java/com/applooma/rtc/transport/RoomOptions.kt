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

package com.applooma.rtc.transport

import com.applooma.rtc.transport.e2ee.E2EEOptions
import com.applooma.rtc.transport.room.Room
import com.applooma.rtc.transport.room.participant.AudioTrackPublishDefaults
import com.applooma.rtc.transport.room.participant.VideoTrackPublishDefaults
import com.applooma.rtc.transport.room.track.LocalAudioTrackOptions
import com.applooma.rtc.transport.room.track.LocalVideoTrackOptions

data class RoomOptions(
    /**
     * @see [Room.adaptiveStream]
     */
    val adaptiveStream: Boolean = false,

    /**
     * @see [Room.dynacast]
     */
    val dynacast: Boolean = false,

    /**
     * @see [Room.e2eeOptions]
     */
    val e2eeOptions: E2EEOptions? = null,

    val audioTrackCaptureDefaults: LocalAudioTrackOptions? = null,
    val videoTrackCaptureDefaults: LocalVideoTrackOptions? = null,
    val audioTrackPublishDefaults: AudioTrackPublishDefaults? = null,
    val videoTrackPublishDefaults: VideoTrackPublishDefaults? = null,
    val screenShareTrackCaptureDefaults: LocalVideoTrackOptions? = null,
    val screenShareTrackPublishDefaults: VideoTrackPublishDefaults? = null,
)
