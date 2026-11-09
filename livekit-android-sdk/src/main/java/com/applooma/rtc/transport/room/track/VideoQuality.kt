/*
 * Copyright 2024 Transport, Inc.
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

import com.applooma.rtc.transport.proto.Models

enum class VideoQuality {
    LOW,
    MEDIUM,
    HIGH,
    ;

    fun toProto(): Models.VideoQuality {
        return when (this) {
            LOW -> Models.VideoQuality.LOW
            MEDIUM -> Models.VideoQuality.MEDIUM
            HIGH -> Models.VideoQuality.HIGH
        }
    }

    companion object {
        fun fromProto(quality: Models.VideoQuality): VideoQuality? {
            return when (quality) {
                Models.VideoQuality.LOW -> LOW
                Models.VideoQuality.MEDIUM -> MEDIUM
                Models.VideoQuality.HIGH -> HIGH
                Models.VideoQuality.OFF -> null
                Models.VideoQuality.UNRECOGNIZED -> null
            }
        }
    }
}
