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

package com.applooma.rtc.transport.room.util

import com.applooma.rtc.transport.room.track.VideoEncoding
import com.applooma.rtc.transport.room.track.VideoPreset
import com.applooma.rtc.transport.room.track.VideoPreset169
import com.applooma.rtc.transport.room.track.VideoPreset43
import com.applooma.rtc.transport.room.track.video.ScalabilityMode
import com.applooma.rtc.transport.proto.Models
import org.webrtc.RtpParameters
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.roundToInt

internal object EncodingUtils {

    val VIDEO_RIDS = arrayOf("q", "h", "f")

    // Note: maintain order from smallest to biggest.
    private val PRESETS_16_9 = listOf(
        VideoPreset169.H90,
        VideoPreset169.H180,
        VideoPreset169.H216,
        VideoPreset169.H360,
        VideoPreset169.H540,
        VideoPreset169.H720,
        VideoPreset169.H1080,
        VideoPreset169.H1440,
        VideoPreset169.H2160,
    )

    // Note: maintain order from smallest to biggest.
    private val PRESETS_4_3 = listOf(
        VideoPreset43.H120,
        VideoPreset43.H180,
        VideoPreset43.H240,
        VideoPreset43.H360,
        VideoPreset43.H480,
        VideoPreset43.H540,
        VideoPreset43.H720,
        VideoPreset43.H1080,
        VideoPreset43.H1440,
    )

    fun determineAppropriateEncoding(width: Int, height: Int): VideoEncoding {
        val presets = presetsForResolution(width, height)

        // presets assume width is longest size
        val longestSize = max(width, height)
        val preset = presets
            .firstOrNull { it.capture.width >= longestSize }
            ?: presets.last()

        return preset.encoding
    }

    fun presetsForResolution(width: Int, height: Int): List<VideoPreset> {
        val longestSize = max(width, height)
        val shortestSize = min(width, height)
        val aspectRatio = longestSize.toFloat() / shortestSize
        return if (abs(aspectRatio - 16f / 9f) < abs(aspectRatio - 4f / 3f)) {
            PRESETS_16_9
        } else {
            PRESETS_4_3
        }
    }

    fun videoLayersFromEncodings(
        trackWidth: Int,
        trackHeight: Int,
        encodings: List<RtpParameters.Encoding>,
        isSVC: Boolean,
    ): List<Models.VideoLayer> {
        return if (encodings.isEmpty()) {
            listOf(
                Models.VideoLayer.newBuilder().apply {
                    width = trackWidth
                    height = trackHeight
                    quality = Models.VideoQuality.HIGH
                    bitrate = 0
                    ssrc = 0
                }.build(),
            )
        } else if (isSVC) {
            val encodingSM = encodings.first().scalabilityMode!!
            val scalabilityMode = ScalabilityMode.parseFromString(encodingSM)
            val maxBitrate = encodings.first().maxBitrateBps ?: 0
            (0 until scalabilityMode.spatial).map { index ->
                Models.VideoLayer.newBuilder().apply {
                    width = ceil(trackWidth / (2f.pow(index))).roundToInt()
                    height = ceil(trackHeight / (2f.pow(index))).roundToInt()
                    quality = Models.VideoQuality.forNumber(Models.VideoQuality.HIGH.number - index)
                    bitrate = ceil(maxBitrate / 3f.pow(index)).roundToInt()
                    ssrc = 0
                }.build()
            }
        } else {
            encodings.map { encoding ->
                val scaleDownBy = encoding.scaleResolutionDownBy ?: 1.0
                var videoQuality = videoQualityForRid(encoding.rid ?: "")
                if (videoQuality == Models.VideoQuality.UNRECOGNIZED && encodings.size == 1) {
                    videoQuality = Models.VideoQuality.HIGH
                }
                Models.VideoLayer.newBuilder().apply {
                    // Internally, WebRTC casts directly to int without rounding.
                    // https://github.com/webrtc-sdk/webrtc/blob/8c7139f8e6fa19ddf2c91510c177a19746e1ded3/media/engine/webrtc_video_engine.cc#L3676
                    width = (trackWidth / scaleDownBy).toInt()
                    height = (trackHeight / scaleDownBy).toInt()
                    quality = videoQuality
                    bitrate = encoding.maxBitrateBps ?: 0
                    ssrc = 0
                }.build()
            }
        }
    }

    fun videoQualityForRid(rid: String): Models.VideoQuality {
        return when (rid) {
            "f" -> Models.VideoQuality.HIGH
            "h" -> Models.VideoQuality.MEDIUM
            "q" -> Models.VideoQuality.LOW
            else -> Models.VideoQuality.UNRECOGNIZED
        }
    }

    fun ridForVideoQuality(quality: Models.VideoQuality): String? {
        return when (quality) {
            Models.VideoQuality.HIGH -> "f"
            Models.VideoQuality.MEDIUM -> "h"
            Models.VideoQuality.LOW -> "q"
            else -> null
        }
    }
}
