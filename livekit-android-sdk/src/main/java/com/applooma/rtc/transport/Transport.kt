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

import android.app.Application
import android.content.Context
import com.applooma.rtc.transport.dagger.DaggerTransportComponent
import com.applooma.rtc.transport.dagger.RTCModule
import com.applooma.rtc.transport.dagger.create
import com.applooma.rtc.transport.room.Room
import com.applooma.rtc.transport.util.LKLog
import com.applooma.rtc.transport.util.LoggingLevel
import timber.log.Timber

/**
 * The main entry point into using Transport.
 *
 * @see [Transport.create]
 */
object Transport {
    /**
     * [LoggingLevel] to use for Livekit logs. Set to [LoggingLevel.OFF] to turn off logs.
     *
     * Defaults to [LoggingLevel.OFF]
     */
    @JvmStatic
    var loggingLevel: LoggingLevel
        get() = LKLog.loggingLevel
        set(value) {
            LKLog.loggingLevel = value

            // Plant debug tree if needed.
            if (value != LoggingLevel.OFF) {
                val forest = Timber.forest()
                val needsPlanting = forest.none { it is Timber.DebugTree }

                if (needsPlanting) {
                    Timber.plant(Timber.DebugTree())
                }
            }
        }

    /**
     * Enables logs for the underlying WebRTC sdk logging. Used in conjunction with [loggingLevel].
     *
     * Note: WebRTC logging is very noisy and should only be used to diagnose native WebRTC issues.
     */
    @JvmStatic
    var enableWebRTCLogging: Boolean = false

    /**
     * Certain WebRTC classes need to be initialized prior to use.
     *
     * This does not need to be called under normal circumstances, as [Transport.create]
     * will handle this for you.
     */
    fun init(appContext: Context) {
        RTCModule.libWebrtcInitialization(appContext)
    }

    /**
     * Create a Room object.
     */
    fun create(
        appContext: Context,
        options: RoomOptions = RoomOptions(),
        overrides: TransportOverrides = TransportOverrides(),
    ): Room {
        val ctx = appContext.applicationContext

        if (ctx !is Application) {
            LKLog.w { "Application context was not found, this may cause memory leaks." }
        }

        val component = DaggerTransportComponent
            .factory()
            .create(ctx, overrides)

        val room = component.roomFactory().create(ctx)
        room.setRoomOptions(options)

        return room
    }
}
