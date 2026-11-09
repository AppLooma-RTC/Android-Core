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

package com.applooma.rtc.transport.dagger

import android.content.Context
import dagger.BindsInstance
import dagger.Component
import com.applooma.rtc.transport.TransportOverrides
import com.applooma.rtc.transport.room.Room
import org.webrtc.EglBase
import org.webrtc.PeerConnectionFactory
import javax.inject.Singleton

/**
 * @suppress
 */
@Singleton
@Component(
    modules = [
        CoroutinesModule::class,
        RTCModule::class,
        WebModule::class,
        JsonFormatModule::class,
        OverridesModule::class,
        AudioHandlerModule::class,
        MemoryModule::class,
    ],
)
interface TransportComponent {

    fun roomFactory(): Room.Factory

    fun peerConnectionFactory(): PeerConnectionFactory

    fun eglBase(): EglBase

    @Component.Factory
    interface Factory {
        fun create(
            @BindsInstance appContext: Context,
            overridesModule: OverridesModule,
        ): TransportComponent
    }
}

internal fun TransportComponent.Factory.create(
    context: Context,
    overrides: TransportOverrides,
): TransportComponent {
    return create(
        appContext = context,
        overridesModule = OverridesModule(overrides),
    )
}
