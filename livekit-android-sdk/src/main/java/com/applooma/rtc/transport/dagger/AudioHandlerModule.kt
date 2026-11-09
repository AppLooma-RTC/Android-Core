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

import android.media.AudioAttributes
import android.media.AudioManager
import android.os.Build
import dagger.Module
import dagger.Provides
import com.applooma.rtc.transport.AudioType
import com.applooma.rtc.transport.audio.AudioHandler
import com.applooma.rtc.transport.audio.AudioSwitchHandler
import com.applooma.rtc.transport.audio.CommunicationWorkaround
import com.applooma.rtc.transport.audio.CommunicationWorkaroundImpl
import com.applooma.rtc.transport.audio.NoopCommunicationWorkaround
import com.applooma.rtc.transport.memory.CloseableManager
import javax.inject.Named
import javax.inject.Provider
import javax.inject.Singleton

/**
 * @suppress
 */
@Module
internal object AudioHandlerModule {

    @Provides
    fun audioOutputType(
        @Named(InjectionNames.OVERRIDE_AUDIO_OUTPUT_TYPE)
        audioOutputOverride: AudioType?,
    ): AudioType {
        return audioOutputOverride ?: AudioType.CallAudioType()
    }

    @Provides
    fun audioOutputAttributes(
        audioType: AudioType,
    ): AudioAttributes {
        return audioType.audioAttributes
    }

    @Provides
    @Singleton
    fun audioHandler(
        audioSwitchHandler: Provider<AudioSwitchHandler>,
        @Named(InjectionNames.OVERRIDE_AUDIO_HANDLER)
        audioHandlerOverride: AudioHandler?,
        audioOutputType: AudioType,
    ): AudioHandler {
        return audioHandlerOverride ?: audioSwitchHandler.get().apply {
            audioMode = audioOutputType.audioMode
            audioAttributeContentType = audioOutputType.audioAttributes.contentType
            audioAttributeUsageType = audioOutputType.audioAttributes.usage
            audioStreamType = audioOutputType.audioStreamType
        }
    }

    @Provides
    @Singleton
    @JvmSuppressWildcards
    fun communicationWorkaround(
        @Named(InjectionNames.OVERRIDE_DISABLE_COMMUNICATION_WORKAROUND)
        disableCommunicationWorkaround: Boolean,
        audioType: AudioType,
        closeableManager: CloseableManager,
        commWorkaroundImplProvider: Provider<CommunicationWorkaroundImpl>,
    ): CommunicationWorkaround {
        return if (
            !disableCommunicationWorkaround &&
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.R &&
            audioType.audioMode == AudioManager.MODE_IN_COMMUNICATION
        ) {
            commWorkaroundImplProvider.get().apply {
                closeableManager.registerClosable { this.dispose() }
            }
        } else {
            NoopCommunicationWorkaround()
        }
    }
}
